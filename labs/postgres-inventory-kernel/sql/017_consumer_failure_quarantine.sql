BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE domain_event_consumer_failures (
  consumer_name text NOT NULL CHECK (
    consumer_name = btrim(consumer_name) AND consumer_name <> ''
  ),
  event_id uuid NOT NULL,
  envelope jsonb NOT NULL CHECK (
    jsonb_typeof(envelope) = 'object'
    AND envelope ? 'event_id'
    AND envelope ->> 'event_id' = event_id::text
  ),
  delivery_metadata jsonb NOT NULL CHECK (
    jsonb_typeof(delivery_metadata) = 'object'
  ),
  status text NOT NULL CHECK (status IN ('deferred', 'quarantined', 'resolved')),
  failure_code text NOT NULL CHECK (
    failure_code = btrim(failure_code)
    AND failure_code ~ '^[a-z][a-z0-9_]{0,99}$'
  ),
  retryable boolean NOT NULL,
  attempt_count integer NOT NULL CHECK (attempt_count >= 0),
  available_at timestamptz NOT NULL,
  claimed_by text,
  claim_token uuid,
  claimed_until timestamptz,
  last_error text NOT NULL,
  first_failed_at timestamptz NOT NULL,
  last_failed_at timestamptz NOT NULL,
  quarantined_at timestamptz,
  quarantine_reason text,
  resolved_at timestamptz,
  resolution text CHECK (resolution IN ('applied', 'duplicate')),
  PRIMARY KEY (consumer_name, event_id),
  CONSTRAINT domain_event_consumer_failure_claim_ck CHECK (
    (claimed_by IS NULL AND claim_token IS NULL AND claimed_until IS NULL)
    OR
    (
      status = 'deferred'
      AND claimed_by IS NOT NULL
      AND claim_token IS NOT NULL
      AND claimed_until IS NOT NULL
    )
  ),
  CONSTRAINT domain_event_consumer_failure_state_ck CHECK (
    (
      status = 'deferred'
      AND retryable
      AND quarantined_at IS NULL
      AND quarantine_reason IS NULL
      AND resolved_at IS NULL
      AND resolution IS NULL
    )
    OR
    (
      status = 'quarantined'
      AND quarantined_at IS NOT NULL
      AND quarantine_reason IS NOT NULL
      AND resolved_at IS NULL
      AND resolution IS NULL
    )
    OR
    (
      status = 'resolved'
      AND quarantined_at IS NULL
      AND quarantine_reason IS NULL
      AND resolved_at IS NOT NULL
      AND resolution IS NOT NULL
    )
  )
);

CREATE INDEX domain_event_consumer_failures_ready_idx
  ON domain_event_consumer_failures (
    consumer_name, available_at, first_failed_at, event_id
  )
  WHERE status = 'deferred';

CREATE INDEX domain_event_consumer_failures_quarantined_idx
  ON domain_event_consumer_failures (consumer_name, quarantined_at, event_id)
  WHERE status = 'quarantined';

CREATE TABLE domain_event_consumer_failure_actions (
  replay_id uuid PRIMARY KEY,
  consumer_name text NOT NULL,
  event_id uuid NOT NULL,
  action text NOT NULL CHECK (action = 'replay'),
  operator_id text NOT NULL CHECK (
    operator_id = btrim(operator_id) AND operator_id <> ''
  ),
  reason text NOT NULL CHECK (reason = btrim(reason) AND reason <> ''),
  previous_attempt_count integer NOT NULL CHECK (previous_attempt_count >= 1),
  previous_failure_code text NOT NULL,
  previous_retryable boolean NOT NULL,
  previous_last_error text NOT NULL,
  previous_quarantine_reason text NOT NULL,
  acted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT domain_event_consumer_failure_actions_event_fk
    FOREIGN KEY (consumer_name, event_id)
    REFERENCES domain_event_consumer_failures (consumer_name, event_id)
    ON DELETE RESTRICT
);

CREATE INDEX domain_event_consumer_failure_actions_event_idx
  ON domain_event_consumer_failure_actions (
    consumer_name, event_id, acted_at, replay_id
  );

CREATE FUNCTION capture_domain_event_consumer_failure(
  p_consumer_name text,
  p_event_id uuid,
  p_envelope jsonb,
  p_delivery_metadata jsonb,
  p_failure_code text,
  p_retryable boolean,
  p_error text,
  p_retry_after_seconds integer,
  p_max_attempts integer
) RETURNS TABLE (
  failure_status text,
  persisted_failure_code text,
  failure_attempt_count integer,
  persisted_retryable boolean,
  created boolean
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_failure domain_event_consumer_failures%ROWTYPE;
  v_now timestamptz := clock_timestamp();
  v_status text;
  v_quarantine_reason text;
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_event_id IS NULL THEN
    RAISE EXCEPTION 'consumer failure event id is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_envelope IS NULL OR jsonb_typeof(p_envelope) IS DISTINCT FROM 'object'
     OR p_envelope ->> 'event_id' IS DISTINCT FROM p_event_id::text THEN
    RAISE EXCEPTION 'consumer failure envelope must match event id'
      USING ERRCODE = '23514';
  END IF;
  IF p_delivery_metadata IS NULL
     OR jsonb_typeof(p_delivery_metadata) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'consumer failure delivery metadata must be an object'
      USING ERRCODE = '23514';
  END IF;
  IF p_failure_code IS NULL
     OR p_failure_code !~ '^[a-z][a-z0-9_]{0,99}$' THEN
    RAISE EXCEPTION 'consumer failure code must be a stable lowercase identifier'
      USING ERRCODE = '23514';
  END IF;
  IF p_retryable IS NULL THEN
    RAISE EXCEPTION 'consumer failure retryable decision is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_retry_after_seconds < 0 OR p_retry_after_seconds > 86400 THEN
    RAISE EXCEPTION 'consumer retry delay must be between 0 and 86400 seconds'
      USING ERRCODE = '23514';
  END IF;
  IF p_max_attempts < 1 OR p_max_attempts > 1000 THEN
    RAISE EXCEPTION 'consumer max attempts must be between 1 and 1000'
      USING ERRCODE = '23514';
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(p_consumer_name || ':' || p_event_id::text, 0)
  );

  SELECT f.* INTO v_failure
  FROM domain_event_consumer_failures AS f
  WHERE f.consumer_name = p_consumer_name
    AND f.event_id = p_event_id
  FOR UPDATE;

  IF FOUND THEN
    IF v_failure.envelope <> p_envelope THEN
      RAISE EXCEPTION 'consumer failure event identity belongs to another envelope'
        USING ERRCODE = '23505';
    END IF;

    RETURN QUERY SELECT
      v_failure.status,
      v_failure.failure_code,
      v_failure.attempt_count,
      v_failure.retryable,
      false;
    RETURN;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM domain_event_inbox AS i
    WHERE i.consumer_name = p_consumer_name
      AND i.event_id = p_event_id
  ) THEN
    RETURN QUERY SELECT
      'resolved'::text,
      p_failure_code,
      0,
      p_retryable,
      false;
    RETURN;
  END IF;

  v_status := CASE
    WHEN p_retryable AND p_max_attempts > 1 THEN 'deferred'
    ELSE 'quarantined'
  END;
  v_quarantine_reason := CASE
    WHEN v_status = 'quarantined' AND p_retryable
      THEN format('attempt_limit_exhausted:%s', p_failure_code)
    WHEN v_status = 'quarantined'
      THEN format('terminal:%s', p_failure_code)
    ELSE NULL
  END;

  INSERT INTO domain_event_consumer_failures (
    consumer_name,
    event_id,
    envelope,
    delivery_metadata,
    status,
    failure_code,
    retryable,
    attempt_count,
    available_at,
    last_error,
    first_failed_at,
    last_failed_at,
    quarantined_at,
    quarantine_reason
  ) VALUES (
    p_consumer_name,
    p_event_id,
    p_envelope,
    p_delivery_metadata,
    v_status,
    p_failure_code,
    p_retryable,
    1,
    v_now + make_interval(secs => p_retry_after_seconds),
    left(COALESCE(p_error, 'consumer handler failed'), 4000),
    v_now,
    v_now,
    CASE WHEN v_status = 'quarantined' THEN v_now ELSE NULL END,
    v_quarantine_reason
  )
  RETURNING * INTO v_failure;

  RETURN QUERY SELECT
    v_failure.status,
    v_failure.failure_code,
    v_failure.attempt_count,
    v_failure.retryable,
    true;
END;
$$;

CREATE FUNCTION claim_domain_event_consumer_failures(
  p_consumer_name text,
  p_worker_id text,
  p_limit integer DEFAULT 100,
  p_lease_seconds integer DEFAULT 30
) RETURNS TABLE (
  consumer_name text,
  event_id uuid,
  claim_token uuid,
  attempt_count integer,
  failure_code text,
  envelope jsonb,
  delivery_metadata jsonb
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_worker_id IS NULL OR btrim(p_worker_id) = ''
     OR p_worker_id <> btrim(p_worker_id) THEN
    RAISE EXCEPTION 'consumer retry worker id is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_limit < 1 OR p_limit > 1000 THEN
    RAISE EXCEPTION 'consumer failure claim limit must be between 1 and 1000'
      USING ERRCODE = '23514';
  END IF;
  IF p_lease_seconds < 1 OR p_lease_seconds > 3600 THEN
    RAISE EXCEPTION 'consumer failure lease seconds must be between 1 and 3600'
      USING ERRCODE = '23514';
  END IF;

  RETURN QUERY
  WITH candidates AS (
    SELECT f.consumer_name, f.event_id
    FROM domain_event_consumer_failures AS f
    WHERE f.consumer_name = p_consumer_name
      AND f.status = 'deferred'
      AND f.available_at <= clock_timestamp()
      AND (f.claimed_until IS NULL OR f.claimed_until <= clock_timestamp())
    ORDER BY f.available_at, f.first_failed_at, f.event_id
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  ), claimed AS (
    UPDATE domain_event_consumer_failures AS f
    SET claimed_by = p_worker_id,
        claim_token = uuidv7(),
        claimed_until = clock_timestamp() + make_interval(secs => p_lease_seconds)
    FROM candidates AS c
    WHERE f.consumer_name = c.consumer_name
      AND f.event_id = c.event_id
    RETURNING f.*
  )
  SELECT
    c.consumer_name,
    c.event_id,
    c.claim_token,
    c.attempt_count,
    c.failure_code,
    c.envelope,
    c.delivery_metadata
  FROM claimed AS c
  ORDER BY c.available_at, c.first_failed_at, c.event_id;
END;
$$;

CREATE FUNCTION fail_domain_event_consumer_failure(
  p_consumer_name text,
  p_event_id uuid,
  p_claim_token uuid,
  p_failure_code text,
  p_retryable boolean,
  p_error text,
  p_retry_after_seconds integer,
  p_max_attempts integer
) RETURNS TABLE (
  failure_status text,
  failure_attempt_count integer
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_failure domain_event_consumer_failures%ROWTYPE;
  v_attempt_count integer;
  v_status text;
  v_now timestamptz := clock_timestamp();
  v_quarantine_reason text;
BEGIN
  IF p_failure_code IS NULL
     OR p_failure_code !~ '^[a-z][a-z0-9_]{0,99}$' THEN
    RAISE EXCEPTION 'consumer failure code must be a stable lowercase identifier'
      USING ERRCODE = '23514';
  END IF;
  IF p_retryable IS NULL THEN
    RAISE EXCEPTION 'consumer failure retryable decision is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_retry_after_seconds < 0 OR p_retry_after_seconds > 86400 THEN
    RAISE EXCEPTION 'consumer retry delay must be between 0 and 86400 seconds'
      USING ERRCODE = '23514';
  END IF;
  IF p_max_attempts < 1 OR p_max_attempts > 1000 THEN
    RAISE EXCEPTION 'consumer max attempts must be between 1 and 1000'
      USING ERRCODE = '23514';
  END IF;

  SELECT f.* INTO v_failure
  FROM domain_event_consumer_failures AS f
  WHERE f.consumer_name = p_consumer_name
    AND f.event_id = p_event_id
    AND f.status = 'deferred'
    AND f.claim_token = p_claim_token
    AND f.claimed_until > v_now
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  v_attempt_count := v_failure.attempt_count + 1;
  v_status := CASE
    WHEN p_retryable AND v_attempt_count < p_max_attempts THEN 'deferred'
    ELSE 'quarantined'
  END;
  v_quarantine_reason := CASE
    WHEN v_status = 'quarantined' AND p_retryable
      THEN format('attempt_limit_exhausted:%s', p_failure_code)
    WHEN v_status = 'quarantined'
      THEN format('terminal:%s', p_failure_code)
    ELSE NULL
  END;

  UPDATE domain_event_consumer_failures AS f
  SET status = v_status,
      failure_code = p_failure_code,
      retryable = p_retryable,
      attempt_count = v_attempt_count,
      available_at = v_now + make_interval(secs => p_retry_after_seconds),
      claimed_by = NULL,
      claim_token = NULL,
      claimed_until = NULL,
      last_error = left(COALESCE(p_error, 'consumer handler failed'), 4000),
      last_failed_at = v_now,
      quarantined_at = CASE WHEN v_status = 'quarantined' THEN v_now ELSE NULL END,
      quarantine_reason = v_quarantine_reason
  WHERE f.consumer_name = p_consumer_name
    AND f.event_id = p_event_id;

  failure_status := v_status;
  failure_attempt_count := v_attempt_count;
  RETURN NEXT;
END;
$$;

CREATE FUNCTION replay_domain_event_consumer_failure(
  p_replay_id uuid,
  p_consumer_name text,
  p_event_id uuid,
  p_operator_id text,
  p_reason text
) RETURNS text
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_action domain_event_consumer_failure_actions%ROWTYPE;
  v_failure domain_event_consumer_failures%ROWTYPE;
BEGIN
  IF p_replay_id IS NULL THEN
    RAISE EXCEPTION 'consumer failure replay id is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_event_id IS NULL THEN
    RAISE EXCEPTION 'consumer failure event id is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_operator_id IS NULL OR btrim(p_operator_id) = ''
     OR p_operator_id <> btrim(p_operator_id)
     OR p_reason IS NULL OR btrim(p_reason) = ''
     OR p_reason <> btrim(p_reason) THEN
    RAISE EXCEPTION 'replay operator and reason are required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended('consumer-failure-replay:' || p_replay_id::text, 0)
  );

  SELECT a.* INTO v_action
  FROM domain_event_consumer_failure_actions AS a
  WHERE a.replay_id = p_replay_id;

  IF FOUND THEN
    IF v_action.consumer_name <> p_consumer_name
       OR v_action.event_id <> p_event_id
       OR v_action.operator_id <> p_operator_id
       OR v_action.reason <> p_reason THEN
      RAISE EXCEPTION 'consumer failure replay identity belongs to another request'
        USING ERRCODE = '23505';
    END IF;
    RETURN 'duplicate';
  END IF;

  SELECT f.* INTO v_failure
  FROM domain_event_consumer_failures AS f
  WHERE f.consumer_name = p_consumer_name
    AND f.event_id = p_event_id
    AND f.status = 'quarantined'
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN 'not_quarantined';
  END IF;

  INSERT INTO domain_event_consumer_failure_actions (
    replay_id,
    consumer_name,
    event_id,
    action,
    operator_id,
    reason,
    previous_attempt_count,
    previous_failure_code,
    previous_retryable,
    previous_last_error,
    previous_quarantine_reason
  ) VALUES (
    p_replay_id,
    p_consumer_name,
    p_event_id,
    'replay',
    p_operator_id,
    p_reason,
    v_failure.attempt_count,
    v_failure.failure_code,
    v_failure.retryable,
    v_failure.last_error,
    v_failure.quarantine_reason
  );

  UPDATE domain_event_consumer_failures AS f
  SET status = 'deferred',
      retryable = true,
      attempt_count = 0,
      available_at = clock_timestamp(),
      claimed_by = NULL,
      claim_token = NULL,
      claimed_until = NULL,
      quarantined_at = NULL,
      quarantine_reason = NULL
  WHERE f.consumer_name = p_consumer_name
    AND f.event_id = p_event_id;

  RETURN 'replayed';
END;
$$;

COMMIT;
