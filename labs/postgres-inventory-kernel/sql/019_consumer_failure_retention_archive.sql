BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE INDEX domain_event_consumer_failures_resolved_retention_idx
  ON domain_event_consumer_failures (
    consumer_name, resolved_at, event_id
  )
  WHERE status = 'resolved';

CREATE TABLE domain_event_consumer_failure_archive (
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
  status text NOT NULL CHECK (status = 'resolved'),
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
  resolved_at timestamptz NOT NULL,
  resolution text NOT NULL CHECK (resolution IN ('applied', 'duplicate')),
  archived_at timestamptz NOT NULL,
  archive_run_id uuid NOT NULL,
  PRIMARY KEY (consumer_name, event_id),
  CONSTRAINT domain_event_consumer_failure_archive_run_uq UNIQUE (
    consumer_name, event_id, archive_run_id
  ),
  CONSTRAINT domain_event_consumer_failure_archive_inbox_fk
    FOREIGN KEY (consumer_name, event_id)
    REFERENCES domain_event_inbox (consumer_name, event_id),
  CONSTRAINT domain_event_consumer_failure_archive_resolved_ck CHECK (
    claimed_by IS NULL
    AND claim_token IS NULL
    AND claimed_until IS NULL
    AND quarantined_at IS NULL
    AND quarantine_reason IS NULL
  ),
  CONSTRAINT domain_event_consumer_failure_archive_time_ck CHECK (
    archived_at >= resolved_at
  )
);

CREATE INDEX domain_event_consumer_failure_archive_resolved_idx
  ON domain_event_consumer_failure_archive (
    consumer_name, resolved_at, event_id
  );

CREATE INDEX domain_event_consumer_failure_archive_run_idx
  ON domain_event_consumer_failure_archive (archive_run_id, event_id);

CREATE TABLE domain_event_consumer_failure_action_archive (
  replay_id uuid PRIMARY KEY,
  consumer_name text NOT NULL,
  event_id uuid NOT NULL,
  action text NOT NULL CHECK (action = 'replay'),
  operator_id text NOT NULL CHECK (
    operator_id = btrim(operator_id) AND operator_id <> ''
  ),
  reason text NOT NULL CHECK (reason = btrim(reason) AND reason <> ''),
  previous_attempt_count integer NOT NULL CHECK (previous_attempt_count >= 1),
  previous_failure_code text NOT NULL CHECK (
    previous_failure_code ~ '^[a-z][a-z0-9_]{0,99}$'
  ),
  previous_retryable boolean NOT NULL,
  previous_last_error text NOT NULL,
  previous_quarantine_reason text NOT NULL,
  acted_at timestamptz NOT NULL,
  archived_at timestamptz NOT NULL,
  archive_run_id uuid NOT NULL,
  CONSTRAINT domain_event_consumer_failure_action_archive_event_fk
    FOREIGN KEY (consumer_name, event_id, archive_run_id)
    REFERENCES domain_event_consumer_failure_archive (
      consumer_name, event_id, archive_run_id
    )
);

CREATE INDEX domain_event_consumer_failure_action_archive_event_idx
  ON domain_event_consumer_failure_action_archive (
    consumer_name, event_id, acted_at, replay_id
  );

CREATE INDEX domain_event_consumer_failure_action_archive_run_idx
  ON domain_event_consumer_failure_action_archive (archive_run_id, replay_id);

CREATE FUNCTION archive_resolved_domain_event_consumer_failures(
  p_consumer_name text,
  p_before timestamptz,
  p_limit integer DEFAULT 1000
) RETURNS TABLE (
  archive_run_id uuid,
  archived_at timestamptz,
  archived_consumer_name text,
  archived_failure_count integer,
  archived_action_count integer
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_event_ids uuid[];
  v_archive_run_id uuid;
  v_archived_at timestamptz;
  v_failure_count integer := 0;
  v_action_count integer := 0;
  v_deleted_count integer := 0;
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_before IS NULL THEN
    RAISE EXCEPTION 'consumer failure archive cutoff is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_before > statement_timestamp() THEN
    RAISE EXCEPTION 'consumer failure archive cutoff cannot be in the future'
      USING ERRCODE = '23514';
  END IF;
  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
    RAISE EXCEPTION 'consumer failure archive limit must be between 1 and 1000'
      USING ERRCODE = '23514';
  END IF;

  SELECT array_agg(
    candidate.event_id ORDER BY candidate.resolved_at, candidate.event_id
  )
  INTO v_event_ids
  FROM (
    SELECT f.event_id, f.resolved_at
    FROM domain_event_consumer_failures AS f
    WHERE f.consumer_name = p_consumer_name
      AND f.status = 'resolved'
      AND f.resolved_at < p_before
      AND EXISTS (
        SELECT 1
        FROM domain_event_inbox AS i
        WHERE i.consumer_name = f.consumer_name
          AND i.event_id = f.event_id
      )
    ORDER BY f.resolved_at, f.event_id
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  ) AS candidate;

  v_archived_at := clock_timestamp();

  IF v_event_ids IS NULL THEN
    RETURN QUERY
    SELECT NULL::uuid, v_archived_at, p_consumer_name, 0, 0;
    RETURN;
  END IF;

  v_archive_run_id := uuidv7();

  INSERT INTO domain_event_consumer_failure_archive (
    consumer_name,
    event_id,
    envelope,
    delivery_metadata,
    status,
    failure_code,
    retryable,
    attempt_count,
    available_at,
    claimed_by,
    claim_token,
    claimed_until,
    last_error,
    first_failed_at,
    last_failed_at,
    quarantined_at,
    quarantine_reason,
    resolved_at,
    resolution,
    archived_at,
    archive_run_id
  )
  SELECT
    f.consumer_name,
    f.event_id,
    f.envelope,
    f.delivery_metadata,
    f.status,
    f.failure_code,
    f.retryable,
    f.attempt_count,
    f.available_at,
    f.claimed_by,
    f.claim_token,
    f.claimed_until,
    f.last_error,
    f.first_failed_at,
    f.last_failed_at,
    f.quarantined_at,
    f.quarantine_reason,
    f.resolved_at,
    f.resolution,
    v_archived_at,
    v_archive_run_id
  FROM domain_event_consumer_failures AS f
  WHERE f.consumer_name = p_consumer_name
    AND f.event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_failure_count = ROW_COUNT;
  IF v_failure_count <> cardinality(v_event_ids) THEN
    RAISE EXCEPTION 'consumer failure archive copy count mismatch';
  END IF;

  INSERT INTO domain_event_consumer_failure_action_archive (
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
    previous_quarantine_reason,
    acted_at,
    archived_at,
    archive_run_id
  )
  SELECT
    a.replay_id,
    a.consumer_name,
    a.event_id,
    a.action,
    a.operator_id,
    a.reason,
    a.previous_attempt_count,
    a.previous_failure_code,
    a.previous_retryable,
    a.previous_last_error,
    a.previous_quarantine_reason,
    a.acted_at,
    v_archived_at,
    v_archive_run_id
  FROM domain_event_consumer_failure_actions AS a
  WHERE a.consumer_name = p_consumer_name
    AND a.event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_action_count = ROW_COUNT;

  DELETE FROM domain_event_consumer_failure_actions AS a
  WHERE a.consumer_name = p_consumer_name
    AND a.event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
  IF v_deleted_count <> v_action_count THEN
    RAISE EXCEPTION 'consumer failure action delete count mismatch';
  END IF;

  DELETE FROM domain_event_consumer_failures AS f
  WHERE f.consumer_name = p_consumer_name
    AND f.event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
  IF v_deleted_count <> v_failure_count THEN
    RAISE EXCEPTION 'consumer failure delete count mismatch';
  END IF;

  RETURN QUERY
  SELECT
    v_archive_run_id,
    v_archived_at,
    p_consumer_name,
    v_failure_count,
    v_action_count;
END;
$$;

CREATE OR REPLACE FUNCTION replay_domain_event_consumer_failure(
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
  v_action record;
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

  SELECT
    action_identity.*,
    count(*) OVER () AS identity_count
  INTO v_action
  FROM (
    SELECT
      a.consumer_name,
      a.event_id,
      a.operator_id,
      a.reason
    FROM domain_event_consumer_failure_actions AS a
    WHERE a.replay_id = p_replay_id

    UNION ALL

    SELECT
      a.consumer_name,
      a.event_id,
      a.operator_id,
      a.reason
    FROM domain_event_consumer_failure_action_archive AS a
    WHERE a.replay_id = p_replay_id
  ) AS action_identity;

  IF FOUND THEN
    IF v_action.identity_count <> 1 THEN
      RAISE EXCEPTION 'consumer failure replay identity exists in live and archive'
        USING ERRCODE = '23505';
    END IF;
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
