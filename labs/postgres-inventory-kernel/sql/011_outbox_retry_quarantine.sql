BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

ALTER TABLE domain_event_outbox
  ADD COLUMN last_failed_at timestamptz,
  ADD COLUMN quarantined_at timestamptz,
  ADD COLUMN quarantine_reason text,
  ADD CONSTRAINT domain_event_outbox_quarantine_pair_ck CHECK (
    (quarantined_at IS NULL AND quarantine_reason IS NULL)
    OR
    (quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)
  ),
  ADD CONSTRAINT domain_event_outbox_quarantine_unclaimed_ck CHECK (
    quarantined_at IS NULL
    OR (claimed_by IS NULL AND claim_token IS NULL AND claimed_until IS NULL)
  ),
  ADD CONSTRAINT domain_event_outbox_published_not_quarantined_ck CHECK (
    published_at IS NULL OR quarantined_at IS NULL
  );

DROP INDEX domain_event_outbox_ready_idx;

CREATE INDEX domain_event_outbox_ready_idx
  ON domain_event_outbox (available_at, recorded_at, event_id)
  WHERE published_at IS NULL AND quarantined_at IS NULL;

CREATE TABLE domain_event_outbox_operator_actions (
  action_id uuid PRIMARY KEY DEFAULT uuidv7(),
  event_id uuid NOT NULL REFERENCES domain_event_outbox(event_id),
  action text NOT NULL CHECK (action = 'replay'),
  operator_id text NOT NULL CHECK (btrim(operator_id) <> ''),
  reason text NOT NULL CHECK (btrim(reason) <> ''),
  previous_attempt_count integer NOT NULL CHECK (previous_attempt_count >= 1),
  previous_last_error text,
  previous_quarantine_reason text NOT NULL,
  acted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX domain_event_outbox_operator_actions_event_idx
  ON domain_event_outbox_operator_actions (event_id, acted_at);

CREATE INDEX domain_event_outbox_quarantined_idx
  ON domain_event_outbox (quarantined_at, event_id)
  WHERE quarantined_at IS NOT NULL;

DROP FUNCTION claim_domain_events(text, integer, integer);

CREATE FUNCTION claim_domain_events(
  p_worker_id text,
  p_limit integer DEFAULT 100,
  p_lease_seconds integer DEFAULT 30
) RETURNS TABLE (
  event_id uuid,
  claim_token uuid,
  attempt_count integer,
  event_type text,
  aggregate_type text,
  aggregate_id text,
  aggregate_version bigint,
  envelope jsonb
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_worker_id IS NULL OR btrim(p_worker_id) = '' THEN
    RAISE EXCEPTION 'publisher worker id is required' USING ERRCODE = '23514';
  END IF;
  IF p_limit < 1 OR p_limit > 1000 THEN
    RAISE EXCEPTION 'claim limit must be between 1 and 1000' USING ERRCODE = '23514';
  END IF;
  IF p_lease_seconds < 1 OR p_lease_seconds > 3600 THEN
    RAISE EXCEPTION 'lease seconds must be between 1 and 3600' USING ERRCODE = '23514';
  END IF;

  RETURN QUERY
  WITH candidates AS (
    SELECT o.event_id
    FROM domain_event_outbox o
    WHERE o.published_at IS NULL
      AND o.quarantined_at IS NULL
      AND o.available_at <= clock_timestamp()
      AND (o.claimed_until IS NULL OR o.claimed_until <= clock_timestamp())
    ORDER BY o.recorded_at, o.event_id
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  ), claimed AS (
    UPDATE domain_event_outbox o
    SET claimed_by = p_worker_id,
        claim_token = uuidv7(),
        claimed_until = clock_timestamp() + make_interval(secs => p_lease_seconds),
        attempt_count = o.attempt_count + 1
    FROM candidates c
    WHERE o.event_id = c.event_id
    RETURNING o.*
  )
  SELECT
    c.event_id,
    c.claim_token,
    c.attempt_count,
    c.event_type,
    c.aggregate_type,
    c.aggregate_id,
    c.aggregate_version,
    jsonb_strip_nulls(jsonb_build_object(
      'event_id', c.event_id,
      'type', c.event_type,
      'source', c.source,
      'subject', c.subject,
      'occurred_at', c.occurred_at,
      'recorded_at', c.recorded_at,
      'aggregate_type', c.aggregate_type,
      'aggregate_id', c.aggregate_id,
      'aggregate_version', c.aggregate_version,
      'causation_id', c.causation_id,
      'correlation_id', c.correlation_id,
      'actor', c.actor,
      'warehouse_id', c.warehouse_id,
      'schema_version', c.schema_version,
      'data', c.data
    )) AS envelope
  FROM claimed c
  ORDER BY c.recorded_at, c.event_id;
END;
$$;

CREATE OR REPLACE FUNCTION nack_domain_event(
  p_event_id uuid,
  p_claim_token uuid,
  p_error text,
  p_retry_after_seconds integer DEFAULT 0
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_retry_after_seconds < 0 OR p_retry_after_seconds > 86400 THEN
    RAISE EXCEPTION 'retry delay must be between 0 and 86400 seconds'
      USING ERRCODE = '23514';
  END IF;

  UPDATE domain_event_outbox
  SET available_at = clock_timestamp() + make_interval(secs => p_retry_after_seconds),
      claimed_by = NULL,
      claim_token = NULL,
      claimed_until = NULL,
      last_error = left(COALESCE(p_error, 'publication failed'), 4000),
      last_failed_at = clock_timestamp()
  WHERE event_id = p_event_id
    AND published_at IS NULL
    AND quarantined_at IS NULL
    AND claim_token = p_claim_token
    AND claimed_until > clock_timestamp();

  RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION quarantine_domain_event(
  p_event_id uuid,
  p_claim_token uuid,
  p_error text,
  p_reason text
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_reason IS NULL OR btrim(p_reason) = '' THEN
    RAISE EXCEPTION 'quarantine reason is required' USING ERRCODE = '23514';
  END IF;

  UPDATE domain_event_outbox
  SET claimed_by = NULL,
      claim_token = NULL,
      claimed_until = NULL,
      last_error = left(COALESCE(p_error, 'publication failed'), 4000),
      last_failed_at = clock_timestamp(),
      quarantined_at = clock_timestamp(),
      quarantine_reason = left(p_reason, 1000)
  WHERE event_id = p_event_id
    AND published_at IS NULL
    AND quarantined_at IS NULL
    AND claim_token = p_claim_token
    AND claimed_until > clock_timestamp();

  RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION replay_quarantined_domain_event(
  p_event_id uuid,
  p_operator_id text,
  p_reason text
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_event domain_event_outbox%ROWTYPE;
BEGIN
  IF p_operator_id IS NULL OR btrim(p_operator_id) = '' THEN
    RAISE EXCEPTION 'replay operator id is required' USING ERRCODE = '23514';
  END IF;
  IF p_reason IS NULL OR btrim(p_reason) = '' THEN
    RAISE EXCEPTION 'replay reason is required' USING ERRCODE = '23514';
  END IF;

  SELECT * INTO v_event
  FROM domain_event_outbox
  WHERE event_id = p_event_id
    AND published_at IS NULL
    AND quarantined_at IS NOT NULL
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  INSERT INTO domain_event_outbox_operator_actions (
    event_id,
    action,
    operator_id,
    reason,
    previous_attempt_count,
    previous_last_error,
    previous_quarantine_reason
  ) VALUES (
    v_event.event_id,
    'replay',
    btrim(p_operator_id),
    btrim(p_reason),
    v_event.attempt_count,
    v_event.last_error,
    v_event.quarantine_reason
  );

  UPDATE domain_event_outbox
  SET available_at = clock_timestamp(),
      claimed_by = NULL,
      claim_token = NULL,
      claimed_until = NULL,
      attempt_count = 0,
      last_error = NULL,
      last_failed_at = NULL,
      quarantined_at = NULL,
      quarantine_reason = NULL
  WHERE event_id = p_event_id;

  RETURN true;
END;
$$;

COMMIT;
