BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION claim_domain_events(
  p_worker_id text,
  p_limit integer DEFAULT 100,
  p_lease_seconds integer DEFAULT 30
) RETURNS TABLE (
  event_id uuid,
  claim_token uuid,
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

CREATE OR REPLACE FUNCTION ack_domain_event(
  p_event_id uuid,
  p_claim_token uuid
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM domain_event_outbox
    WHERE event_id = p_event_id
      AND published_at IS NOT NULL
  ) THEN
    RETURN true;
  END IF;

  UPDATE domain_event_outbox
  SET published_at = clock_timestamp(),
      claimed_by = NULL,
      claim_token = NULL,
      claimed_until = NULL,
      last_error = NULL
  WHERE event_id = p_event_id
    AND published_at IS NULL
    AND claim_token = p_claim_token
    AND claimed_until > clock_timestamp();

  RETURN FOUND;
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
      last_error = left(COALESCE(p_error, 'publication failed'), 4000)
  WHERE event_id = p_event_id
    AND published_at IS NULL
    AND claim_token = p_claim_token
    AND claimed_until > clock_timestamp();

  RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION try_record_domain_event_receipt(
  p_consumer_name text,
  p_event_id uuid,
  p_metadata jsonb DEFAULT '{}'::jsonb
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = '' THEN
    RAISE EXCEPTION 'consumer name is required' USING ERRCODE = '23514';
  END IF;

  INSERT INTO domain_event_inbox (consumer_name, event_id, metadata)
  VALUES (p_consumer_name, p_event_id, COALESCE(p_metadata, '{}'::jsonb))
  ON CONFLICT (consumer_name, event_id) DO NOTHING;

  RETURN FOUND;
END;
$$;

COMMIT;
