BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE event_pipeline_canary_runs (
  canary_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  event_id uuid NOT NULL UNIQUE,
  started_at timestamptz NOT NULL,
  projected_at timestamptz,
  projected_consumer_name text,
  inbox_first_seen_at timestamptz,
  transport_message_id text,
  stream_name text,
  durable_name text,
  stream_sequence bigint CHECK (stream_sequence IS NULL OR stream_sequence > 0),
  consumer_sequence bigint CHECK (consumer_sequence IS NULL OR consumer_sequence > 0),
  delivery_count integer CHECK (delivery_count IS NULL OR delivery_count > 0),
  broker_timestamp timestamptz,
  CONSTRAINT event_pipeline_canary_projection_evidence_ck CHECK (
    (
      projected_at IS NULL
      AND projected_consumer_name IS NULL
      AND inbox_first_seen_at IS NULL
      AND transport_message_id IS NULL
      AND stream_name IS NULL
      AND durable_name IS NULL
      AND stream_sequence IS NULL
      AND consumer_sequence IS NULL
      AND delivery_count IS NULL
      AND broker_timestamp IS NULL
    )
    OR
    (
      projected_at IS NOT NULL
      AND projected_consumer_name IS NOT NULL
      AND btrim(projected_consumer_name) <> ''
      AND inbox_first_seen_at IS NOT NULL
      AND transport_message_id IS NOT NULL
      AND btrim(transport_message_id) <> ''
      AND stream_name IS NOT NULL
      AND btrim(stream_name) <> ''
      AND durable_name IS NOT NULL
      AND btrim(durable_name) <> ''
      AND stream_sequence IS NOT NULL
      AND consumer_sequence IS NOT NULL
      AND delivery_count IS NOT NULL
      AND broker_timestamp IS NOT NULL
      AND projected_at >= inbox_first_seen_at
    )
  )
);

CREATE INDEX event_pipeline_canary_runs_started_idx
  ON event_pipeline_canary_runs (started_at, canary_id);

CREATE FUNCTION start_event_pipeline_canary(
  p_tenant_id uuid
) RETURNS TABLE (
  canary_id uuid,
  event_id uuid,
  recorded_at timestamptz
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_canary_id uuid := uuidv7();
  v_event_id uuid;
  v_recorded_at timestamptz;
BEGIN
  IF p_tenant_id IS NULL THEN
    RAISE EXCEPTION 'canary tenant id is required' USING ERRCODE = '23514';
  END IF;

  v_event_id := enqueue_domain_event(
    p_tenant_id,
    'event-pipeline-canary:' || v_canary_id::text,
    'health_check.ping',
    'event-pipeline-canary/' || v_canary_id::text,
    'EventPipelineCanary',
    v_canary_id::text,
    1,
    jsonb_build_object(
      'canary_id', v_canary_id::text,
      'synthetic', true
    ),
    clock_timestamp(),
    NULL,
    v_canary_id::text,
    jsonb_build_object('kind', 'synthetic-health-check'),
    NULL,
    1
  );

  SELECT k.recorded_at
  INTO STRICT v_recorded_at
  FROM domain_event_idempotency_keys k
  WHERE k.event_id = v_event_id;

  INSERT INTO event_pipeline_canary_runs (
    canary_id,
    tenant_id,
    event_id,
    started_at
  ) VALUES (
    v_canary_id,
    p_tenant_id,
    v_event_id,
    v_recorded_at
  );

  RETURN QUERY
  SELECT v_canary_id, v_event_id, v_recorded_at;
END;
$$;

CREATE FUNCTION apply_event_pipeline_canary(
  p_canary_id uuid,
  p_event_id uuid,
  p_consumer_name text
) RETURNS timestamptz
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_run event_pipeline_canary_runs%ROWTYPE;
  v_first_seen_at timestamptz;
  v_metadata jsonb;
  v_stream_name text;
  v_durable_name text;
  v_transport_message_id text;
  v_stream_sequence bigint;
  v_consumer_sequence bigint;
  v_delivery_count integer;
  v_broker_timestamp timestamptz;
  v_projected_at timestamptz;
BEGIN
  IF p_canary_id IS NULL OR p_event_id IS NULL THEN
    RAISE EXCEPTION 'canary and event ids are required' USING ERRCODE = '23514';
  END IF;
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = '' THEN
    RAISE EXCEPTION 'canary consumer name is required' USING ERRCODE = '23514';
  END IF;

  SELECT *
  INTO v_run
  FROM event_pipeline_canary_runs
  WHERE canary_id = p_canary_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'event pipeline canary does not exist' USING ERRCODE = '23503';
  END IF;
  IF v_run.event_id <> p_event_id THEN
    RAISE EXCEPTION 'canary event identity mismatch' USING ERRCODE = '23505';
  END IF;

  SELECT i.first_seen_at, i.metadata
  INTO v_first_seen_at, v_metadata
  FROM domain_event_inbox i
  WHERE i.consumer_name = p_consumer_name
    AND i.event_id = p_event_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'canary Inbox receipt must exist before projection'
      USING ERRCODE = '23514';
  END IF;
  IF v_metadata->>'transport' IS DISTINCT FROM 'nats-jetstream' THEN
    RAISE EXCEPTION 'canary requires NATS JetStream delivery metadata'
      USING ERRCODE = '23514';
  END IF;

  v_transport_message_id := NULLIF(v_metadata->>'transport_message_id', '');
  v_stream_name := NULLIF(v_metadata->>'stream', '');
  v_durable_name := NULLIF(v_metadata->>'consumer', '');
  v_stream_sequence := NULLIF(v_metadata->>'stream_sequence', '')::bigint;
  v_consumer_sequence := NULLIF(v_metadata->>'consumer_sequence', '')::bigint;
  v_delivery_count := NULLIF(v_metadata->>'delivery_count', '')::integer;
  v_broker_timestamp := NULLIF(v_metadata->>'broker_timestamp', '')::timestamptz;

  IF v_transport_message_id IS NULL
     OR v_stream_name IS NULL
     OR v_durable_name IS NULL
     OR v_stream_sequence IS NULL OR v_stream_sequence < 1
     OR v_consumer_sequence IS NULL OR v_consumer_sequence < 1
     OR v_delivery_count IS NULL OR v_delivery_count < 1
     OR v_broker_timestamp IS NULL THEN
    RAISE EXCEPTION 'canary delivery metadata is incomplete'
      USING ERRCODE = '23514';
  END IF;

  IF v_run.projected_at IS NOT NULL THEN
    IF v_run.projected_consumer_name <> p_consumer_name
       OR v_run.inbox_first_seen_at <> v_first_seen_at
       OR v_run.transport_message_id <> v_transport_message_id
       OR v_run.stream_name <> v_stream_name
       OR v_run.durable_name <> v_durable_name
       OR v_run.stream_sequence <> v_stream_sequence
       OR v_run.consumer_sequence <> v_consumer_sequence
       OR v_run.delivery_count <> v_delivery_count
       OR v_run.broker_timestamp <> v_broker_timestamp THEN
      RAISE EXCEPTION 'canary projection evidence collision' USING ERRCODE = '23505';
    END IF;
    RETURN v_run.projected_at;
  END IF;

  UPDATE event_pipeline_canary_runs
  SET projected_at = clock_timestamp(),
      projected_consumer_name = p_consumer_name,
      inbox_first_seen_at = v_first_seen_at,
      transport_message_id = v_transport_message_id,
      stream_name = v_stream_name,
      durable_name = v_durable_name,
      stream_sequence = v_stream_sequence,
      consumer_sequence = v_consumer_sequence,
      delivery_count = v_delivery_count,
      broker_timestamp = v_broker_timestamp
  WHERE canary_id = p_canary_id
  RETURNING projected_at INTO v_projected_at;

  RETURN v_projected_at;
END;
$$;

CREATE FUNCTION read_event_pipeline_canary(
  p_canary_id uuid
) RETURNS TABLE (
  canary_id uuid,
  tenant_id uuid,
  event_id uuid,
  started_at timestamptz,
  published_at timestamptz,
  projected_at timestamptz,
  projected_consumer_name text,
  inbox_first_seen_at timestamptz,
  transport_message_id text,
  stream_name text,
  durable_name text,
  stream_sequence bigint,
  consumer_sequence bigint,
  delivery_count integer,
  broker_timestamp timestamptz
)
LANGUAGE sql
STABLE
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
  SELECT
    r.canary_id,
    r.tenant_id,
    r.event_id,
    r.started_at,
    publication.published_at,
    r.projected_at,
    r.projected_consumer_name,
    r.inbox_first_seen_at,
    r.transport_message_id,
    r.stream_name,
    r.durable_name,
    r.stream_sequence,
    r.consumer_sequence,
    r.delivery_count,
    r.broker_timestamp
  FROM event_pipeline_canary_runs r
  LEFT JOIN LATERAL (
    SELECT o.published_at
    FROM domain_event_outbox o
    WHERE o.event_id = r.event_id
    UNION ALL
    SELECT a.published_at
    FROM domain_event_outbox_archive a
    WHERE a.event_id = r.event_id
    LIMIT 1
  ) publication ON true
  WHERE r.canary_id = p_canary_id;
$$;

COMMIT;
