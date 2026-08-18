BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE INDEX nats_jetstream_consumer_poison_deliveries_retention_idx
  ON nats_jetstream_consumer_poison_deliveries (
    consumer_name,
    last_seen_at,
    stream,
    durable_consumer,
    stream_sequence
  );

CREATE TABLE nats_jetstream_consumer_poison_delivery_archive (
  consumer_name text NOT NULL CHECK (
    consumer_name = btrim(consumer_name) AND consumer_name <> ''
  ),
  stream text NOT NULL CHECK (stream = btrim(stream) AND stream <> ''),
  durable_consumer text NOT NULL CHECK (
    durable_consumer = btrim(durable_consumer) AND durable_consumer <> ''
  ),
  stream_sequence bigint NOT NULL CHECK (stream_sequence > 0),
  subject text NOT NULL CHECK (subject = btrim(subject) AND subject <> ''),
  failure_code text NOT NULL CHECK (
    failure_code = btrim(failure_code)
    AND failure_code ~ '^[a-z][a-z0-9_]{0,99}$'
  ),
  payload_sha256 text NOT NULL CHECK (
    payload_sha256 ~ '^[0-9a-f]{64}$'
  ),
  payload_size bigint NOT NULL CHECK (payload_size >= 0),
  payload_preview bytea NOT NULL,
  payload_truncated boolean NOT NULL,
  headers jsonb NOT NULL CHECK (jsonb_typeof(headers) = 'object'),
  first_delivery_metadata jsonb NOT NULL CHECK (
    jsonb_typeof(first_delivery_metadata) = 'object'
  ),
  last_delivery_metadata jsonb NOT NULL CHECK (
    jsonb_typeof(last_delivery_metadata) = 'object'
  ),
  last_error text NOT NULL,
  observation_count integer NOT NULL CHECK (observation_count >= 1),
  first_seen_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  quarantined_at timestamptz NOT NULL,
  archived_at timestamptz NOT NULL,
  archive_run_id uuid NOT NULL,
  PRIMARY KEY (
    consumer_name,
    stream,
    durable_consumer,
    stream_sequence,
    archive_run_id
  ),
  CONSTRAINT nats_poison_delivery_archive_preview_ck CHECK (
    octet_length(payload_preview) <= payload_size
    AND payload_truncated = (octet_length(payload_preview) < payload_size)
  ),
  CONSTRAINT nats_poison_delivery_archive_time_ck CHECK (
    first_seen_at <= last_seen_at
    AND quarantined_at <= last_seen_at
    AND archived_at >= last_seen_at
  )
);

CREATE INDEX nats_poison_delivery_archive_identity_idx
  ON nats_jetstream_consumer_poison_delivery_archive (
    consumer_name,
    stream,
    durable_consumer,
    stream_sequence,
    archived_at DESC
  );

CREATE INDEX nats_poison_delivery_archive_run_idx
  ON nats_jetstream_consumer_poison_delivery_archive (
    archive_run_id,
    consumer_name,
    stream_sequence
  );

CREATE FUNCTION archive_nats_jetstream_consumer_poison_deliveries(
  p_consumer_name text,
  p_before timestamptz,
  p_limit integer DEFAULT 1000
) RETURNS TABLE (
  archive_run_id uuid,
  archived_at timestamptz,
  archived_consumer_name text,
  archived_delivery_count integer
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_archive_run_id uuid := uuidv7();
  v_archived_at timestamptz := clock_timestamp();
  v_archived_count integer := 0;
  v_deleted_count integer := 0;
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_before IS NULL THEN
    RAISE EXCEPTION 'malformed delivery archive cutoff is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_before > statement_timestamp() THEN
    RAISE EXCEPTION 'malformed delivery archive cutoff cannot be in the future'
      USING ERRCODE = '23514';
  END IF;
  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
    RAISE EXCEPTION 'malformed delivery archive limit must be between 1 and 1000'
      USING ERRCODE = '23514';
  END IF;

  WITH candidate AS (
    SELECT d.*
    FROM nats_jetstream_consumer_poison_deliveries AS d
    WHERE d.consumer_name = p_consumer_name
      AND d.last_seen_at < p_before
    ORDER BY
      d.last_seen_at,
      d.stream,
      d.durable_consumer,
      d.stream_sequence
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  )
  INSERT INTO nats_jetstream_consumer_poison_delivery_archive (
    consumer_name,
    stream,
    durable_consumer,
    stream_sequence,
    subject,
    failure_code,
    payload_sha256,
    payload_size,
    payload_preview,
    payload_truncated,
    headers,
    first_delivery_metadata,
    last_delivery_metadata,
    last_error,
    observation_count,
    first_seen_at,
    last_seen_at,
    quarantined_at,
    archived_at,
    archive_run_id
  )
  SELECT
    candidate.consumer_name,
    candidate.stream,
    candidate.durable_consumer,
    candidate.stream_sequence,
    candidate.subject,
    candidate.failure_code,
    candidate.payload_sha256,
    candidate.payload_size,
    candidate.payload_preview,
    candidate.payload_truncated,
    candidate.headers,
    candidate.first_delivery_metadata,
    candidate.last_delivery_metadata,
    candidate.last_error,
    candidate.observation_count,
    candidate.first_seen_at,
    candidate.last_seen_at,
    candidate.quarantined_at,
    v_archived_at,
    v_archive_run_id
  FROM candidate;

  GET DIAGNOSTICS v_archived_count = ROW_COUNT;
  IF v_archived_count = 0 THEN
    RETURN QUERY
    SELECT NULL::uuid, v_archived_at, p_consumer_name, 0;
    RETURN;
  END IF;

  DELETE FROM nats_jetstream_consumer_poison_deliveries AS d
  USING nats_jetstream_consumer_poison_delivery_archive AS a
  WHERE a.archive_run_id = v_archive_run_id
    AND d.consumer_name = a.consumer_name
    AND d.stream = a.stream
    AND d.durable_consumer = a.durable_consumer
    AND d.stream_sequence = a.stream_sequence;

  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
  IF v_deleted_count <> v_archived_count THEN
    RAISE EXCEPTION 'malformed delivery archive delete count mismatch';
  END IF;

  RETURN QUERY
  SELECT
    v_archive_run_id,
    v_archived_at,
    p_consumer_name,
    v_archived_count;
END;
$$;

CREATE OR REPLACE FUNCTION capture_nats_jetstream_consumer_poison_delivery(
  p_consumer_name text,
  p_stream text,
  p_durable_consumer text,
  p_stream_sequence bigint,
  p_subject text,
  p_failure_code text,
  p_error text,
  p_payload_sha256 text,
  p_payload_size bigint,
  p_payload_preview bytea,
  p_payload_truncated boolean,
  p_headers jsonb,
  p_delivery_metadata jsonb
) RETURNS TABLE (
  created boolean,
  observation_count integer,
  persisted_failure_code text
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing nats_jetstream_consumer_poison_deliveries%ROWTYPE;
  v_archived nats_jetstream_consumer_poison_delivery_archive%ROWTYPE;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_stream IS NULL OR btrim(p_stream) = '' OR p_stream <> btrim(p_stream) THEN
    RAISE EXCEPTION 'JetStream stream is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_durable_consumer IS NULL OR btrim(p_durable_consumer) = ''
     OR p_durable_consumer <> btrim(p_durable_consumer) THEN
    RAISE EXCEPTION 'JetStream durable consumer is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_stream_sequence IS NULL OR p_stream_sequence < 1 THEN
    RAISE EXCEPTION 'JetStream stream sequence must be positive'
      USING ERRCODE = '23514';
  END IF;
  IF p_subject IS NULL OR btrim(p_subject) = '' OR p_subject <> btrim(p_subject) THEN
    RAISE EXCEPTION 'JetStream subject is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_failure_code IS NULL
     OR p_failure_code !~ '^[a-z][a-z0-9_]{0,99}$' THEN
    RAISE EXCEPTION 'poison failure code must be a stable lowercase identifier'
      USING ERRCODE = '23514';
  END IF;
  IF p_payload_sha256 IS NULL
     OR p_payload_sha256 !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'payload SHA-256 must be lowercase hexadecimal'
      USING ERRCODE = '23514';
  END IF;
  IF p_payload_size IS NULL OR p_payload_size < 0 THEN
    RAISE EXCEPTION 'payload size must be nonnegative'
      USING ERRCODE = '23514';
  END IF;
  IF p_payload_preview IS NULL OR octet_length(p_payload_preview) > p_payload_size THEN
    RAISE EXCEPTION 'payload preview cannot exceed payload size'
      USING ERRCODE = '23514';
  END IF;
  IF p_payload_truncated IS NULL
     OR p_payload_truncated IS DISTINCT FROM (
       octet_length(p_payload_preview) < p_payload_size
     ) THEN
    RAISE EXCEPTION 'payload truncation flag must match bounded preview'
      USING ERRCODE = '23514';
  END IF;
  IF p_headers IS NULL OR jsonb_typeof(p_headers) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'JetStream headers must be an object'
      USING ERRCODE = '23514';
  END IF;
  IF p_delivery_metadata IS NULL
     OR jsonb_typeof(p_delivery_metadata) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'JetStream delivery metadata must be an object'
      USING ERRCODE = '23514';
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      p_consumer_name || ':' || p_stream || ':' || p_durable_consumer || ':' ||
      p_stream_sequence::text,
      0
    )
  );

  SELECT d.* INTO v_existing
  FROM nats_jetstream_consumer_poison_deliveries AS d
  WHERE d.consumer_name = p_consumer_name
    AND d.stream = p_stream
    AND d.durable_consumer = p_durable_consumer
    AND d.stream_sequence = p_stream_sequence
  FOR UPDATE;

  IF FOUND THEN
    IF v_existing.subject <> p_subject
       OR v_existing.payload_sha256 <> p_payload_sha256
       OR v_existing.payload_size <> p_payload_size
       OR v_existing.payload_preview <> p_payload_preview
       OR v_existing.payload_truncated <> p_payload_truncated
       OR v_existing.headers <> p_headers THEN
      RAISE EXCEPTION 'JetStream delivery identity belongs to different poison evidence'
        USING ERRCODE = '23505';
    END IF;

    UPDATE nats_jetstream_consumer_poison_deliveries
    SET last_delivery_metadata = p_delivery_metadata,
        last_error = left(COALESCE(NULLIF(p_error, ''), 'malformed JetStream delivery'), 4000),
        observation_count = nats_jetstream_consumer_poison_deliveries.observation_count + 1,
        last_seen_at = v_now
    WHERE consumer_name = p_consumer_name
      AND stream = p_stream
      AND durable_consumer = p_durable_consumer
      AND stream_sequence = p_stream_sequence
    RETURNING * INTO v_existing;

    RETURN QUERY SELECT
      false,
      v_existing.observation_count,
      v_existing.failure_code;
    RETURN;
  END IF;

  SELECT a.* INTO v_archived
  FROM nats_jetstream_consumer_poison_delivery_archive AS a
  WHERE a.consumer_name = p_consumer_name
    AND a.stream = p_stream
    AND a.durable_consumer = p_durable_consumer
    AND a.stream_sequence = p_stream_sequence
  ORDER BY a.archived_at DESC, a.archive_run_id DESC
  LIMIT 1;

  IF FOUND THEN
    IF v_archived.subject <> p_subject
       OR v_archived.payload_sha256 <> p_payload_sha256
       OR v_archived.payload_size <> p_payload_size
       OR v_archived.payload_preview <> p_payload_preview
       OR v_archived.payload_truncated <> p_payload_truncated
       OR v_archived.headers <> p_headers THEN
      RAISE EXCEPTION 'archived JetStream delivery identity belongs to different poison evidence'
        USING ERRCODE = '23505';
    END IF;

    INSERT INTO nats_jetstream_consumer_poison_deliveries (
      consumer_name,
      stream,
      durable_consumer,
      stream_sequence,
      subject,
      failure_code,
      payload_sha256,
      payload_size,
      payload_preview,
      payload_truncated,
      headers,
      first_delivery_metadata,
      last_delivery_metadata,
      last_error,
      observation_count,
      first_seen_at,
      last_seen_at,
      quarantined_at
    ) VALUES (
      p_consumer_name,
      p_stream,
      p_durable_consumer,
      p_stream_sequence,
      p_subject,
      v_archived.failure_code,
      p_payload_sha256,
      p_payload_size,
      p_payload_preview,
      p_payload_truncated,
      p_headers,
      v_archived.first_delivery_metadata,
      p_delivery_metadata,
      left(COALESCE(NULLIF(p_error, ''), 'malformed JetStream delivery'), 4000),
      v_archived.observation_count + 1,
      v_archived.first_seen_at,
      v_now,
      v_archived.quarantined_at
    )
    RETURNING * INTO v_existing;

    RETURN QUERY SELECT
      false,
      v_existing.observation_count,
      v_existing.failure_code;
    RETURN;
  END IF;

  INSERT INTO nats_jetstream_consumer_poison_deliveries (
    consumer_name,
    stream,
    durable_consumer,
    stream_sequence,
    subject,
    failure_code,
    payload_sha256,
    payload_size,
    payload_preview,
    payload_truncated,
    headers,
    first_delivery_metadata,
    last_delivery_metadata,
    last_error,
    observation_count,
    first_seen_at,
    last_seen_at,
    quarantined_at
  ) VALUES (
    p_consumer_name,
    p_stream,
    p_durable_consumer,
    p_stream_sequence,
    p_subject,
    p_failure_code,
    p_payload_sha256,
    p_payload_size,
    p_payload_preview,
    p_payload_truncated,
    p_headers,
    p_delivery_metadata,
    p_delivery_metadata,
    left(COALESCE(NULLIF(p_error, ''), 'malformed JetStream delivery'), 4000),
    1,
    v_now,
    v_now,
    v_now
  )
  RETURNING * INTO v_existing;

  RETURN QUERY SELECT
    true,
    v_existing.observation_count,
    v_existing.failure_code;
END;
$$;

COMMIT;
