BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE nats_jetstream_consumer_poison_deliveries (
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
  PRIMARY KEY (consumer_name, stream, durable_consumer, stream_sequence),
  CONSTRAINT nats_jetstream_consumer_poison_payload_preview_ck CHECK (
    octet_length(payload_preview) <= payload_size
    AND payload_truncated = (octet_length(payload_preview) < payload_size)
  )
);

CREATE INDEX nats_jetstream_consumer_poison_deliveries_quarantine_idx
  ON nats_jetstream_consumer_poison_deliveries (
    consumer_name, quarantined_at, stream, durable_consumer, stream_sequence
  );

CREATE FUNCTION capture_nats_jetstream_consumer_poison_delivery(
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
  );

  RETURN QUERY SELECT true, 1, p_failure_code;
END;
$$;

COMMIT;
