BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE FUNCTION read_nats_jetstream_consumer_poison_telemetry(
  p_observed_at timestamptz DEFAULT statement_timestamp(),
  p_consumer_name text DEFAULT NULL,
  p_lookback_seconds integer DEFAULT 300
) RETURNS TABLE (
  observed_at timestamptz,
  consumer_name text,
  lookback_seconds integer,
  retained_count bigint,
  recent_count bigint,
  recent_reobserved_count bigint,
  total_observation_count bigint,
  invalid_encoding_count bigint,
  invalid_json_count bigint,
  invalid_envelope_count bigint,
  invalid_headers_count bigint,
  other_failure_count bigint,
  max_observation_count integer,
  oldest_age_seconds numeric,
  newest_age_seconds numeric,
  latest_observation_age_seconds numeric
)
LANGUAGE plpgsql
STABLE
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_consumer_name IS NOT NULL AND (
    btrim(p_consumer_name) = '' OR p_consumer_name <> btrim(p_consumer_name)
  ) THEN
    RAISE EXCEPTION 'consumer name cannot be blank or contain surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_lookback_seconds IS NULL OR p_lookback_seconds < 1 OR p_lookback_seconds > 604800 THEN
    RAISE EXCEPTION 'lookback seconds must be between 1 and 604800'
      USING ERRCODE = '23514';
  END IF;

  RETURN QUERY
  WITH observation AS (
    SELECT
      COALESCE(p_observed_at, statement_timestamp()) AS observed_at,
      make_interval(secs => p_lookback_seconds) AS lookback
  ), aggregate_snapshot AS (
    SELECT
      count(*) AS retained_count,
      count(*) FILTER (
        WHERE d.first_seen_at >= o.observed_at - o.lookback
          AND d.first_seen_at <= o.observed_at
      ) AS recent_count,
      count(*) FILTER (
        WHERE d.observation_count > 1
          AND d.last_seen_at >= o.observed_at - o.lookback
          AND d.last_seen_at <= o.observed_at
      ) AS recent_reobserved_count,
      COALESCE(sum(d.observation_count), 0)::bigint AS total_observation_count,
      count(*) FILTER (
        WHERE d.failure_code = 'invalid_message_encoding'
      ) AS invalid_encoding_count,
      count(*) FILTER (
        WHERE d.failure_code = 'invalid_message_json'
      ) AS invalid_json_count,
      count(*) FILTER (
        WHERE d.failure_code = 'invalid_event_envelope'
      ) AS invalid_envelope_count,
      count(*) FILTER (
        WHERE d.failure_code = 'invalid_transport_headers'
      ) AS invalid_headers_count,
      count(*) FILTER (
        WHERE d.failure_code NOT IN (
          'invalid_message_encoding',
          'invalid_message_json',
          'invalid_event_envelope',
          'invalid_transport_headers'
        )
      ) AS other_failure_count,
      max(d.observation_count) AS max_observation_count,
      min(d.first_seen_at) AS oldest_first_seen_at,
      max(d.first_seen_at) AS newest_first_seen_at,
      max(d.last_seen_at) AS latest_observation_at
    FROM nats_jetstream_consumer_poison_deliveries AS d
    CROSS JOIN observation AS o
    WHERE p_consumer_name IS NULL OR d.consumer_name = p_consumer_name
  )
  SELECT
    o.observed_at,
    p_consumer_name,
    p_lookback_seconds,
    a.retained_count,
    a.recent_count,
    a.recent_reobserved_count,
    a.total_observation_count,
    a.invalid_encoding_count,
    a.invalid_json_count,
    a.invalid_envelope_count,
    a.invalid_headers_count,
    a.other_failure_count,
    a.max_observation_count,
    CASE
      WHEN a.oldest_first_seen_at IS NULL THEN NULL
      ELSE greatest(0, extract(epoch FROM o.observed_at - a.oldest_first_seen_at))
    END,
    CASE
      WHEN a.newest_first_seen_at IS NULL THEN NULL
      ELSE greatest(0, extract(epoch FROM o.observed_at - a.newest_first_seen_at))
    END,
    CASE
      WHEN a.latest_observation_at IS NULL THEN NULL
      ELSE greatest(0, extract(epoch FROM o.observed_at - a.latest_observation_at))
    END
  FROM aggregate_snapshot AS a
  CROSS JOIN observation AS o;
END;
$$;

COMMIT;
