BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE FUNCTION read_domain_event_consumer_failure_telemetry(
  p_observed_at timestamptz DEFAULT statement_timestamp(),
  p_consumer_name text DEFAULT NULL
) RETURNS TABLE (
  observed_at timestamptz,
  consumer_name text,
  backlog_count bigint,
  ready_count bigint,
  delayed_count bigint,
  leased_count bigint,
  quarantined_count bigint,
  terminal_quarantined_count bigint,
  exhausted_quarantined_count bigint,
  attempts_0_count bigint,
  attempts_1_count bigint,
  attempts_2_to_4_count bigint,
  attempts_5_plus_count bigint,
  max_attempt_count integer,
  oldest_backlog_age_seconds numeric,
  oldest_ready_age_seconds numeric,
  oldest_quarantined_age_seconds numeric
)
LANGUAGE sql
STABLE
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
  WITH observation AS (
    SELECT COALESCE(p_observed_at, statement_timestamp()) AS observed_at
  ), classified AS (
    SELECT
      f.retryable,
      f.attempt_count,
      f.first_failed_at,
      f.quarantined_at,
      CASE
        WHEN f.status = 'quarantined' THEN 'quarantined'
        WHEN f.claimed_until > t.observed_at THEN 'leased'
        WHEN f.available_at > t.observed_at THEN 'delayed'
        ELSE 'ready'
      END AS failure_state
    FROM domain_event_consumer_failures AS f
    CROSS JOIN observation AS t
    WHERE f.status IN ('deferred', 'quarantined')
      AND (p_consumer_name IS NULL OR f.consumer_name = p_consumer_name)
  ), aggregate_snapshot AS (
    SELECT
      count(*) AS backlog_count,
      count(*) FILTER (WHERE failure_state = 'ready') AS ready_count,
      count(*) FILTER (WHERE failure_state = 'delayed') AS delayed_count,
      count(*) FILTER (WHERE failure_state = 'leased') AS leased_count,
      count(*) FILTER (WHERE failure_state = 'quarantined') AS quarantined_count,
      count(*) FILTER (
        WHERE failure_state = 'quarantined' AND NOT retryable
      ) AS terminal_quarantined_count,
      count(*) FILTER (
        WHERE failure_state = 'quarantined' AND retryable
      ) AS exhausted_quarantined_count,
      count(*) FILTER (WHERE attempt_count = 0) AS attempts_0_count,
      count(*) FILTER (WHERE attempt_count = 1) AS attempts_1_count,
      count(*) FILTER (WHERE attempt_count BETWEEN 2 AND 4) AS attempts_2_to_4_count,
      count(*) FILTER (WHERE attempt_count >= 5) AS attempts_5_plus_count,
      max(attempt_count) AS max_attempt_count,
      min(first_failed_at) AS oldest_backlog_at,
      min(first_failed_at) FILTER (
        WHERE failure_state = 'ready'
      ) AS oldest_ready_at,
      min(quarantined_at) FILTER (
        WHERE failure_state = 'quarantined'
      ) AS oldest_quarantined_at
    FROM classified
  )
  SELECT
    t.observed_at,
    p_consumer_name,
    a.backlog_count,
    a.ready_count,
    a.delayed_count,
    a.leased_count,
    a.quarantined_count,
    a.terminal_quarantined_count,
    a.exhausted_quarantined_count,
    a.attempts_0_count,
    a.attempts_1_count,
    a.attempts_2_to_4_count,
    a.attempts_5_plus_count,
    a.max_attempt_count,
    CASE
      WHEN a.oldest_backlog_at IS NULL THEN NULL
      ELSE greatest(0, extract(epoch FROM t.observed_at - a.oldest_backlog_at))
    END,
    CASE
      WHEN a.oldest_ready_at IS NULL THEN NULL
      ELSE greatest(0, extract(epoch FROM t.observed_at - a.oldest_ready_at))
    END,
    CASE
      WHEN a.oldest_quarantined_at IS NULL THEN NULL
      ELSE greatest(
        0,
        extract(epoch FROM t.observed_at - a.oldest_quarantined_at)
      )
    END
  FROM aggregate_snapshot AS a
  CROSS JOIN observation AS t;
$$;

COMMIT;
