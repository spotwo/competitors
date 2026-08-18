BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE FUNCTION read_domain_event_outbox_telemetry(
  p_observed_at timestamptz DEFAULT statement_timestamp()
) RETURNS TABLE (
  observed_at timestamptz,
  backlog_count bigint,
  ready_count bigint,
  delayed_count bigint,
  leased_count bigint,
  quarantined_count bigint,
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
      o.recorded_at,
      o.quarantined_at,
      o.attempt_count,
      CASE
        WHEN o.quarantined_at IS NOT NULL THEN 'quarantined'
        WHEN o.claimed_until > t.observed_at THEN 'leased'
        WHEN o.available_at > t.observed_at THEN 'delayed'
        ELSE 'ready'
      END AS delivery_state
    FROM domain_event_outbox o
    CROSS JOIN observation t
    WHERE o.published_at IS NULL
  ), aggregate_snapshot AS (
    SELECT
      count(*) AS backlog_count,
      count(*) FILTER (WHERE delivery_state = 'ready') AS ready_count,
      count(*) FILTER (WHERE delivery_state = 'delayed') AS delayed_count,
      count(*) FILTER (WHERE delivery_state = 'leased') AS leased_count,
      count(*) FILTER (WHERE delivery_state = 'quarantined') AS quarantined_count,
      count(*) FILTER (WHERE attempt_count = 0) AS attempts_0_count,
      count(*) FILTER (WHERE attempt_count = 1) AS attempts_1_count,
      count(*) FILTER (WHERE attempt_count BETWEEN 2 AND 4) AS attempts_2_to_4_count,
      count(*) FILTER (WHERE attempt_count >= 5) AS attempts_5_plus_count,
      max(attempt_count) AS max_attempt_count,
      min(recorded_at) AS oldest_backlog_at,
      min(recorded_at) FILTER (WHERE delivery_state = 'ready') AS oldest_ready_at,
      min(quarantined_at) FILTER (
        WHERE delivery_state = 'quarantined'
      ) AS oldest_quarantined_at
    FROM classified
  )
  SELECT
    t.observed_at,
    a.backlog_count,
    a.ready_count,
    a.delayed_count,
    a.leased_count,
    a.quarantined_count,
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
      ELSE greatest(0, extract(epoch FROM t.observed_at - a.oldest_quarantined_at))
    END
  FROM aggregate_snapshot a
  CROSS JOIN observation t;
$$;

COMMIT;
