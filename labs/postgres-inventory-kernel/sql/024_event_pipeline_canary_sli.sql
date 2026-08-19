BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE event_pipeline_canary_outcomes (
  canary_id uuid PRIMARY KEY
    REFERENCES event_pipeline_canary_runs(canary_id) ON DELETE RESTRICT,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  started_at timestamptz NOT NULL,
  deadline_at timestamptz NOT NULL,
  finalized_at timestamptz,
  stream_name text NOT NULL CHECK (btrim(stream_name) <> ''),
  durable_name text NOT NULL CHECK (btrim(durable_name) <> ''),
  consumer_name text NOT NULL CHECK (btrim(consumer_name) <> ''),
  outcome text NOT NULL DEFAULT 'pending'
    CHECK (outcome IN ('pending', 'succeeded', 'failed')),
  health_status text CHECK (
    health_status IS NULL OR health_status IN ('ok', 'warning', 'critical')
  ),
  terminal_code text,
  timed_out boolean,
  ack_confirmed boolean,
  clock_skew_detected boolean,
  outbox_publish_confirm_seconds double precision,
  broker_to_inbox_seconds double precision,
  inbox_to_projection_seconds double precision,
  end_to_end_projection_seconds double precision,
  CONSTRAINT event_pipeline_canary_outcome_deadline_ck CHECK (
    deadline_at > started_at
  ),
  CONSTRAINT event_pipeline_canary_outcome_time_ck CHECK (
    finalized_at IS NULL OR finalized_at >= started_at
  ),
  CONSTRAINT event_pipeline_canary_outcome_publish_latency_ck CHECK (
    outbox_publish_confirm_seconds IS NULL OR outbox_publish_confirm_seconds >= 0
  ),
  CONSTRAINT event_pipeline_canary_outcome_projection_latency_ck CHECK (
    inbox_to_projection_seconds IS NULL OR inbox_to_projection_seconds >= 0
  ),
  CONSTRAINT event_pipeline_canary_outcome_end_to_end_latency_ck CHECK (
    end_to_end_projection_seconds IS NULL OR end_to_end_projection_seconds >= 0
  ),
  CONSTRAINT event_pipeline_canary_outcome_terminal_code_ck CHECK (
    terminal_code IS NULL OR terminal_code IN (
      'canary_consumer_configuration_invalid',
      'canary_stream_identity_mismatch',
      'canary_durable_identity_mismatch',
      'canary_consumer_identity_mismatch',
      'canary_publish_timeout',
      'canary_delivery_timeout',
      'canary_projection_timeout',
      'canary_ack_timeout',
      'canary_incomplete_timeout',
      'canary_ack_observer_unavailable',
      'canary_incomplete'
    )
  ),
  CONSTRAINT event_pipeline_canary_outcome_state_ck CHECK (
    (
      outcome = 'pending'
      AND finalized_at IS NULL
      AND health_status IS NULL
      AND terminal_code IS NULL
      AND timed_out IS NULL
      AND ack_confirmed IS NULL
      AND clock_skew_detected IS NULL
      AND outbox_publish_confirm_seconds IS NULL
      AND broker_to_inbox_seconds IS NULL
      AND inbox_to_projection_seconds IS NULL
      AND end_to_end_projection_seconds IS NULL
    )
    OR
    (
      outcome = 'succeeded'
      AND finalized_at IS NOT NULL
      AND health_status IS NOT NULL
      AND terminal_code IS NULL
      AND timed_out = false
      AND ack_confirmed = true
      AND clock_skew_detected IS NOT NULL
      AND outbox_publish_confirm_seconds IS NOT NULL
      AND broker_to_inbox_seconds IS NOT NULL
      AND inbox_to_projection_seconds IS NOT NULL
      AND end_to_end_projection_seconds IS NOT NULL
    )
    OR
    (
      outcome = 'failed'
      AND finalized_at IS NOT NULL
      AND health_status IS NOT NULL
      AND terminal_code IS NOT NULL
      AND timed_out IS NOT NULL
      AND ack_confirmed IS NOT NULL
      AND clock_skew_detected IS NOT NULL
    )
  )
);

CREATE INDEX event_pipeline_canary_outcomes_scope_start_idx
  ON event_pipeline_canary_outcomes (
    stream_name,
    durable_name,
    consumer_name,
    started_at DESC,
    canary_id
  );

CREATE INDEX event_pipeline_canary_outcomes_scope_finalized_idx
  ON event_pipeline_canary_outcomes (
    stream_name,
    durable_name,
    consumer_name,
    finalized_at DESC,
    canary_id
  )
  WHERE finalized_at IS NOT NULL;

CREATE FUNCTION start_tracked_event_pipeline_canary(
  p_tenant_id uuid,
  p_stream_name text,
  p_durable_name text,
  p_consumer_name text,
  p_timeout_seconds double precision DEFAULT 30.0
) RETURNS TABLE (
  canary_id uuid,
  event_id uuid,
  recorded_at timestamptz
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_canary_id uuid;
  v_event_id uuid;
  v_recorded_at timestamptz;
BEGIN
  IF p_stream_name IS NULL OR btrim(p_stream_name) = ''
     OR p_durable_name IS NULL OR btrim(p_durable_name) = ''
     OR p_consumer_name IS NULL OR btrim(p_consumer_name) = '' THEN
    RAISE EXCEPTION 'canary SLI scope is required' USING ERRCODE = '23514';
  END IF;
  IF p_timeout_seconds IS NULL OR p_timeout_seconds <= 0 OR p_timeout_seconds > 300 THEN
    RAISE EXCEPTION 'canary timeout must be between zero and 300 seconds'
      USING ERRCODE = '23514';
  END IF;

  SELECT started.canary_id, started.event_id, started.recorded_at
  INTO STRICT v_canary_id, v_event_id, v_recorded_at
  FROM start_event_pipeline_canary(p_tenant_id) started;

  INSERT INTO event_pipeline_canary_outcomes (
    canary_id,
    tenant_id,
    started_at,
    deadline_at,
    stream_name,
    durable_name,
    consumer_name
  ) VALUES (
    v_canary_id,
    p_tenant_id,
    v_recorded_at,
    v_recorded_at + make_interval(secs => p_timeout_seconds),
    p_stream_name,
    p_durable_name,
    p_consumer_name
  );

  RETURN QUERY SELECT v_canary_id, v_event_id, v_recorded_at;
END;
$$;

CREATE FUNCTION record_event_pipeline_canary_outcome(
  p_canary_id uuid,
  p_outcome text,
  p_health_status text,
  p_terminal_code text,
  p_timed_out boolean,
  p_ack_confirmed boolean,
  p_clock_skew_detected boolean,
  p_outbox_publish_confirm_seconds double precision,
  p_broker_to_inbox_seconds double precision,
  p_inbox_to_projection_seconds double precision,
  p_end_to_end_projection_seconds double precision
) RETURNS TABLE (
  created boolean,
  finalized_at timestamptz
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing event_pipeline_canary_outcomes%ROWTYPE;
  v_finalized_at timestamptz := clock_timestamp();
BEGIN
  IF p_canary_id IS NULL THEN
    RAISE EXCEPTION 'canary id is required' USING ERRCODE = '23514';
  END IF;
  IF p_outcome IS NULL OR p_outcome NOT IN ('succeeded', 'failed') THEN
    RAISE EXCEPTION 'canary SLI outcome is invalid' USING ERRCODE = '23514';
  END IF;
  IF p_health_status IS NULL OR p_health_status NOT IN ('ok', 'warning', 'critical') THEN
    RAISE EXCEPTION 'canary SLI health status is invalid' USING ERRCODE = '23514';
  END IF;

  UPDATE event_pipeline_canary_outcomes
  SET outcome = p_outcome,
      health_status = p_health_status,
      terminal_code = p_terminal_code,
      timed_out = p_timed_out,
      ack_confirmed = p_ack_confirmed,
      clock_skew_detected = p_clock_skew_detected,
      outbox_publish_confirm_seconds = p_outbox_publish_confirm_seconds,
      broker_to_inbox_seconds = p_broker_to_inbox_seconds,
      inbox_to_projection_seconds = p_inbox_to_projection_seconds,
      end_to_end_projection_seconds = p_end_to_end_projection_seconds,
      finalized_at = v_finalized_at
  WHERE canary_id = p_canary_id
    AND outcome = 'pending';

  IF FOUND THEN
    RETURN QUERY SELECT true, v_finalized_at;
    RETURN;
  END IF;

  SELECT *
  INTO v_existing
  FROM event_pipeline_canary_outcomes
  WHERE canary_id = p_canary_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'tracked event pipeline canary does not exist' USING ERRCODE = '23503';
  END IF;

  IF v_existing.outcome IS DISTINCT FROM p_outcome
     OR v_existing.health_status IS DISTINCT FROM p_health_status
     OR v_existing.terminal_code IS DISTINCT FROM p_terminal_code
     OR v_existing.timed_out IS DISTINCT FROM p_timed_out
     OR v_existing.ack_confirmed IS DISTINCT FROM p_ack_confirmed
     OR v_existing.clock_skew_detected IS DISTINCT FROM p_clock_skew_detected
     OR v_existing.outbox_publish_confirm_seconds IS DISTINCT FROM p_outbox_publish_confirm_seconds
     OR v_existing.broker_to_inbox_seconds IS DISTINCT FROM p_broker_to_inbox_seconds
     OR v_existing.inbox_to_projection_seconds IS DISTINCT FROM p_inbox_to_projection_seconds
     OR v_existing.end_to_end_projection_seconds IS DISTINCT FROM p_end_to_end_projection_seconds THEN
    RAISE EXCEPTION 'canary outcome already belongs to different evidence'
      USING ERRCODE = '23505';
  END IF;

  RETURN QUERY SELECT false, v_existing.finalized_at;
END;
$$;

CREATE FUNCTION read_event_pipeline_canary_sli(
  p_stream_name text,
  p_durable_name text,
  p_consumer_name text,
  p_window_seconds integer,
  p_availability_target double precision DEFAULT 0.999,
  p_latency_target_ratio double precision DEFAULT 0.99,
  p_latency_target_seconds double precision DEFAULT 5.0
) RETURNS TABLE (
  observed_at timestamptz,
  window_seconds integer,
  total_runs bigint,
  successful_runs bigint,
  failed_runs bigint,
  availability_success_ratio double precision,
  availability_burn_rate double precision,
  latency_good_runs bigint,
  latency_success_ratio double precision,
  latency_burn_rate double precision,
  last_run_at timestamptz,
  last_success_at timestamptz,
  consecutive_failures bigint,
  failure_codes jsonb,
  latency_quantiles jsonb
)
LANGUAGE plpgsql
STABLE
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_observed_at timestamptz := statement_timestamp();
BEGIN
  IF p_stream_name IS NULL OR btrim(p_stream_name) = ''
     OR p_durable_name IS NULL OR btrim(p_durable_name) = ''
     OR p_consumer_name IS NULL OR btrim(p_consumer_name) = '' THEN
    RAISE EXCEPTION 'canary SLI scope is required' USING ERRCODE = '23514';
  END IF;
  IF p_window_seconds IS NULL OR p_window_seconds < 60 OR p_window_seconds > 604800 THEN
    RAISE EXCEPTION 'canary SLI window must be between 60 and 604800 seconds'
      USING ERRCODE = '23514';
  END IF;
  IF p_availability_target <= 0 OR p_availability_target >= 1 THEN
    RAISE EXCEPTION 'availability target must be between zero and one'
      USING ERRCODE = '23514';
  END IF;
  IF p_latency_target_ratio <= 0 OR p_latency_target_ratio >= 1 THEN
    RAISE EXCEPTION 'latency target ratio must be between zero and one'
      USING ERRCODE = '23514';
  END IF;
  IF p_latency_target_seconds <= 0 THEN
    RAISE EXCEPTION 'latency target seconds must be positive'
      USING ERRCODE = '23514';
  END IF;

  RETURN QUERY
  WITH scope_all AS (
    SELECT
      o.*,
      CASE
        WHEN o.outcome = 'pending' AND o.deadline_at <= v_observed_at THEN 'failed'
        ELSE o.outcome
      END AS effective_outcome,
      CASE
        WHEN o.outcome = 'pending' AND o.deadline_at <= v_observed_at THEN o.deadline_at
        ELSE o.finalized_at
      END AS effective_finalized_at,
      CASE
        WHEN o.outcome = 'pending' AND o.deadline_at <= v_observed_at THEN 'canary_probe_abandoned'
        ELSE o.terminal_code
      END AS effective_terminal_code
    FROM event_pipeline_canary_outcomes o
    WHERE o.stream_name = p_stream_name
      AND o.durable_name = p_durable_name
      AND o.consumer_name = p_consumer_name
  ), terminal_all AS (
    SELECT *
    FROM scope_all
    WHERE effective_outcome <> 'pending'
  ), scoped AS (
    SELECT o.*
    FROM terminal_all o
    WHERE o.effective_finalized_at >= v_observed_at - make_interval(secs => p_window_seconds)
      AND o.effective_finalized_at <= v_observed_at
  ), aggregate_counts AS (
    SELECT
      count(*)::bigint AS total_runs,
      count(*) FILTER (WHERE effective_outcome = 'succeeded')::bigint AS successful_runs,
      count(*) FILTER (WHERE effective_outcome = 'failed')::bigint AS failed_runs,
      count(*) FILTER (
        WHERE effective_outcome = 'succeeded'
          AND end_to_end_projection_seconds <= p_latency_target_seconds
      )::bigint AS latency_good_runs
    FROM scoped
  ), latest AS (
    SELECT
      max(started_at) AS last_run_at,
      max(effective_finalized_at) FILTER (
        WHERE effective_outcome = 'succeeded'
      ) AS last_success_at
    FROM scope_all
  ), ordered AS (
    SELECT
      effective_outcome,
      row_number() OVER (
        ORDER BY effective_finalized_at DESC, canary_id DESC
      ) AS ordinal
    FROM terminal_all
  ), first_success AS (
    SELECT min(ordinal) AS ordinal
    FROM ordered
    WHERE effective_outcome = 'succeeded'
  ), failure_streak AS (
    SELECT count(*)::bigint AS consecutive_failures
    FROM ordered, first_success
    WHERE ordered.effective_outcome = 'failed'
      AND ordered.ordinal < COALESCE(first_success.ordinal, 9223372036854775807)
  ), failure_code_counts AS (
    SELECT effective_terminal_code, count(*)::bigint AS failure_count
    FROM scoped
    WHERE effective_outcome = 'failed'
    GROUP BY effective_terminal_code
  ), failure_code_map AS (
    SELECT COALESCE(
      jsonb_object_agg(
        effective_terminal_code,
        failure_count
        ORDER BY effective_terminal_code
      ),
      '{}'::jsonb
    ) AS failure_codes
    FROM failure_code_counts
  ), quantiles AS (
    SELECT
      percentile_cont(ARRAY[0.5, 0.95, 0.99])
        WITHIN GROUP (ORDER BY outbox_publish_confirm_seconds)
        FILTER (
          WHERE effective_outcome = 'succeeded'
            AND outbox_publish_confirm_seconds IS NOT NULL
        ) AS publish_values,
      percentile_cont(ARRAY[0.5, 0.95, 0.99])
        WITHIN GROUP (ORDER BY broker_to_inbox_seconds)
        FILTER (
          WHERE effective_outcome = 'succeeded'
            AND broker_to_inbox_seconds IS NOT NULL
            AND broker_to_inbox_seconds >= 0
            AND clock_skew_detected = false
        ) AS delivery_values,
      percentile_cont(ARRAY[0.5, 0.95, 0.99])
        WITHIN GROUP (ORDER BY inbox_to_projection_seconds)
        FILTER (
          WHERE effective_outcome = 'succeeded'
            AND inbox_to_projection_seconds IS NOT NULL
        ) AS projection_values,
      percentile_cont(ARRAY[0.5, 0.95, 0.99])
        WITHIN GROUP (ORDER BY end_to_end_projection_seconds)
        FILTER (
          WHERE effective_outcome = 'succeeded'
            AND end_to_end_projection_seconds IS NOT NULL
        ) AS end_to_end_values
    FROM scoped
  )
  SELECT
    v_observed_at,
    p_window_seconds,
    counts.total_runs,
    counts.successful_runs,
    counts.failed_runs,
    CASE
      WHEN counts.total_runs = 0 THEN NULL
      ELSE counts.successful_runs::double precision / counts.total_runs
    END,
    CASE
      WHEN counts.total_runs = 0 THEN NULL
      ELSE (
        counts.failed_runs::double precision / counts.total_runs
      ) / (1.0 - p_availability_target)
    END,
    counts.latency_good_runs,
    CASE
      WHEN counts.successful_runs = 0 THEN NULL
      ELSE counts.latency_good_runs::double precision / counts.successful_runs
    END,
    CASE
      WHEN counts.successful_runs = 0 THEN NULL
      ELSE (
        (counts.successful_runs - counts.latency_good_runs)::double precision
        / counts.successful_runs
      ) / (1.0 - p_latency_target_ratio)
    END,
    latest.last_run_at,
    latest.last_success_at,
    streak.consecutive_failures,
    failure_map.failure_codes,
    jsonb_build_object(
      'outbox_publish_confirm', jsonb_build_object(
        'p50', quantiles.publish_values[1],
        'p95', quantiles.publish_values[2],
        'p99', quantiles.publish_values[3]
      ),
      'broker_to_inbox', jsonb_build_object(
        'p50', quantiles.delivery_values[1],
        'p95', quantiles.delivery_values[2],
        'p99', quantiles.delivery_values[3]
      ),
      'inbox_to_projection', jsonb_build_object(
        'p50', quantiles.projection_values[1],
        'p95', quantiles.projection_values[2],
        'p99', quantiles.projection_values[3]
      ),
      'end_to_end_projection', jsonb_build_object(
        'p50', quantiles.end_to_end_values[1],
        'p95', quantiles.end_to_end_values[2],
        'p99', quantiles.end_to_end_values[3]
      )
    )
  FROM aggregate_counts counts
  CROSS JOIN latest
  CROSS JOIN failure_streak streak
  CROSS JOIN failure_code_map failure_map
  CROSS JOIN quantiles;
END;
$$;

COMMIT;
