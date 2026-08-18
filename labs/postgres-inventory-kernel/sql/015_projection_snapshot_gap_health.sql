BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

ALTER TABLE inventory_position_quantity_projection
  DROP CONSTRAINT inventory_position_projection_cursor_ck;

ALTER TABLE inventory_position_quantity_projection
  ADD COLUMN bootstrap_id uuid,
  ADD COLUMN bootstrap_version bigint,
  ADD COLUMN bootstrap_physical_qty numeric(24, 6),
  ADD COLUMN bootstrap_reserved_qty numeric(24, 6),
  ADD COLUMN bootstrap_allocated_qty numeric(24, 6),
  ADD COLUMN bootstrap_source text,
  ADD COLUMN bootstrap_reference text,
  ADD COLUMN bootstrap_checksum text,
  ADD COLUMN bootstrap_recorded_at timestamptz,
  ADD COLUMN bootstrapped_by text,
  ADD COLUMN bootstrap_reason text,
  ADD COLUMN bootstrapped_at timestamptz,
  ADD COLUMN cursor_source text GENERATED ALWAYS AS (
    CASE
      WHEN aggregate_version = 0 THEN 'empty'
      WHEN last_event_id IS NULL THEN 'snapshot'
      ELSE 'event'
    END
  ) STORED;

ALTER TABLE inventory_position_quantity_projection
  ALTER COLUMN cursor_source SET NOT NULL,
  ADD CONSTRAINT inventory_position_projection_bootstrap_fields_ck CHECK (
    (
      bootstrap_id IS NULL
      AND bootstrap_version IS NULL
      AND bootstrap_physical_qty IS NULL
      AND bootstrap_reserved_qty IS NULL
      AND bootstrap_allocated_qty IS NULL
      AND bootstrap_source IS NULL
      AND bootstrap_reference IS NULL
      AND bootstrap_checksum IS NULL
      AND bootstrap_recorded_at IS NULL
      AND bootstrapped_by IS NULL
      AND bootstrap_reason IS NULL
      AND bootstrapped_at IS NULL
    )
    OR
    (
      bootstrap_id IS NOT NULL
      AND bootstrap_version IS NOT NULL
      AND bootstrap_version >= 1
      AND bootstrap_version <= aggregate_version
      AND bootstrap_physical_qty IS NOT NULL
      AND bootstrap_reserved_qty IS NOT NULL
      AND bootstrap_allocated_qty IS NOT NULL
      AND bootstrap_physical_qty >= 0
      AND bootstrap_reserved_qty >= 0
      AND bootstrap_allocated_qty >= 0
      AND bootstrap_reserved_qty <= bootstrap_physical_qty
      AND bootstrap_allocated_qty <= bootstrap_physical_qty
      AND bootstrap_source IS NOT NULL
      AND bootstrap_source = btrim(bootstrap_source)
      AND bootstrap_source <> ''
      AND bootstrap_reference IS NOT NULL
      AND bootstrap_reference = btrim(bootstrap_reference)
      AND bootstrap_reference <> ''
      AND bootstrap_checksum IS NOT NULL
      AND bootstrap_checksum ~ '^sha256:[0-9a-f]{64}$'
      AND bootstrap_recorded_at IS NOT NULL
      AND bootstrapped_by IS NOT NULL
      AND bootstrapped_by = btrim(bootstrapped_by)
      AND bootstrapped_by <> ''
      AND bootstrap_reason IS NOT NULL
      AND bootstrap_reason = btrim(bootstrap_reason)
      AND bootstrap_reason <> ''
      AND bootstrapped_at IS NOT NULL
    )
  ),
  ADD CONSTRAINT inventory_position_projection_cursor_ck CHECK (
    (
      cursor_source = 'empty'
      AND aggregate_version = 0
      AND last_event_id IS NULL
      AND last_recorded_at IS NULL
      AND bootstrap_id IS NULL
    )
    OR
    (
      cursor_source = 'snapshot'
      AND aggregate_version = bootstrap_version
      AND last_event_id IS NULL
      AND last_recorded_at IS NULL
      AND bootstrap_id IS NOT NULL
    )
    OR
    (
      cursor_source = 'event'
      AND aggregate_version >= 1
      AND last_event_id IS NOT NULL
      AND last_recorded_at IS NOT NULL
      AND (
        bootstrap_id IS NULL
        OR bootstrap_version < aggregate_version
      )
    )
  );

CREATE UNIQUE INDEX inventory_position_projection_bootstrap_id_uq
  ON inventory_position_quantity_projection (bootstrap_id)
  WHERE bootstrap_id IS NOT NULL;

CREATE FUNCTION bootstrap_inventory_position_quantity_projection(
  p_bootstrap_id uuid,
  p_consumer_name text,
  p_position_id uuid,
  p_aggregate_version bigint,
  p_physical_qty numeric,
  p_reserved_qty numeric,
  p_allocated_qty numeric,
  p_snapshot_source text,
  p_snapshot_reference text,
  p_snapshot_checksum text,
  p_snapshot_recorded_at timestamptz,
  p_bootstrapped_by text,
  p_bootstrap_reason text
) RETURNS TABLE (
  outcome text,
  bootstrap_version bigint,
  projection_version bigint
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing inventory_position_quantity_projection%ROWTYPE;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_bootstrap_id IS NULL OR p_position_id IS NULL THEN
    RAISE EXCEPTION 'bootstrap and position identity are required'
      USING ERRCODE = '23514';
  END IF;
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_aggregate_version IS NULL OR p_aggregate_version < 1 THEN
    RAISE EXCEPTION 'snapshot aggregate version must be positive'
      USING ERRCODE = '23514';
  END IF;
  IF p_physical_qty IS NULL OR p_reserved_qty IS NULL OR p_allocated_qty IS NULL THEN
    RAISE EXCEPTION 'snapshot quantities are required'
      USING ERRCODE = '23514';
  END IF;
  IF p_physical_qty < 0 OR p_reserved_qty < 0 OR p_allocated_qty < 0
     OR p_reserved_qty > p_physical_qty
     OR p_allocated_qty > p_physical_qty THEN
    RAISE EXCEPTION 'snapshot quantities violate projection invariants'
      USING ERRCODE = '23514';
  END IF;
  IF scale(p_physical_qty) > 6 OR scale(p_reserved_qty) > 6
     OR scale(p_allocated_qty) > 6 THEN
    RAISE EXCEPTION 'snapshot quantities support at most six fractional digits'
      USING ERRCODE = '23514';
  END IF;
  IF abs(p_physical_qty) >= 1000000000000000000
     OR abs(p_reserved_qty) >= 1000000000000000000
     OR abs(p_allocated_qty) >= 1000000000000000000 THEN
    RAISE EXCEPTION 'snapshot quantities exceed projection precision'
      USING ERRCODE = '23514';
  END IF;
  IF p_snapshot_source IS NULL OR btrim(p_snapshot_source) = ''
     OR p_snapshot_source <> btrim(p_snapshot_source)
     OR p_snapshot_reference IS NULL OR btrim(p_snapshot_reference) = ''
     OR p_snapshot_reference <> btrim(p_snapshot_reference) THEN
    RAISE EXCEPTION 'snapshot source and reference are required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_snapshot_checksum IS NULL
     OR p_snapshot_checksum !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'snapshot checksum must be lowercase sha256:<64 hex>'
      USING ERRCODE = '23514';
  END IF;
  IF p_snapshot_recorded_at IS NULL THEN
    RAISE EXCEPTION 'snapshot recorded timestamp is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_bootstrapped_by IS NULL OR btrim(p_bootstrapped_by) = ''
     OR p_bootstrapped_by <> btrim(p_bootstrapped_by)
     OR p_bootstrap_reason IS NULL OR btrim(p_bootstrap_reason) = ''
     OR p_bootstrap_reason <> btrim(p_bootstrap_reason) THEN
    RAISE EXCEPTION 'bootstrap operator and reason are required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_position_quantity_projection (
    consumer_name,
    position_id,
    aggregate_version,
    physical_qty,
    reserved_qty,
    allocated_qty,
    bootstrap_id,
    bootstrap_version,
    bootstrap_physical_qty,
    bootstrap_reserved_qty,
    bootstrap_allocated_qty,
    bootstrap_source,
    bootstrap_reference,
    bootstrap_checksum,
    bootstrap_recorded_at,
    bootstrapped_by,
    bootstrap_reason,
    bootstrapped_at,
    updated_at
  ) VALUES (
    p_consumer_name,
    p_position_id,
    p_aggregate_version,
    p_physical_qty,
    p_reserved_qty,
    p_allocated_qty,
    p_bootstrap_id,
    p_aggregate_version,
    p_physical_qty,
    p_reserved_qty,
    p_allocated_qty,
    p_snapshot_source,
    p_snapshot_reference,
    p_snapshot_checksum,
    p_snapshot_recorded_at,
    p_bootstrapped_by,
    p_bootstrap_reason,
    v_now,
    v_now
  )
  ON CONFLICT (consumer_name, position_id) DO NOTHING;

  IF FOUND THEN
    RETURN QUERY SELECT
      'bootstrapped'::text,
      p_aggregate_version,
      p_aggregate_version;
    RETURN;
  END IF;

  SELECT *
  INTO v_existing
  FROM inventory_position_quantity_projection
  WHERE consumer_name = p_consumer_name
    AND position_id = p_position_id
  FOR UPDATE;

  IF v_existing.bootstrap_id = p_bootstrap_id
     AND v_existing.bootstrap_version = p_aggregate_version
     AND v_existing.bootstrap_physical_qty = p_physical_qty
     AND v_existing.bootstrap_reserved_qty = p_reserved_qty
     AND v_existing.bootstrap_allocated_qty = p_allocated_qty
     AND v_existing.bootstrap_source = p_snapshot_source
     AND v_existing.bootstrap_reference = p_snapshot_reference
     AND v_existing.bootstrap_checksum = p_snapshot_checksum
     AND v_existing.bootstrap_recorded_at = p_snapshot_recorded_at
     AND v_existing.bootstrapped_by = p_bootstrapped_by
     AND v_existing.bootstrap_reason = p_bootstrap_reason THEN
    RETURN QUERY SELECT
      'duplicate'::text,
      v_existing.bootstrap_version,
      v_existing.aggregate_version;
    RETURN;
  END IF;

  RAISE EXCEPTION 'projection cursor already initialized by another event or bootstrap payload'
    USING ERRCODE = '23505';
END;
$$;

CREATE OR REPLACE FUNCTION apply_inventory_position_quantity_event(
  p_consumer_name text,
  p_event_id uuid,
  p_position_id uuid,
  p_aggregate_version bigint,
  p_recorded_at timestamptz,
  p_data jsonb
) RETURNS TABLE (
  outcome text,
  applied_count integer,
  projection_version bigint,
  expected_version bigint
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_projection_version bigint;
  v_last_event_id uuid;
  v_bootstrap_id uuid;
  v_current_event_id uuid := p_event_id;
  v_current_version bigint := p_aggregate_version;
  v_current_recorded_at timestamptz := p_recorded_at;
  v_current_data jsonb := p_data;
  v_existing_pending inventory_position_projection_pending%ROWTYPE;
  v_applied_count integer := 0;
  v_now timestamptz;
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_event_id IS NULL OR p_position_id IS NULL THEN
    RAISE EXCEPTION 'event and position identity are required'
      USING ERRCODE = '23514';
  END IF;
  IF p_aggregate_version IS NULL OR p_aggregate_version < 1 THEN
    RAISE EXCEPTION 'aggregate version must be positive'
      USING ERRCODE = '23514';
  END IF;
  IF p_recorded_at IS NULL THEN
    RAISE EXCEPTION 'recorded timestamp is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_data IS NULL OR jsonb_typeof(p_data) IS DISTINCT FROM 'object'
     OR jsonb_typeof(p_data -> 'physical_delta') IS DISTINCT FROM 'number'
     OR jsonb_typeof(p_data -> 'reserved_delta') IS DISTINCT FROM 'number'
     OR jsonb_typeof(p_data -> 'allocated_delta') IS DISTINCT FROM 'number' THEN
    RAISE EXCEPTION 'position projection requires numeric quantity deltas'
      USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1
    FROM domain_event_inbox
    WHERE consumer_name = p_consumer_name
      AND event_id = p_event_id
  ) THEN
    RAISE EXCEPTION 'projection event requires an Inbox receipt in the same transaction'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_position_quantity_projection (consumer_name, position_id)
  VALUES (p_consumer_name, p_position_id)
  ON CONFLICT (consumer_name, position_id) DO NOTHING;

  SELECT aggregate_version, last_event_id, bootstrap_id
  INTO v_projection_version, v_last_event_id, v_bootstrap_id
  FROM inventory_position_quantity_projection
  WHERE consumer_name = p_consumer_name
    AND position_id = p_position_id
  FOR UPDATE;

  IF p_aggregate_version = v_projection_version
     AND v_last_event_id IS NOT NULL THEN
    IF p_event_id <> v_last_event_id THEN
      RAISE EXCEPTION 'current projection version belongs to another event'
        USING ERRCODE = '23505';
    END IF;

    RETURN QUERY SELECT
      'duplicate'::text,
      0,
      v_projection_version,
      v_projection_version + 1;
    RETURN;
  END IF;

  IF p_aggregate_version < v_projection_version
     OR (
       p_aggregate_version = v_projection_version
       AND v_bootstrap_id IS NOT NULL
     ) THEN
    v_now := clock_timestamp();
    UPDATE domain_event_inbox
    SET metadata = metadata || jsonb_build_object(
      'projection', jsonb_build_object(
        'name', 'inventory-position-quantity',
        'status', 'stale',
        'position_id', p_position_id,
        'aggregate_version', p_aggregate_version,
        'projection_version', v_projection_version,
        'projection_cursor_source', CASE
          WHEN v_last_event_id IS NULL THEN 'snapshot'
          ELSE 'event'
        END,
        'recorded_at', p_recorded_at,
        'decided_at', v_now
      )
    )
    WHERE consumer_name = p_consumer_name
      AND event_id = p_event_id;

    RETURN QUERY SELECT
      'stale'::text,
      0,
      v_projection_version,
      v_projection_version + 1;
    RETURN;
  END IF;

  IF p_aggregate_version > v_projection_version + 1 THEN
    INSERT INTO inventory_position_projection_pending (
      consumer_name,
      position_id,
      aggregate_version,
      event_id,
      recorded_at,
      data
    ) VALUES (
      p_consumer_name,
      p_position_id,
      p_aggregate_version,
      p_event_id,
      p_recorded_at,
      p_data
    )
    ON CONFLICT (consumer_name, position_id, aggregate_version) DO NOTHING;

    IF NOT FOUND THEN
      SELECT *
      INTO v_existing_pending
      FROM inventory_position_projection_pending
      WHERE consumer_name = p_consumer_name
        AND position_id = p_position_id
        AND aggregate_version = p_aggregate_version;

      IF v_existing_pending.event_id <> p_event_id
         OR v_existing_pending.recorded_at <> p_recorded_at
         OR v_existing_pending.data <> p_data THEN
        RAISE EXCEPTION 'projection version already belongs to another event'
          USING ERRCODE = '23505';
      END IF;
    END IF;

    v_now := clock_timestamp();
    UPDATE domain_event_inbox
    SET metadata = metadata || jsonb_build_object(
      'projection', jsonb_build_object(
        'name', 'inventory-position-quantity',
        'status', 'buffered',
        'position_id', p_position_id,
        'aggregate_version', p_aggregate_version,
        'projection_version', v_projection_version,
        'expected_version', v_projection_version + 1,
        'recorded_at', p_recorded_at,
        'buffered_at', v_now
      )
    )
    WHERE consumer_name = p_consumer_name
      AND event_id = p_event_id;

    RETURN QUERY SELECT
      'buffered'::text,
      0,
      v_projection_version,
      v_projection_version + 1;
    RETURN;
  END IF;

  LOOP
    v_now := clock_timestamp();
    UPDATE inventory_position_quantity_projection
    SET physical_qty = physical_qty + (v_current_data ->> 'physical_delta')::numeric,
        reserved_qty = reserved_qty + (v_current_data ->> 'reserved_delta')::numeric,
        allocated_qty = allocated_qty + (v_current_data ->> 'allocated_delta')::numeric,
        aggregate_version = v_current_version,
        last_event_id = v_current_event_id,
        last_recorded_at = v_current_recorded_at,
        updated_at = v_now
    WHERE consumer_name = p_consumer_name
      AND position_id = p_position_id;

    v_projection_version := v_current_version;
    v_applied_count := v_applied_count + 1;

    UPDATE domain_event_inbox
    SET metadata = metadata || jsonb_build_object(
      'projection', COALESCE(metadata -> 'projection', '{}'::jsonb)
        || jsonb_strip_nulls(jsonb_build_object(
          'name', 'inventory-position-quantity',
          'status', 'applied',
          'position_id', p_position_id,
          'aggregate_version', v_current_version,
          'projection_version', v_projection_version,
          'recorded_at', v_current_recorded_at,
          'applied_at', v_now,
          'drained_by_event_id', CASE
            WHEN v_current_event_id <> p_event_id THEN p_event_id
            ELSE NULL
          END
        ))
    )
    WHERE consumer_name = p_consumer_name
      AND event_id = v_current_event_id;

    DELETE FROM inventory_position_projection_pending
    WHERE consumer_name = p_consumer_name
      AND position_id = p_position_id
      AND aggregate_version = v_projection_version + 1
    RETURNING event_id, aggregate_version, recorded_at, data
    INTO v_current_event_id, v_current_version, v_current_recorded_at, v_current_data;

    EXIT WHEN NOT FOUND;
  END LOOP;

  RETURN QUERY SELECT
    'applied'::text,
    v_applied_count,
    v_projection_version,
    v_projection_version + 1;
END;
$$;

CREATE FUNCTION read_inventory_position_projection_gap_telemetry(
  p_observed_at timestamptz DEFAULT statement_timestamp(),
  p_consumer_name text DEFAULT NULL
) RETURNS TABLE (
  observed_at timestamptz,
  consumer_name text,
  gap_count bigint,
  pending_event_count bigint,
  max_pending_per_gap bigint,
  oldest_gap_age_seconds numeric
)
LANGUAGE sql
STABLE
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
  WITH observation AS (
    SELECT COALESCE(p_observed_at, statement_timestamp()) AS observed_at
  ), filtered AS (
    SELECT g.pending_count, g.oldest_buffered_at
    FROM inventory_position_projection_gaps AS g
    WHERE p_consumer_name IS NULL
       OR g.consumer_name = p_consumer_name
  ), aggregate_snapshot AS (
    SELECT
      count(*) AS gap_count,
      COALESCE(sum(pending_count), 0)::bigint AS pending_event_count,
      COALESCE(max(pending_count), 0) AS max_pending_per_gap,
      min(oldest_buffered_at) AS oldest_buffered_at
    FROM filtered
  )
  SELECT
    t.observed_at,
    p_consumer_name,
    a.gap_count,
    a.pending_event_count,
    a.max_pending_per_gap,
    CASE
      WHEN a.oldest_buffered_at IS NULL THEN NULL
      ELSE greatest(0, extract(epoch FROM t.observed_at - a.oldest_buffered_at))
    END
  FROM aggregate_snapshot a
  CROSS JOIN observation t;
$$;

COMMIT;
