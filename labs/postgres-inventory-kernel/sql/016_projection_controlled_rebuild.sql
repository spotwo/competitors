BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE inventory_position_projection_rebuilds (
  rebuild_id uuid PRIMARY KEY,
  consumer_name text NOT NULL CHECK (
    consumer_name = btrim(consumer_name) AND consumer_name <> ''
  ),
  position_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('prepared', 'completed', 'cancelled')),
  pending_disposition text NOT NULL CHECK (
    pending_disposition IN (
      'require-empty',
      'supersede-covered-retain-future'
    )
  ),
  expected_projection_version bigint NOT NULL CHECK (
    expected_projection_version >= 0
  ),
  before_projection jsonb NOT NULL CHECK (
    jsonb_typeof(before_projection) = 'object'
  ),
  before_pending_count bigint NOT NULL CHECK (before_pending_count >= 0),
  snapshot_version bigint NOT NULL CHECK (snapshot_version >= 1),
  snapshot_physical_qty numeric(24, 6) NOT NULL CHECK (
    snapshot_physical_qty >= 0
  ),
  snapshot_reserved_qty numeric(24, 6) NOT NULL CHECK (
    snapshot_reserved_qty >= 0
    AND snapshot_reserved_qty <= snapshot_physical_qty
  ),
  snapshot_allocated_qty numeric(24, 6) NOT NULL CHECK (
    snapshot_allocated_qty >= 0
    AND snapshot_allocated_qty <= snapshot_physical_qty
  ),
  snapshot_source text NOT NULL CHECK (
    snapshot_source = btrim(snapshot_source) AND snapshot_source <> ''
  ),
  snapshot_reference text NOT NULL CHECK (
    snapshot_reference = btrim(snapshot_reference) AND snapshot_reference <> ''
  ),
  snapshot_checksum text NOT NULL CHECK (
    snapshot_checksum ~ '^sha256:[0-9a-f]{64}$'
  ),
  snapshot_recorded_at timestamptz NOT NULL,
  prepared_by text NOT NULL CHECK (
    prepared_by = btrim(prepared_by) AND prepared_by <> ''
  ),
  reason text NOT NULL CHECK (reason = btrim(reason) AND reason <> ''),
  prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  executed_by text,
  executed_at timestamptz,
  superseded_pending_count bigint,
  drained_pending_count bigint,
  remaining_pending_count bigint,
  final_projection jsonb,
  cancelled_by text,
  cancel_reason text,
  cancelled_at timestamptz,
  CONSTRAINT inventory_position_projection_rebuild_target_uq
    UNIQUE (consumer_name, position_id, rebuild_id),
  CONSTRAINT inventory_position_projection_rebuild_status_ck CHECK (
    (
      status = 'prepared'
      AND executed_by IS NULL
      AND executed_at IS NULL
      AND superseded_pending_count IS NULL
      AND drained_pending_count IS NULL
      AND remaining_pending_count IS NULL
      AND final_projection IS NULL
      AND cancelled_by IS NULL
      AND cancel_reason IS NULL
      AND cancelled_at IS NULL
    )
    OR
    (
      status = 'completed'
      AND executed_by IS NOT NULL
      AND executed_by = btrim(executed_by)
      AND executed_by <> ''
      AND executed_at IS NOT NULL
      AND superseded_pending_count IS NOT NULL
      AND superseded_pending_count >= 0
      AND drained_pending_count IS NOT NULL
      AND drained_pending_count >= 0
      AND remaining_pending_count IS NOT NULL
      AND remaining_pending_count >= 0
      AND final_projection IS NOT NULL
      AND jsonb_typeof(final_projection) = 'object'
      AND cancelled_by IS NULL
      AND cancel_reason IS NULL
      AND cancelled_at IS NULL
    )
    OR
    (
      status = 'cancelled'
      AND executed_by IS NULL
      AND executed_at IS NULL
      AND superseded_pending_count IS NULL
      AND drained_pending_count IS NULL
      AND remaining_pending_count IS NULL
      AND final_projection IS NULL
      AND cancelled_by IS NOT NULL
      AND cancelled_by = btrim(cancelled_by)
      AND cancelled_by <> ''
      AND cancel_reason IS NOT NULL
      AND cancel_reason = btrim(cancel_reason)
      AND cancel_reason <> ''
      AND cancelled_at IS NOT NULL
    )
  )
);

CREATE UNIQUE INDEX inventory_position_projection_rebuild_active_uq
  ON inventory_position_projection_rebuilds (consumer_name, position_id)
  WHERE status = 'prepared';

CREATE INDEX inventory_position_projection_rebuild_prepared_at_idx
  ON inventory_position_projection_rebuilds (prepared_at, rebuild_id);

CREATE TABLE inventory_position_projection_control (
  consumer_name text NOT NULL CHECK (
    consumer_name = btrim(consumer_name) AND consumer_name <> ''
  ),
  position_id uuid NOT NULL,
  active_rebuild_id uuid,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer_name, position_id),
  CONSTRAINT inventory_position_projection_control_rebuild_fk
    FOREIGN KEY (consumer_name, position_id, active_rebuild_id)
    REFERENCES inventory_position_projection_rebuilds (
      consumer_name, position_id, rebuild_id
    )
);

CREATE FUNCTION prepare_inventory_position_quantity_projection_rebuild(
  p_rebuild_id uuid,
  p_consumer_name text,
  p_position_id uuid,
  p_expected_projection_version bigint,
  p_expected_pending_count bigint,
  p_pending_disposition text,
  p_snapshot_version bigint,
  p_snapshot_physical_qty numeric,
  p_snapshot_reserved_qty numeric,
  p_snapshot_allocated_qty numeric,
  p_snapshot_source text,
  p_snapshot_reference text,
  p_snapshot_checksum text,
  p_snapshot_recorded_at timestamptz,
  p_prepared_by text,
  p_reason text
) RETURNS TABLE (
  outcome text,
  rebuild_status text,
  projection_version bigint,
  pending_event_count bigint
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing inventory_position_projection_rebuilds%ROWTYPE;
  v_projection inventory_position_quantity_projection%ROWTYPE;
  v_active_rebuild_id uuid;
  v_pending_count bigint;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_rebuild_id IS NULL OR p_position_id IS NULL THEN
    RAISE EXCEPTION 'rebuild and position identity are required'
      USING ERRCODE = '23514';
  END IF;
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_expected_projection_version IS NULL OR p_expected_projection_version < 0
     OR p_expected_pending_count IS NULL OR p_expected_pending_count < 0 THEN
    RAISE EXCEPTION 'expected projection version and pending count must be nonnegative'
      USING ERRCODE = '23514';
  END IF;
  IF p_pending_disposition NOT IN (
    'require-empty',
    'supersede-covered-retain-future'
  ) THEN
    RAISE EXCEPTION 'unsupported pending event disposition'
      USING ERRCODE = '23514';
  END IF;
  IF p_snapshot_version IS NULL OR p_snapshot_version < 1 THEN
    RAISE EXCEPTION 'rebuild snapshot version must be positive'
      USING ERRCODE = '23514';
  END IF;
  IF p_snapshot_physical_qty IS NULL
     OR p_snapshot_reserved_qty IS NULL
     OR p_snapshot_allocated_qty IS NULL THEN
    RAISE EXCEPTION 'rebuild snapshot quantities are required'
      USING ERRCODE = '23514';
  END IF;
  IF p_snapshot_physical_qty < 0
     OR p_snapshot_reserved_qty < 0
     OR p_snapshot_allocated_qty < 0
     OR p_snapshot_reserved_qty > p_snapshot_physical_qty
     OR p_snapshot_allocated_qty > p_snapshot_physical_qty THEN
    RAISE EXCEPTION 'rebuild snapshot quantities violate projection invariants'
      USING ERRCODE = '23514';
  END IF;
  IF scale(p_snapshot_physical_qty) > 6
     OR scale(p_snapshot_reserved_qty) > 6
     OR scale(p_snapshot_allocated_qty) > 6 THEN
    RAISE EXCEPTION 'rebuild snapshot quantities support at most six fractional digits'
      USING ERRCODE = '23514';
  END IF;
  IF abs(p_snapshot_physical_qty) >= 1000000000000000000
     OR abs(p_snapshot_reserved_qty) >= 1000000000000000000
     OR abs(p_snapshot_allocated_qty) >= 1000000000000000000 THEN
    RAISE EXCEPTION 'rebuild snapshot quantities exceed projection precision'
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
  IF p_prepared_by IS NULL OR btrim(p_prepared_by) = ''
     OR p_prepared_by <> btrim(p_prepared_by)
     OR p_reason IS NULL OR btrim(p_reason) = ''
     OR p_reason <> btrim(p_reason) THEN
    RAISE EXCEPTION 'rebuild operator and reason are required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;

  SELECT r.*
  INTO v_existing
  FROM inventory_position_projection_rebuilds AS r
  WHERE r.rebuild_id = p_rebuild_id
  FOR UPDATE;

  IF FOUND THEN
    IF v_existing.consumer_name IS DISTINCT FROM p_consumer_name
       OR v_existing.position_id IS DISTINCT FROM p_position_id
       OR v_existing.expected_projection_version IS DISTINCT FROM p_expected_projection_version
       OR v_existing.before_pending_count IS DISTINCT FROM p_expected_pending_count
       OR v_existing.pending_disposition IS DISTINCT FROM p_pending_disposition
       OR v_existing.snapshot_version IS DISTINCT FROM p_snapshot_version
       OR v_existing.snapshot_physical_qty IS DISTINCT FROM p_snapshot_physical_qty
       OR v_existing.snapshot_reserved_qty IS DISTINCT FROM p_snapshot_reserved_qty
       OR v_existing.snapshot_allocated_qty IS DISTINCT FROM p_snapshot_allocated_qty
       OR v_existing.snapshot_source IS DISTINCT FROM p_snapshot_source
       OR v_existing.snapshot_reference IS DISTINCT FROM p_snapshot_reference
       OR v_existing.snapshot_checksum IS DISTINCT FROM p_snapshot_checksum
       OR v_existing.snapshot_recorded_at IS DISTINCT FROM p_snapshot_recorded_at
       OR v_existing.prepared_by IS DISTINCT FROM p_prepared_by
       OR v_existing.reason IS DISTINCT FROM p_reason THEN
      RAISE EXCEPTION 'rebuild identity already belongs to another payload'
        USING ERRCODE = '23505';
    END IF;

    IF v_existing.status = 'prepared' THEN
      SELECT c.active_rebuild_id
      INTO v_active_rebuild_id
      FROM inventory_position_projection_control AS c
      WHERE c.consumer_name = p_consumer_name
        AND c.position_id = p_position_id
      FOR UPDATE;

      IF v_active_rebuild_id IS DISTINCT FROM p_rebuild_id THEN
        RAISE EXCEPTION 'prepared rebuild fence is inconsistent'
          USING ERRCODE = '55000';
      END IF;
    END IF;

    RETURN QUERY SELECT
      'duplicate'::text,
      v_existing.status,
      COALESCE(
        (v_existing.final_projection ->> 'aggregate_version')::bigint,
        v_existing.expected_projection_version
      ),
      CASE
        WHEN v_existing.status = 'completed'
          THEN v_existing.remaining_pending_count
        ELSE v_existing.before_pending_count
      END;
    RETURN;
  END IF;

  INSERT INTO inventory_position_projection_control (
    consumer_name,
    position_id
  ) VALUES (
    p_consumer_name,
    p_position_id
  )
  ON CONFLICT (consumer_name, position_id) DO NOTHING;

  SELECT c.active_rebuild_id
  INTO v_active_rebuild_id
  FROM inventory_position_projection_control AS c
  WHERE c.consumer_name = p_consumer_name
    AND c.position_id = p_position_id
  FOR UPDATE;

  IF v_active_rebuild_id IS NOT NULL THEN
    RAISE EXCEPTION 'another projection rebuild fence is active'
      USING ERRCODE = '55000';
  END IF;

  SELECT p.*
  INTO v_projection
  FROM inventory_position_quantity_projection AS p
  WHERE p.consumer_name = p_consumer_name
    AND p.position_id = p_position_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'projection cursor does not exist; use non-destructive bootstrap'
      USING ERRCODE = '55000';
  END IF;

  SELECT count(*)
  INTO v_pending_count
  FROM inventory_position_projection_pending AS p
  WHERE p.consumer_name = p_consumer_name
    AND p.position_id = p_position_id;

  IF v_projection.aggregate_version <> p_expected_projection_version
     OR v_pending_count <> p_expected_pending_count THEN
    RAISE EXCEPTION 'projection changed since operator inspection'
      USING ERRCODE = '40001';
  END IF;
  IF p_snapshot_version < v_projection.aggregate_version THEN
    RAISE EXCEPTION 'rebuild snapshot cannot move the projection cursor backward'
      USING ERRCODE = '23514';
  END IF;
  IF p_pending_disposition = 'require-empty' AND v_pending_count <> 0 THEN
    RAISE EXCEPTION 'require-empty disposition rejects pending events'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_position_projection_rebuilds (
    rebuild_id,
    consumer_name,
    position_id,
    status,
    pending_disposition,
    expected_projection_version,
    before_projection,
    before_pending_count,
    snapshot_version,
    snapshot_physical_qty,
    snapshot_reserved_qty,
    snapshot_allocated_qty,
    snapshot_source,
    snapshot_reference,
    snapshot_checksum,
    snapshot_recorded_at,
    prepared_by,
    reason,
    prepared_at
  ) VALUES (
    p_rebuild_id,
    p_consumer_name,
    p_position_id,
    'prepared',
    p_pending_disposition,
    p_expected_projection_version,
    to_jsonb(v_projection),
    v_pending_count,
    p_snapshot_version,
    p_snapshot_physical_qty,
    p_snapshot_reserved_qty,
    p_snapshot_allocated_qty,
    p_snapshot_source,
    p_snapshot_reference,
    p_snapshot_checksum,
    p_snapshot_recorded_at,
    p_prepared_by,
    p_reason,
    v_now
  );

  UPDATE inventory_position_projection_control
  SET active_rebuild_id = p_rebuild_id,
      updated_at = v_now
  WHERE consumer_name = p_consumer_name
    AND position_id = p_position_id;

  RETURN QUERY SELECT
    'prepared'::text,
    'prepared'::text,
    v_projection.aggregate_version,
    v_pending_count;
END;
$$;

CREATE FUNCTION execute_inventory_position_quantity_projection_rebuild(
  p_rebuild_id uuid,
  p_executed_by text
) RETURNS TABLE (
  outcome text,
  rebuild_status text,
  projection_version bigint,
  superseded_pending_count bigint,
  drained_pending_count bigint,
  remaining_pending_count bigint
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_rebuild inventory_position_projection_rebuilds%ROWTYPE;
  v_projection inventory_position_quantity_projection%ROWTYPE;
  v_active_rebuild_id uuid;
  v_current_event_id uuid;
  v_current_version bigint;
  v_current_recorded_at timestamptz;
  v_current_data jsonb;
  v_projection_version bigint;
  v_superseded_count bigint := 0;
  v_drained_count bigint := 0;
  v_remaining_count bigint := 0;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_rebuild_id IS NULL THEN
    RAISE EXCEPTION 'rebuild identity is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_executed_by IS NULL OR btrim(p_executed_by) = ''
     OR p_executed_by <> btrim(p_executed_by) THEN
    RAISE EXCEPTION 'rebuild executor is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;

  SELECT r.*
  INTO v_rebuild
  FROM inventory_position_projection_rebuilds AS r
  WHERE r.rebuild_id = p_rebuild_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'prepared projection rebuild does not exist'
      USING ERRCODE = '55000';
  END IF;

  IF v_rebuild.status = 'completed' THEN
    IF v_rebuild.executed_by IS DISTINCT FROM p_executed_by THEN
      RAISE EXCEPTION 'completed rebuild identity belongs to another executor payload'
        USING ERRCODE = '23505';
    END IF;

    RETURN QUERY SELECT
      'duplicate'::text,
      v_rebuild.status,
      (v_rebuild.final_projection ->> 'aggregate_version')::bigint,
      v_rebuild.superseded_pending_count,
      v_rebuild.drained_pending_count,
      v_rebuild.remaining_pending_count;
    RETURN;
  END IF;

  IF v_rebuild.status <> 'prepared' THEN
    RAISE EXCEPTION 'cancelled projection rebuild cannot execute'
      USING ERRCODE = '55000';
  END IF;

  SELECT c.active_rebuild_id
  INTO v_active_rebuild_id
  FROM inventory_position_projection_control AS c
  WHERE c.consumer_name = v_rebuild.consumer_name
    AND c.position_id = v_rebuild.position_id
  FOR UPDATE;

  IF v_active_rebuild_id IS DISTINCT FROM p_rebuild_id THEN
    RAISE EXCEPTION 'projection rebuild fence is not active for this operation'
      USING ERRCODE = '55000';
  END IF;

  SELECT p.*
  INTO v_projection
  FROM inventory_position_quantity_projection AS p
  WHERE p.consumer_name = v_rebuild.consumer_name
    AND p.position_id = v_rebuild.position_id
  FOR UPDATE;

  IF NOT FOUND OR to_jsonb(v_projection) IS DISTINCT FROM v_rebuild.before_projection THEN
    RAISE EXCEPTION 'fenced projection state differs from prepared evidence'
      USING ERRCODE = '40001';
  END IF;

  SELECT count(*)
  INTO v_remaining_count
  FROM inventory_position_projection_pending AS p
  WHERE p.consumer_name = v_rebuild.consumer_name
    AND p.position_id = v_rebuild.position_id;

  IF v_remaining_count <> v_rebuild.before_pending_count THEN
    RAISE EXCEPTION 'fenced pending state differs from prepared evidence'
      USING ERRCODE = '40001';
  END IF;
  IF v_rebuild.pending_disposition = 'require-empty'
     AND v_remaining_count <> 0 THEN
    RAISE EXCEPTION 'require-empty disposition rejects pending events'
      USING ERRCODE = '23514';
  END IF;

  IF v_rebuild.pending_disposition = 'supersede-covered-retain-future' THEN
    SELECT count(*)
    INTO v_superseded_count
    FROM inventory_position_projection_pending AS p
    WHERE p.consumer_name = v_rebuild.consumer_name
      AND p.position_id = v_rebuild.position_id
      AND p.aggregate_version <= v_rebuild.snapshot_version;

    UPDATE domain_event_inbox AS i
    SET metadata = i.metadata || jsonb_build_object(
      'projection', COALESCE(i.metadata -> 'projection', '{}'::jsonb)
        || jsonb_build_object(
          'name', 'inventory-position-quantity',
          'status', 'superseded',
          'superseded_by_rebuild_id', p_rebuild_id,
          'snapshot_version', v_rebuild.snapshot_version,
          'superseded_at', v_now
        )
    )
    FROM inventory_position_projection_pending AS p
    WHERE p.consumer_name = v_rebuild.consumer_name
      AND p.position_id = v_rebuild.position_id
      AND p.aggregate_version <= v_rebuild.snapshot_version
      AND i.consumer_name = p.consumer_name
      AND i.event_id = p.event_id;

    DELETE FROM inventory_position_projection_pending AS p
    WHERE p.consumer_name = v_rebuild.consumer_name
      AND p.position_id = v_rebuild.position_id
      AND p.aggregate_version <= v_rebuild.snapshot_version;
  END IF;

  UPDATE inventory_position_quantity_projection
  SET aggregate_version = v_rebuild.snapshot_version,
      physical_qty = v_rebuild.snapshot_physical_qty,
      reserved_qty = v_rebuild.snapshot_reserved_qty,
      allocated_qty = v_rebuild.snapshot_allocated_qty,
      last_event_id = NULL,
      last_recorded_at = NULL,
      bootstrap_id = v_rebuild.rebuild_id,
      bootstrap_version = v_rebuild.snapshot_version,
      bootstrap_physical_qty = v_rebuild.snapshot_physical_qty,
      bootstrap_reserved_qty = v_rebuild.snapshot_reserved_qty,
      bootstrap_allocated_qty = v_rebuild.snapshot_allocated_qty,
      bootstrap_source = v_rebuild.snapshot_source,
      bootstrap_reference = v_rebuild.snapshot_reference,
      bootstrap_checksum = v_rebuild.snapshot_checksum,
      bootstrap_recorded_at = v_rebuild.snapshot_recorded_at,
      bootstrapped_by = p_executed_by,
      bootstrap_reason = v_rebuild.reason,
      bootstrapped_at = v_now,
      updated_at = v_now
  WHERE consumer_name = v_rebuild.consumer_name
    AND position_id = v_rebuild.position_id;

  v_projection_version := v_rebuild.snapshot_version;

  LOOP
    DELETE FROM inventory_position_projection_pending AS p
    WHERE p.consumer_name = v_rebuild.consumer_name
      AND p.position_id = v_rebuild.position_id
      AND p.aggregate_version = v_projection_version + 1
    RETURNING p.event_id, p.aggregate_version, p.recorded_at, p.data
    INTO v_current_event_id, v_current_version, v_current_recorded_at, v_current_data;

    EXIT WHEN NOT FOUND;

    v_now := clock_timestamp();
    UPDATE inventory_position_quantity_projection
    SET physical_qty = physical_qty + (v_current_data ->> 'physical_delta')::numeric,
        reserved_qty = reserved_qty + (v_current_data ->> 'reserved_delta')::numeric,
        allocated_qty = allocated_qty + (v_current_data ->> 'allocated_delta')::numeric,
        aggregate_version = v_current_version,
        last_event_id = v_current_event_id,
        last_recorded_at = v_current_recorded_at,
        updated_at = v_now
    WHERE consumer_name = v_rebuild.consumer_name
      AND position_id = v_rebuild.position_id;

    v_projection_version := v_current_version;
    v_drained_count := v_drained_count + 1;

    UPDATE domain_event_inbox AS i
    SET metadata = i.metadata || jsonb_build_object(
      'projection', COALESCE(i.metadata -> 'projection', '{}'::jsonb)
        || jsonb_build_object(
          'name', 'inventory-position-quantity',
          'status', 'applied',
          'position_id', v_rebuild.position_id,
          'aggregate_version', v_current_version,
          'projection_version', v_projection_version,
          'recorded_at', v_current_recorded_at,
          'applied_at', v_now,
          'drained_by_rebuild_id', p_rebuild_id
        )
    )
    WHERE i.consumer_name = v_rebuild.consumer_name
      AND i.event_id = v_current_event_id;
  END LOOP;

  SELECT count(*)
  INTO v_remaining_count
  FROM inventory_position_projection_pending AS p
  WHERE p.consumer_name = v_rebuild.consumer_name
    AND p.position_id = v_rebuild.position_id;

  SELECT p.*
  INTO v_projection
  FROM inventory_position_quantity_projection AS p
  WHERE p.consumer_name = v_rebuild.consumer_name
    AND p.position_id = v_rebuild.position_id;

  UPDATE inventory_position_projection_control
  SET active_rebuild_id = NULL,
      updated_at = clock_timestamp()
  WHERE consumer_name = v_rebuild.consumer_name
    AND position_id = v_rebuild.position_id
    AND active_rebuild_id = p_rebuild_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'projection rebuild fence release failed'
      USING ERRCODE = '55000';
  END IF;

  UPDATE inventory_position_projection_rebuilds
  SET status = 'completed',
      executed_by = p_executed_by,
      executed_at = clock_timestamp(),
      superseded_pending_count = v_superseded_count,
      drained_pending_count = v_drained_count,
      remaining_pending_count = v_remaining_count,
      final_projection = to_jsonb(v_projection)
  WHERE rebuild_id = p_rebuild_id;

  RETURN QUERY SELECT
    'completed'::text,
    'completed'::text,
    v_projection.aggregate_version,
    v_superseded_count,
    v_drained_count,
    v_remaining_count;
END;
$$;

CREATE FUNCTION cancel_inventory_position_quantity_projection_rebuild(
  p_rebuild_id uuid,
  p_cancelled_by text,
  p_cancel_reason text
) RETURNS TABLE (
  outcome text,
  rebuild_status text
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_rebuild inventory_position_projection_rebuilds%ROWTYPE;
  v_active_rebuild_id uuid;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_rebuild_id IS NULL THEN
    RAISE EXCEPTION 'rebuild identity is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_cancelled_by IS NULL OR btrim(p_cancelled_by) = ''
     OR p_cancelled_by <> btrim(p_cancelled_by)
     OR p_cancel_reason IS NULL OR btrim(p_cancel_reason) = ''
     OR p_cancel_reason <> btrim(p_cancel_reason) THEN
    RAISE EXCEPTION 'cancellation operator and reason are required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;

  SELECT r.*
  INTO v_rebuild
  FROM inventory_position_projection_rebuilds AS r
  WHERE r.rebuild_id = p_rebuild_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'prepared projection rebuild does not exist'
      USING ERRCODE = '55000';
  END IF;

  IF v_rebuild.status = 'cancelled' THEN
    IF v_rebuild.cancelled_by IS DISTINCT FROM p_cancelled_by
       OR v_rebuild.cancel_reason IS DISTINCT FROM p_cancel_reason THEN
      RAISE EXCEPTION 'cancelled rebuild identity belongs to another payload'
        USING ERRCODE = '23505';
    END IF;

    RETURN QUERY SELECT 'duplicate'::text, 'cancelled'::text;
    RETURN;
  END IF;

  IF v_rebuild.status <> 'prepared' THEN
    RAISE EXCEPTION 'completed projection rebuild cannot be cancelled'
      USING ERRCODE = '55000';
  END IF;

  SELECT c.active_rebuild_id
  INTO v_active_rebuild_id
  FROM inventory_position_projection_control AS c
  WHERE c.consumer_name = v_rebuild.consumer_name
    AND c.position_id = v_rebuild.position_id
  FOR UPDATE;

  IF v_active_rebuild_id IS DISTINCT FROM p_rebuild_id THEN
    RAISE EXCEPTION 'projection rebuild fence is not active for this operation'
      USING ERRCODE = '55000';
  END IF;

  UPDATE inventory_position_projection_control
  SET active_rebuild_id = NULL,
      updated_at = v_now
  WHERE consumer_name = v_rebuild.consumer_name
    AND position_id = v_rebuild.position_id;

  UPDATE inventory_position_projection_rebuilds
  SET status = 'cancelled',
      cancelled_by = p_cancelled_by,
      cancel_reason = p_cancel_reason,
      cancelled_at = v_now
  WHERE rebuild_id = p_rebuild_id;

  RETURN QUERY SELECT 'cancelled'::text, 'cancelled'::text;
END;
$$;

CREATE OR REPLACE FUNCTION bootstrap_inventory_position_quantity_projection(
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
  v_active_rebuild_id uuid;
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

  INSERT INTO inventory_position_projection_control (
    consumer_name,
    position_id
  ) VALUES (
    p_consumer_name,
    p_position_id
  )
  ON CONFLICT (consumer_name, position_id) DO NOTHING;

  SELECT c.active_rebuild_id
  INTO v_active_rebuild_id
  FROM inventory_position_projection_control AS c
  WHERE c.consumer_name = p_consumer_name
    AND c.position_id = p_position_id
  FOR UPDATE;

  IF v_active_rebuild_id IS NOT NULL THEN
    RAISE EXCEPTION 'projection rebuild fence is active'
      USING ERRCODE = '55000';
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

  SELECT p.*
  INTO v_existing
  FROM inventory_position_quantity_projection AS p
  WHERE p.consumer_name = p_consumer_name
    AND p.position_id = p_position_id
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
  v_active_rebuild_id uuid;
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
    FROM domain_event_inbox AS i
    WHERE i.consumer_name = p_consumer_name
      AND i.event_id = p_event_id
  ) THEN
    RAISE EXCEPTION 'projection event requires an Inbox receipt in the same transaction'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_position_projection_control (
    consumer_name,
    position_id
  ) VALUES (
    p_consumer_name,
    p_position_id
  )
  ON CONFLICT (consumer_name, position_id) DO NOTHING;

  SELECT c.active_rebuild_id
  INTO v_active_rebuild_id
  FROM inventory_position_projection_control AS c
  WHERE c.consumer_name = p_consumer_name
    AND c.position_id = p_position_id
  FOR UPDATE;

  IF v_active_rebuild_id IS NOT NULL THEN
    RAISE EXCEPTION 'projection rebuild fence is active'
      USING ERRCODE = '55000';
  END IF;

  INSERT INTO inventory_position_quantity_projection (consumer_name, position_id)
  VALUES (p_consumer_name, p_position_id)
  ON CONFLICT (consumer_name, position_id) DO NOTHING;

  SELECT p.aggregate_version, p.last_event_id, p.bootstrap_id
  INTO v_projection_version, v_last_event_id, v_bootstrap_id
  FROM inventory_position_quantity_projection AS p
  WHERE p.consumer_name = p_consumer_name
    AND p.position_id = p_position_id
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
    UPDATE domain_event_inbox AS i
    SET metadata = i.metadata || jsonb_build_object(
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
    WHERE i.consumer_name = p_consumer_name
      AND i.event_id = p_event_id;

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
      SELECT p.*
      INTO v_existing_pending
      FROM inventory_position_projection_pending AS p
      WHERE p.consumer_name = p_consumer_name
        AND p.position_id = p_position_id
        AND p.aggregate_version = p_aggregate_version;

      IF v_existing_pending.event_id <> p_event_id
         OR v_existing_pending.recorded_at <> p_recorded_at
         OR v_existing_pending.data <> p_data THEN
        RAISE EXCEPTION 'projection version already belongs to another event'
          USING ERRCODE = '23505';
      END IF;
    END IF;

    v_now := clock_timestamp();
    UPDATE domain_event_inbox AS i
    SET metadata = i.metadata || jsonb_build_object(
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
    WHERE i.consumer_name = p_consumer_name
      AND i.event_id = p_event_id;

    RETURN QUERY SELECT
      'buffered'::text,
      0,
      v_projection_version,
      v_projection_version + 1;
    RETURN;
  END IF;

  LOOP
    v_now := clock_timestamp();
    UPDATE inventory_position_quantity_projection AS p
    SET physical_qty = p.physical_qty + (v_current_data ->> 'physical_delta')::numeric,
        reserved_qty = p.reserved_qty + (v_current_data ->> 'reserved_delta')::numeric,
        allocated_qty = p.allocated_qty + (v_current_data ->> 'allocated_delta')::numeric,
        aggregate_version = v_current_version,
        last_event_id = v_current_event_id,
        last_recorded_at = v_current_recorded_at,
        updated_at = v_now
    WHERE p.consumer_name = p_consumer_name
      AND p.position_id = p_position_id;

    v_projection_version := v_current_version;
    v_applied_count := v_applied_count + 1;

    UPDATE domain_event_inbox AS i
    SET metadata = i.metadata || jsonb_build_object(
      'projection', COALESCE(i.metadata -> 'projection', '{}'::jsonb)
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
    WHERE i.consumer_name = p_consumer_name
      AND i.event_id = v_current_event_id;

    DELETE FROM inventory_position_projection_pending AS p
    WHERE p.consumer_name = p_consumer_name
      AND p.position_id = p_position_id
      AND p.aggregate_version = v_projection_version + 1
    RETURNING p.event_id, p.aggregate_version, p.recorded_at, p.data
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

COMMIT;
