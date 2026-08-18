BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE inventory_position_quantity_projection (
  consumer_name text NOT NULL,
  position_id uuid NOT NULL,
  aggregate_version bigint NOT NULL DEFAULT 0 CHECK (aggregate_version >= 0),
  physical_qty numeric(24, 6) NOT NULL DEFAULT 0,
  reserved_qty numeric(24, 6) NOT NULL DEFAULT 0,
  allocated_qty numeric(24, 6) NOT NULL DEFAULT 0,
  last_event_id uuid,
  last_recorded_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer_name, position_id),
  CONSTRAINT inventory_position_projection_nonnegative_ck
    CHECK (physical_qty >= 0 AND reserved_qty >= 0 AND allocated_qty >= 0),
  CONSTRAINT inventory_position_projection_commitment_bounds_ck
    CHECK (reserved_qty <= physical_qty AND allocated_qty <= physical_qty),
  CONSTRAINT inventory_position_projection_cursor_ck CHECK (
    (aggregate_version = 0 AND last_event_id IS NULL AND last_recorded_at IS NULL)
    OR
    (aggregate_version >= 1 AND last_event_id IS NOT NULL AND last_recorded_at IS NOT NULL)
  )
);

CREATE TABLE inventory_position_projection_pending (
  consumer_name text NOT NULL,
  position_id uuid NOT NULL,
  aggregate_version bigint NOT NULL CHECK (aggregate_version >= 1),
  event_id uuid NOT NULL,
  recorded_at timestamptz NOT NULL,
  data jsonb NOT NULL,
  buffered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer_name, position_id, aggregate_version),
  CONSTRAINT inventory_position_projection_pending_event_uq
    UNIQUE (consumer_name, event_id),
  CONSTRAINT inventory_position_projection_pending_inbox_fk
    FOREIGN KEY (consumer_name, event_id)
    REFERENCES domain_event_inbox (consumer_name, event_id)
    ON DELETE RESTRICT
);

CREATE INDEX inventory_position_projection_pending_age_idx
  ON inventory_position_projection_pending (consumer_name, buffered_at);

CREATE VIEW inventory_position_projection_gaps AS
SELECT
  projection.consumer_name,
  projection.position_id,
  projection.aggregate_version + 1 AS expected_version,
  min(pending.aggregate_version) AS first_pending_version,
  max(pending.aggregate_version) AS last_pending_version,
  count(*) AS pending_count,
  min(pending.buffered_at) AS oldest_buffered_at
FROM inventory_position_quantity_projection projection
JOIN inventory_position_projection_pending pending
  USING (consumer_name, position_id)
GROUP BY
  projection.consumer_name,
  projection.position_id,
  projection.aggregate_version;

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

  SELECT aggregate_version, last_event_id
  INTO v_projection_version, v_last_event_id
  FROM inventory_position_quantity_projection
  WHERE consumer_name = p_consumer_name
    AND position_id = p_position_id
  FOR UPDATE;

  IF p_aggregate_version = v_projection_version THEN
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

  IF p_aggregate_version < v_projection_version THEN
    v_now := clock_timestamp();
    UPDATE domain_event_inbox
    SET metadata = metadata || jsonb_build_object(
      'projection', jsonb_build_object(
        'name', 'inventory-position-quantity',
        'status', 'stale',
        'position_id', p_position_id,
        'aggregate_version', p_aggregate_version,
        'projection_version', v_projection_version,
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

COMMIT;
