BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

-- Golden scenarios exercise warehouse-boundary postings and count reconciliation.
ALTER TABLE inventory_transactions
  DROP CONSTRAINT inventory_transactions_transaction_type_check;

ALTER TABLE inventory_transactions
  ADD CONSTRAINT inventory_transactions_transaction_type_check
  CHECK (transaction_type IN (
    'allocation',
    'allocation_release',
    'count_reconciliation',
    'hold_apply',
    'hold_release',
    'issue',
    'movement',
    'receipt',
    'reservation',
    'reservation_release'
  ));

-- Successful execution consumes commitments; it does not release fulfilled
-- quantity back to free capacity.
DROP VIEW inventory_scope_availability;
DROP FUNCTION inventory_scope_available_qty(uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid);

ALTER TABLE inventory_reservations
  DROP CONSTRAINT inventory_reservation_bounds_ck;
ALTER TABLE inventory_reservations
  DROP COLUMN remaining_qty;
ALTER TABLE inventory_reservations
  ADD COLUMN consumed_qty numeric(24, 6) NOT NULL DEFAULT 0;
ALTER TABLE inventory_reservations
  ADD COLUMN remaining_qty numeric(24, 6)
    GENERATED ALWAYS AS (quantity - allocated_qty - consumed_qty - released_qty) STORED;
ALTER TABLE inventory_reservations
  ADD CONSTRAINT inventory_reservation_bounds_ck
    CHECK (
      consumed_qty >= 0
      AND allocated_qty + consumed_qty + released_qty <= quantity
    );

ALTER TABLE inventory_allocations
  ADD COLUMN consumed_qty numeric(24, 6) NOT NULL DEFAULT 0,
  ADD COLUMN released_qty numeric(24, 6) NOT NULL DEFAULT 0,
  ADD COLUMN remaining_qty numeric(24, 6)
    GENERATED ALWAYS AS (quantity - consumed_qty - released_qty) STORED,
  ADD CONSTRAINT inventory_allocation_lifecycle_bounds_ck
    CHECK (
      consumed_qty >= 0
      AND released_qty >= 0
      AND consumed_qty + released_qty <= quantity
    );

CREATE TABLE inventory_allocation_consumptions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  allocation_id uuid NOT NULL REFERENCES inventory_allocations(id),
  transaction_id uuid NOT NULL UNIQUE REFERENCES inventory_transactions(id),
  quantity numeric(24, 6) NOT NULL CHECK (quantity > 0),
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX inventory_allocation_consumptions_allocation_idx
  ON inventory_allocation_consumptions (tenant_id, allocation_id, recorded_at);

CREATE OR REPLACE FUNCTION inventory_scope_available_qty(
  p_tenant_id uuid,
  p_warehouse_id uuid,
  p_item_id uuid,
  p_owner_id uuid,
  p_condition_id uuid,
  p_lot_id uuid,
  p_stock_scope_id uuid,
  p_attribute_set_id uuid,
  p_stock_segment_id uuid
) RETURNS numeric
LANGUAGE sql
STABLE
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
  SELECT GREATEST(
    COALESCE((
      SELECT sum(p.physical_qty - p.allocated_qty - p.reserved_qty)
      FROM inventory_positions p
      WHERE p.tenant_id = p_tenant_id
        AND p.warehouse_id = p_warehouse_id
        AND p.item_id = p_item_id
        AND p.owner_id = p_owner_id
        AND p.inventory_condition_id = p_condition_id
        AND p.lot_id IS NOT DISTINCT FROM p_lot_id
        AND p.stock_scope_id IS NOT DISTINCT FROM p_stock_scope_id
        AND p.attribute_set_id IS NOT DISTINCT FROM p_attribute_set_id
        AND p.stock_segment_id IS NOT DISTINCT FROM p_stock_segment_id
        AND inventory_position_action_eligible(p.id, 'allocation')
    ), 0)
    - COALESCE((
      SELECT sum(r.remaining_qty)
      FROM inventory_reservations r
      WHERE r.tenant_id = p_tenant_id
        AND r.warehouse_id = p_warehouse_id
        AND r.item_id = p_item_id
        AND r.owner_id = p_owner_id
        AND r.inventory_condition_id = p_condition_id
        AND r.lot_id IS NOT DISTINCT FROM p_lot_id
        AND r.stock_scope_id IS NOT DISTINCT FROM p_stock_scope_id
        AND r.attribute_set_id IS NOT DISTINCT FROM p_attribute_set_id
        AND r.stock_segment_id IS NOT DISTINCT FROM p_stock_segment_id
    ), 0),
    0
  );
$$;

CREATE VIEW inventory_scope_availability AS
WITH position_scope AS (
  SELECT
    tenant_id,
    warehouse_id,
    item_id,
    owner_id,
    inventory_condition_id,
    lot_id,
    stock_scope_id,
    attribute_set_id,
    stock_segment_id,
    sum(physical_qty) AS physical_qty,
    sum(reserved_qty) AS exact_reserved_qty,
    sum(allocated_qty) AS allocated_qty
  FROM inventory_positions
  GROUP BY
    tenant_id,
    warehouse_id,
    item_id,
    owner_id,
    inventory_condition_id,
    lot_id,
    stock_scope_id,
    attribute_set_id,
    stock_segment_id
),
reservation_scope AS (
  SELECT
    tenant_id,
    warehouse_id,
    item_id,
    owner_id,
    inventory_condition_id,
    lot_id,
    stock_scope_id,
    attribute_set_id,
    stock_segment_id,
    sum(remaining_qty) AS coarse_reserved_remaining_qty
  FROM inventory_reservations
  GROUP BY
    tenant_id,
    warehouse_id,
    item_id,
    owner_id,
    inventory_condition_id,
    lot_id,
    stock_scope_id,
    attribute_set_id,
    stock_segment_id
)
SELECT
  p.tenant_id,
  p.warehouse_id,
  p.item_id,
  p.owner_id,
  p.inventory_condition_id,
  p.lot_id,
  p.stock_scope_id,
  p.attribute_set_id,
  p.stock_segment_id,
  p.physical_qty,
  p.exact_reserved_qty,
  p.allocated_qty,
  COALESCE(r.coarse_reserved_remaining_qty, 0::numeric) AS coarse_reserved_remaining_qty,
  inventory_scope_available_qty(
    p.tenant_id,
    p.warehouse_id,
    p.item_id,
    p.owner_id,
    p.inventory_condition_id,
    p.lot_id,
    p.stock_scope_id,
    p.attribute_set_id,
    p.stock_segment_id
  ) AS available_qty
FROM position_scope p
LEFT JOIN reservation_scope r
  ON r.tenant_id = p.tenant_id
 AND r.warehouse_id = p.warehouse_id
 AND r.item_id = p.item_id
 AND r.owner_id = p.owner_id
 AND r.inventory_condition_id = p.inventory_condition_id
 AND r.lot_id IS NOT DISTINCT FROM p.lot_id
 AND r.stock_scope_id IS NOT DISTINCT FROM p.stock_scope_id
 AND r.attribute_set_id IS NOT DISTINCT FROM p.attribute_set_id
 AND r.stock_segment_id IS NOT DISTINCT FROM p.stock_segment_id;

-- Allocation release now releases only the unconsumed remainder.
CREATE OR REPLACE FUNCTION release_inventory_allocation(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_allocation_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_request_fingerprint text;
  v_existing inventory_transactions%ROWTYPE;
  v_allocation inventory_allocations%ROWTYPE;
  v_position inventory_positions%ROWTYPE;
  v_reservation inventory_reservations%ROWTYPE;
  v_release_qty numeric;
BEGIN
  v_request_fingerprint := format('allocation-release:%s', p_allocation_id);

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'allocation_release', p_idempotency_key,
    v_request_fingerprint, p_allocation_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id
      AND idempotency_key = p_idempotency_key;

    IF v_existing.transaction_type <> 'allocation_release'
       OR v_existing.request_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;
    RETURN v_existing.id;
  END IF;

  SELECT * INTO v_allocation
  FROM inventory_allocations
  WHERE id = p_allocation_id
    AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'allocation not found' USING ERRCODE = '23503';
  END IF;
  IF v_allocation.released_at IS NOT NULL THEN
    RAISE EXCEPTION 'allocation is already released' USING ERRCODE = '23514';
  END IF;

  v_release_qty := v_allocation.remaining_qty;
  IF v_release_qty <= 0 THEN
    RAISE EXCEPTION 'allocation has no remaining quantity to release' USING ERRCODE = '23514';
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = v_allocation.position_id
    AND tenant_id = p_tenant_id;

  PERFORM lock_inventory_scope(
    v_position.tenant_id,
    v_position.warehouse_id,
    v_position.item_id,
    v_position.owner_id,
    v_position.inventory_condition_id,
    v_position.lot_id,
    v_position.stock_scope_id,
    v_position.attribute_set_id,
    v_position.stock_segment_id
  );

  SELECT * INTO v_allocation
  FROM inventory_allocations
  WHERE id = p_allocation_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = v_allocation.position_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  v_release_qty := v_allocation.remaining_qty;
  IF v_position.allocated_qty < v_release_qty THEN
    RAISE EXCEPTION 'position allocation quantity is inconsistent' USING ERRCODE = '23514';
  END IF;

  IF v_allocation.reservation_id IS NOT NULL THEN
    SELECT * INTO v_reservation
    FROM inventory_reservations
    WHERE id = v_allocation.reservation_id AND tenant_id = p_tenant_id
    FOR UPDATE;

    IF v_reservation.allocated_qty < v_release_qty THEN
      RAISE EXCEPTION 'reservation allocation quantity is inconsistent' USING ERRCODE = '23514';
    END IF;
  END IF;

  UPDATE inventory_positions
  SET allocated_qty = allocated_qty - v_release_qty,
      version = version + 1,
      updated_at = now()
  WHERE id = v_allocation.position_id;

  IF v_allocation.reservation_id IS NOT NULL THEN
    IF v_reservation.released_at IS NULL THEN
      UPDATE inventory_reservations
      SET allocated_qty = allocated_qty - v_release_qty
      WHERE id = v_allocation.reservation_id;
    ELSE
      UPDATE inventory_reservations
      SET allocated_qty = allocated_qty - v_release_qty,
          released_qty = released_qty + v_release_qty
      WHERE id = v_allocation.reservation_id;
    END IF;
  END IF;

  UPDATE inventory_allocations
  SET released_qty = released_qty + v_release_qty,
      released_transaction_id = p_transaction_id,
      released_at = now()
  WHERE id = p_allocation_id;

  INSERT INTO inventory_transaction_legs (
    transaction_id, position_id, allocated_delta
  ) VALUES (p_transaction_id, v_allocation.position_id, -v_release_qty);

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION post_inventory_receipt(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_position_id uuid,
  p_qty numeric,
  p_source_reference text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing inventory_transactions%ROWTYPE;
  v_fingerprint text;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'receipt quantity must be positive' USING ERRCODE = '23514';
  END IF;

  v_fingerprint := format(
    'receipt:%s:%s:%s', p_position_id, p_qty::numeric(24, 6)::text, p_source_reference
  );

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'receipt', p_idempotency_key, v_fingerprint, p_source_reference
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id AND idempotency_key = p_idempotency_key;
    IF v_existing.transaction_type <> 'receipt'
       OR v_existing.request_fingerprint <> v_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;
    RETURN v_existing.id;
  END IF;

  PERFORM 1
  FROM inventory_positions
  WHERE id = p_position_id AND tenant_id = p_tenant_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found' USING ERRCODE = '23503';
  END IF;

  UPDATE inventory_positions
  SET physical_qty = physical_qty + p_qty,
      version = version + 1,
      updated_at = now()
  WHERE id = p_position_id;

  INSERT INTO inventory_transaction_legs (transaction_id, position_id, physical_delta)
  VALUES (p_transaction_id, p_position_id, p_qty);

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION post_inventory_issue(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_position_id uuid,
  p_qty numeric,
  p_source_reference text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing inventory_transactions%ROWTYPE;
  v_position inventory_positions%ROWTYPE;
  v_fingerprint text;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'issue quantity must be positive' USING ERRCODE = '23514';
  END IF;

  v_fingerprint := format(
    'issue:%s:%s:%s', p_position_id, p_qty::numeric(24, 6)::text, p_source_reference
  );

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'issue', p_idempotency_key, v_fingerprint, p_source_reference
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id AND idempotency_key = p_idempotency_key;
    IF v_existing.transaction_type <> 'issue'
       OR v_existing.request_fingerprint <> v_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;
    RETURN v_existing.id;
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = p_position_id AND tenant_id = p_tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found' USING ERRCODE = '23503';
  END IF;

  PERFORM lock_inventory_scope(
    v_position.tenant_id,
    v_position.warehouse_id,
    v_position.item_id,
    v_position.owner_id,
    v_position.inventory_condition_id,
    v_position.lot_id,
    v_position.stock_scope_id,
    v_position.attribute_set_id,
    v_position.stock_segment_id
  );

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = p_position_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF NOT inventory_position_action_eligible(p_position_id, 'shipping') THEN
    RAISE EXCEPTION 'inventory position is not eligible for shipping' USING ERRCODE = '23514';
  END IF;
  IF v_position.physical_qty < p_qty THEN
    RAISE EXCEPTION 'issue exceeds physical quantity' USING ERRCODE = '23514';
  END IF;
  IF v_position.physical_qty - p_qty < GREATEST(v_position.reserved_qty, v_position.allocated_qty) THEN
    RAISE EXCEPTION 'issue would strand exact-position commitments' USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_positions
  SET physical_qty = physical_qty - p_qty,
      version = version + 1,
      updated_at = now()
  WHERE id = p_position_id;

  INSERT INTO inventory_transaction_legs (transaction_id, position_id, physical_delta)
  VALUES (p_transaction_id, p_position_id, -p_qty);

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION consume_inventory_allocation_movement(
  p_consumption_id uuid,
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_allocation_id uuid,
  p_target_position_id uuid,
  p_qty numeric
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing inventory_transactions%ROWTYPE;
  v_allocation inventory_allocations%ROWTYPE;
  v_reservation inventory_reservations%ROWTYPE;
  v_source inventory_positions%ROWTYPE;
  v_target inventory_positions%ROWTYPE;
  v_locked_count integer;
  v_fingerprint text;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'allocation consumption quantity must be positive' USING ERRCODE = '23514';
  END IF;

  v_fingerprint := format(
    'allocation-consume-move:%s:%s:%s',
    p_allocation_id, p_target_position_id, p_qty::numeric(24, 6)::text
  );

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'movement', p_idempotency_key, v_fingerprint, p_allocation_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id AND idempotency_key = p_idempotency_key;
    IF v_existing.transaction_type <> 'movement'
       OR v_existing.request_fingerprint <> v_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;
    RETURN v_existing.id;
  END IF;

  SELECT * INTO v_allocation
  FROM inventory_allocations
  WHERE id = p_allocation_id AND tenant_id = p_tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'allocation not found' USING ERRCODE = '23503';
  END IF;
  IF v_allocation.released_at IS NOT NULL THEN
    RAISE EXCEPTION 'allocation is released' USING ERRCODE = '23514';
  END IF;
  IF v_allocation.remaining_qty < p_qty THEN
    RAISE EXCEPTION 'pick exceeds allocation remainder' USING ERRCODE = '23514';
  END IF;

  SELECT * INTO v_source
  FROM inventory_positions
  WHERE id = v_allocation.position_id AND tenant_id = p_tenant_id;

  PERFORM lock_inventory_scope(
    v_source.tenant_id,
    v_source.warehouse_id,
    v_source.item_id,
    v_source.owner_id,
    v_source.inventory_condition_id,
    v_source.lot_id,
    v_source.stock_scope_id,
    v_source.attribute_set_id,
    v_source.stock_segment_id
  );

  SELECT * INTO v_allocation
  FROM inventory_allocations
  WHERE id = p_allocation_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  PERFORM id
  FROM inventory_positions
  WHERE tenant_id = p_tenant_id
    AND id IN (v_allocation.position_id, p_target_position_id)
  ORDER BY id
  FOR UPDATE;
  GET DIAGNOSTICS v_locked_count = ROW_COUNT;
  IF v_locked_count <> 2 THEN
    RAISE EXCEPTION 'source or target inventory position not found' USING ERRCODE = '23503';
  END IF;

  SELECT * INTO v_source FROM inventory_positions WHERE id = v_allocation.position_id;
  SELECT * INTO v_target FROM inventory_positions WHERE id = p_target_position_id;

  IF v_source.warehouse_id <> v_target.warehouse_id
     OR v_source.item_id <> v_target.item_id
     OR v_source.owner_id <> v_target.owner_id
     OR v_source.inventory_condition_id <> v_target.inventory_condition_id
     OR v_source.lot_id IS DISTINCT FROM v_target.lot_id
     OR v_source.stock_scope_id IS DISTINCT FROM v_target.stock_scope_id
     OR v_source.attribute_set_id IS DISTINCT FROM v_target.attribute_set_id
     OR v_source.stock_segment_id IS DISTINCT FROM v_target.stock_segment_id THEN
    RAISE EXCEPTION 'pick target differs in non-anchor stock identity' USING ERRCODE = '23514';
  END IF;

  IF NOT inventory_position_action_eligible(v_source.id, 'picking')
     OR NOT inventory_position_action_eligible(v_source.id, 'movement')
     OR NOT inventory_position_action_eligible(v_target.id, 'movement') THEN
    RAISE EXCEPTION 'inventory is not eligible for pick movement' USING ERRCODE = '23514';
  END IF;

  IF v_allocation.remaining_qty < p_qty THEN
    RAISE EXCEPTION 'pick exceeds allocation remainder' USING ERRCODE = '23514';
  END IF;
  IF v_source.allocated_qty < p_qty OR v_source.physical_qty < p_qty THEN
    RAISE EXCEPTION 'source position cannot satisfy allocation consumption' USING ERRCODE = '23514';
  END IF;
  IF v_source.physical_qty - p_qty < GREATEST(v_source.reserved_qty, v_source.allocated_qty - p_qty) THEN
    RAISE EXCEPTION 'pick movement would strand remaining exact commitments' USING ERRCODE = '23514';
  END IF;

  IF v_allocation.reservation_id IS NOT NULL THEN
    SELECT * INTO v_reservation
    FROM inventory_reservations
    WHERE id = v_allocation.reservation_id AND tenant_id = p_tenant_id
    FOR UPDATE;
    IF v_reservation.allocated_qty < p_qty THEN
      RAISE EXCEPTION 'reservation allocated quantity is inconsistent' USING ERRCODE = '23514';
    END IF;
  END IF;

  UPDATE inventory_positions
  SET physical_qty = physical_qty - p_qty,
      allocated_qty = allocated_qty - p_qty,
      version = version + 1,
      updated_at = now()
  WHERE id = v_source.id;

  UPDATE inventory_positions
  SET physical_qty = physical_qty + p_qty,
      version = version + 1,
      updated_at = now()
  WHERE id = v_target.id;

  UPDATE inventory_allocations
  SET consumed_qty = consumed_qty + p_qty
  WHERE id = p_allocation_id;

  IF v_allocation.reservation_id IS NOT NULL THEN
    UPDATE inventory_reservations
    SET allocated_qty = allocated_qty - p_qty,
        consumed_qty = consumed_qty + p_qty
    WHERE id = v_allocation.reservation_id;
  END IF;

  INSERT INTO inventory_transaction_legs (
    transaction_id, position_id, physical_delta, allocated_delta
  ) VALUES
    (p_transaction_id, v_source.id, -p_qty, -p_qty),
    (p_transaction_id, v_target.id, p_qty, 0);

  INSERT INTO inventory_allocation_consumptions (
    id, tenant_id, allocation_id, transaction_id, quantity
  ) VALUES (
    p_consumption_id, p_tenant_id, p_allocation_id, p_transaction_id, p_qty
  );

  RETURN p_transaction_id;
END;
$$;

CREATE TABLE inventory_count_results (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  position_id uuid NOT NULL REFERENCES inventory_positions(id),
  idempotency_key text NOT NULL,
  request_fingerprint text NOT NULL,
  source_reference text,
  system_quantity_snapshot numeric(24, 6) NOT NULL CHECK (system_quantity_snapshot >= 0),
  counted_quantity numeric(24, 6) NOT NULL CHECK (counted_quantity >= 0),
  variance_quantity numeric(24, 6)
    GENERATED ALWAYS AS (counted_quantity - system_quantity_snapshot) STORED,
  state text NOT NULL DEFAULT 'observed' CHECK (state IN ('observed', 'reconciled')),
  reconciliation_transaction_id uuid UNIQUE REFERENCES inventory_transactions(id),
  observed_at timestamptz NOT NULL DEFAULT now(),
  reconciled_at timestamptz,
  UNIQUE (tenant_id, idempotency_key),
  CONSTRAINT inventory_count_reconciliation_pair_ck
    CHECK ((state = 'reconciled') = (reconciliation_transaction_id IS NOT NULL))
);

CREATE INDEX inventory_count_results_position_idx
  ON inventory_count_results (tenant_id, position_id, observed_at DESC);

CREATE OR REPLACE FUNCTION record_inventory_count_result(
  p_count_result_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_position_id uuid,
  p_counted_quantity numeric,
  p_source_reference text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_position inventory_positions%ROWTYPE;
  v_existing inventory_count_results%ROWTYPE;
  v_fingerprint text;
BEGIN
  IF p_counted_quantity < 0 THEN
    RAISE EXCEPTION 'counted quantity must be nonnegative' USING ERRCODE = '23514';
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = p_position_id AND tenant_id = p_tenant_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found' USING ERRCODE = '23503';
  END IF;

  v_fingerprint := format(
    'count:%s:%s:%s', p_position_id, p_counted_quantity::numeric(24, 6)::text, p_source_reference
  );

  INSERT INTO inventory_count_results (
    id, tenant_id, position_id, idempotency_key, request_fingerprint,
    source_reference, system_quantity_snapshot, counted_quantity
  ) VALUES (
    p_count_result_id, p_tenant_id, p_position_id, p_idempotency_key, v_fingerprint,
    p_source_reference, v_position.physical_qty, p_counted_quantity
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_count_results
    WHERE tenant_id = p_tenant_id AND idempotency_key = p_idempotency_key;
    IF v_existing.request_fingerprint <> v_fingerprint THEN
      RAISE EXCEPTION 'count idempotency key reused with different payload'
        USING ERRCODE = '23505';
    END IF;
    RETURN v_existing.id;
  END IF;

  RETURN p_count_result_id;
END;
$$;

CREATE OR REPLACE FUNCTION reconcile_inventory_count_result(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_count_result_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing inventory_transactions%ROWTYPE;
  v_count inventory_count_results%ROWTYPE;
  v_position inventory_positions%ROWTYPE;
  v_fingerprint text;
BEGIN
  v_fingerprint := format('count-reconciliation:%s', p_count_result_id);

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'count_reconciliation', p_idempotency_key,
    v_fingerprint, p_count_result_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id AND idempotency_key = p_idempotency_key;
    IF v_existing.transaction_type <> 'count_reconciliation'
       OR v_existing.request_fingerprint <> v_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;
    RETURN v_existing.id;
  END IF;

  SELECT * INTO v_count
  FROM inventory_count_results
  WHERE id = p_count_result_id AND tenant_id = p_tenant_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'count result not found' USING ERRCODE = '23503';
  END IF;
  IF v_count.state <> 'observed' THEN
    RAISE EXCEPTION 'count result is already reconciled' USING ERRCODE = '23514';
  END IF;
  IF v_count.variance_quantity = 0 THEN
    RAISE EXCEPTION 'zero-variance count does not require reconciliation' USING ERRCODE = '23514';
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = v_count.position_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_position.physical_qty <> v_count.system_quantity_snapshot THEN
    RAISE EXCEPTION 'count result is stale because physical quantity changed after observation'
      USING ERRCODE = '40001';
  END IF;
  IF v_position.physical_qty + v_count.variance_quantity < 0 THEN
    RAISE EXCEPTION 'count reconciliation would make physical quantity negative'
      USING ERRCODE = '23514';
  END IF;
  IF v_position.physical_qty + v_count.variance_quantity
       < GREATEST(v_position.reserved_qty, v_position.allocated_qty) THEN
    RAISE EXCEPTION 'count reconciliation would strand exact-position commitments'
      USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_positions
  SET physical_qty = physical_qty + v_count.variance_quantity,
      version = version + 1,
      updated_at = now()
  WHERE id = v_count.position_id;

  INSERT INTO inventory_transaction_legs (transaction_id, position_id, physical_delta)
  VALUES (p_transaction_id, v_count.position_id, v_count.variance_quantity);

  UPDATE inventory_count_results
  SET state = 'reconciled',
      reconciliation_transaction_id = p_transaction_id,
      reconciled_at = now()
  WHERE id = p_count_result_id;

  RETURN p_transaction_id;
END;
$$;

-- Capability-owned bridges keep domain mutation and Work confirmation atomic.
CREATE OR REPLACE FUNCTION confirm_inventory_movement_task(
  p_confirmation_id uuid,
  p_movement_transaction_id uuid,
  p_tenant_id uuid,
  p_inventory_idempotency_key text,
  p_work_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_source_position_id uuid,
  p_target_position_id uuid,
  p_qty numeric
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  PERFORM transfer_inventory_quantity(
    p_movement_transaction_id,
    p_tenant_id,
    p_inventory_idempotency_key,
    p_source_position_id,
    p_target_position_id,
    p_qty
  );

  RETURN confirm_warehouse_task(
    p_confirmation_id,
    p_tenant_id,
    p_work_idempotency_key,
    p_task_id,
    p_resource_kind,
    p_resource_id,
    'confirmed',
    p_qty,
    'inventory-transaction:' || p_movement_transaction_id::text,
    '{}'::jsonb
  );
END;
$$;

CREATE OR REPLACE FUNCTION confirm_pick_task(
  p_confirmation_id uuid,
  p_consumption_id uuid,
  p_movement_transaction_id uuid,
  p_staging_hold_id uuid,
  p_staging_hold_transaction_id uuid,
  p_tenant_id uuid,
  p_inventory_idempotency_key text,
  p_hold_idempotency_key text,
  p_work_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_allocation_id uuid,
  p_target_position_id uuid,
  p_qty numeric,
  p_staging_policy_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  PERFORM consume_inventory_allocation_movement(
    p_consumption_id,
    p_movement_transaction_id,
    p_tenant_id,
    p_inventory_idempotency_key,
    p_allocation_id,
    p_target_position_id,
    p_qty
  );

  IF p_staging_hold_id IS NOT NULL THEN
    PERFORM apply_inventory_position_hold(
      p_staging_hold_id,
      p_staging_hold_transaction_id,
      p_tenant_id,
      p_hold_idempotency_key,
      p_staging_policy_id,
      'picked stock awaiting shipment',
      p_target_position_id
    );
  END IF;

  RETURN confirm_warehouse_task(
    p_confirmation_id,
    p_tenant_id,
    p_work_idempotency_key,
    p_task_id,
    p_resource_kind,
    p_resource_id,
    'confirmed',
    p_qty,
    'inventory-allocation:' || p_allocation_id::text,
    '{}'::jsonb
  );
END;
$$;

CREATE OR REPLACE FUNCTION confirm_shipping_task(
  p_confirmation_id uuid,
  p_issue_transaction_id uuid,
  p_hold_release_transaction_id uuid,
  p_tenant_id uuid,
  p_issue_idempotency_key text,
  p_hold_release_idempotency_key text,
  p_work_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_position_id uuid,
  p_qty numeric,
  p_staging_hold_id uuid,
  p_source_reference text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_staging_hold_id IS NOT NULL THEN
    PERFORM release_inventory_hold(
      p_hold_release_transaction_id,
      p_tenant_id,
      p_hold_release_idempotency_key,
      p_staging_hold_id
    );
  END IF;

  PERFORM post_inventory_issue(
    p_issue_transaction_id,
    p_tenant_id,
    p_issue_idempotency_key,
    p_position_id,
    p_qty,
    p_source_reference
  );

  RETURN confirm_warehouse_task(
    p_confirmation_id,
    p_tenant_id,
    p_work_idempotency_key,
    p_task_id,
    p_resource_kind,
    p_resource_id,
    'shipped',
    p_qty,
    'inventory-transaction:' || p_issue_transaction_id::text,
    '{}'::jsonb
  );
END;
$$;

CREATE OR REPLACE FUNCTION confirm_count_task(
  p_confirmation_id uuid,
  p_count_result_id uuid,
  p_tenant_id uuid,
  p_count_idempotency_key text,
  p_work_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_position_id uuid,
  p_counted_quantity numeric,
  p_source_reference text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  PERFORM record_inventory_count_result(
    p_count_result_id,
    p_tenant_id,
    p_count_idempotency_key,
    p_position_id,
    p_counted_quantity,
    p_source_reference
  );

  RETURN confirm_warehouse_task(
    p_confirmation_id,
    p_tenant_id,
    p_work_idempotency_key,
    p_task_id,
    p_resource_kind,
    p_resource_id,
    'counted',
    p_counted_quantity,
    'count-result:' || p_count_result_id::text,
    '{}'::jsonb
  );
END;
$$;

CREATE OR REPLACE FUNCTION confirm_hu_relocation_task(
  p_confirmation_id uuid,
  p_movement_id uuid,
  p_tenant_id uuid,
  p_work_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_handling_unit_id uuid,
  p_to_location_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  PERFORM relocate_handling_unit(
    p_movement_id,
    p_tenant_id,
    p_handling_unit_id,
    p_to_location_id
  );

  RETURN confirm_warehouse_task(
    p_confirmation_id,
    p_tenant_id,
    p_work_idempotency_key,
    p_task_id,
    p_resource_kind,
    p_resource_id,
    'relocated',
    NULL,
    'handling-unit-movement:' || p_movement_id::text,
    '{}'::jsonb
  );
END;
$$;

COMMIT;
