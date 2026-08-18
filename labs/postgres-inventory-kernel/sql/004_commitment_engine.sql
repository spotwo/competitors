BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

ALTER TABLE kernel_lab.inventory_transactions
  DROP CONSTRAINT inventory_transactions_transaction_type_check;

ALTER TABLE kernel_lab.inventory_transactions
  ADD CONSTRAINT inventory_transactions_transaction_type_check
  CHECK (transaction_type IN (
    'allocation',
    'allocation_release',
    'movement',
    'reservation',
    'reservation_release'
  ));

CREATE TABLE kernel_lab.inventory_reservations (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES kernel_lab.tenants(id),
  warehouse_id uuid NOT NULL REFERENCES kernel_lab.warehouses(id),
  demand_reference text NOT NULL,
  item_id uuid NOT NULL REFERENCES kernel_lab.items(id),
  owner_id uuid NOT NULL REFERENCES kernel_lab.owners(id),
  inventory_condition_id uuid NOT NULL REFERENCES kernel_lab.inventory_conditions(id),
  lot_id uuid REFERENCES kernel_lab.lots(id),
  stock_scope_id uuid REFERENCES kernel_lab.stock_scopes(id),
  attribute_set_id uuid REFERENCES kernel_lab.inventory_attribute_sets(id),
  stock_segment_id uuid REFERENCES kernel_lab.stock_segments(id),
  quantity numeric(24, 6) NOT NULL,
  allocated_qty numeric(24, 6) NOT NULL DEFAULT 0,
  released_qty numeric(24, 6) NOT NULL DEFAULT 0,
  remaining_qty numeric(24, 6)
    GENERATED ALWAYS AS (quantity - allocated_qty - released_qty) STORED,
  created_transaction_id uuid NOT NULL UNIQUE REFERENCES kernel_lab.inventory_transactions(id),
  released_transaction_id uuid UNIQUE REFERENCES kernel_lab.inventory_transactions(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  CONSTRAINT inventory_reservation_positive_ck CHECK (quantity > 0),
  CONSTRAINT inventory_reservation_nonnegative_ck
    CHECK (allocated_qty >= 0 AND released_qty >= 0),
  CONSTRAINT inventory_reservation_bounds_ck
    CHECK (allocated_qty + released_qty <= quantity),
  CONSTRAINT inventory_reservation_release_pair_ck
    CHECK ((released_at IS NULL) = (released_transaction_id IS NULL))
);

CREATE INDEX inventory_reservations_scope_idx
  ON kernel_lab.inventory_reservations (
    tenant_id,
    warehouse_id,
    item_id,
    owner_id,
    inventory_condition_id,
    lot_id,
    stock_scope_id,
    attribute_set_id,
    stock_segment_id
  ) INCLUDE (remaining_qty, released_at);

CREATE INDEX inventory_reservations_demand_idx
  ON kernel_lab.inventory_reservations (tenant_id, demand_reference);

CREATE TABLE kernel_lab.inventory_allocations (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES kernel_lab.tenants(id),
  demand_reference text NOT NULL,
  reservation_id uuid REFERENCES kernel_lab.inventory_reservations(id),
  position_id uuid NOT NULL REFERENCES kernel_lab.inventory_positions(id),
  quantity numeric(24, 6) NOT NULL,
  created_transaction_id uuid NOT NULL UNIQUE REFERENCES kernel_lab.inventory_transactions(id),
  released_transaction_id uuid UNIQUE REFERENCES kernel_lab.inventory_transactions(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  CONSTRAINT inventory_allocation_positive_ck CHECK (quantity > 0),
  CONSTRAINT inventory_allocation_release_pair_ck
    CHECK ((released_at IS NULL) = (released_transaction_id IS NULL))
);

CREATE INDEX inventory_allocations_position_idx
  ON kernel_lab.inventory_allocations (tenant_id, position_id)
  WHERE released_at IS NULL;

CREATE INDEX inventory_allocations_reservation_idx
  ON kernel_lab.inventory_allocations (tenant_id, reservation_id)
  WHERE reservation_id IS NOT NULL AND released_at IS NULL;

CREATE INDEX inventory_allocations_demand_idx
  ON kernel_lab.inventory_allocations (tenant_id, demand_reference);

CREATE OR REPLACE FUNCTION kernel_lab.lock_inventory_scope(
  p_tenant_id uuid,
  p_warehouse_id uuid,
  p_item_id uuid,
  p_owner_id uuid,
  p_condition_id uuid,
  p_lot_id uuid,
  p_stock_scope_id uuid,
  p_attribute_set_id uuid,
  p_stock_segment_id uuid
) RETURNS void
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_scope text;
BEGIN
  v_scope := concat_ws('|',
    p_tenant_id::text,
    p_warehouse_id::text,
    p_item_id::text,
    p_owner_id::text,
    p_condition_id::text,
    COALESCE(p_lot_id::text, '-'),
    COALESCE(p_stock_scope_id::text, '-'),
    COALESCE(p_attribute_set_id::text, '-'),
    COALESCE(p_stock_segment_id::text, '-')
  );

  PERFORM pg_advisory_xact_lock(hashtextextended(v_scope, 0));
END;
$$;

CREATE OR REPLACE FUNCTION kernel_lab.inventory_scope_available_qty(
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

CREATE OR REPLACE VIEW kernel_lab.inventory_scope_availability AS
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
  FROM kernel_lab.inventory_positions
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
), reservation_scope AS (
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
  FROM kernel_lab.inventory_reservations
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
  p.*,
  COALESCE(r.coarse_reserved_remaining_qty, 0::numeric) AS coarse_reserved_remaining_qty,
  GREATEST(
    p.physical_qty
      - p.exact_reserved_qty
      - p.allocated_qty
      - COALESCE(r.coarse_reserved_remaining_qty, 0::numeric),
    0::numeric
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

CREATE OR REPLACE FUNCTION kernel_lab.create_inventory_reservation(
  p_reservation_id uuid,
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_demand_reference text,
  p_warehouse_id uuid,
  p_item_id uuid,
  p_owner_id uuid,
  p_condition_id uuid,
  p_qty numeric,
  p_lot_id uuid DEFAULT NULL,
  p_stock_scope_id uuid DEFAULT NULL,
  p_attribute_set_id uuid DEFAULT NULL,
  p_stock_segment_id uuid DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_request_fingerprint text;
  v_existing inventory_transactions%ROWTYPE;
  v_available numeric;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'reservation quantity must be positive'
      USING ERRCODE = '23514';
  END IF;

  v_request_fingerprint := format(
    'reservation:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
    p_demand_reference,
    p_warehouse_id,
    p_item_id,
    p_owner_id,
    p_condition_id,
    COALESCE(p_lot_id::text, '-'),
    COALESCE(p_stock_scope_id::text, '-'),
    COALESCE(p_attribute_set_id::text, '-'),
    COALESCE(p_stock_segment_id::text, '-'),
    p_qty::numeric(24, 6)::text
  );

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'reservation', p_idempotency_key,
    v_request_fingerprint, p_reservation_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id
      AND idempotency_key = p_idempotency_key;

    IF v_existing.transaction_type <> 'reservation'
       OR v_existing.request_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing.id;
  END IF;

  PERFORM lock_inventory_scope(
    p_tenant_id, p_warehouse_id, p_item_id, p_owner_id, p_condition_id,
    p_lot_id, p_stock_scope_id, p_attribute_set_id, p_stock_segment_id
  );

  v_available := inventory_scope_available_qty(
    p_tenant_id, p_warehouse_id, p_item_id, p_owner_id, p_condition_id,
    p_lot_id, p_stock_scope_id, p_attribute_set_id, p_stock_segment_id
  );

  IF v_available < p_qty THEN
    RAISE EXCEPTION 'reservation exceeds scope availability'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_reservations (
    id, tenant_id, warehouse_id, demand_reference,
    item_id, owner_id, inventory_condition_id,
    lot_id, stock_scope_id, attribute_set_id, stock_segment_id,
    quantity, created_transaction_id
  ) VALUES (
    p_reservation_id, p_tenant_id, p_warehouse_id, p_demand_reference,
    p_item_id, p_owner_id, p_condition_id,
    p_lot_id, p_stock_scope_id, p_attribute_set_id, p_stock_segment_id,
    p_qty, p_transaction_id
  );

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION kernel_lab.release_inventory_reservation(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_reservation_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_request_fingerprint text;
  v_existing inventory_transactions%ROWTYPE;
  v_reservation inventory_reservations%ROWTYPE;
BEGIN
  v_request_fingerprint := format('reservation-release:%s', p_reservation_id);

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'reservation_release', p_idempotency_key,
    v_request_fingerprint, p_reservation_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id
      AND idempotency_key = p_idempotency_key;

    IF v_existing.transaction_type <> 'reservation_release'
       OR v_existing.request_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing.id;
  END IF;

  SELECT * INTO v_reservation
  FROM inventory_reservations
  WHERE id = p_reservation_id
    AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'reservation not found'
      USING ERRCODE = '23503';
  END IF;

  PERFORM lock_inventory_scope(
    v_reservation.tenant_id,
    v_reservation.warehouse_id,
    v_reservation.item_id,
    v_reservation.owner_id,
    v_reservation.inventory_condition_id,
    v_reservation.lot_id,
    v_reservation.stock_scope_id,
    v_reservation.attribute_set_id,
    v_reservation.stock_segment_id
  );

  SELECT * INTO v_reservation
  FROM inventory_reservations
  WHERE id = p_reservation_id
    AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_reservation.released_at IS NOT NULL THEN
    RAISE EXCEPTION 'reservation is already released'
      USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_reservations
  SET released_qty = released_qty + remaining_qty,
      released_transaction_id = p_transaction_id,
      released_at = now()
  WHERE id = p_reservation_id;

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION kernel_lab.allocate_inventory_commitment(
  p_allocation_id uuid,
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_demand_reference text,
  p_position_id uuid,
  p_qty numeric,
  p_reservation_id uuid DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_request_fingerprint text;
  v_existing inventory_transactions%ROWTYPE;
  v_position inventory_positions%ROWTYPE;
  v_reservation inventory_reservations%ROWTYPE;
  v_scope_available numeric;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'allocation quantity must be positive'
      USING ERRCODE = '23514';
  END IF;

  v_request_fingerprint := format(
    'allocation-commitment:%s:%s:%s:%s',
    p_demand_reference,
    p_position_id,
    COALESCE(p_reservation_id::text, '-'),
    p_qty::numeric(24, 6)::text
  );

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'allocation', p_idempotency_key,
    v_request_fingerprint, p_allocation_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT * INTO v_existing
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id
      AND idempotency_key = p_idempotency_key;

    IF v_existing.transaction_type <> 'allocation'
       OR v_existing.request_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing.id;
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = p_position_id
    AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found'
      USING ERRCODE = '23503';
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

  IF p_reservation_id IS NOT NULL THEN
    SELECT * INTO v_reservation
    FROM inventory_reservations
    WHERE id = p_reservation_id
      AND tenant_id = p_tenant_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'reservation not found'
        USING ERRCODE = '23503';
    END IF;

    IF v_reservation.released_at IS NOT NULL THEN
      RAISE EXCEPTION 'reservation is released'
        USING ERRCODE = '23514';
    END IF;

    IF v_reservation.demand_reference <> p_demand_reference
       OR v_reservation.warehouse_id <> v_position.warehouse_id
       OR v_reservation.item_id <> v_position.item_id
       OR v_reservation.owner_id <> v_position.owner_id
       OR v_reservation.inventory_condition_id <> v_position.inventory_condition_id
       OR v_reservation.lot_id IS DISTINCT FROM v_position.lot_id
       OR v_reservation.stock_scope_id IS DISTINCT FROM v_position.stock_scope_id
       OR v_reservation.attribute_set_id IS DISTINCT FROM v_position.attribute_set_id
       OR v_reservation.stock_segment_id IS DISTINCT FROM v_position.stock_segment_id THEN
      RAISE EXCEPTION 'reservation scope does not match allocation position'
        USING ERRCODE = '23514';
    END IF;

    IF v_reservation.remaining_qty < p_qty THEN
      RAISE EXCEPTION 'allocation exceeds reservation remainder'
        USING ERRCODE = '23514';
    END IF;
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = p_position_id
    AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_position.physical_qty - v_position.allocated_qty - v_position.reserved_qty < p_qty THEN
    RAISE EXCEPTION 'allocation exceeds exact-position free quantity'
      USING ERRCODE = '23514';
  END IF;

  IF p_reservation_id IS NULL THEN
    v_scope_available := inventory_scope_available_qty(
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

    IF v_scope_available < p_qty THEN
      RAISE EXCEPTION 'unreserved allocation would consume reserved scope capacity'
        USING ERRCODE = '23514';
    END IF;
  END IF;

  UPDATE inventory_positions
  SET allocated_qty = allocated_qty + p_qty,
      version = version + 1,
      updated_at = now()
  WHERE id = p_position_id;

  IF p_reservation_id IS NOT NULL THEN
    UPDATE inventory_reservations
    SET allocated_qty = allocated_qty + p_qty
    WHERE id = p_reservation_id;
  END IF;

  INSERT INTO inventory_allocations (
    id, tenant_id, demand_reference, reservation_id, position_id,
    quantity, created_transaction_id
  ) VALUES (
    p_allocation_id, p_tenant_id, p_demand_reference, p_reservation_id,
    p_position_id, p_qty, p_transaction_id
  );

  INSERT INTO inventory_transaction_legs (
    transaction_id, position_id, allocated_delta
  ) VALUES (p_transaction_id, p_position_id, p_qty);

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION kernel_lab.release_inventory_allocation(
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
    RAISE EXCEPTION 'allocation not found'
      USING ERRCODE = '23503';
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
  WHERE id = p_allocation_id
    AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_allocation.released_at IS NOT NULL THEN
    RAISE EXCEPTION 'allocation is already released'
      USING ERRCODE = '23514';
  END IF;

  IF v_allocation.reservation_id IS NOT NULL THEN
    SELECT * INTO v_reservation
    FROM inventory_reservations
    WHERE id = v_allocation.reservation_id
      AND tenant_id = p_tenant_id
    FOR UPDATE;
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = v_allocation.position_id
    AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_position.allocated_qty < v_allocation.quantity THEN
    RAISE EXCEPTION 'position allocation quantity is inconsistent'
      USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_positions
  SET allocated_qty = allocated_qty - v_allocation.quantity,
      version = version + 1,
      updated_at = now()
  WHERE id = v_allocation.position_id;

  IF v_allocation.reservation_id IS NOT NULL THEN
    IF v_reservation.allocated_qty < v_allocation.quantity THEN
      RAISE EXCEPTION 'reservation allocation quantity is inconsistent'
        USING ERRCODE = '23514';
    END IF;

    IF v_reservation.released_at IS NULL THEN
      UPDATE inventory_reservations
      SET allocated_qty = allocated_qty - v_allocation.quantity
      WHERE id = v_allocation.reservation_id;
    ELSE
      UPDATE inventory_reservations
      SET allocated_qty = allocated_qty - v_allocation.quantity,
          released_qty = released_qty + v_allocation.quantity
      WHERE id = v_allocation.reservation_id;
    END IF;
  END IF;

  UPDATE inventory_allocations
  SET released_transaction_id = p_transaction_id,
      released_at = now()
  WHERE id = p_allocation_id;

  INSERT INTO inventory_transaction_legs (
    transaction_id, position_id, allocated_delta
  ) VALUES (p_transaction_id, v_allocation.position_id, -v_allocation.quantity);

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION kernel_lab.allocate_inventory_position(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_position_id uuid,
  p_qty numeric
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing_id uuid;
  v_existing_type text;
  v_existing_fingerprint text;
  v_request_fingerprint text;
  v_position inventory_positions%ROWTYPE;
  v_scope_available numeric;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'allocation quantity must be positive'
      USING ERRCODE = '23514';
  END IF;

  v_request_fingerprint := format(
    'allocation:%s:%s',
    p_position_id,
    p_qty::numeric(24, 6)::text
  );

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'allocation', p_idempotency_key,
    v_request_fingerprint, p_position_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT id, transaction_type, request_fingerprint
    INTO v_existing_id, v_existing_type, v_existing_fingerprint
    FROM inventory_transactions
    WHERE tenant_id = p_tenant_id
      AND idempotency_key = p_idempotency_key;

    IF v_existing_type <> 'allocation'
       OR v_existing_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing_id;
  END IF;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = p_position_id
    AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found'
      USING ERRCODE = '23503';
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
  WHERE id = p_position_id
    AND tenant_id = p_tenant_id
  FOR UPDATE;

  v_scope_available := inventory_scope_available_qty(
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

  IF v_position.physical_qty - v_position.allocated_qty - v_position.reserved_qty < p_qty
     OR v_scope_available < p_qty THEN
    RAISE EXCEPTION 'allocation exceeds free unreserved quantity'
      USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_positions
  SET allocated_qty = allocated_qty + p_qty,
      version = version + 1,
      updated_at = now()
  WHERE id = p_position_id;

  INSERT INTO inventory_transaction_legs (
    transaction_id, position_id, allocated_delta
  ) VALUES (p_transaction_id, p_position_id, p_qty);

  RETURN p_transaction_id;
END;
$$;

COMMIT;
