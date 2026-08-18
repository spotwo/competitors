BEGIN;

CREATE SCHEMA kernel_lab;
SET search_path = kernel_lab, public;

CREATE TABLE tenants (
  id uuid PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE warehouses (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  code text NOT NULL,
  UNIQUE (tenant_id, code)
);

CREATE TABLE locations (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  warehouse_id uuid NOT NULL REFERENCES warehouses(id),
  code text NOT NULL,
  UNIQUE (tenant_id, warehouse_id, code)
);

CREATE TABLE items (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  sku text NOT NULL,
  exact_serial_tracking boolean NOT NULL DEFAULT false,
  UNIQUE (tenant_id, sku)
);

CREATE TABLE owners (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  code text NOT NULL,
  UNIQUE (tenant_id, code)
);

CREATE TABLE inventory_conditions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  code text NOT NULL,
  UNIQUE (tenant_id, code)
);

CREATE TABLE lots (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  item_id uuid NOT NULL REFERENCES items(id),
  lot_no text NOT NULL,
  UNIQUE (tenant_id, item_id, lot_no)
);

CREATE TABLE stock_scopes (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  code text NOT NULL,
  UNIQUE (tenant_id, code)
);

CREATE TABLE inventory_attribute_sets (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  fingerprint text NOT NULL,
  attributes jsonb NOT NULL,
  UNIQUE (tenant_id, fingerprint)
);

CREATE TABLE stock_segments (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  code text NOT NULL,
  UNIQUE (tenant_id, code)
);

CREATE TABLE handling_units (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  warehouse_id uuid NOT NULL REFERENCES warehouses(id),
  parent_handling_unit_id uuid REFERENCES handling_units(id),
  location_id uuid REFERENCES locations(id),
  version bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT handling_unit_one_anchor_ck
    CHECK ((parent_handling_unit_id IS NULL) <> (location_id IS NULL)),
  CONSTRAINT handling_unit_not_own_parent_ck
    CHECK (parent_handling_unit_id IS NULL OR parent_handling_unit_id <> id)
);

CREATE TABLE inventory_positions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  warehouse_id uuid NOT NULL REFERENCES warehouses(id),
  location_id uuid REFERENCES locations(id),
  handling_unit_id uuid REFERENCES handling_units(id),
  item_id uuid NOT NULL REFERENCES items(id),
  owner_id uuid NOT NULL REFERENCES owners(id),
  inventory_condition_id uuid NOT NULL REFERENCES inventory_conditions(id),
  lot_id uuid REFERENCES lots(id),
  stock_scope_id uuid REFERENCES stock_scopes(id),
  attribute_set_id uuid REFERENCES inventory_attribute_sets(id),
  stock_segment_id uuid REFERENCES stock_segments(id),
  physical_qty numeric(24, 6) NOT NULL DEFAULT 0,
  reserved_qty numeric(24, 6) NOT NULL DEFAULT 0,
  allocated_qty numeric(24, 6) NOT NULL DEFAULT 0,
  version bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT inventory_position_one_anchor_ck
    CHECK ((location_id IS NULL) <> (handling_unit_id IS NULL)),
  CONSTRAINT inventory_position_nonnegative_ck
    CHECK (physical_qty >= 0 AND reserved_qty >= 0 AND allocated_qty >= 0),
  CONSTRAINT inventory_position_commitment_bounds_ck
    CHECK (reserved_qty <= physical_qty AND allocated_qty <= physical_qty),
  CONSTRAINT inventory_position_key_uq
    UNIQUE NULLS NOT DISTINCT (
      tenant_id,
      warehouse_id,
      location_id,
      handling_unit_id,
      item_id,
      owner_id,
      inventory_condition_id,
      lot_id,
      stock_scope_id,
      attribute_set_id,
      stock_segment_id
    )
);

CREATE INDEX inventory_positions_item_warehouse_idx
  ON inventory_positions (tenant_id, warehouse_id, item_id, inventory_condition_id)
  INCLUDE (physical_qty, reserved_qty, allocated_qty, version);

CREATE INDEX inventory_positions_location_idx
  ON inventory_positions (tenant_id, warehouse_id, location_id, item_id)
  WHERE location_id IS NOT NULL;

CREATE INDEX inventory_positions_hu_idx
  ON inventory_positions (tenant_id, warehouse_id, handling_unit_id, item_id)
  WHERE handling_unit_id IS NOT NULL;

CREATE TABLE serials (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  item_id uuid NOT NULL REFERENCES items(id),
  serial_no text NOT NULL,
  UNIQUE (tenant_id, item_id, serial_no)
);

CREATE TABLE inventory_serial_memberships (
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  serial_id uuid NOT NULL REFERENCES serials(id),
  position_id uuid NOT NULL REFERENCES inventory_positions(id),
  assigned_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, serial_id)
);

CREATE INDEX inventory_serial_memberships_position_idx
  ON inventory_serial_memberships (tenant_id, position_id);

CREATE TABLE inventory_transactions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  transaction_type text NOT NULL CHECK (transaction_type IN ('allocation', 'movement')),
  idempotency_key text NOT NULL,
  source_reference text,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE inventory_transaction_legs (
  transaction_id uuid NOT NULL REFERENCES inventory_transactions(id),
  position_id uuid NOT NULL REFERENCES inventory_positions(id),
  physical_delta numeric(24, 6) NOT NULL DEFAULT 0,
  reserved_delta numeric(24, 6) NOT NULL DEFAULT 0,
  allocated_delta numeric(24, 6) NOT NULL DEFAULT 0,
  PRIMARY KEY (transaction_id, position_id),
  CONSTRAINT inventory_leg_nonzero_ck
    CHECK (physical_delta <> 0 OR reserved_delta <> 0 OR allocated_delta <> 0)
);

CREATE TABLE handling_unit_movements (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  handling_unit_id uuid NOT NULL REFERENCES handling_units(id),
  from_location_id uuid NOT NULL REFERENCES locations(id),
  to_location_id uuid NOT NULL REFERENCES locations(id),
  recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ensure_inventory_position(
  p_candidate_id uuid,
  p_tenant_id uuid,
  p_warehouse_id uuid,
  p_location_id uuid,
  p_handling_unit_id uuid,
  p_item_id uuid,
  p_owner_id uuid,
  p_condition_id uuid,
  p_lot_id uuid DEFAULT NULL,
  p_stock_scope_id uuid DEFAULT NULL,
  p_attribute_set_id uuid DEFAULT NULL,
  p_stock_segment_id uuid DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  v_id uuid;
BEGIN
  IF (p_location_id IS NULL) = (p_handling_unit_id IS NULL) THEN
    RAISE EXCEPTION 'exactly one inventory anchor is required'
      USING ERRCODE = '23514';
  END IF;

  IF p_location_id IS NOT NULL THEN
    PERFORM 1
      FROM locations
     WHERE id = p_location_id
       AND tenant_id = p_tenant_id
       AND warehouse_id = p_warehouse_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'location is not in the requested tenant/warehouse'
        USING ERRCODE = '23503';
    END IF;
  ELSE
    PERFORM 1
      FROM handling_units
     WHERE id = p_handling_unit_id
       AND tenant_id = p_tenant_id
       AND warehouse_id = p_warehouse_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'handling unit is not in the requested tenant/warehouse'
        USING ERRCODE = '23503';
    END IF;
  END IF;

  INSERT INTO inventory_positions (
    id, tenant_id, warehouse_id, location_id, handling_unit_id,
    item_id, owner_id, inventory_condition_id, lot_id,
    stock_scope_id, attribute_set_id, stock_segment_id
  ) VALUES (
    p_candidate_id, p_tenant_id, p_warehouse_id, p_location_id, p_handling_unit_id,
    p_item_id, p_owner_id, p_condition_id, p_lot_id,
    p_stock_scope_id, p_attribute_set_id, p_stock_segment_id
  )
  ON CONFLICT ON CONSTRAINT inventory_position_key_uq
  DO UPDATE SET updated_at = inventory_positions.updated_at
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION allocate_inventory_position(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_position_id uuid,
  p_qty numeric
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id uuid;
  v_existing_type text;
  v_physical numeric;
  v_allocated numeric;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'allocation quantity must be positive'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'allocation', p_idempotency_key, p_position_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT id, transaction_type
      INTO v_existing_id, v_existing_type
      FROM inventory_transactions
     WHERE tenant_id = p_tenant_id
       AND idempotency_key = p_idempotency_key;

    IF v_existing_type <> 'allocation' THEN
      RAISE EXCEPTION 'idempotency key already belongs to %', v_existing_type
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing_id;
  END IF;

  SELECT physical_qty, allocated_qty
    INTO v_physical, v_allocated
    FROM inventory_positions
   WHERE id = p_position_id
     AND tenant_id = p_tenant_id
   FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found'
      USING ERRCODE = '23503';
  END IF;

  IF v_allocated + p_qty > v_physical THEN
    RAISE EXCEPTION 'allocation exceeds physical quantity'
      USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_positions
     SET allocated_qty = allocated_qty + p_qty,
         version = version + 1,
         updated_at = now()
   WHERE id = p_position_id;

  INSERT INTO inventory_transaction_legs (
    transaction_id, position_id, allocated_delta
  ) VALUES (
    p_transaction_id, p_position_id, p_qty
  );

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION transfer_inventory_quantity(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_source_position_id uuid,
  p_target_position_id uuid,
  p_qty numeric
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id uuid;
  v_existing_type text;
  v_locked_count integer;
  v_source inventory_positions%ROWTYPE;
  v_target inventory_positions%ROWTYPE;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'movement quantity must be positive'
      USING ERRCODE = '23514';
  END IF;

  IF p_source_position_id = p_target_position_id THEN
    RAISE EXCEPTION 'source and target positions must differ'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'movement', p_idempotency_key,
    p_source_position_id::text || '->' || p_target_position_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT id, transaction_type
      INTO v_existing_id, v_existing_type
      FROM inventory_transactions
     WHERE tenant_id = p_tenant_id
       AND idempotency_key = p_idempotency_key;

    IF v_existing_type <> 'movement' THEN
      RAISE EXCEPTION 'idempotency key already belongs to %', v_existing_type
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing_id;
  END IF;

  PERFORM id
    FROM inventory_positions
   WHERE tenant_id = p_tenant_id
     AND id IN (p_source_position_id, p_target_position_id)
   ORDER BY id
   FOR UPDATE;

  GET DIAGNOSTICS v_locked_count = ROW_COUNT;
  IF v_locked_count <> 2 THEN
    RAISE EXCEPTION 'source or target inventory position not found'
      USING ERRCODE = '23503';
  END IF;

  SELECT * INTO v_source
    FROM inventory_positions
   WHERE id = p_source_position_id;

  SELECT * INTO v_target
    FROM inventory_positions
   WHERE id = p_target_position_id;

  IF v_source.warehouse_id <> v_target.warehouse_id
     OR v_source.item_id <> v_target.item_id
     OR v_source.owner_id <> v_target.owner_id
     OR v_source.inventory_condition_id <> v_target.inventory_condition_id
     OR v_source.lot_id IS DISTINCT FROM v_target.lot_id
     OR v_source.stock_scope_id IS DISTINCT FROM v_target.stock_scope_id
     OR v_source.attribute_set_id IS DISTINCT FROM v_target.attribute_set_id
     OR v_source.stock_segment_id IS DISTINCT FROM v_target.stock_segment_id THEN
    RAISE EXCEPTION 'movement target differs in non-anchor stock identity'
      USING ERRCODE = '23514';
  END IF;

  IF v_source.physical_qty < p_qty THEN
    RAISE EXCEPTION 'movement exceeds source physical quantity'
      USING ERRCODE = '23514';
  END IF;

  IF v_source.physical_qty - p_qty < GREATEST(v_source.reserved_qty, v_source.allocated_qty) THEN
    RAISE EXCEPTION 'movement would strand exact-position commitments'
      USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_positions
     SET physical_qty = physical_qty - p_qty,
         version = version + 1,
         updated_at = now()
   WHERE id = p_source_position_id;

  UPDATE inventory_positions
     SET physical_qty = physical_qty + p_qty,
         version = version + 1,
         updated_at = now()
   WHERE id = p_target_position_id;

  INSERT INTO inventory_transaction_legs (
    transaction_id, position_id, physical_delta
  ) VALUES
    (p_transaction_id, p_source_position_id, -p_qty),
    (p_transaction_id, p_target_position_id, p_qty);

  RETURN p_transaction_id;
END;
$$;

CREATE OR REPLACE FUNCTION assign_serial_to_position(
  p_tenant_id uuid,
  p_serial_id uuid,
  p_position_id uuid
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_position_item uuid;
  v_physical numeric;
  v_exact boolean;
  v_serial_item uuid;
  v_memberships bigint;
BEGIN
  SELECT p.item_id, p.physical_qty, i.exact_serial_tracking
    INTO v_position_item, v_physical, v_exact
    FROM inventory_positions p
    JOIN items i ON i.id = p.item_id
   WHERE p.id = p_position_id
     AND p.tenant_id = p_tenant_id
   FOR UPDATE OF p;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found'
      USING ERRCODE = '23503';
  END IF;

  IF NOT v_exact THEN
    RAISE EXCEPTION 'item is not configured for exact serial tracking'
      USING ERRCODE = '23514';
  END IF;

  SELECT item_id INTO v_serial_item
    FROM serials
   WHERE id = p_serial_id
     AND tenant_id = p_tenant_id;

  IF NOT FOUND OR v_serial_item <> v_position_item THEN
    RAISE EXCEPTION 'serial does not belong to the position item'
      USING ERRCODE = '23514';
  END IF;

  SELECT count(*) INTO v_memberships
    FROM inventory_serial_memberships
   WHERE tenant_id = p_tenant_id
     AND position_id = p_position_id;

  IF v_memberships + 1 > v_physical THEN
    RAISE EXCEPTION 'serial membership count would exceed physical quantity'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO inventory_serial_memberships (tenant_id, serial_id, position_id)
  VALUES (p_tenant_id, p_serial_id, p_position_id);
END;
$$;

CREATE OR REPLACE FUNCTION relocate_handling_unit(
  p_movement_id uuid,
  p_tenant_id uuid,
  p_handling_unit_id uuid,
  p_to_location_id uuid
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_warehouse_id uuid;
  v_parent_id uuid;
  v_from_location_id uuid;
BEGIN
  SELECT warehouse_id, parent_handling_unit_id, location_id
    INTO v_warehouse_id, v_parent_id, v_from_location_id
    FROM handling_units
   WHERE id = p_handling_unit_id
     AND tenant_id = p_tenant_id
   FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'handling unit not found'
      USING ERRCODE = '23503';
  END IF;

  IF v_parent_id IS NOT NULL OR v_from_location_id IS NULL THEN
    RAISE EXCEPTION 'lab relocation supports top-level HUs only'
      USING ERRCODE = '23514';
  END IF;

  PERFORM 1
    FROM locations
   WHERE id = p_to_location_id
     AND tenant_id = p_tenant_id
     AND warehouse_id = v_warehouse_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'destination location belongs to a different warehouse'
      USING ERRCODE = '23514';
  END IF;

  IF v_from_location_id = p_to_location_id THEN
    RETURN;
  END IF;

  UPDATE handling_units
     SET location_id = p_to_location_id,
         version = version + 1,
         updated_at = now()
   WHERE id = p_handling_unit_id;

  INSERT INTO handling_unit_movements (
    id, tenant_id, handling_unit_id, from_location_id, to_location_id
  ) VALUES (
    p_movement_id, p_tenant_id, p_handling_unit_id, v_from_location_id, p_to_location_id
  );
END;
$$;

COMMIT;
