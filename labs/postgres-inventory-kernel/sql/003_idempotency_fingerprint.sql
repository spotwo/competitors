BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

ALTER TABLE kernel_lab.inventory_transactions
  ADD COLUMN request_fingerprint text NOT NULL;

CREATE OR REPLACE FUNCTION kernel_lab.allocate_inventory_position(
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
  v_existing_fingerprint text;
  v_request_fingerprint text;
  v_physical numeric;
  v_allocated numeric;
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

CREATE OR REPLACE FUNCTION kernel_lab.transfer_inventory_quantity(
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
  v_existing_fingerprint text;
  v_request_fingerprint text;
  v_locked_count integer;
  v_source kernel_lab.inventory_positions%ROWTYPE;
  v_target kernel_lab.inventory_positions%ROWTYPE;
BEGIN
  IF p_qty <= 0 THEN
    RAISE EXCEPTION 'movement quantity must be positive'
      USING ERRCODE = '23514';
  END IF;

  IF p_source_position_id = p_target_position_id THEN
    RAISE EXCEPTION 'source and target positions must differ'
      USING ERRCODE = '23514';
  END IF;

  v_request_fingerprint := format(
    'movement:%s:%s:%s',
    p_source_position_id,
    p_target_position_id,
    p_qty::numeric(24, 6)::text
  );

  INSERT INTO inventory_transactions (
    id, tenant_id, transaction_type, idempotency_key, request_fingerprint, source_reference
  ) VALUES (
    p_transaction_id, p_tenant_id, 'movement', p_idempotency_key,
    v_request_fingerprint,
    p_source_position_id::text || '->' || p_target_position_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT id, transaction_type, request_fingerprint
      INTO v_existing_id, v_existing_type, v_existing_fingerprint
      FROM inventory_transactions
     WHERE tenant_id = p_tenant_id
       AND idempotency_key = p_idempotency_key;

    IF v_existing_type <> 'movement'
       OR v_existing_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
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

ALTER FUNCTION kernel_lab.allocate_inventory_position(
  uuid, uuid, text, uuid, numeric
) SET search_path = kernel_lab, pg_catalog, pg_temp;

ALTER FUNCTION kernel_lab.transfer_inventory_quantity(
  uuid, uuid, text, uuid, uuid, numeric
) SET search_path = kernel_lab, pg_catalog, pg_temp;

COMMIT;
