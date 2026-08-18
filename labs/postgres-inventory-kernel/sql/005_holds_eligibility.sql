BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

ALTER TABLE kernel_lab.inventory_transactions
  DROP CONSTRAINT inventory_transactions_transaction_type_check;

ALTER TABLE kernel_lab.inventory_transactions
  ADD CONSTRAINT inventory_transactions_transaction_type_check
  CHECK (transaction_type IN (
    'allocation',
    'allocation_release',
    'hold_apply',
    'hold_release',
    'movement',
    'reservation',
    'reservation_release'
  ));

CREATE TABLE kernel_lab.inventory_hold_policies (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES kernel_lab.tenants(id),
  code text NOT NULL,
  description text,
  blocks_allocation boolean NOT NULL DEFAULT false,
  blocks_movement boolean NOT NULL DEFAULT false,
  blocks_picking boolean NOT NULL DEFAULT false,
  blocks_shipping boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, code),
  CONSTRAINT inventory_hold_policy_blocks_something_ck
    CHECK (
      blocks_allocation
      OR blocks_movement
      OR blocks_picking
      OR blocks_shipping
    )
);

CREATE TABLE kernel_lab.inventory_holds (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES kernel_lab.tenants(id),
  policy_id uuid NOT NULL REFERENCES kernel_lab.inventory_hold_policies(id),
  position_id uuid REFERENCES kernel_lab.inventory_positions(id),
  warehouse_id uuid NOT NULL REFERENCES kernel_lab.warehouses(id),
  item_id uuid NOT NULL REFERENCES kernel_lab.items(id),
  owner_id uuid NOT NULL REFERENCES kernel_lab.owners(id),
  inventory_condition_id uuid NOT NULL REFERENCES kernel_lab.inventory_conditions(id),
  lot_id uuid REFERENCES kernel_lab.lots(id),
  stock_scope_id uuid REFERENCES kernel_lab.stock_scopes(id),
  attribute_set_id uuid REFERENCES kernel_lab.inventory_attribute_sets(id),
  stock_segment_id uuid REFERENCES kernel_lab.stock_segments(id),
  reason text NOT NULL,
  starts_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  released_at timestamptz,
  created_transaction_id uuid NOT NULL UNIQUE REFERENCES kernel_lab.inventory_transactions(id),
  released_transaction_id uuid UNIQUE REFERENCES kernel_lab.inventory_transactions(id),
  CONSTRAINT inventory_hold_reason_nonempty_ck
    CHECK (btrim(reason) <> ''),
  CONSTRAINT inventory_hold_expiration_ck
    CHECK (expires_at IS NULL OR expires_at > starts_at),
  CONSTRAINT inventory_hold_release_time_ck
    CHECK (released_at IS NULL OR released_transaction_id IS NOT NULL)
);

CREATE INDEX inventory_holds_scope_idx
  ON kernel_lab.inventory_holds (
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
  INCLUDE (policy_id, position_id, starts_at, expires_at, released_at);

CREATE INDEX inventory_holds_position_idx
  ON kernel_lab.inventory_holds (tenant_id, position_id)
  WHERE position_id IS NOT NULL;

CREATE INDEX inventory_holds_active_idx
  ON kernel_lab.inventory_holds (tenant_id, policy_id, starts_at)
  WHERE released_at IS NULL;

CREATE OR REPLACE FUNCTION kernel_lab.validate_inventory_hold_target()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_position kernel_lab.inventory_positions%ROWTYPE;
  v_policy_tenant uuid;
BEGIN
  SELECT tenant_id
    INTO v_policy_tenant
    FROM inventory_hold_policies
   WHERE id = NEW.policy_id;

  IF NOT FOUND OR v_policy_tenant <> NEW.tenant_id THEN
    RAISE EXCEPTION 'hold policy does not belong to the hold tenant'
      USING ERRCODE = '23503';
  END IF;

  IF NEW.position_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT *
    INTO v_position
    FROM inventory_positions
   WHERE id = NEW.position_id
     AND tenant_id = NEW.tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'hold position does not belong to the hold tenant'
      USING ERRCODE = '23503';
  END IF;

  IF v_position.warehouse_id <> NEW.warehouse_id
     OR v_position.item_id <> NEW.item_id
     OR v_position.owner_id <> NEW.owner_id
     OR v_position.inventory_condition_id <> NEW.inventory_condition_id
     OR v_position.lot_id IS DISTINCT FROM NEW.lot_id
     OR v_position.stock_scope_id IS DISTINCT FROM NEW.stock_scope_id
     OR v_position.attribute_set_id IS DISTINCT FROM NEW.attribute_set_id
     OR v_position.stock_segment_id IS DISTINCT FROM NEW.stock_segment_id THEN
    RAISE EXCEPTION 'position hold scope does not match the target InventoryPosition'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER inventory_holds_validate_target_trg
BEFORE INSERT OR UPDATE OF
  tenant_id,
  policy_id,
  position_id,
  warehouse_id,
  item_id,
  owner_id,
  inventory_condition_id,
  lot_id,
  stock_scope_id,
  attribute_set_id,
  stock_segment_id
ON kernel_lab.inventory_holds
FOR EACH ROW
EXECUTE FUNCTION kernel_lab.validate_inventory_hold_target();

CREATE OR REPLACE FUNCTION kernel_lab.inventory_position_action_eligible(
  p_position_id uuid,
  p_action text,
  p_at timestamptz DEFAULT now()
) RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_position kernel_lab.inventory_positions%ROWTYPE;
  v_blocked boolean;
BEGIN
  IF p_action NOT IN ('allocation', 'movement', 'picking', 'shipping') THEN
    RAISE EXCEPTION 'unsupported inventory eligibility action: %', p_action
      USING ERRCODE = '22023';
  END IF;

  SELECT *
    INTO v_position
    FROM inventory_positions
   WHERE id = p_position_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found'
      USING ERRCODE = '23503';
  END IF;

  SELECT EXISTS (
    SELECT 1
      FROM inventory_holds h
      JOIN inventory_hold_policies hp
        ON hp.id = h.policy_id
       AND hp.tenant_id = h.tenant_id
     WHERE h.tenant_id = v_position.tenant_id
       AND h.warehouse_id = v_position.warehouse_id
       AND h.item_id = v_position.item_id
       AND h.owner_id = v_position.owner_id
       AND h.inventory_condition_id = v_position.inventory_condition_id
       AND h.lot_id IS NOT DISTINCT FROM v_position.lot_id
       AND h.stock_scope_id IS NOT DISTINCT FROM v_position.stock_scope_id
       AND h.attribute_set_id IS NOT DISTINCT FROM v_position.attribute_set_id
       AND h.stock_segment_id IS NOT DISTINCT FROM v_position.stock_segment_id
       AND (h.position_id IS NULL OR h.position_id = v_position.id)
       AND h.starts_at <= p_at
       AND h.released_at IS NULL
       AND (h.expires_at IS NULL OR h.expires_at > p_at)
       AND CASE p_action
         WHEN 'allocation' THEN hp.blocks_allocation
         WHEN 'movement' THEN hp.blocks_movement
         WHEN 'picking' THEN hp.blocks_picking
         WHEN 'shipping' THEN hp.blocks_shipping
       END
  ) INTO v_blocked;

  RETURN NOT v_blocked;
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
  kernel_lab.inventory_scope_available_qty(
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

CREATE OR REPLACE FUNCTION kernel_lab.enforce_inventory_allocation_eligibility()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF NEW.allocated_qty <= OLD.allocated_qty THEN
    RETURN NEW;
  END IF;

  PERFORM lock_inventory_scope(
    NEW.tenant_id,
    NEW.warehouse_id,
    NEW.item_id,
    NEW.owner_id,
    NEW.inventory_condition_id,
    NEW.lot_id,
    NEW.stock_scope_id,
    NEW.attribute_set_id,
    NEW.stock_segment_id
  );

  IF NOT inventory_position_action_eligible(NEW.id, 'allocation') THEN
    RAISE EXCEPTION 'inventory position is not eligible for allocation'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER inventory_position_allocation_eligibility_trg
BEFORE UPDATE OF allocated_qty
ON kernel_lab.inventory_positions
FOR EACH ROW
WHEN (NEW.allocated_qty > OLD.allocated_qty)
EXECUTE FUNCTION kernel_lab.enforce_inventory_allocation_eligibility();

CREATE OR REPLACE FUNCTION kernel_lab.apply_inventory_scope_hold(
  p_hold_id uuid,
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_policy_id uuid,
  p_reason text,
  p_warehouse_id uuid,
  p_item_id uuid,
  p_owner_id uuid,
  p_condition_id uuid,
  p_lot_id uuid DEFAULT NULL,
  p_stock_scope_id uuid DEFAULT NULL,
  p_attribute_set_id uuid DEFAULT NULL,
  p_stock_segment_id uuid DEFAULT NULL,
  p_starts_at timestamptz DEFAULT NULL,
  p_expires_at timestamptz DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing kernel_lab.inventory_transactions%ROWTYPE;
  v_request_fingerprint text;
  v_starts_at timestamptz;
BEGIN
  IF btrim(p_reason) = '' THEN
    RAISE EXCEPTION 'hold reason must not be empty'
      USING ERRCODE = '23514';
  END IF;

  v_starts_at := COALESCE(p_starts_at, now());

  IF p_expires_at IS NOT NULL AND p_expires_at <= v_starts_at THEN
    RAISE EXCEPTION 'hold expiration must be after its start time'
      USING ERRCODE = '23514';
  END IF;

  PERFORM lock_inventory_scope(
    p_tenant_id,
    p_warehouse_id,
    p_item_id,
    p_owner_id,
    p_condition_id,
    p_lot_id,
    p_stock_scope_id,
    p_attribute_set_id,
    p_stock_segment_id
  );

  PERFORM 1
    FROM inventory_hold_policies
   WHERE id = p_policy_id
     AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'hold policy not found for tenant'
      USING ERRCODE = '23503';
  END IF;

  v_request_fingerprint := jsonb_build_array(
    'hold-apply-scope',
    p_policy_id,
    p_reason,
    p_warehouse_id,
    p_item_id,
    p_owner_id,
    p_condition_id,
    p_lot_id,
    p_stock_scope_id,
    p_attribute_set_id,
    p_stock_segment_id,
    p_starts_at,
    p_expires_at
  )::text;

  INSERT INTO inventory_transactions (
    id,
    tenant_id,
    transaction_type,
    idempotency_key,
    request_fingerprint,
    source_reference
  ) VALUES (
    p_transaction_id,
    p_tenant_id,
    'hold_apply',
    p_idempotency_key,
    v_request_fingerprint,
    p_hold_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT *
      INTO v_existing
      FROM inventory_transactions
     WHERE tenant_id = p_tenant_id
       AND idempotency_key = p_idempotency_key;

    IF v_existing.transaction_type <> 'hold_apply'
       OR v_existing.request_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing.source_reference::uuid;
  END IF;

  INSERT INTO inventory_holds (
    id,
    tenant_id,
    policy_id,
    position_id,
    warehouse_id,
    item_id,
    owner_id,
    inventory_condition_id,
    lot_id,
    stock_scope_id,
    attribute_set_id,
    stock_segment_id,
    reason,
    starts_at,
    expires_at,
    created_transaction_id
  ) VALUES (
    p_hold_id,
    p_tenant_id,
    p_policy_id,
    NULL,
    p_warehouse_id,
    p_item_id,
    p_owner_id,
    p_condition_id,
    p_lot_id,
    p_stock_scope_id,
    p_attribute_set_id,
    p_stock_segment_id,
    p_reason,
    v_starts_at,
    p_expires_at,
    p_transaction_id
  );

  RETURN p_hold_id;
END;
$$;

CREATE OR REPLACE FUNCTION kernel_lab.apply_inventory_position_hold(
  p_hold_id uuid,
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_policy_id uuid,
  p_reason text,
  p_position_id uuid,
  p_starts_at timestamptz DEFAULT NULL,
  p_expires_at timestamptz DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing kernel_lab.inventory_transactions%ROWTYPE;
  v_position kernel_lab.inventory_positions%ROWTYPE;
  v_request_fingerprint text;
  v_starts_at timestamptz;
BEGIN
  IF btrim(p_reason) = '' THEN
    RAISE EXCEPTION 'hold reason must not be empty'
      USING ERRCODE = '23514';
  END IF;

  SELECT *
    INTO v_position
    FROM inventory_positions
   WHERE id = p_position_id
     AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory position not found'
      USING ERRCODE = '23503';
  END IF;

  v_starts_at := COALESCE(p_starts_at, now());

  IF p_expires_at IS NOT NULL AND p_expires_at <= v_starts_at THEN
    RAISE EXCEPTION 'hold expiration must be after its start time'
      USING ERRCODE = '23514';
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

  PERFORM 1
    FROM inventory_hold_policies
   WHERE id = p_policy_id
     AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'hold policy not found for tenant'
      USING ERRCODE = '23503';
  END IF;

  v_request_fingerprint := jsonb_build_array(
    'hold-apply-position',
    p_policy_id,
    p_reason,
    p_position_id,
    p_starts_at,
    p_expires_at
  )::text;

  INSERT INTO inventory_transactions (
    id,
    tenant_id,
    transaction_type,
    idempotency_key,
    request_fingerprint,
    source_reference
  ) VALUES (
    p_transaction_id,
    p_tenant_id,
    'hold_apply',
    p_idempotency_key,
    v_request_fingerprint,
    p_hold_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT *
      INTO v_existing
      FROM inventory_transactions
     WHERE tenant_id = p_tenant_id
       AND idempotency_key = p_idempotency_key;

    IF v_existing.transaction_type <> 'hold_apply'
       OR v_existing.request_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing.source_reference::uuid;
  END IF;

  INSERT INTO inventory_holds (
    id,
    tenant_id,
    policy_id,
    position_id,
    warehouse_id,
    item_id,
    owner_id,
    inventory_condition_id,
    lot_id,
    stock_scope_id,
    attribute_set_id,
    stock_segment_id,
    reason,
    starts_at,
    expires_at,
    created_transaction_id
  ) VALUES (
    p_hold_id,
    p_tenant_id,
    p_policy_id,
    p_position_id,
    v_position.warehouse_id,
    v_position.item_id,
    v_position.owner_id,
    v_position.inventory_condition_id,
    v_position.lot_id,
    v_position.stock_scope_id,
    v_position.attribute_set_id,
    v_position.stock_segment_id,
    p_reason,
    v_starts_at,
    p_expires_at,
    p_transaction_id
  );

  RETURN p_hold_id;
END;
$$;

CREATE OR REPLACE FUNCTION kernel_lab.release_inventory_hold(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_hold_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_existing kernel_lab.inventory_transactions%ROWTYPE;
  v_hold kernel_lab.inventory_holds%ROWTYPE;
  v_request_fingerprint text;
BEGIN
  SELECT *
    INTO v_hold
    FROM inventory_holds
   WHERE id = p_hold_id
     AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'inventory hold not found'
      USING ERRCODE = '23503';
  END IF;

  PERFORM lock_inventory_scope(
    v_hold.tenant_id,
    v_hold.warehouse_id,
    v_hold.item_id,
    v_hold.owner_id,
    v_hold.inventory_condition_id,
    v_hold.lot_id,
    v_hold.stock_scope_id,
    v_hold.attribute_set_id,
    v_hold.stock_segment_id
  );

  SELECT *
    INTO v_hold
    FROM inventory_holds
   WHERE id = p_hold_id
     AND tenant_id = p_tenant_id
   FOR UPDATE;

  v_request_fingerprint := jsonb_build_array(
    'hold-release',
    p_hold_id
  )::text;

  INSERT INTO inventory_transactions (
    id,
    tenant_id,
    transaction_type,
    idempotency_key,
    request_fingerprint,
    source_reference
  ) VALUES (
    p_transaction_id,
    p_tenant_id,
    'hold_release',
    p_idempotency_key,
    v_request_fingerprint,
    p_hold_id::text
  )
  ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

  IF NOT FOUND THEN
    SELECT *
      INTO v_existing
      FROM inventory_transactions
     WHERE tenant_id = p_tenant_id
       AND idempotency_key = p_idempotency_key;

    IF v_existing.transaction_type <> 'hold_release'
       OR v_existing.request_fingerprint <> v_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key already belongs to a different request'
        USING ERRCODE = '23505';
    END IF;

    RETURN v_existing.id;
  END IF;

  IF v_hold.released_at IS NOT NULL THEN
    RAISE EXCEPTION 'inventory hold is already released'
      USING ERRCODE = '23514';
  END IF;

  UPDATE inventory_holds
     SET released_at = now(),
         released_transaction_id = p_transaction_id
   WHERE id = p_hold_id;

  RETURN p_transaction_id;
END;
$$;

ALTER FUNCTION kernel_lab.transfer_inventory_quantity(
  uuid, uuid, text, uuid, uuid, numeric
) RENAME TO transfer_inventory_quantity_without_hold_eligibility;

CREATE OR REPLACE FUNCTION kernel_lab.transfer_inventory_quantity(
  p_transaction_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_source_position_id uuid,
  p_target_position_id uuid,
  p_qty numeric
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_source kernel_lab.inventory_positions%ROWTYPE;
BEGIN
  SELECT *
    INTO v_source
    FROM inventory_positions
   WHERE id = p_source_position_id
     AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'source inventory position not found'
      USING ERRCODE = '23503';
  END IF;

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

  IF NOT inventory_position_action_eligible(p_source_position_id, 'movement') THEN
    RAISE EXCEPTION 'source inventory position is not eligible for movement'
      USING ERRCODE = '23514';
  END IF;

  IF NOT inventory_position_action_eligible(p_target_position_id, 'movement') THEN
    RAISE EXCEPTION 'target inventory position is not eligible for movement'
      USING ERRCODE = '23514';
  END IF;

  RETURN transfer_inventory_quantity_without_hold_eligibility(
    p_transaction_id,
    p_tenant_id,
    p_idempotency_key,
    p_source_position_id,
    p_target_position_id,
    p_qty
  );
END;
$$;

ALTER FUNCTION kernel_lab.relocate_handling_unit(
  uuid, uuid, uuid, uuid
) RENAME TO relocate_handling_unit_without_hold_eligibility;

CREATE OR REPLACE FUNCTION kernel_lab.relocate_handling_unit(
  p_movement_id uuid,
  p_tenant_id uuid,
  p_handling_unit_id uuid,
  p_to_location_id uuid
) RETURNS void
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_scope text;
BEGIN
  PERFORM 1
    FROM handling_units
   WHERE id = p_handling_unit_id
     AND tenant_id = p_tenant_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'handling unit not found'
      USING ERRCODE = '23503';
  END IF;

  FOR v_scope IN
    WITH RECURSIVE hu_tree AS (
      SELECT id
        FROM handling_units
       WHERE id = p_handling_unit_id
         AND tenant_id = p_tenant_id
      UNION ALL
      SELECT child.id
        FROM handling_units child
        JOIN hu_tree parent
          ON child.parent_handling_unit_id = parent.id
       WHERE child.tenant_id = p_tenant_id
    ),
    scope_keys AS (
      SELECT DISTINCT concat_ws(
        '|',
        p.tenant_id::text,
        p.warehouse_id::text,
        p.item_id::text,
        p.owner_id::text,
        p.inventory_condition_id::text,
        COALESCE(p.lot_id::text, '-'),
        COALESCE(p.stock_scope_id::text, '-'),
        COALESCE(p.attribute_set_id::text, '-'),
        COALESCE(p.stock_segment_id::text, '-')
      ) AS scope_key
      FROM inventory_positions p
      JOIN hu_tree h
        ON h.id = p.handling_unit_id
    )
    SELECT scope_key
      FROM scope_keys
     ORDER BY scope_key
  LOOP
    PERFORM pg_advisory_xact_lock(hashtextextended(v_scope, 0));
  END LOOP;

  IF EXISTS (
    WITH RECURSIVE hu_tree AS (
      SELECT id
        FROM handling_units
       WHERE id = p_handling_unit_id
         AND tenant_id = p_tenant_id
      UNION ALL
      SELECT child.id
        FROM handling_units child
        JOIN hu_tree parent
          ON child.parent_handling_unit_id = parent.id
       WHERE child.tenant_id = p_tenant_id
    )
    SELECT 1
      FROM inventory_positions p
      JOIN hu_tree h
        ON h.id = p.handling_unit_id
     WHERE NOT inventory_position_action_eligible(p.id, 'movement')
  ) THEN
    RAISE EXCEPTION 'handling unit contains inventory that is not eligible for movement'
      USING ERRCODE = '23514';
  END IF;

  PERFORM relocate_handling_unit_without_hold_eligibility(
    p_movement_id,
    p_tenant_id,
    p_handling_unit_id,
    p_to_location_id
  );
END;
$$;

COMMIT;
