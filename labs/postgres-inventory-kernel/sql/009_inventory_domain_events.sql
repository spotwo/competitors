BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION emit_inventory_transaction_events()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  PERFORM enqueue_domain_event(
    NEW.tenant_id,
    format('inventory.transaction.posted:%s', NEW.id),
    'inventory.transaction.posted',
    format('inventory-transaction/%s', NEW.id),
    'InventoryTransaction',
    NEW.id::text,
    1,
    jsonb_strip_nulls(jsonb_build_object(
      'transaction_id', NEW.id,
      'transaction_type', NEW.transaction_type,
      'source_reference', NEW.source_reference
    )),
    NEW.recorded_at
  );

  IF NEW.transaction_type = 'reservation' THEN
    PERFORM enqueue_domain_event(
      NEW.tenant_id,
      format('inventory.reservation.created:%s', NEW.source_reference),
      'inventory.reservation.created',
      format('inventory-reservation/%s', NEW.source_reference),
      'InventoryReservation',
      NEW.source_reference,
      1,
      jsonb_build_object('transaction_id', NEW.id, 'reservation_id', NEW.source_reference),
      NEW.recorded_at
    );
  ELSIF NEW.transaction_type = 'reservation_release' THEN
    PERFORM enqueue_domain_event(
      NEW.tenant_id,
      format('inventory.reservation.released:%s', NEW.source_reference),
      'inventory.reservation.released',
      format('inventory-reservation/%s', NEW.source_reference),
      'InventoryReservation',
      NEW.source_reference,
      2,
      jsonb_build_object('transaction_id', NEW.id, 'reservation_id', NEW.source_reference),
      NEW.recorded_at
    );
  ELSIF NEW.transaction_type = 'allocation'
        AND NEW.request_fingerprint LIKE 'allocation-commitment:%' THEN
    PERFORM enqueue_domain_event(
      NEW.tenant_id,
      format('inventory.allocation.created:%s', NEW.source_reference),
      'inventory.allocation.created',
      format('inventory-allocation/%s', NEW.source_reference),
      'InventoryAllocation',
      NEW.source_reference,
      1,
      jsonb_build_object('transaction_id', NEW.id, 'allocation_id', NEW.source_reference),
      NEW.recorded_at
    );
  ELSIF NEW.transaction_type = 'allocation_release' THEN
    PERFORM enqueue_domain_event(
      NEW.tenant_id,
      format('inventory.allocation.released:%s', NEW.source_reference),
      'inventory.allocation.released',
      format('inventory-allocation/%s', NEW.source_reference),
      'InventoryAllocation',
      NEW.source_reference,
      2,
      jsonb_build_object('transaction_id', NEW.id, 'allocation_id', NEW.source_reference),
      NEW.recorded_at
    );
  ELSIF NEW.transaction_type = 'movement' THEN
    PERFORM enqueue_domain_event(
      NEW.tenant_id,
      format('inventory.movement.confirmed:%s', NEW.id),
      'inventory.movement.confirmed',
      format('inventory-transaction/%s', NEW.id),
      'InventoryTransaction',
      NEW.id::text,
      1,
      jsonb_strip_nulls(jsonb_build_object(
        'transaction_id', NEW.id,
        'source_reference', NEW.source_reference
      )),
      NEW.recorded_at
    );
  ELSIF NEW.transaction_type = 'count_reconciliation' THEN
    PERFORM enqueue_domain_event(
      NEW.tenant_id,
      format('inventory.reconciliation.posted:%s', NEW.id),
      'inventory.reconciliation.posted',
      format('inventory-transaction/%s', NEW.id),
      'InventoryTransaction',
      NEW.id::text,
      1,
      jsonb_strip_nulls(jsonb_build_object(
        'transaction_id', NEW.id,
        'source_reference', NEW.source_reference
      )),
      NEW.recorded_at
    );
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER inventory_transaction_domain_events_trg
AFTER INSERT ON inventory_transactions
FOR EACH ROW
EXECUTE FUNCTION emit_inventory_transaction_events();

CREATE OR REPLACE FUNCTION emit_inventory_position_changed_event()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_transaction inventory_transactions%ROWTYPE;
  v_position inventory_positions%ROWTYPE;
BEGIN
  SELECT * INTO v_transaction
  FROM inventory_transactions
  WHERE id = NEW.transaction_id;

  SELECT * INTO v_position
  FROM inventory_positions
  WHERE id = NEW.position_id;

  PERFORM enqueue_domain_event(
    v_transaction.tenant_id,
    format('inventory.position.changed:%s:%s', NEW.transaction_id, NEW.position_id),
    'inventory.position.changed',
    format('inventory-position/%s', NEW.position_id),
    'InventoryPosition',
    NEW.position_id::text,
    v_position.version,
    jsonb_build_object(
      'transaction_id', NEW.transaction_id,
      'transaction_type', v_transaction.transaction_type,
      'position_id', NEW.position_id,
      'physical_delta', NEW.physical_delta,
      'reserved_delta', NEW.reserved_delta,
      'allocated_delta', NEW.allocated_delta
    ),
    v_transaction.recorded_at,
    NULL,
    NULL,
    NULL,
    v_position.warehouse_id
  );

  RETURN NEW;
END;
$$;

CREATE TRIGGER inventory_position_domain_event_trg
AFTER INSERT ON inventory_transaction_legs
FOR EACH ROW
EXECUTE FUNCTION emit_inventory_position_changed_event();

COMMIT;
