BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

-- Contract V2 for new inventory.transaction.posted facts only.
-- Do not mutate/re-encode old V1 Outbox rows: in-flight retries and replays
-- must preserve the exact envelope version originally committed.
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
    NEW.recorded_at,
    NULL, NULL, NULL, NULL,
    2
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

-- Project the already-durable Outbox tenant identity into the published
-- envelope only for V2+. Other existing V1 event types retain their shape.
CREATE OR REPLACE FUNCTION claim_domain_events(
  p_worker_id text,
  p_limit integer DEFAULT 100,
  p_lease_seconds integer DEFAULT 30
) RETURNS TABLE (
  event_id uuid,
  claim_token uuid,
  attempt_count integer,
  event_type text,
  aggregate_type text,
  aggregate_id text,
  aggregate_version bigint,
  envelope jsonb
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_worker_id IS NULL OR btrim(p_worker_id) = '' THEN
    RAISE EXCEPTION 'publisher worker id is required' USING ERRCODE = '23514';
  END IF;
  IF p_limit < 1 OR p_limit > 1000 THEN
    RAISE EXCEPTION 'claim limit must be between 1 and 1000' USING ERRCODE = '23514';
  END IF;
  IF p_lease_seconds < 1 OR p_lease_seconds > 3600 THEN
    RAISE EXCEPTION 'lease seconds must be between 1 and 3600' USING ERRCODE = '23514';
  END IF;

  RETURN QUERY
  WITH candidates AS (
    SELECT o.event_id
    FROM domain_event_outbox o
    WHERE o.published_at IS NULL
      AND o.quarantined_at IS NULL
      AND o.available_at <= clock_timestamp()
      AND (o.claimed_until IS NULL OR o.claimed_until <= clock_timestamp())
    ORDER BY o.recorded_at, o.event_id
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  ), claimed AS (
    UPDATE domain_event_outbox o
    SET claimed_by = p_worker_id,
        claim_token = uuidv7(),
        claimed_until = clock_timestamp() + make_interval(secs => p_lease_seconds),
        attempt_count = o.attempt_count + 1
    FROM candidates c
    WHERE o.event_id = c.event_id
    RETURNING o.*
  )
  SELECT
    c.event_id,
    c.claim_token,
    c.attempt_count,
    c.event_type,
    c.aggregate_type,
    c.aggregate_id,
    c.aggregate_version,
    jsonb_strip_nulls(jsonb_build_object(
      'event_id', c.event_id,
      'tenant_id', CASE WHEN c.schema_version >= 2 THEN c.tenant_id ELSE NULL END,
      'type', c.event_type,
      'source', c.source,
      'subject', c.subject,
      'occurred_at', c.occurred_at,
      'recorded_at', c.recorded_at,
      'aggregate_type', c.aggregate_type,
      'aggregate_id', c.aggregate_id,
      'aggregate_version', c.aggregate_version,
      'causation_id', c.causation_id,
      'correlation_id', c.correlation_id,
      'actor', c.actor,
      'warehouse_id', c.warehouse_id,
      'schema_version', c.schema_version,
      'data', c.data
    )) AS envelope
  FROM claimed c
  ORDER BY c.recorded_at, c.event_id;
END;
$$;

-- The Transaction Index is a disposable read model that must not depend on
-- FKs to the producer ledger/tables, including its tenants table.
ALTER TABLE inventory_transaction_index_projection
  DROP CONSTRAINT inventory_transaction_index_projection_transaction_id_fkey,
  DROP CONSTRAINT inventory_transaction_index_projection_tenant_id_fkey,
  DROP CONSTRAINT inventory_transaction_index_projection_pkey;

ALTER TABLE inventory_transaction_index_projection
  ADD CONSTRAINT inventory_transaction_index_projection_pkey
  PRIMARY KEY (consumer_name, tenant_id, transaction_id);

COMMIT;
