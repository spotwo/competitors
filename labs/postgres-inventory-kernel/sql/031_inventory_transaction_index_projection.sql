BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

-- A disposable read model for transaction search, not a replacement for the
-- immutable Inventory Transaction ledger. Inbox and projection commit together.
CREATE TABLE inventory_transaction_index_projection (
  consumer_name text NOT NULL CHECK (btrim(consumer_name) <> ''),
  transaction_id uuid NOT NULL REFERENCES inventory_transactions(id),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  transaction_type text NOT NULL CHECK (btrim(transaction_type) <> ''),
  source_reference text,
  occurred_at timestamptz NOT NULL,
  event_id uuid NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer_name, transaction_id),
  UNIQUE (consumer_name, event_id)
);

CREATE INDEX inventory_transaction_index_scope_idx
  ON inventory_transaction_index_projection
  (consumer_name, tenant_id, occurred_at DESC, transaction_id);

COMMIT;
