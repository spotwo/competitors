BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE domain_event_outbox (
  event_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  dedup_key text NOT NULL,
  event_type text NOT NULL,
  source text NOT NULL DEFAULT 'spotwo.wms.inventory-kernel',
  subject text NOT NULL,
  occurred_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  aggregate_version bigint NOT NULL CHECK (aggregate_version >= 1),
  causation_id text,
  correlation_id text,
  actor jsonb,
  warehouse_id uuid REFERENCES warehouses(id),
  schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
  data jsonb NOT NULL,
  available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  claimed_by text,
  claim_token uuid,
  claimed_until timestamptz,
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  last_error text,
  published_at timestamptz,
  CONSTRAINT domain_event_outbox_dedup_uq UNIQUE (tenant_id, dedup_key),
  CONSTRAINT domain_event_outbox_claim_triplet_ck CHECK (
    (claimed_by IS NULL AND claim_token IS NULL AND claimed_until IS NULL)
    OR
    (claimed_by IS NOT NULL AND claim_token IS NOT NULL AND claimed_until IS NOT NULL)
  ),
  CONSTRAINT domain_event_outbox_published_unclaimed_ck CHECK (
    published_at IS NULL
    OR (claimed_by IS NULL AND claim_token IS NULL AND claimed_until IS NULL)
  )
);

CREATE INDEX domain_event_outbox_ready_idx
  ON domain_event_outbox (available_at, recorded_at, event_id)
  WHERE published_at IS NULL;

CREATE INDEX domain_event_outbox_aggregate_idx
  ON domain_event_outbox (
    tenant_id, aggregate_type, aggregate_id, aggregate_version, recorded_at
  );

CREATE TABLE domain_event_inbox (
  consumer_name text NOT NULL,
  event_id uuid NOT NULL,
  first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (consumer_name, event_id)
);

CREATE OR REPLACE FUNCTION enqueue_domain_event(
  p_tenant_id uuid,
  p_dedup_key text,
  p_event_type text,
  p_subject text,
  p_aggregate_type text,
  p_aggregate_id text,
  p_aggregate_version bigint,
  p_data jsonb,
  p_occurred_at timestamptz DEFAULT clock_timestamp(),
  p_causation_id text DEFAULT NULL,
  p_correlation_id text DEFAULT NULL,
  p_actor jsonb DEFAULT NULL,
  p_warehouse_id uuid DEFAULT NULL,
  p_schema_version integer DEFAULT 1
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_event_id uuid;
  v_existing domain_event_outbox%ROWTYPE;
BEGIN
  IF p_dedup_key IS NULL OR btrim(p_dedup_key) = '' THEN
    RAISE EXCEPTION 'domain event dedup key is required' USING ERRCODE = '23514';
  END IF;
  IF p_event_type IS NULL OR btrim(p_event_type) = '' THEN
    RAISE EXCEPTION 'domain event type is required' USING ERRCODE = '23514';
  END IF;
  IF p_subject IS NULL OR btrim(p_subject) = '' THEN
    RAISE EXCEPTION 'domain event subject is required' USING ERRCODE = '23514';
  END IF;
  IF p_aggregate_type IS NULL OR btrim(p_aggregate_type) = ''
     OR p_aggregate_id IS NULL OR btrim(p_aggregate_id) = '' THEN
    RAISE EXCEPTION 'domain event aggregate identity is required' USING ERRCODE = '23514';
  END IF;
  IF p_aggregate_version < 1 THEN
    RAISE EXCEPTION 'domain event aggregate version must be positive' USING ERRCODE = '23514';
  END IF;

  INSERT INTO domain_event_outbox (
    tenant_id, dedup_key, event_type, subject, occurred_at,
    aggregate_type, aggregate_id, aggregate_version,
    causation_id, correlation_id, actor, warehouse_id, schema_version, data
  ) VALUES (
    p_tenant_id, p_dedup_key, p_event_type, p_subject, p_occurred_at,
    p_aggregate_type, p_aggregate_id, p_aggregate_version,
    p_causation_id, p_correlation_id, p_actor, p_warehouse_id, p_schema_version, p_data
  )
  ON CONFLICT (tenant_id, dedup_key) DO NOTHING
  RETURNING event_id INTO v_event_id;

  IF FOUND THEN
    RETURN v_event_id;
  END IF;

  SELECT * INTO v_existing
  FROM domain_event_outbox
  WHERE tenant_id = p_tenant_id
    AND dedup_key = p_dedup_key;

  IF v_existing.event_type <> p_event_type
     OR v_existing.subject <> p_subject
     OR v_existing.aggregate_type <> p_aggregate_type
     OR v_existing.aggregate_id <> p_aggregate_id
     OR v_existing.aggregate_version <> p_aggregate_version
     OR v_existing.causation_id IS DISTINCT FROM p_causation_id
     OR v_existing.correlation_id IS DISTINCT FROM p_correlation_id
     OR v_existing.actor IS DISTINCT FROM p_actor
     OR v_existing.warehouse_id IS DISTINCT FROM p_warehouse_id
     OR v_existing.schema_version <> p_schema_version
     OR v_existing.data <> p_data THEN
    RAISE EXCEPTION 'domain event dedup key already belongs to a different event payload'
      USING ERRCODE = '23505';
  END IF;

  RETURN v_existing.event_id;
END;
$$;

COMMIT;
