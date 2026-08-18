BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

CREATE TABLE domain_event_idempotency_keys (
  tenant_id uuid NOT NULL,
  dedup_key text NOT NULL,
  event_id uuid NOT NULL DEFAULT uuidv7(),
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
  warehouse_id uuid,
  schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
  data jsonb NOT NULL,
  PRIMARY KEY (tenant_id, dedup_key),
  CONSTRAINT domain_event_idempotency_keys_event_uq UNIQUE (event_id),
  CONSTRAINT domain_event_idempotency_keys_identity_uq UNIQUE (
    tenant_id, dedup_key, event_id
  )
);

INSERT INTO domain_event_idempotency_keys (
  tenant_id,
  dedup_key,
  event_id,
  event_type,
  source,
  subject,
  occurred_at,
  recorded_at,
  aggregate_type,
  aggregate_id,
  aggregate_version,
  causation_id,
  correlation_id,
  actor,
  warehouse_id,
  schema_version,
  data
)
SELECT
  tenant_id,
  dedup_key,
  event_id,
  event_type,
  source,
  subject,
  occurred_at,
  recorded_at,
  aggregate_type,
  aggregate_id,
  aggregate_version,
  causation_id,
  correlation_id,
  actor,
  warehouse_id,
  schema_version,
  data
FROM domain_event_outbox;

ALTER TABLE domain_event_outbox
  ADD CONSTRAINT domain_event_outbox_idempotency_key_fk
  FOREIGN KEY (tenant_id, dedup_key, event_id)
  REFERENCES domain_event_idempotency_keys (tenant_id, dedup_key, event_id);

CREATE INDEX domain_event_outbox_published_retention_idx
  ON domain_event_outbox (published_at, event_id)
  WHERE published_at IS NOT NULL;

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
  v_key domain_event_idempotency_keys%ROWTYPE;
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
  IF p_aggregate_version IS NULL OR p_aggregate_version < 1 THEN
    RAISE EXCEPTION 'domain event aggregate version must be positive' USING ERRCODE = '23514';
  END IF;
  IF p_schema_version IS NULL OR p_schema_version < 1 THEN
    RAISE EXCEPTION 'domain event schema version must be positive' USING ERRCODE = '23514';
  END IF;
  IF p_data IS NULL THEN
    RAISE EXCEPTION 'domain event data is required' USING ERRCODE = '23514';
  END IF;

  INSERT INTO domain_event_idempotency_keys (
    tenant_id,
    dedup_key,
    event_type,
    subject,
    occurred_at,
    aggregate_type,
    aggregate_id,
    aggregate_version,
    causation_id,
    correlation_id,
    actor,
    warehouse_id,
    schema_version,
    data
  ) VALUES (
    p_tenant_id,
    p_dedup_key,
    p_event_type,
    p_subject,
    p_occurred_at,
    p_aggregate_type,
    p_aggregate_id,
    p_aggregate_version,
    p_causation_id,
    p_correlation_id,
    p_actor,
    p_warehouse_id,
    p_schema_version,
    p_data
  )
  ON CONFLICT (tenant_id, dedup_key) DO NOTHING
  RETURNING * INTO v_key;

  IF FOUND THEN
    INSERT INTO domain_event_outbox (
      event_id,
      tenant_id,
      dedup_key,
      event_type,
      source,
      subject,
      occurred_at,
      recorded_at,
      aggregate_type,
      aggregate_id,
      aggregate_version,
      causation_id,
      correlation_id,
      actor,
      warehouse_id,
      schema_version,
      data
    ) VALUES (
      v_key.event_id,
      v_key.tenant_id,
      v_key.dedup_key,
      v_key.event_type,
      v_key.source,
      v_key.subject,
      v_key.occurred_at,
      v_key.recorded_at,
      v_key.aggregate_type,
      v_key.aggregate_id,
      v_key.aggregate_version,
      v_key.causation_id,
      v_key.correlation_id,
      v_key.actor,
      v_key.warehouse_id,
      v_key.schema_version,
      v_key.data
    );

    RETURN v_key.event_id;
  END IF;

  SELECT * INTO STRICT v_key
  FROM domain_event_idempotency_keys
  WHERE tenant_id = p_tenant_id
    AND dedup_key = p_dedup_key;

  IF v_key.event_type <> p_event_type
     OR v_key.subject <> p_subject
     OR v_key.aggregate_type <> p_aggregate_type
     OR v_key.aggregate_id <> p_aggregate_id
     OR v_key.aggregate_version <> p_aggregate_version
     OR v_key.causation_id IS DISTINCT FROM p_causation_id
     OR v_key.correlation_id IS DISTINCT FROM p_correlation_id
     OR v_key.actor IS DISTINCT FROM p_actor
     OR v_key.warehouse_id IS DISTINCT FROM p_warehouse_id
     OR v_key.schema_version <> p_schema_version
     OR v_key.data IS DISTINCT FROM p_data THEN
    RAISE EXCEPTION 'domain event dedup key already belongs to a different event payload'
      USING ERRCODE = '23505';
  END IF;

  RETURN v_key.event_id;
END;
$$;

CREATE TABLE domain_event_outbox_archive (
  event_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  dedup_key text NOT NULL,
  event_type text NOT NULL,
  source text NOT NULL,
  subject text NOT NULL,
  occurred_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  aggregate_version bigint NOT NULL CHECK (aggregate_version >= 1),
  causation_id text,
  correlation_id text,
  actor jsonb,
  warehouse_id uuid,
  schema_version integer NOT NULL CHECK (schema_version >= 1),
  data jsonb NOT NULL,
  available_at timestamptz NOT NULL,
  claimed_by text,
  claim_token uuid,
  claimed_until timestamptz,
  attempt_count integer NOT NULL CHECK (attempt_count >= 0),
  last_error text,
  published_at timestamptz NOT NULL,
  last_failed_at timestamptz,
  quarantined_at timestamptz,
  quarantine_reason text,
  archived_at timestamptz NOT NULL,
  archive_run_id uuid NOT NULL,
  CONSTRAINT domain_event_outbox_archive_dedup_uq UNIQUE (tenant_id, dedup_key),
  CONSTRAINT domain_event_outbox_archive_run_uq UNIQUE (event_id, archive_run_id),
  CONSTRAINT domain_event_outbox_archive_identity_fk
    FOREIGN KEY (tenant_id, dedup_key, event_id)
    REFERENCES domain_event_idempotency_keys (tenant_id, dedup_key, event_id),
  CONSTRAINT domain_event_outbox_archive_published_ck CHECK (
    archived_at >= published_at
  ),
  CONSTRAINT domain_event_outbox_archive_unclaimed_ck CHECK (
    claimed_by IS NULL AND claim_token IS NULL AND claimed_until IS NULL
  ),
  CONSTRAINT domain_event_outbox_archive_not_quarantined_ck CHECK (
    quarantined_at IS NULL AND quarantine_reason IS NULL
  )
);

CREATE INDEX domain_event_outbox_archive_published_idx
  ON domain_event_outbox_archive (published_at, event_id);

CREATE INDEX domain_event_outbox_archive_run_idx
  ON domain_event_outbox_archive (archive_run_id, event_id);

CREATE INDEX domain_event_outbox_archive_aggregate_idx
  ON domain_event_outbox_archive (
    tenant_id, aggregate_type, aggregate_id, aggregate_version, recorded_at
  );

CREATE TABLE domain_event_outbox_operator_action_archive (
  action_id uuid PRIMARY KEY,
  event_id uuid NOT NULL,
  action text NOT NULL CHECK (action = 'replay'),
  operator_id text NOT NULL CHECK (btrim(operator_id) <> ''),
  reason text NOT NULL CHECK (btrim(reason) <> ''),
  previous_attempt_count integer NOT NULL CHECK (previous_attempt_count >= 1),
  previous_last_error text,
  previous_quarantine_reason text NOT NULL,
  acted_at timestamptz NOT NULL,
  archived_at timestamptz NOT NULL,
  archive_run_id uuid NOT NULL,
  CONSTRAINT domain_event_outbox_operator_action_archive_event_fk
    FOREIGN KEY (event_id, archive_run_id)
    REFERENCES domain_event_outbox_archive (event_id, archive_run_id)
);

CREATE INDEX domain_event_outbox_operator_action_archive_event_idx
  ON domain_event_outbox_operator_action_archive (event_id, acted_at);

CREATE INDEX domain_event_outbox_operator_action_archive_run_idx
  ON domain_event_outbox_operator_action_archive (archive_run_id, action_id);

CREATE FUNCTION archive_published_domain_events(
  p_before timestamptz,
  p_limit integer DEFAULT 1000
) RETURNS TABLE (
  archive_run_id uuid,
  archived_at timestamptz,
  archived_event_count integer,
  archived_operator_action_count integer
)
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_event_ids uuid[];
  v_archive_run_id uuid;
  v_archived_at timestamptz := clock_timestamp();
  v_event_count integer := 0;
  v_action_count integer := 0;
  v_deleted_count integer := 0;
BEGIN
  IF p_before IS NULL THEN
    RAISE EXCEPTION 'archive cutoff is required' USING ERRCODE = '23514';
  END IF;
  IF p_before > statement_timestamp() THEN
    RAISE EXCEPTION 'archive cutoff cannot be in the future' USING ERRCODE = '23514';
  END IF;
  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
    RAISE EXCEPTION 'archive limit must be between 1 and 1000' USING ERRCODE = '23514';
  END IF;

  SELECT array_agg(candidate.event_id ORDER BY candidate.published_at, candidate.event_id)
  INTO v_event_ids
  FROM (
    SELECT o.event_id, o.published_at
    FROM domain_event_outbox o
    WHERE o.published_at IS NOT NULL
      AND o.published_at < p_before
    ORDER BY o.published_at, o.event_id
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  ) candidate;

  IF v_event_ids IS NULL THEN
    RETURN QUERY SELECT NULL::uuid, v_archived_at, 0, 0;
    RETURN;
  END IF;

  v_archive_run_id := uuidv7();

  INSERT INTO domain_event_outbox_archive (
    event_id,
    tenant_id,
    dedup_key,
    event_type,
    source,
    subject,
    occurred_at,
    recorded_at,
    aggregate_type,
    aggregate_id,
    aggregate_version,
    causation_id,
    correlation_id,
    actor,
    warehouse_id,
    schema_version,
    data,
    available_at,
    claimed_by,
    claim_token,
    claimed_until,
    attempt_count,
    last_error,
    published_at,
    last_failed_at,
    quarantined_at,
    quarantine_reason,
    archived_at,
    archive_run_id
  )
  SELECT
    o.event_id,
    o.tenant_id,
    o.dedup_key,
    o.event_type,
    o.source,
    o.subject,
    o.occurred_at,
    o.recorded_at,
    o.aggregate_type,
    o.aggregate_id,
    o.aggregate_version,
    o.causation_id,
    o.correlation_id,
    o.actor,
    o.warehouse_id,
    o.schema_version,
    o.data,
    o.available_at,
    o.claimed_by,
    o.claim_token,
    o.claimed_until,
    o.attempt_count,
    o.last_error,
    o.published_at,
    o.last_failed_at,
    o.quarantined_at,
    o.quarantine_reason,
    v_archived_at,
    v_archive_run_id
  FROM domain_event_outbox o
  WHERE o.event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_event_count = ROW_COUNT;
  IF v_event_count <> cardinality(v_event_ids) THEN
    RAISE EXCEPTION 'archive event copy count mismatch';
  END IF;

  INSERT INTO domain_event_outbox_operator_action_archive (
    action_id,
    event_id,
    action,
    operator_id,
    reason,
    previous_attempt_count,
    previous_last_error,
    previous_quarantine_reason,
    acted_at,
    archived_at,
    archive_run_id
  )
  SELECT
    a.action_id,
    a.event_id,
    a.action,
    a.operator_id,
    a.reason,
    a.previous_attempt_count,
    a.previous_last_error,
    a.previous_quarantine_reason,
    a.acted_at,
    v_archived_at,
    v_archive_run_id
  FROM domain_event_outbox_operator_actions a
  WHERE a.event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_action_count = ROW_COUNT;

  DELETE FROM domain_event_outbox_operator_actions
  WHERE event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
  IF v_deleted_count <> v_action_count THEN
    RAISE EXCEPTION 'archive operator action delete count mismatch';
  END IF;

  DELETE FROM domain_event_outbox
  WHERE event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
  IF v_deleted_count <> v_event_count THEN
    RAISE EXCEPTION 'archive event delete count mismatch';
  END IF;

  RETURN QUERY
  SELECT v_archive_run_id, v_archived_at, v_event_count, v_action_count;
END;
$$;

CREATE OR REPLACE FUNCTION ack_domain_event(
  p_event_id uuid,
  p_claim_token uuid
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM domain_event_outbox
    WHERE event_id = p_event_id
      AND published_at IS NOT NULL
  ) OR EXISTS (
    SELECT 1
    FROM domain_event_outbox_archive
    WHERE event_id = p_event_id
  ) THEN
    RETURN true;
  END IF;

  UPDATE domain_event_outbox
  SET published_at = clock_timestamp(),
      claimed_by = NULL,
      claim_token = NULL,
      claimed_until = NULL,
      last_error = NULL
  WHERE event_id = p_event_id
    AND published_at IS NULL
    AND claim_token = p_claim_token
    AND claimed_until > clock_timestamp();

  RETURN FOUND;
END;
$$;

COMMIT;
