BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

-- Existing V1 and V2 events stay on shared delivery unless an authorized,
-- unclaimed individual V2 posting is explicitly staged for scoped delivery.
ALTER TABLE domain_event_outbox
  ADD COLUMN delivery_route text NOT NULL DEFAULT 'shared'
    CHECK (delivery_route IN ('shared', 'tenant')),
  ADD CONSTRAINT domain_event_outbox_tenant_route_contract_ck CHECK (
    delivery_route <> 'tenant' OR
    (event_type = 'inventory.transaction.posted' AND schema_version = 2)
  );

CREATE INDEX domain_event_outbox_tenant_claim_idx
  ON domain_event_outbox (tenant_id, available_at, recorded_at, event_id)
  WHERE published_at IS NULL AND quarantined_at IS NULL
    AND delivery_route = 'tenant';

CREATE TABLE domain_event_outbox_route_actions (
  event_id uuid PRIMARY KEY REFERENCES domain_event_outbox(event_id),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  operator_id text NOT NULL CHECK (btrim(operator_id) <> ''),
  previous_route text NOT NULL CHECK (previous_route = 'shared'),
  new_route text NOT NULL CHECK (new_route = 'tenant'),
  staged_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Route staging is operator-reviewed and fenced against any publisher lease.
-- It never changes event_id, schema version, published facts or existing claims.
CREATE FUNCTION stage_tenant_event_route(
  p_event_id uuid,
  p_tenant_id uuid,
  p_operator_id text
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF p_event_id IS NULL OR p_tenant_id IS NULL
     OR p_operator_id IS NULL OR btrim(p_operator_id) = '' THEN
    RAISE EXCEPTION 'event, tenant and operator identities are required'
      USING ERRCODE = '23514';
  END IF;

  UPDATE domain_event_outbox o
  SET delivery_route = 'tenant'
  WHERE o.event_id = p_event_id
    AND o.tenant_id = p_tenant_id
    AND o.delivery_route = 'shared'
    AND o.event_type = 'inventory.transaction.posted'
    AND o.schema_version = 2
    AND o.published_at IS NULL
    AND o.quarantined_at IS NULL
    AND o.claimed_by IS NULL
    AND o.claim_token IS NULL
    AND o.claimed_until IS NULL;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  INSERT INTO domain_event_outbox_route_actions(
    event_id, tenant_id, operator_id, previous_route, new_route
  ) VALUES(p_event_id, p_tenant_id, p_operator_id, 'shared', 'tenant');

  RETURN true;
END;
$$;

-- Global publisher cannot race and steal an explicitly tenant-staged row.
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
      AND o.delivery_route = 'shared'
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

-- The tenant publisher is a disjoint claim path (same lease, retries, ACK,
-- quarantine contract and envelope identity; different candidate predicate).
CREATE FUNCTION claim_tenant_domain_events(
  p_tenant_id uuid,
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
  IF p_tenant_id IS NULL THEN
    RAISE EXCEPTION 'tenant id is required' USING ERRCODE = '23514';
  END IF;
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
      AND o.delivery_route = 'tenant'
      AND o.tenant_id = p_tenant_id
      AND o.event_type = 'inventory.transaction.posted'
      AND o.schema_version = 2
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


-- Archive shape evolves with live Outbox, but archive metadata columns are
-- historically before new columns. Copy by EXPLICIT column name, not SELECT o.*.
ALTER TABLE domain_event_outbox_archive
  ADD COLUMN delivery_route text NOT NULL DEFAULT 'shared'
    CHECK (delivery_route IN ('shared', 'tenant'));

CREATE TABLE domain_event_outbox_route_action_archive (
  event_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  operator_id text NOT NULL CHECK (btrim(operator_id) <> ''),
  previous_route text NOT NULL CHECK (previous_route = 'shared'),
  new_route text NOT NULL CHECK (new_route = 'tenant'),
  staged_at timestamptz NOT NULL,
  archived_at timestamptz NOT NULL,
  archive_run_id uuid NOT NULL,
  CONSTRAINT domain_event_outbox_route_action_archive_parent_fk
    FOREIGN KEY (event_id, archive_run_id)
    REFERENCES domain_event_outbox_archive (event_id, archive_run_id)
);

CREATE OR REPLACE FUNCTION archive_published_domain_events(
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
AS $
DECLARE
  v_event_ids uuid[];
  v_archive_run_id uuid;
  v_archived_at timestamptz := clock_timestamp();
  v_event_count integer := 0;
  v_action_count integer := 0;
  v_deleted_count integer := 0;
  v_route_count integer := 0;
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
    archive_run_id,
    delivery_route
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
    v_archive_run_id,
    o.delivery_route
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

  -- New route staging audit is archived atomically before deleting its
  -- parent Outbox event. Operator replay counters retain prior semantics.
  INSERT INTO domain_event_outbox_route_action_archive (
    event_id, tenant_id, operator_id, previous_route, new_route,
    staged_at, archived_at, archive_run_id
  )
  SELECT a.event_id, a.tenant_id, a.operator_id, a.previous_route,
         a.new_route, a.staged_at, v_archived_at, v_archive_run_id
  FROM domain_event_outbox_route_actions a
  WHERE a.event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_route_count = ROW_COUNT;

  DELETE FROM domain_event_outbox_route_actions
  WHERE event_id = ANY (v_event_ids);

  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
  IF v_deleted_count <> v_route_count THEN
    RAISE EXCEPTION 'archive route action delete count mismatch';
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
$;


-- A deployment must explicitly grant each restricted DB role the necessary
-- function EXECUTE privileges. PostgreSQL grants PUBLIC by default.
REVOKE ALL ON FUNCTION stage_tenant_event_route(uuid, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION claim_tenant_domain_events(uuid, text, integer, integer) FROM PUBLIC;

COMMIT;
