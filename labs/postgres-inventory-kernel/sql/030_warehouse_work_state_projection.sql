BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

-- Warehouse Work has an internal mutation version that can advance without a
-- externally meaningful state transition. Keep a separate contiguous event
-- stream version so consumers never infer ordering from unrelated mutations.
ALTER TABLE warehouse_works
  ADD COLUMN domain_event_version bigint NOT NULL DEFAULT 0
    CHECK (domain_event_version >= 0);

CREATE TABLE warehouse_work_state_projection (
  consumer_name text NOT NULL,
  work_id uuid NOT NULL,
  tenant_id uuid NOT NULL,
  warehouse_id uuid NOT NULL,
  capability text NOT NULL,
  domain_reference text NOT NULL,
  state text NOT NULL
    CHECK (state IN ('planned', 'released', 'assigned', 'in_progress', 'exception', 'completed', 'cancelled')),
  aggregate_version bigint NOT NULL CHECK (aggregate_version >= 1),
  work_version bigint NOT NULL CHECK (work_version >= 1),
  last_event_id uuid NOT NULL,
  last_recorded_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer_name, work_id)
);

CREATE INDEX warehouse_work_state_projection_scope_idx
  ON warehouse_work_state_projection (
    consumer_name, tenant_id, warehouse_id, state, capability
  );

CREATE OR REPLACE FUNCTION prepare_warehouse_work_domain_event_version()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF NEW.state IS DISTINCT FROM OLD.state THEN
    IF NEW.version <> OLD.version + 1 THEN
      RAISE EXCEPTION 'warehouse work state transition must advance internal version exactly once'
        USING ERRCODE = '23514';
    END IF;
    IF NEW.domain_event_version <> OLD.domain_event_version THEN
      RAISE EXCEPTION 'warehouse work domain event version is trigger-managed'
        USING ERRCODE = '23514';
    END IF;
    NEW.domain_event_version := OLD.domain_event_version + 1;
  ELSIF NEW.domain_event_version <> OLD.domain_event_version THEN
    RAISE EXCEPTION 'warehouse work domain event version may only advance on state transition'
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER warehouse_work_prepare_domain_event_version
BEFORE UPDATE ON warehouse_works
FOR EACH ROW
EXECUTE FUNCTION prepare_warehouse_work_domain_event_version();

CREATE OR REPLACE FUNCTION enqueue_warehouse_work_state_changed_event()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  IF NEW.domain_event_version = OLD.domain_event_version THEN
    RETURN NEW;
  END IF;

  PERFORM enqueue_domain_event(
    NEW.tenant_id,
    format('warehouse.work.state.changed:%s:%s', NEW.id, NEW.domain_event_version),
    'warehouse.work.state.changed',
    format('warehouse-work/%s', NEW.id),
    'WarehouseWork',
    NEW.id::text,
    NEW.domain_event_version,
    jsonb_build_object(
      'work_id', NEW.id,
      'warehouse_id', NEW.warehouse_id,
      'capability', NEW.capability,
      'domain_reference', NEW.domain_reference,
      'previous_state', OLD.state,
      'state', NEW.state,
      'previous_work_version', OLD.version,
      'work_version', NEW.version
    ),
    clock_timestamp(),
    NULL,
    NULL,
    NULL,
    NEW.warehouse_id,
    1
  );

  RETURN NEW;
END;
$$;

CREATE TRIGGER warehouse_work_state_domain_event_outbox
AFTER UPDATE ON warehouse_works
FOR EACH ROW
EXECUTE FUNCTION enqueue_warehouse_work_state_changed_event();

CREATE OR REPLACE FUNCTION apply_warehouse_work_state_event(
  p_consumer_name text,
  p_event_id uuid,
  p_work_id uuid,
  p_aggregate_version bigint,
  p_recorded_at timestamptz,
  p_data jsonb
) RETURNS text
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_projection warehouse_work_state_projection%ROWTYPE;
  v_previous_state text;
  v_state text;
  v_tenant_id uuid;
  v_warehouse_id uuid;
  v_capability text;
  v_domain_reference text;
  v_work_version bigint;
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_consumer_name IS NULL OR btrim(p_consumer_name) = ''
     OR p_consumer_name <> btrim(p_consumer_name) THEN
    RAISE EXCEPTION 'consumer name is required without surrounding whitespace'
      USING ERRCODE = '23514';
  END IF;
  IF p_event_id IS NULL OR p_work_id IS NULL THEN
    RAISE EXCEPTION 'event and Warehouse Work identity are required'
      USING ERRCODE = '23514';
  END IF;
  IF p_aggregate_version IS NULL OR p_aggregate_version < 1 THEN
    RAISE EXCEPTION 'aggregate version must be positive'
      USING ERRCODE = '23514';
  END IF;
  IF p_recorded_at IS NULL THEN
    RAISE EXCEPTION 'recorded timestamp is required'
      USING ERRCODE = '23514';
  END IF;
  IF p_data IS NULL OR jsonb_typeof(p_data) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'Warehouse Work projection data must be a JSON object'
      USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1
    FROM domain_event_inbox
    WHERE consumer_name = p_consumer_name
      AND event_id = p_event_id
  ) THEN
    RAISE EXCEPTION 'projection event requires an Inbox receipt in the same transaction'
      USING ERRCODE = '23514';
  END IF;

  BEGIN
    v_tenant_id := (p_data ->> 'tenant_id')::uuid;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'Warehouse Work event tenant_id must be a UUID'
      USING ERRCODE = '23514';
  END;

  BEGIN
    v_warehouse_id := (p_data ->> 'warehouse_id')::uuid;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'Warehouse Work event warehouse_id must be a UUID'
      USING ERRCODE = '23514';
  END;

  v_previous_state := p_data ->> 'previous_state';
  v_state := p_data ->> 'state';
  v_capability := p_data ->> 'capability';
  v_domain_reference := p_data ->> 'domain_reference';

  BEGIN
    v_work_version := (p_data ->> 'work_version')::bigint;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'Warehouse Work event work_version must be an integer'
      USING ERRCODE = '23514';
  END;

  IF p_data ->> 'work_id' IS DISTINCT FROM p_work_id::text THEN
    RAISE EXCEPTION 'Warehouse Work event work_id must match aggregate identity'
      USING ERRCODE = '23514';
  END IF;
  IF v_tenant_id IS NULL OR v_warehouse_id IS NULL THEN
    RAISE EXCEPTION 'Warehouse Work event tenant and warehouse are required'
      USING ERRCODE = '23514';
  END IF;
  IF v_previous_state IS NULL OR v_state IS NULL
     OR v_previous_state NOT IN ('planned', 'released', 'assigned', 'in_progress', 'exception', 'completed', 'cancelled')
     OR v_state NOT IN ('planned', 'released', 'assigned', 'in_progress', 'exception', 'completed', 'cancelled') THEN
    RAISE EXCEPTION 'Warehouse Work event states are invalid'
      USING ERRCODE = '23514';
  END IF;
  IF v_capability IS NULL OR btrim(v_capability) = ''
     OR v_domain_reference IS NULL OR btrim(v_domain_reference) = '' THEN
    RAISE EXCEPTION 'Warehouse Work event capability and domain reference are required'
      USING ERRCODE = '23514';
  END IF;
  IF v_work_version IS NULL OR v_work_version < 1 THEN
    RAISE EXCEPTION 'Warehouse Work event work_version must be positive'
      USING ERRCODE = '23514';
  END IF;

  SELECT * INTO v_projection
  FROM warehouse_work_state_projection
  WHERE consumer_name = p_consumer_name
    AND work_id = p_work_id
  FOR UPDATE;

  IF NOT FOUND THEN
    IF p_aggregate_version <> 1 THEN
      RAISE EXCEPTION 'first Warehouse Work projection event must have aggregate version 1'
        USING ERRCODE = '23514';
    END IF;
    IF v_previous_state <> 'planned' THEN
      RAISE EXCEPTION 'first Warehouse Work state transition must start from planned'
        USING ERRCODE = '23514';
    END IF;

    INSERT INTO warehouse_work_state_projection (
      consumer_name,
      work_id,
      tenant_id,
      warehouse_id,
      capability,
      domain_reference,
      state,
      aggregate_version,
      work_version,
      last_event_id,
      last_recorded_at,
      updated_at
    ) VALUES (
      p_consumer_name,
      p_work_id,
      v_tenant_id,
      v_warehouse_id,
      v_capability,
      v_domain_reference,
      v_state,
      p_aggregate_version,
      v_work_version,
      p_event_id,
      p_recorded_at,
      v_now
    );
  ELSE
    IF p_aggregate_version = v_projection.aggregate_version THEN
      IF p_event_id <> v_projection.last_event_id THEN
        RAISE EXCEPTION 'Warehouse Work projection version belongs to another event'
          USING ERRCODE = '23505';
      END IF;
      RETURN 'duplicate';
    END IF;

    IF p_aggregate_version < v_projection.aggregate_version THEN
      RETURN 'stale';
    END IF;

    IF p_aggregate_version <> v_projection.aggregate_version + 1 THEN
      RAISE EXCEPTION 'Warehouse Work projection detected an aggregate version gap'
        USING ERRCODE = '23514';
    END IF;
    IF v_previous_state <> v_projection.state THEN
      RAISE EXCEPTION 'Warehouse Work projection previous state does not match current state'
        USING ERRCODE = '23514';
    END IF;
    IF v_tenant_id <> v_projection.tenant_id
       OR v_warehouse_id <> v_projection.warehouse_id
       OR v_capability <> v_projection.capability
       OR v_domain_reference <> v_projection.domain_reference THEN
      RAISE EXCEPTION 'Warehouse Work immutable projection identity changed'
        USING ERRCODE = '23514';
    END IF;
    IF v_work_version <= v_projection.work_version THEN
      RAISE EXCEPTION 'Warehouse Work internal version must advance'
        USING ERRCODE = '23514';
    END IF;

    UPDATE warehouse_work_state_projection
    SET state = v_state,
        aggregate_version = p_aggregate_version,
        work_version = v_work_version,
        last_event_id = p_event_id,
        last_recorded_at = p_recorded_at,
        updated_at = v_now
    WHERE consumer_name = p_consumer_name
      AND work_id = p_work_id;
  END IF;

  UPDATE domain_event_inbox
  SET metadata = metadata || jsonb_build_object(
    'projection', jsonb_build_object(
      'name', 'warehouse-work-state',
      'status', 'applied',
      'work_id', p_work_id,
      'aggregate_version', p_aggregate_version,
      'work_version', v_work_version,
      'state', v_state,
      'applied_at', v_now
    )
  )
  WHERE consumer_name = p_consumer_name
    AND event_id = p_event_id;

  RETURN 'applied';
END;
$$;

COMMIT;
