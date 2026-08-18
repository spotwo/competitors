BEGIN;
SET search_path = kernel_lab, public;

CREATE TABLE warehouse_works (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  warehouse_id uuid NOT NULL REFERENCES warehouses(id),
  capability text NOT NULL CHECK (btrim(capability) <> ''),
  domain_reference text NOT NULL CHECK (btrim(domain_reference) <> ''),
  reason text,
  priority integer NOT NULL DEFAULT 50 CHECK (priority BETWEEN 0 AND 1000),
  state text NOT NULL DEFAULT 'planned'
    CHECK (state IN ('planned', 'released', 'assigned', 'in_progress', 'exception', 'completed', 'cancelled')),
  due_at timestamptz,
  resource_requirements jsonb NOT NULL DEFAULT '{}'::jsonb,
  version bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  cancelled_at timestamptz,
  UNIQUE (id, tenant_id)
);

CREATE INDEX warehouse_works_queue_idx
  ON warehouse_works (tenant_id, warehouse_id, state, priority DESC, due_at, created_at);

CREATE TABLE warehouse_tasks (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  work_id uuid NOT NULL,
  sequence integer NOT NULL CHECK (sequence > 0),
  operation text NOT NULL CHECK (btrim(operation) <> ''),
  state text NOT NULL DEFAULT 'pending'
    CHECK (state IN ('pending', 'ready', 'in_progress', 'exception', 'completed', 'cancelled')),
  planned_quantity numeric(24, 6) CHECK (planned_quantity IS NULL OR planned_quantity >= 0),
  uom text,
  source_context jsonb,
  destination_context jsonb,
  domain_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  requires_domain_confirmation boolean NOT NULL DEFAULT false,
  confirmation_requirements jsonb NOT NULL DEFAULT '{}'::jsonb,
  version bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  CONSTRAINT warehouse_task_work_fk
    FOREIGN KEY (work_id, tenant_id) REFERENCES warehouse_works(id, tenant_id) ON DELETE CASCADE,
  CONSTRAINT warehouse_task_sequence_uq UNIQUE (work_id, sequence),
  UNIQUE (id, work_id, tenant_id)
);

CREATE INDEX warehouse_tasks_work_state_idx
  ON warehouse_tasks (tenant_id, work_id, state, sequence);

CREATE TABLE warehouse_work_assignments (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  work_id uuid NOT NULL,
  resource_kind text NOT NULL
    CHECK (resource_kind IN ('human', 'equipment', 'team', 'automation')),
  resource_id text NOT NULL CHECK (btrim(resource_id) <> ''),
  assignment_method text NOT NULL
    CHECK (assignment_method IN ('manual', 'claim', 'dispatch')),
  state text NOT NULL DEFAULT 'active'
    CHECK (state IN ('active', 'released', 'completed')),
  assigned_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  completed_at timestamptz,
  CONSTRAINT warehouse_assignment_work_fk
    FOREIGN KEY (work_id, tenant_id) REFERENCES warehouse_works(id, tenant_id) ON DELETE CASCADE,
  UNIQUE (id, work_id, tenant_id)
);

CREATE UNIQUE INDEX warehouse_work_one_active_assignment_uq
  ON warehouse_work_assignments (work_id)
  WHERE state = 'active';

CREATE INDEX warehouse_work_assignments_resource_idx
  ON warehouse_work_assignments (tenant_id, resource_kind, resource_id, state, assigned_at);

CREATE TABLE warehouse_task_executions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  work_id uuid NOT NULL,
  task_id uuid NOT NULL,
  assignment_id uuid NOT NULL,
  channel text NOT NULL CHECK (btrim(channel) <> ''),
  state text NOT NULL DEFAULT 'in_progress'
    CHECK (state IN ('in_progress', 'blocked', 'completed', 'aborted')),
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  CONSTRAINT warehouse_execution_task_fk
    FOREIGN KEY (task_id, work_id, tenant_id)
    REFERENCES warehouse_tasks(id, work_id, tenant_id),
  CONSTRAINT warehouse_execution_assignment_fk
    FOREIGN KEY (assignment_id, work_id, tenant_id)
    REFERENCES warehouse_work_assignments(id, work_id, tenant_id),
  UNIQUE (id, task_id, work_id, tenant_id)
);

CREATE UNIQUE INDEX warehouse_task_one_live_execution_uq
  ON warehouse_task_executions (task_id)
  WHERE state IN ('in_progress', 'blocked');

CREATE TABLE warehouse_task_confirmations (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  work_id uuid NOT NULL,
  task_id uuid NOT NULL,
  execution_id uuid NOT NULL UNIQUE,
  outcome text NOT NULL CHECK (btrim(outcome) <> ''),
  actual_quantity numeric(24, 6) CHECK (actual_quantity IS NULL OR actual_quantity >= 0),
  domain_result_reference text,
  data jsonb NOT NULL DEFAULT '{}'::jsonb,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT warehouse_confirmation_execution_fk
    FOREIGN KEY (execution_id, task_id, work_id, tenant_id)
    REFERENCES warehouse_task_executions(id, task_id, work_id, tenant_id)
);

CREATE TABLE warehouse_task_exceptions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  work_id uuid NOT NULL,
  task_id uuid NOT NULL,
  execution_id uuid NOT NULL,
  code text NOT NULL CHECK (btrim(code) <> ''),
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  state text NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'resolved')),
  resolution text,
  opened_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  CONSTRAINT warehouse_exception_execution_fk
    FOREIGN KEY (execution_id, task_id, work_id, tenant_id)
    REFERENCES warehouse_task_executions(id, task_id, work_id, tenant_id)
);

CREATE UNIQUE INDEX warehouse_task_one_open_exception_uq
  ON warehouse_task_exceptions (task_id)
  WHERE state = 'open';

CREATE TABLE warehouse_work_command_receipts (
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  idempotency_key text NOT NULL CHECK (btrim(idempotency_key) <> ''),
  command_type text NOT NULL CHECK (btrim(command_type) <> ''),
  request_fingerprint text NOT NULL,
  result_type text NOT NULL,
  result_id uuid NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE OR REPLACE VIEW warehouse_work_queue AS
SELECT
  w.*,
  row_number() OVER (
    PARTITION BY w.tenant_id, w.warehouse_id
    ORDER BY w.priority DESC, w.due_at ASC NULLS LAST, w.created_at ASC, w.id
  ) AS queue_rank
FROM warehouse_works w
WHERE w.state = 'released'
  AND NOT EXISTS (
    SELECT 1
    FROM warehouse_work_assignments a
    WHERE a.work_id = w.id
      AND a.state = 'active'
  );

CREATE OR REPLACE FUNCTION warehouse_work_command_replay(
  p_tenant_id uuid,
  p_idempotency_key text,
  p_command_type text,
  p_request_fingerprint text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_command_type text;
  v_request_fingerprint text;
  v_result_id uuid;
BEGIN
  IF p_idempotency_key IS NULL OR btrim(p_idempotency_key) = '' THEN
    RAISE EXCEPTION 'idempotency key is required' USING ERRCODE = '23514';
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(p_tenant_id::text || ':warehouse-work-command:' || p_idempotency_key, 0)
  );

  SELECT command_type, request_fingerprint, result_id
    INTO v_command_type, v_request_fingerprint, v_result_id
  FROM warehouse_work_command_receipts
  WHERE tenant_id = p_tenant_id
    AND idempotency_key = p_idempotency_key;

  IF FOUND THEN
    IF v_command_type <> p_command_type OR v_request_fingerprint <> p_request_fingerprint THEN
      RAISE EXCEPTION 'idempotency key reused with different warehouse work command payload'
        USING ERRCODE = '23505';
    END IF;
    RETURN v_result_id;
  END IF;

  RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION warehouse_work_command_record(
  p_tenant_id uuid,
  p_idempotency_key text,
  p_command_type text,
  p_request_fingerprint text,
  p_result_type text,
  p_result_id uuid
) RETURNS void
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
BEGIN
  INSERT INTO warehouse_work_command_receipts (
    tenant_id, idempotency_key, command_type, request_fingerprint, result_type, result_id
  ) VALUES (
    p_tenant_id, p_idempotency_key, p_command_type, p_request_fingerprint, p_result_type, p_result_id
  );
END;
$$;

CREATE OR REPLACE FUNCTION release_warehouse_work(
  p_tenant_id uuid,
  p_idempotency_key text,
  p_work_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_replay uuid;
  v_state text;
  v_first_task uuid;
  v_fingerprint text := format('work=%s', p_work_id);
BEGIN
  v_replay := warehouse_work_command_replay(
    p_tenant_id, p_idempotency_key, 'work.release', v_fingerprint
  );
  IF v_replay IS NOT NULL THEN
    RETURN v_replay;
  END IF;

  SELECT state INTO v_state
  FROM warehouse_works
  WHERE id = p_work_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'warehouse work not found' USING ERRCODE = '23503';
  END IF;
  IF v_state <> 'planned' THEN
    RAISE EXCEPTION 'warehouse work must be planned before release' USING ERRCODE = '23514';
  END IF;
  IF EXISTS (
    SELECT 1 FROM warehouse_tasks
    WHERE work_id = p_work_id AND state <> 'pending'
  ) THEN
    RAISE EXCEPTION 'all tasks must be pending before work release' USING ERRCODE = '23514';
  END IF;

  SELECT id INTO v_first_task
  FROM warehouse_tasks
  WHERE work_id = p_work_id
  ORDER BY sequence
  LIMIT 1;

  IF v_first_task IS NULL THEN
    RAISE EXCEPTION 'warehouse work requires at least one task before release' USING ERRCODE = '23514';
  END IF;

  UPDATE warehouse_tasks
  SET state = 'ready', version = version + 1
  WHERE id = v_first_task;

  UPDATE warehouse_works
  SET state = 'released', released_at = now(), version = version + 1
  WHERE id = p_work_id;

  PERFORM warehouse_work_command_record(
    p_tenant_id, p_idempotency_key, 'work.release', v_fingerprint, 'warehouse-work', p_work_id
  );
  RETURN p_work_id;
END;
$$;

CREATE OR REPLACE FUNCTION claim_warehouse_work(
  p_assignment_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_work_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_assignment_method text DEFAULT 'claim'
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_replay uuid;
  v_state text;
  v_fingerprint text := format(
    'assignment=%s|work=%s|kind=%s|resource=%s|method=%s',
    p_assignment_id, p_work_id, p_resource_kind, p_resource_id, p_assignment_method
  );
BEGIN
  v_replay := warehouse_work_command_replay(
    p_tenant_id, p_idempotency_key, 'work.claim', v_fingerprint
  );
  IF v_replay IS NOT NULL THEN
    RETURN v_replay;
  END IF;

  SELECT state INTO v_state
  FROM warehouse_works
  WHERE id = p_work_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'warehouse work not found' USING ERRCODE = '23503';
  END IF;
  IF v_state <> 'released' THEN
    RAISE EXCEPTION 'only released work can be claimed' USING ERRCODE = '23514';
  END IF;

  INSERT INTO warehouse_work_assignments (
    id, tenant_id, work_id, resource_kind, resource_id, assignment_method, state
  ) VALUES (
    p_assignment_id, p_tenant_id, p_work_id, p_resource_kind, p_resource_id, p_assignment_method, 'active'
  );

  UPDATE warehouse_works
  SET state = 'assigned', version = version + 1
  WHERE id = p_work_id;

  PERFORM warehouse_work_command_record(
    p_tenant_id, p_idempotency_key, 'work.claim', v_fingerprint, 'work-assignment', p_assignment_id
  );
  RETURN p_assignment_id;
END;
$$;

CREATE OR REPLACE FUNCTION start_warehouse_task(
  p_execution_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_channel text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_replay uuid;
  v_work_id uuid;
  v_work_state text;
  v_task_state text;
  v_sequence integer;
  v_assignment_id uuid;
  v_assignment_kind text;
  v_assignment_resource text;
  v_fingerprint text := format(
    'execution=%s|task=%s|kind=%s|resource=%s|channel=%s',
    p_execution_id, p_task_id, p_resource_kind, p_resource_id, p_channel
  );
BEGIN
  v_replay := warehouse_work_command_replay(
    p_tenant_id, p_idempotency_key, 'task.start', v_fingerprint
  );
  IF v_replay IS NOT NULL THEN
    RETURN v_replay;
  END IF;

  SELECT work_id INTO v_work_id
  FROM warehouse_tasks
  WHERE id = p_task_id AND tenant_id = p_tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'warehouse task not found' USING ERRCODE = '23503';
  END IF;

  SELECT state INTO v_work_state
  FROM warehouse_works
  WHERE id = v_work_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  SELECT state, sequence INTO v_task_state, v_sequence
  FROM warehouse_tasks
  WHERE id = p_task_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_work_state NOT IN ('assigned', 'in_progress') THEN
    RAISE EXCEPTION 'warehouse work is not executable by an assignment' USING ERRCODE = '23514';
  END IF;
  IF v_task_state <> 'ready' THEN
    RAISE EXCEPTION 'warehouse task must be ready before start' USING ERRCODE = '23514';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM warehouse_tasks
    WHERE work_id = v_work_id
      AND sequence < v_sequence
      AND state NOT IN ('completed', 'cancelled')
  ) THEN
    RAISE EXCEPTION 'warehouse task sequence cannot be skipped' USING ERRCODE = '23514';
  END IF;

  SELECT id, resource_kind, resource_id
    INTO v_assignment_id, v_assignment_kind, v_assignment_resource
  FROM warehouse_work_assignments
  WHERE work_id = v_work_id
    AND tenant_id = p_tenant_id
    AND state = 'active'
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'active warehouse work assignment not found' USING ERRCODE = '23503';
  END IF;
  IF v_assignment_kind <> p_resource_kind OR v_assignment_resource <> p_resource_id THEN
    RAISE EXCEPTION 'warehouse task can only be started by the assigned resource' USING ERRCODE = '23514';
  END IF;

  INSERT INTO warehouse_task_executions (
    id, tenant_id, work_id, task_id, assignment_id, channel, state
  ) VALUES (
    p_execution_id, p_tenant_id, v_work_id, p_task_id, v_assignment_id, p_channel, 'in_progress'
  );

  UPDATE warehouse_tasks
  SET state = 'in_progress', started_at = COALESCE(started_at, now()), version = version + 1
  WHERE id = p_task_id;

  UPDATE warehouse_works
  SET state = 'in_progress', started_at = COALESCE(started_at, now()), version = version + 1
  WHERE id = v_work_id;

  PERFORM warehouse_work_command_record(
    p_tenant_id, p_idempotency_key, 'task.start', v_fingerprint, 'task-execution', p_execution_id
  );
  RETURN p_execution_id;
END;
$$;

CREATE OR REPLACE FUNCTION confirm_warehouse_task(
  p_confirmation_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_outcome text,
  p_actual_quantity numeric DEFAULT NULL,
  p_domain_result_reference text DEFAULT NULL,
  p_data jsonb DEFAULT '{}'::jsonb
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_replay uuid;
  v_work_id uuid;
  v_task_state text;
  v_sequence integer;
  v_requires_domain_confirmation boolean;
  v_execution_id uuid;
  v_assignment_id uuid;
  v_next_task uuid;
  v_next_state text;
  v_fingerprint text := format(
    'confirmation=%s|task=%s|kind=%s|resource=%s|outcome=%s|qty=%s|domain=%s|data=%s',
    p_confirmation_id, p_task_id, p_resource_kind, p_resource_id, p_outcome,
    COALESCE(p_actual_quantity::text, ''), COALESCE(p_domain_result_reference, ''), p_data::text
  );
BEGIN
  v_replay := warehouse_work_command_replay(
    p_tenant_id, p_idempotency_key, 'task.confirm', v_fingerprint
  );
  IF v_replay IS NOT NULL THEN
    RETURN v_replay;
  END IF;

  SELECT work_id INTO v_work_id
  FROM warehouse_tasks
  WHERE id = p_task_id AND tenant_id = p_tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'warehouse task not found' USING ERRCODE = '23503';
  END IF;

  PERFORM 1
  FROM warehouse_works
  WHERE id = v_work_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  SELECT state, sequence, requires_domain_confirmation
    INTO v_task_state, v_sequence, v_requires_domain_confirmation
  FROM warehouse_tasks
  WHERE id = p_task_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_task_state <> 'in_progress' THEN
    RAISE EXCEPTION 'warehouse task must be in progress before confirmation' USING ERRCODE = '23514';
  END IF;
  IF v_requires_domain_confirmation
     AND (p_domain_result_reference IS NULL OR btrim(p_domain_result_reference) = '') THEN
    RAISE EXCEPTION 'domain result reference is required for this task confirmation' USING ERRCODE = '23514';
  END IF;

  SELECT e.id, e.assignment_id
    INTO v_execution_id, v_assignment_id
  FROM warehouse_task_executions e
  JOIN warehouse_work_assignments a
    ON a.id = e.assignment_id
   AND a.work_id = e.work_id
   AND a.tenant_id = e.tenant_id
  WHERE e.task_id = p_task_id
    AND e.tenant_id = p_tenant_id
    AND e.state = 'in_progress'
    AND a.state = 'active'
    AND a.resource_kind = p_resource_kind
    AND a.resource_id = p_resource_id
  FOR UPDATE OF e, a;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'live task execution for assigned resource not found' USING ERRCODE = '23503';
  END IF;

  INSERT INTO warehouse_task_confirmations (
    id, tenant_id, work_id, task_id, execution_id, outcome,
    actual_quantity, domain_result_reference, data
  ) VALUES (
    p_confirmation_id, p_tenant_id, v_work_id, p_task_id, v_execution_id, p_outcome,
    p_actual_quantity, p_domain_result_reference, p_data
  );

  UPDATE warehouse_task_executions
  SET state = 'completed', finished_at = now()
  WHERE id = v_execution_id;

  UPDATE warehouse_tasks
  SET state = 'completed', completed_at = now(), version = version + 1
  WHERE id = p_task_id;

  SELECT id, state INTO v_next_task, v_next_state
  FROM warehouse_tasks
  WHERE work_id = v_work_id
    AND sequence > v_sequence
    AND state NOT IN ('completed', 'cancelled')
  ORDER BY sequence
  LIMIT 1;

  IF v_next_task IS NOT NULL THEN
    IF v_next_state <> 'pending' THEN
      RAISE EXCEPTION 'next warehouse task is not pending' USING ERRCODE = '23514';
    END IF;
    UPDATE warehouse_tasks
    SET state = 'ready', version = version + 1
    WHERE id = v_next_task;
  ELSE
    UPDATE warehouse_work_assignments
    SET state = 'completed', completed_at = now()
    WHERE id = v_assignment_id AND state = 'active';

    UPDATE warehouse_works
    SET state = 'completed', completed_at = now(), version = version + 1
    WHERE id = v_work_id;
  END IF;

  PERFORM warehouse_work_command_record(
    p_tenant_id, p_idempotency_key, 'task.confirm', v_fingerprint, 'task-confirmation', p_confirmation_id
  );
  RETURN p_confirmation_id;
END;
$$;

CREATE OR REPLACE FUNCTION raise_warehouse_task_exception(
  p_exception_id uuid,
  p_tenant_id uuid,
  p_idempotency_key text,
  p_task_id uuid,
  p_resource_kind text,
  p_resource_id text,
  p_code text,
  p_details jsonb DEFAULT '{}'::jsonb
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_replay uuid;
  v_work_id uuid;
  v_task_state text;
  v_execution_id uuid;
  v_fingerprint text := format(
    'exception=%s|task=%s|kind=%s|resource=%s|code=%s|details=%s',
    p_exception_id, p_task_id, p_resource_kind, p_resource_id, p_code, p_details::text
  );
BEGIN
  v_replay := warehouse_work_command_replay(
    p_tenant_id, p_idempotency_key, 'task.exception.raise', v_fingerprint
  );
  IF v_replay IS NOT NULL THEN
    RETURN v_replay;
  END IF;

  SELECT work_id INTO v_work_id
  FROM warehouse_tasks
  WHERE id = p_task_id AND tenant_id = p_tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'warehouse task not found' USING ERRCODE = '23503';
  END IF;

  PERFORM 1 FROM warehouse_works
  WHERE id = v_work_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  SELECT state INTO v_task_state
  FROM warehouse_tasks
  WHERE id = p_task_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_task_state <> 'in_progress' THEN
    RAISE EXCEPTION 'only in-progress task can raise execution exception' USING ERRCODE = '23514';
  END IF;

  SELECT e.id INTO v_execution_id
  FROM warehouse_task_executions e
  JOIN warehouse_work_assignments a
    ON a.id = e.assignment_id
   AND a.work_id = e.work_id
   AND a.tenant_id = e.tenant_id
  WHERE e.task_id = p_task_id
    AND e.tenant_id = p_tenant_id
    AND e.state = 'in_progress'
    AND a.state = 'active'
    AND a.resource_kind = p_resource_kind
    AND a.resource_id = p_resource_id
  FOR UPDATE OF e, a;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'live task execution for assigned resource not found' USING ERRCODE = '23503';
  END IF;

  INSERT INTO warehouse_task_exceptions (
    id, tenant_id, work_id, task_id, execution_id, code, details, state
  ) VALUES (
    p_exception_id, p_tenant_id, v_work_id, p_task_id, v_execution_id, p_code, p_details, 'open'
  );

  UPDATE warehouse_task_executions
  SET state = 'blocked'
  WHERE id = v_execution_id;

  UPDATE warehouse_tasks
  SET state = 'exception', version = version + 1
  WHERE id = p_task_id;

  UPDATE warehouse_works
  SET state = 'exception', version = version + 1
  WHERE id = v_work_id;

  PERFORM warehouse_work_command_record(
    p_tenant_id, p_idempotency_key, 'task.exception.raise', v_fingerprint, 'task-exception', p_exception_id
  );
  RETURN p_exception_id;
END;
$$;

CREATE OR REPLACE FUNCTION resolve_warehouse_task_exception(
  p_tenant_id uuid,
  p_idempotency_key text,
  p_exception_id uuid,
  p_resolution text DEFAULT 'resume'
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_replay uuid;
  v_work_id uuid;
  v_task_id uuid;
  v_execution_id uuid;
  v_exception_state text;
  v_fingerprint text := format('exception=%s|resolution=%s', p_exception_id, p_resolution);
BEGIN
  v_replay := warehouse_work_command_replay(
    p_tenant_id, p_idempotency_key, 'task.exception.resolve', v_fingerprint
  );
  IF v_replay IS NOT NULL THEN
    RETURN v_replay;
  END IF;

  IF p_resolution <> 'resume' THEN
    RAISE EXCEPTION 'only resume exception resolution is supported in v0.1' USING ERRCODE = '23514';
  END IF;

  SELECT work_id, task_id, execution_id
    INTO v_work_id, v_task_id, v_execution_id
  FROM warehouse_task_exceptions
  WHERE id = p_exception_id AND tenant_id = p_tenant_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'warehouse task exception not found' USING ERRCODE = '23503';
  END IF;

  PERFORM 1 FROM warehouse_works
  WHERE id = v_work_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  PERFORM 1 FROM warehouse_tasks
  WHERE id = v_task_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  SELECT state INTO v_exception_state
  FROM warehouse_task_exceptions
  WHERE id = p_exception_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF v_exception_state <> 'open' THEN
    RAISE EXCEPTION 'warehouse task exception is already resolved' USING ERRCODE = '23514';
  END IF;

  PERFORM 1 FROM warehouse_task_executions
  WHERE id = v_execution_id AND tenant_id = p_tenant_id AND state = 'blocked'
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'blocked execution for exception not found' USING ERRCODE = '23503';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM warehouse_work_assignments
    WHERE work_id = v_work_id AND tenant_id = p_tenant_id AND state = 'active'
  ) THEN
    RAISE EXCEPTION 'active assignment required to resume task exception' USING ERRCODE = '23514';
  END IF;

  UPDATE warehouse_task_exceptions
  SET state = 'resolved', resolution = p_resolution, resolved_at = now()
  WHERE id = p_exception_id;

  UPDATE warehouse_task_executions
  SET state = 'aborted', finished_at = now()
  WHERE id = v_execution_id;

  UPDATE warehouse_tasks
  SET state = 'ready', version = version + 1
  WHERE id = v_task_id;

  UPDATE warehouse_works
  SET state = 'assigned', version = version + 1
  WHERE id = v_work_id;

  PERFORM warehouse_work_command_record(
    p_tenant_id, p_idempotency_key, 'task.exception.resolve', v_fingerprint, 'task-exception', p_exception_id
  );
  RETURN p_exception_id;
END;
$$;

CREATE OR REPLACE FUNCTION cancel_warehouse_work(
  p_tenant_id uuid,
  p_idempotency_key text,
  p_work_id uuid,
  p_reason text DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_replay uuid;
  v_state text;
  v_fingerprint text := format('work=%s|reason=%s', p_work_id, COALESCE(p_reason, ''));
BEGIN
  v_replay := warehouse_work_command_replay(
    p_tenant_id, p_idempotency_key, 'work.cancel', v_fingerprint
  );
  IF v_replay IS NOT NULL THEN
    RETURN v_replay;
  END IF;

  SELECT state INTO v_state
  FROM warehouse_works
  WHERE id = p_work_id AND tenant_id = p_tenant_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'warehouse work not found' USING ERRCODE = '23503';
  END IF;
  IF v_state NOT IN ('planned', 'released', 'assigned') THEN
    RAISE EXCEPTION 'warehouse work cannot be cancelled after execution starts' USING ERRCODE = '23514';
  END IF;
  IF EXISTS (
    SELECT 1 FROM warehouse_tasks
    WHERE work_id = p_work_id
      AND state IN ('in_progress', 'exception', 'completed')
  ) THEN
    RAISE EXCEPTION 'warehouse work has already started execution' USING ERRCODE = '23514';
  END IF;

  UPDATE warehouse_work_assignments
  SET state = 'released', released_at = now()
  WHERE work_id = p_work_id AND state = 'active';

  UPDATE warehouse_tasks
  SET state = 'cancelled', version = version + 1
  WHERE work_id = p_work_id AND state IN ('pending', 'ready');

  UPDATE warehouse_works
  SET state = 'cancelled', cancelled_at = now(), reason = COALESCE(p_reason, reason), version = version + 1
  WHERE id = p_work_id;

  PERFORM warehouse_work_command_record(
    p_tenant_id, p_idempotency_key, 'work.cancel', v_fingerprint, 'warehouse-work', p_work_id
  );
  RETURN p_work_id;
END;
$$;

COMMIT;
