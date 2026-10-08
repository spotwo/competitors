BEGIN;
SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

-- A rollout is append-only by generation. The state row is a lockable
-- traffic fence, never an instruction to mutate shared stream topology.
CREATE TABLE tenant_deployment_rollout_state (
  deployment_id text PRIMARY KEY REFERENCES tenant_deployment_activation_state(deployment_id),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  activation_receipt_id uuid NOT NULL REFERENCES tenant_deployment_activation_journal(receipt_id),
  generation_id uuid NOT NULL UNIQUE DEFAULT uuidv7(),
  phase text NOT NULL CHECK (phase IN ('active', 'paused')),
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE tenant_deployment_rollout_journal (
  journal_id uuid PRIMARY KEY DEFAULT uuidv7(),
  deployment_id text NOT NULL REFERENCES tenant_deployment_rollout_state(deployment_id),
  tenant_id uuid NOT NULL,
  generation_id uuid NOT NULL,
  action text NOT NULL CHECK (action IN ('started', 'staged', 'paused', 'reverted')),
  event_id uuid,
  operator_id text NOT NULL CHECK (btrim(operator_id) <> ''),
  change_ref text NOT NULL CHECK (btrim(change_ref) <> ''),
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((action IN ('staged', 'reverted')) = (event_id IS NOT NULL)),
  UNIQUE (deployment_id, event_id, action)
);

CREATE FUNCTION reject_tenant_rollout_journal_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'tenant rollout journal is append-only' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER tenant_rollout_journal_immutable
BEFORE UPDATE OR DELETE ON tenant_deployment_rollout_journal
FOR EACH ROW EXECUTE FUNCTION reject_tenant_rollout_journal_mutation();

-- A persisted activation receipt is required; callers cannot fabricate
-- deployment/tenant binding by passing unrelated IDs.
CREATE FUNCTION begin_tenant_rollout(
  p_deployment_id text,
  p_tenant_id uuid,
  p_activation_receipt_id uuid,
  p_operator_id text,
  p_change_ref text
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_receipt uuid;
  v_generation uuid;
BEGIN
  IF p_deployment_id IS NULL OR btrim(p_deployment_id) = ''
    OR p_tenant_id IS NULL OR p_activation_receipt_id IS NULL
    OR p_operator_id IS NULL OR btrim(p_operator_id) = ''
    OR p_change_ref IS NULL OR btrim(p_change_ref) = '' THEN
    RAISE EXCEPTION 'rollout requires deployment, activation and operator identities'
      USING ERRCODE = '23514';
  END IF;

  SELECT s.activation_receipt_id INTO v_receipt
  FROM tenant_deployment_activation_state s
  JOIN tenant_deployment_activation_journal j
    ON j.receipt_id = s.activation_receipt_id
  WHERE s.deployment_id = p_deployment_id
    AND s.tenant_id = p_tenant_id
    AND j.tenant_id = p_tenant_id
  FOR UPDATE OF s;
  IF v_receipt IS DISTINCT FROM p_activation_receipt_id THEN
    RAISE EXCEPTION 'rollout activation receipt/tenant mismatch'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO tenant_deployment_rollout_state (
    deployment_id, tenant_id, activation_receipt_id, phase
  ) VALUES (p_deployment_id, p_tenant_id, p_activation_receipt_id, 'active')
  RETURNING generation_id INTO v_generation;

  INSERT INTO tenant_deployment_rollout_journal (
    deployment_id, tenant_id, generation_id, action, operator_id, change_ref
  ) VALUES (
    p_deployment_id, p_tenant_id, v_generation, 'started',
    p_operator_id, p_change_ref
  );
  RETURN v_generation;
END;
$$;

-- No automatic bulk "all tenant events" transition. The operator supplies
-- an explicit reviewed list of event IDs. One transaction, all-or-nothing.
CREATE FUNCTION stage_tenant_rollout_batch(
  p_deployment_id text,
  p_tenant_id uuid,
  p_event_ids uuid[],
  p_operator_id text,
  p_change_ref text
) RETURNS integer
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_generation uuid;
  v_expected integer;
  v_eligible integer;
  v_changed integer;
BEGIN
  v_expected := cardinality(p_event_ids);
  IF p_deployment_id IS NULL OR p_tenant_id IS NULL
    OR p_operator_id IS NULL OR btrim(p_operator_id) = ''
    OR p_change_ref IS NULL OR btrim(p_change_ref) = ''
    OR v_expected IS NULL OR v_expected NOT BETWEEN 1 AND 100
    OR EXISTS (SELECT 1 FROM unnest(p_event_ids) v WHERE v IS NULL)
    OR v_expected <> (SELECT count(DISTINCT v) FROM unnest(p_event_ids) v) THEN
    RAISE EXCEPTION 'invalid bounded tenant cutover request' USING ERRCODE = '23514';
  END IF;

  SELECT generation_id INTO v_generation
  FROM tenant_deployment_rollout_state
  WHERE deployment_id = p_deployment_id AND tenant_id = p_tenant_id
    AND phase = 'active'
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'tenant rollout is not active' USING ERRCODE = '23514';
  END IF;

  -- FOR UPDATE fences concurrent shared publisher claims. Only never-attempted
  -- V2 events can move; unknown publish ACK outcomes can NEVER switch routes.
  PERFORM 1 FROM domain_event_outbox
  WHERE event_id = ANY (p_event_ids) ORDER BY event_id FOR UPDATE;
  SELECT count(*) INTO v_eligible
  FROM domain_event_outbox o
  WHERE o.event_id = ANY (p_event_ids)
    AND o.tenant_id = p_tenant_id
    AND o.event_type = 'inventory.transaction.posted'
    AND o.schema_version = 2
    AND o.delivery_route = 'shared'
    AND o.published_at IS NULL AND o.quarantined_at IS NULL
    AND o.claimed_by IS NULL AND o.claim_token IS NULL
    AND o.claimed_until IS NULL
    AND o.attempt_count = 0
    AND NOT EXISTS (
      SELECT 1 FROM domain_event_outbox_route_actions a WHERE a.event_id = o.event_id
    );
  IF v_eligible <> v_expected THEN
    RAISE EXCEPTION 'tenant cutover batch not entirely eligible'
      USING ERRCODE = '23514';
  END IF;

  UPDATE domain_event_outbox
  SET delivery_route = 'tenant'
  WHERE event_id = ANY (p_event_ids);
  GET DIAGNOSTICS v_changed = ROW_COUNT;
  IF v_changed <> v_expected THEN
    RAISE EXCEPTION 'tenant cutover batch update count mismatch'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO domain_event_outbox_route_actions (
    event_id, tenant_id, operator_id, previous_route, new_route
  )
  SELECT event_id, p_tenant_id, p_operator_id, 'shared', 'tenant'
  FROM domain_event_outbox WHERE event_id = ANY (p_event_ids);

  INSERT INTO tenant_deployment_rollout_journal (
    deployment_id, tenant_id, generation_id, action, event_id,
    operator_id, change_ref
  )
  SELECT p_deployment_id, p_tenant_id, v_generation, 'staged',
         event_id, p_operator_id, p_change_ref
  FROM domain_event_outbox WHERE event_id = ANY (p_event_ids);

  RETURN v_changed;
END;
$$;

CREATE FUNCTION pause_tenant_rollout(
  p_deployment_id text,
  p_tenant_id uuid,
  p_operator_id text,
  p_change_ref text
) RETURNS boolean
LANGUAGE plpgsql SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE v_generation uuid;
BEGIN
  IF p_operator_id IS NULL OR btrim(p_operator_id) = ''
    OR p_change_ref IS NULL OR btrim(p_change_ref) = '' THEN
    RAISE EXCEPTION 'pause requires operator and change reference'
      USING ERRCODE = '23514';
  END IF;
  UPDATE tenant_deployment_rollout_state
  SET phase = 'paused', updated_at = clock_timestamp()
  WHERE deployment_id = p_deployment_id AND tenant_id = p_tenant_id
    AND phase = 'active'
  RETURNING generation_id INTO v_generation;
  IF NOT FOUND THEN
    RETURN false;
  END IF;
  INSERT INTO tenant_deployment_rollout_journal(
    deployment_id, tenant_id, generation_id, action, operator_id, change_ref
  ) VALUES (p_deployment_id, p_tenant_id, v_generation, 'paused',
            p_operator_id, p_change_ref);
  RETURN true;
END;
$$;

-- Rollback is deliberately only a route release for NEVER attempted rows.
-- Already published or even attempted tenant events stay pinned to tenant
-- delivery: rerouting them could produce a second broker delivery.
CREATE FUNCTION rollback_tenant_rollout_batch(
  p_deployment_id text,
  p_tenant_id uuid,
  p_event_ids uuid[],
  p_operator_id text,
  p_change_ref text
) RETURNS integer
LANGUAGE plpgsql SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_generation uuid;
  v_expected integer;
  v_eligible integer;
  v_changed integer;
BEGIN
  v_expected := cardinality(p_event_ids);
  IF p_deployment_id IS NULL OR p_tenant_id IS NULL
    OR p_operator_id IS NULL OR btrim(p_operator_id) = ''
    OR p_change_ref IS NULL OR btrim(p_change_ref) = ''
    OR v_expected IS NULL OR v_expected NOT BETWEEN 1 AND 100
    OR EXISTS (SELECT 1 FROM unnest(p_event_ids) v WHERE v IS NULL)
    OR v_expected <> (SELECT count(DISTINCT v) FROM unnest(p_event_ids) v) THEN
    RAISE EXCEPTION 'invalid bounded tenant rollback request'
      USING ERRCODE = '23514';
  END IF;

  SELECT generation_id INTO v_generation
  FROM tenant_deployment_rollout_state
  WHERE deployment_id = p_deployment_id AND tenant_id = p_tenant_id
    AND phase = 'paused'
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'tenant rollout must be paused before rollback'
      USING ERRCODE = '23514';
  END IF;

  PERFORM 1 FROM domain_event_outbox
  WHERE event_id = ANY (p_event_ids) ORDER BY event_id FOR UPDATE;
  SELECT count(*) INTO v_eligible
  FROM domain_event_outbox o
  WHERE o.event_id = ANY (p_event_ids)
    AND o.tenant_id = p_tenant_id
    AND o.delivery_route = 'tenant'
    AND o.published_at IS NULL AND o.quarantined_at IS NULL
    AND o.claimed_by IS NULL AND o.claim_token IS NULL
    AND o.claimed_until IS NULL AND o.attempt_count = 0
    AND EXISTS (
      SELECT 1 FROM tenant_deployment_rollout_journal j
      WHERE j.event_id = o.event_id AND j.deployment_id = p_deployment_id
        AND j.action = 'staged' AND j.generation_id = v_generation
    )
    AND NOT EXISTS (
      SELECT 1 FROM tenant_deployment_rollout_journal j
      WHERE j.event_id = o.event_id AND j.deployment_id = p_deployment_id
        AND j.action = 'reverted'
    );
  IF v_eligible <> v_expected THEN
    RAISE EXCEPTION 'tenant rollback batch includes claimed, attempted or foreign rows'
      USING ERRCODE = '23514';
  END IF;

  UPDATE domain_event_outbox SET delivery_route = 'shared'
  WHERE event_id = ANY (p_event_ids);
  GET DIAGNOSTICS v_changed = ROW_COUNT;
  IF v_changed <> v_expected THEN
    RAISE EXCEPTION 'tenant rollback batch update count mismatch'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO tenant_deployment_rollout_journal(
    deployment_id, tenant_id, generation_id, action, event_id,
    operator_id, change_ref
  )
  SELECT p_deployment_id, p_tenant_id, v_generation, 'reverted',
         event_id, p_operator_id, p_change_ref
  FROM domain_event_outbox WHERE event_id = ANY (p_event_ids);
  RETURN v_changed;
END;
$$;

-- Privilege control MUST be supplied by deployment role provisioning.
REVOKE ALL ON FUNCTION begin_tenant_rollout(text,uuid,uuid,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION stage_tenant_rollout_batch(text,uuid,uuid[],text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION pause_tenant_rollout(text,uuid,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION rollback_tenant_rollout_batch(text,uuid,uuid[],text,text) FROM PUBLIC;

COMMIT;
