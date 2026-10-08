BEGIN;

SET LOCAL search_path = kernel_lab, pg_catalog, pg_temp;

-- These rows are durable operator receipts, not a replacement for a managed
-- approval system or an authentication proof. Only the controlled activation
-- service must receive EXECUTE privilege on the recorder.
CREATE TABLE tenant_deployment_activation_journal (
  receipt_id uuid PRIMARY KEY DEFAULT uuidv7(),
  deployment_id text NOT NULL CHECK (btrim(deployment_id) <> ''),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  operator_id text NOT NULL CHECK (btrim(operator_id) <> ''),
  approval_id text NOT NULL CHECK (btrim(approval_id) <> ''),
  action text NOT NULL CHECK (action = 'activated'),
  stream_name text NOT NULL CHECK (btrim(stream_name) <> ''),
  business_durable text NOT NULL CHECK (btrim(business_durable) <> ''),
  canary_durable text NOT NULL CHECK (btrim(canary_durable) <> ''),
  business_subject text NOT NULL CHECK (btrim(business_subject) <> ''),
  canary_subject text NOT NULL CHECK (btrim(canary_subject) <> ''),
  broker_acl_verified_at timestamptz NOT NULL,
  tenant_canary_verified_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (deployment_id, approval_id),
  CONSTRAINT tenant_activation_evidence_fresh_ck CHECK (
    broker_acl_verified_at <= recorded_at + interval '5 seconds'
    AND tenant_canary_verified_at <= recorded_at + interval '5 seconds'
    AND broker_acl_verified_at >= recorded_at - interval '120 seconds'
    AND tenant_canary_verified_at >= recorded_at - interval '120 seconds'
  )
);

CREATE TABLE tenant_deployment_activation_state (
  deployment_id text PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  activation_receipt_id uuid NOT NULL UNIQUE
    REFERENCES tenant_deployment_activation_journal(receipt_id),
  activated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Audit history is append-only, including for database superuser sessions
-- unless the trigger is deliberately disabled by privileged maintenance.
CREATE FUNCTION reject_tenant_activation_journal_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'tenant activation journal is append-only'
    USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER tenant_activation_journal_immutable
BEFORE UPDATE OR DELETE ON tenant_deployment_activation_journal
FOR EACH ROW EXECUTE FUNCTION reject_tenant_activation_journal_mutation();

CREATE FUNCTION record_tenant_deployment_activation(
  p_deployment_id text,
  p_tenant_id uuid,
  p_operator_id text,
  p_approval_id text,
  p_stream_name text,
  p_business_durable text,
  p_canary_durable text,
  p_business_subject text,
  p_canary_subject text,
  p_acl_observed_at timestamptz,
  p_canary_observed_at timestamptz
) RETURNS uuid
LANGUAGE plpgsql
SET search_path = kernel_lab, pg_catalog, pg_temp
AS $$
DECLARE
  v_receipt uuid;
  v_existing tenant_deployment_activation_journal%ROWTYPE;
BEGIN
  IF p_deployment_id IS NULL OR btrim(p_deployment_id) = ''
    OR p_tenant_id IS NULL OR p_operator_id IS NULL OR btrim(p_operator_id) = ''
    OR p_approval_id IS NULL OR btrim(p_approval_id) = ''
    OR p_stream_name IS NULL OR btrim(p_stream_name) = ''
    OR p_business_durable IS NULL OR btrim(p_business_durable) = ''
    OR p_canary_durable IS NULL OR btrim(p_canary_durable) = ''
    OR p_business_subject IS NULL OR btrim(p_business_subject) = ''
    OR p_canary_subject IS NULL OR btrim(p_canary_subject) = ''
    OR p_acl_observed_at IS NULL OR p_canary_observed_at IS NULL THEN
    RAISE EXCEPTION 'activation requires complete scoped approval and proof metadata'
      USING ERRCODE = '23514';
  END IF;

  -- Serialize concurrent activation requests for the same deployment without
  -- impacting different tenants or existing shared publishers.
  PERFORM pg_advisory_xact_lock(hashtextextended(p_deployment_id, 0));

  SELECT j.* INTO v_existing
  FROM tenant_deployment_activation_journal j
  WHERE j.deployment_id = p_deployment_id
    AND j.approval_id = p_approval_id;

  IF FOUND THEN
    IF v_existing.tenant_id = p_tenant_id
       AND v_existing.operator_id = p_operator_id
       AND v_existing.stream_name = p_stream_name
       AND v_existing.business_durable = p_business_durable
       AND v_existing.canary_durable = p_canary_durable
       AND v_existing.business_subject = p_business_subject
       AND v_existing.canary_subject = p_canary_subject THEN
      RETURN v_existing.receipt_id;
    END IF;
    RAISE EXCEPTION 'approval reference is already bound to another activation'
      USING ERRCODE = '23505';
  END IF;

  IF EXISTS (
    SELECT 1 FROM tenant_deployment_activation_state
    WHERE deployment_id = p_deployment_id
  ) THEN
    RAISE EXCEPTION 'tenant deployment is already activated; use existing approval'
      USING ERRCODE = '23505';
  END IF;

  INSERT INTO tenant_deployment_activation_journal (
    deployment_id, tenant_id, operator_id, approval_id, action, stream_name,
    business_durable, canary_durable, business_subject, canary_subject,
    broker_acl_verified_at, tenant_canary_verified_at
  ) VALUES (
    p_deployment_id, p_tenant_id, p_operator_id, p_approval_id, 'activated',
    p_stream_name, p_business_durable, p_canary_durable,
    p_business_subject, p_canary_subject, p_acl_observed_at,
    p_canary_observed_at
  ) RETURNING receipt_id INTO v_receipt;

  INSERT INTO tenant_deployment_activation_state (
    deployment_id, tenant_id, activation_receipt_id
  ) VALUES (p_deployment_id, p_tenant_id, v_receipt);

  RETURN v_receipt;
END;
$$;

REVOKE ALL ON FUNCTION record_tenant_deployment_activation(
  text, uuid, text, text, text, text, text, text, text,
  timestamptz, timestamptz
) FROM PUBLIC;

COMMIT;
