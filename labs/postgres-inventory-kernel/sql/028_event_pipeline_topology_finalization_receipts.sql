BEGIN;

SET search_path TO kernel_lab, public;

CREATE TABLE event_pipeline_topology_finalization_receipts (
  receipt_id uuid PRIMARY KEY,
  pipeline_id text NOT NULL CHECK (pipeline_id <> '' AND pipeline_id = btrim(pipeline_id)),
  migration_id text NOT NULL CHECK (migration_id <> '' AND migration_id = btrim(migration_id)),
  run_id uuid NOT NULL REFERENCES event_pipeline_topology_provisioning_runs(run_id),
  lineage text NOT NULL CHECK (lineage IN ('completed', 'resolved_target')),
  target_topology jsonb NOT NULL CHECK (jsonb_typeof(target_topology) = 'object'),
  target_fingerprint text NOT NULL CHECK (target_fingerprint ~ '^[0-9a-f]{64}$'),
  migration_contract jsonb NOT NULL CHECK (jsonb_typeof(migration_contract) = 'object'),
  migration_fingerprint text NOT NULL CHECK (migration_fingerprint ~ '^[0-9a-f]{64}$'),
  live_topology jsonb NOT NULL CHECK (jsonb_typeof(live_topology) = 'object'),
  live_fingerprint text NOT NULL CHECK (live_fingerprint ~ '^[0-9a-f]{64}$'),
  readiness_status text NOT NULL CHECK (readiness_status = 'ready'),
  finalization_code text NOT NULL CHECK (finalization_code <> '' AND finalization_code = btrim(finalization_code)),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (pipeline_id, migration_id)
);

CREATE INDEX event_pipeline_topology_finalization_receipts_pipeline_idx
  ON event_pipeline_topology_finalization_receipts (pipeline_id, created_at DESC);

CREATE TABLE event_pipeline_topology_finalization_verifications (
  verification_id uuid PRIMARY KEY,
  receipt_id uuid NOT NULL REFERENCES event_pipeline_topology_finalization_receipts(receipt_id),
  status text NOT NULL CHECK (status IN ('closed', 'blocked')),
  code text NOT NULL CHECK (code <> '' AND code = btrim(code)),
  registry_target_fingerprint text NOT NULL CHECK (registry_target_fingerprint ~ '^[0-9a-f]{64}$'),
  live_fingerprint text CHECK (live_fingerprint IS NULL OR live_fingerprint ~ '^[0-9a-f]{64}$'),
  readiness_status text CHECK (readiness_status IN ('ready', 'degraded', 'not_ready')),
  evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
  verified_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX event_pipeline_topology_finalization_verifications_closed_idx
  ON event_pipeline_topology_finalization_verifications (receipt_id)
  WHERE status = 'closed';

CREATE INDEX event_pipeline_topology_finalization_verifications_history_idx
  ON event_pipeline_topology_finalization_verifications (receipt_id, verified_at DESC);

CREATE FUNCTION reject_event_pipeline_topology_finalization_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, public
AS $$
BEGIN
  RAISE EXCEPTION 'topology finalization evidence is append-only';
END;
$$;

CREATE TRIGGER event_pipeline_topology_finalization_receipts_immutable
BEFORE UPDATE OR DELETE
ON event_pipeline_topology_finalization_receipts
FOR EACH ROW
EXECUTE FUNCTION reject_event_pipeline_topology_finalization_receipt_mutation();

CREATE TRIGGER event_pipeline_topology_finalization_verifications_immutable
BEFORE UPDATE OR DELETE
ON event_pipeline_topology_finalization_verifications
FOR EACH ROW
EXECUTE FUNCTION reject_event_pipeline_topology_finalization_receipt_mutation();

COMMIT;
