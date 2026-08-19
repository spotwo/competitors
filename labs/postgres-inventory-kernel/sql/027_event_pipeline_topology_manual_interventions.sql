BEGIN;

SET search_path TO kernel_lab, public;

CREATE TABLE event_pipeline_topology_interventions (
  intervention_id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES event_pipeline_topology_provisioning_runs(run_id),
  pipeline_id text NOT NULL CHECK (pipeline_id <> '' AND pipeline_id = btrim(pipeline_id)),
  migration_id text NOT NULL CHECK (migration_id <> '' AND migration_id = btrim(migration_id)),
  resource text NOT NULL CHECK (resource IN ('stream', 'business_consumer', 'canary_consumer')),
  state text NOT NULL CHECK (
    state IN (
      'detected',
      'proposed',
      'confirmed',
      'executing',
      'resolved_source',
      'resolved_target',
      'aborted',
      'failed'
    )
  ),
  evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
  evidence_fingerprint text NOT NULL CHECK (evidence_fingerprint ~ '^[0-9a-f]{64}$'),
  proposed_action text CHECK (proposed_action IN ('restore_source', 'accept_target', 'abort')),
  proposed_by text CHECK (proposed_by IS NULL OR (proposed_by <> '' AND proposed_by = btrim(proposed_by))),
  proposed_at timestamptz,
  confirmed_by text CHECK (confirmed_by IS NULL OR (confirmed_by <> '' AND confirmed_by = btrim(confirmed_by))),
  confirmed_at timestamptz,
  resolution_code text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  state_changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT event_pipeline_topology_intervention_proposal_shape CHECK (
    (state = 'detected' AND proposed_action IS NULL AND proposed_by IS NULL AND proposed_at IS NULL AND confirmed_by IS NULL AND confirmed_at IS NULL)
    OR
    (state = 'proposed' AND proposed_action IS NOT NULL AND proposed_by IS NOT NULL AND proposed_at IS NOT NULL AND confirmed_by IS NULL AND confirmed_at IS NULL)
    OR
    (state IN ('confirmed', 'executing', 'resolved_source', 'resolved_target', 'aborted', 'failed')
      AND proposed_action IS NOT NULL AND proposed_by IS NOT NULL AND proposed_at IS NOT NULL
      AND confirmed_by IS NOT NULL AND confirmed_at IS NOT NULL
      AND confirmed_by <> proposed_by)
  )
);

CREATE UNIQUE INDEX event_pipeline_topology_interventions_active_run_idx
  ON event_pipeline_topology_interventions (run_id)
  WHERE state IN ('detected', 'proposed', 'confirmed', 'executing');

CREATE INDEX event_pipeline_topology_interventions_pipeline_history_idx
  ON event_pipeline_topology_interventions (pipeline_id, created_at DESC);

CREATE INDEX event_pipeline_topology_interventions_run_history_idx
  ON event_pipeline_topology_interventions (run_id, created_at DESC);

CREATE FUNCTION set_event_pipeline_topology_intervention_state_changed_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, public
AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  IF NEW.state IS DISTINCT FROM OLD.state THEN
    NEW.state_changed_at := clock_timestamp();
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER event_pipeline_topology_intervention_state_changed_at
BEFORE UPDATE
ON event_pipeline_topology_interventions
FOR EACH ROW
EXECUTE FUNCTION set_event_pipeline_topology_intervention_state_changed_at();

COMMIT;
