BEGIN;

SET search_path TO kernel_lab, public;

ALTER TABLE event_pipeline_topology_provisioning_runs
  ADD COLUMN state_changed_at timestamptz;

UPDATE event_pipeline_topology_provisioning_runs
SET state_changed_at = started_at
WHERE state_changed_at IS NULL;

ALTER TABLE event_pipeline_topology_provisioning_runs
  ALTER COLUMN state_changed_at SET DEFAULT clock_timestamp(),
  ALTER COLUMN state_changed_at SET NOT NULL;

CREATE FUNCTION set_event_pipeline_topology_provisioning_state_changed_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = kernel_lab, public
AS $$
BEGIN
  IF NEW.state IS DISTINCT FROM OLD.state THEN
    NEW.state_changed_at := clock_timestamp();
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER event_pipeline_topology_provisioning_state_changed_at
BEFORE UPDATE OF state
ON event_pipeline_topology_provisioning_runs
FOR EACH ROW
EXECUTE FUNCTION set_event_pipeline_topology_provisioning_state_changed_at();

COMMIT;
