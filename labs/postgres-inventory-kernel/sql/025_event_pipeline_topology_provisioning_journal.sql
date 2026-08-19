BEGIN;

SET search_path TO kernel_lab, public;

CREATE TABLE event_pipeline_topology_provisioning_runs (
  run_id uuid PRIMARY KEY,
  pipeline_id text NOT NULL CHECK (pipeline_id <> '' AND pipeline_id = btrim(pipeline_id)),
  migration_id text NOT NULL CHECK (migration_id <> '' AND migration_id = btrim(migration_id)),
  resource text NOT NULL CHECK (resource IN ('stream', 'business_consumer', 'canary_consumer')),
  state text NOT NULL CHECK (
    state IN (
      'prepared',
      'applying',
      'applied',
      'target_verified',
      'canary_verified',
      'readiness_verified',
      'rollback_started',
      'source_verified',
      'completed',
      'rolled_back',
      'failed',
      'manual_intervention'
    )
  ),
  state_code text,
  source_snapshot jsonb NOT NULL CHECK (jsonb_typeof(source_snapshot) = 'object'),
  changes jsonb NOT NULL CHECK (jsonb_typeof(changes) = 'array'),
  intent_fingerprint text NOT NULL CHECK (intent_fingerprint ~ '^[0-9a-f]{64}$'),
  lease_owner text,
  lease_expires_at timestamptz,
  attempt_count integer NOT NULL DEFAULT 1 CHECK (attempt_count > 0),
  recovery_count integer NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT event_pipeline_topology_provisioning_lease_shape CHECK (
    (
      state IN ('completed', 'rolled_back', 'failed', 'manual_intervention')
      AND lease_owner IS NULL
      AND lease_expires_at IS NULL
    )
    OR
    (
      state NOT IN ('completed', 'rolled_back', 'failed', 'manual_intervention')
      AND lease_owner IS NOT NULL
      AND lease_owner <> ''
      AND lease_owner = btrim(lease_owner)
      AND lease_expires_at IS NOT NULL
    )
  )
);

CREATE UNIQUE INDEX event_pipeline_topology_provisioning_active_pipeline_idx
  ON event_pipeline_topology_provisioning_runs (pipeline_id)
  WHERE state NOT IN ('completed', 'rolled_back', 'failed', 'manual_intervention');

CREATE INDEX event_pipeline_topology_provisioning_history_idx
  ON event_pipeline_topology_provisioning_runs (pipeline_id, started_at DESC);

CREATE INDEX event_pipeline_topology_provisioning_migration_idx
  ON event_pipeline_topology_provisioning_runs (migration_id, started_at DESC);

COMMIT;
