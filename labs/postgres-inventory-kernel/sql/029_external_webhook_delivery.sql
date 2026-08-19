BEGIN;

SET search_path TO kernel_lab, public;

CREATE TABLE external_event_subscriptions (
  subscription_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  endpoint_url text NOT NULL CHECK (endpoint_url ~ '^https://'),
  state text NOT NULL CHECK (state IN ('active', 'paused', 'revoked')),
  event_types text[] NOT NULL CHECK (cardinality(event_types) > 0),
  signing_secret_ref text NOT NULL CHECK (signing_secret_ref <> '' AND signing_secret_ref = btrim(signing_secret_ref)),
  signing_secret_version integer NOT NULL CHECK (signing_secret_version > 0),
  max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 32),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE external_event_deliveries (
  delivery_id uuid PRIMARY KEY,
  subscription_id uuid NOT NULL REFERENCES external_event_subscriptions(subscription_id) ON DELETE CASCADE,
  event_id text NOT NULL CHECK (event_id <> '' AND event_id = btrim(event_id)),
  event_type text NOT NULL CHECK (event_type <> '' AND event_type = btrim(event_type)),
  generation integer NOT NULL DEFAULT 0 CHECK (generation >= 0),
  envelope jsonb NOT NULL CHECK (jsonb_typeof(envelope) = 'object'),
  state text NOT NULL CHECK (state IN ('pending', 'in_flight', 'delivered', 'dead_letter')),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  lease_owner text,
  lease_expires_at timestamptz,
  signing_secret_ref text NOT NULL CHECK (signing_secret_ref <> '' AND signing_secret_ref = btrim(signing_secret_ref)),
  signing_secret_version integer NOT NULL CHECK (signing_secret_version > 0),
  replay_of_delivery_id uuid REFERENCES external_event_deliveries(delivery_id),
  replay_reason text,
  replay_requested_by text,
  last_http_status integer CHECK (last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599),
  last_error_code text,
  delivered_at timestamptz,
  dead_lettered_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT external_event_delivery_identity UNIQUE (subscription_id, event_id, generation),
  CONSTRAINT external_event_delivery_lease_shape CHECK (
    (state = 'in_flight' AND lease_owner IS NOT NULL AND lease_owner <> '' AND lease_owner = btrim(lease_owner) AND lease_expires_at IS NOT NULL)
    OR
    (state <> 'in_flight' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  CONSTRAINT external_event_delivery_terminal_shape CHECK (
    (state = 'delivered' AND delivered_at IS NOT NULL AND dead_lettered_at IS NULL)
    OR
    (state = 'dead_letter' AND delivered_at IS NULL AND dead_lettered_at IS NOT NULL)
    OR
    (state IN ('pending', 'in_flight') AND delivered_at IS NULL AND dead_lettered_at IS NULL)
  ),
  CONSTRAINT external_event_delivery_replay_shape CHECK (
    (generation = 0 AND replay_of_delivery_id IS NULL AND replay_reason IS NULL AND replay_requested_by IS NULL)
    OR
    (generation > 0 AND replay_of_delivery_id IS NOT NULL AND replay_reason IS NOT NULL AND replay_reason <> '' AND replay_requested_by IS NOT NULL AND replay_requested_by <> '')
  )
);

CREATE INDEX external_event_delivery_claim_idx
  ON external_event_deliveries (next_attempt_at, created_at, delivery_id)
  WHERE state IN ('pending', 'in_flight');

CREATE INDEX external_event_delivery_event_history_idx
  ON external_event_deliveries (subscription_id, event_id, generation DESC);

CREATE TABLE external_event_delivery_attempts (
  attempt_id uuid PRIMARY KEY,
  delivery_id uuid NOT NULL REFERENCES external_event_deliveries(delivery_id) ON DELETE CASCADE,
  attempt_no integer NOT NULL CHECK (attempt_no > 0),
  worker_id text NOT NULL CHECK (worker_id <> '' AND worker_id = btrim(worker_id)),
  started_at timestamptz NOT NULL,
  completed_at timestamptz,
  outcome text CHECK (outcome IS NULL OR outcome IN ('success', 'retry', 'dead_letter', 'lease_expired')),
  http_status integer CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
  error_code text,
  next_attempt_at timestamptz,
  signing_secret_version integer NOT NULL CHECK (signing_secret_version > 0),
  CONSTRAINT external_event_delivery_attempt_number UNIQUE (delivery_id, attempt_no),
  CONSTRAINT external_event_delivery_attempt_completion CHECK (
    (completed_at IS NULL AND outcome IS NULL)
    OR
    (completed_at IS NOT NULL AND outcome IS NOT NULL)
  )
);

CREATE INDEX external_event_delivery_attempt_history_idx
  ON external_event_delivery_attempts (delivery_id, attempt_no DESC);

COMMIT;
