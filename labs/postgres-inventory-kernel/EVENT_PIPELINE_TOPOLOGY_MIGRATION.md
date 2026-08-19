# Event pipeline staged topology migration

This runbook applies when the reviewed JetStream topology must change without weakening the steady-state exact-match readiness contract.

## Model

The normal `topology` object is always the desired steady-state target.

An optional migration reconstructs the previously valid source by overriding only fields that differ from the target:

```yaml
topology:
  stream:
    subjects:
      - spotwo.wms.events.>
    storage: file
    retention: limits
    replicas: 3
    duplicate_window_seconds: 120
  business_consumer:
    delivery_mode: pull
    deliver_policy: all
    ack_policy: explicit
    filter_subject: spotwo.wms.events.inventory.position.changed
    max_ack_pending: 10
    max_deliver: 3
  canary_consumer:
    delivery_mode: pull
    deliver_policy: all
    ack_policy: explicit
    filter_subject: spotwo.wms.events.health_check.ping
    max_ack_pending: 10
    max_deliver: 3

topology_migration:
  id: replicas-1-to-3
  valid_until: "2026-08-19T12:00:00Z"
  from_overrides:
    stream:
      replicas: 1
```

The source in this example is identical to the target except for `stream.replicas: 1`.

## Why overrides instead of `from` plus `to`

The target already exists in `topology`. Duplicating the entire target inside a migration would create two sources of truth.

`from_overrides` describes only the reviewed differences while the runtime still reconstructs and validates a complete source topology before accepting it.

## Phase is derived

There is no persisted `phase: rollout` field.

```text
observed_at <= valid_until  -> active
observed_at >  valid_until  -> expired
```

This prevents a stale registry value from extending a rollout forever.

## Preflight

Validate the registry before touching NATS:

```bash
python scripts/validate_event_pipeline_deployments.py
```

Then inspect the current broker:

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

At the start of an active migration, the expected result may be:

```json
{
  "status": "ok",
  "code": "topology_migration_source_accepted",
  "matched": "source"
}
```

The target comparison remains visible inside the JSON, so accepting the source does not hide how it differs from the target.

## Rollout

1. Commit the target topology and time-bounded migration contract.
2. Wait for repository validation to pass.
3. Apply the intended JetStream configuration through the normal deployment/provisioning mechanism.
4. Re-run the topology inspector.
5. Continue only when `matched` becomes `target`.
6. Verify the complete event-pipeline readiness gate.
7. Remove `topology_migration` from the registry.

The inspector itself never applies the topology change.

## Readiness states

### Active, source still live

```text
status  ok
matched source
code    topology_migration_source_accepted
```

The deployment may remain `READY` if all other readiness signals are healthy.

### Active, target live

```text
status  ok
matched target
code    null
```

The rollout has reached its target. Remove the migration contract when the deployment is confirmed stable.

### Active, unexpected third state

```text
status  critical
matched neither
code    topology_migration_drift
```

Do not expand `from_overrides` merely to make an accidental state pass. Either repair the deployment or review a new intentional migration.

### Expired, source still live

```text
status  critical
matched source
code    topology_migration_expired
```

The old topology is no longer authorized. The readiness gate becomes `NOT_READY`.

### Expired, target live

```text
status  warning
matched target
code    topology_migration_contract_expired
```

Runtime topology is correct, but the stale migration declaration should be removed. The deployment is `DEGRADED`, not `NOT_READY`.

## CLI exit codes

```text
0  topology contract ok
1  warning
2  critical
```

Example:

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --check
```

## Prometheus

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --format prometheus
```

The contract exporter exposes bounded status, target-match, active-migration, and `target|source|neither` match-state signals. Migration IDs, deadlines, subjects, stream names, durable names, and raw configuration values are not labels.

## Failure recovery

If target provisioning fails during the active window, restore the reviewed source topology and confirm `matched: source` before deciding whether to retry.

If the deadline will be exceeded, do not edit the timestamp in place as an operational shortcut. Treat extending the window as a new reviewed change with a reason visible in Git history.

## Definition of done

A topology migration is complete only when:

- live topology exactly matches `topology`;
- `bin/inspect-kernel-event-pipeline-topology --check` exits 0;
- event-pipeline readiness is healthy;
- `topology_migration` has been removed from the registry;
- the steady-state exact-match contract is again the only accepted topology.
