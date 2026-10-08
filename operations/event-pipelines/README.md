# Event pipeline deployment registry

This directory is the canonical machine-readable deployment contract for operational event pipelines.

The registry deliberately stores **identities, desired topology, bounded migration intent, policy, and environment-variable names**, not database URLs, NATS credentials, tokens, or other secrets. Runtime endpoints remain deployment inputs supplied through the named environment variables.

Each pipeline binds five operational views:

```text
static deployment configuration
        +
JetStream topology contract
        +
current component health
        +
external canary execution watchdog
        +
canary SLI and error-budget burn
        ↓
READY / DEGRADED / NOT_READY
```

## Why this exists

Before this registry, stream names, durable consumer names, PostgreSQL Inbox consumer identities, canary identities, cadence, SLO targets, watchdog thresholds, and broker provisioning could drift independently between commands and deployment manifests.

The registry makes those values one reviewed contract and gives the readiness gate one source of truth.

## Boundaries

- `transport.stream` is the JetStream stream carrying the business pipeline.
- `transport.subject_prefix` is the publisher routing prefix.
- `consumer.durable` is the durable JetStream consumer for the real pipeline.
- `consumer.inbox_consumer_name` is the logical PostgreSQL Inbox/projection consumer identity. It is intentionally separate from the NATS durable name.
- `consumer.projection_gap_monitor` selects `inventory_position` for a projector with persisted pending version gaps, or `none` for strict-order projectors with no such buffer (Warehouse Work State). `none` omits that component from aggregate health; it does **not** waive the consumer failure or JetStream checks.
- `canary.durable` and `canary.consumer_name` identify the dedicated synthetic canary path.
- `canary.cadence_seconds` is defined once and is also the expected watchdog cadence.
- `topology` declares the singular steady-state JetStream target.
- optional `topology_migration` authorizes one reviewed source topology only until a deadline.
- `runtime.*_env` values name environment variables; they do not contain secrets.

## Generate a third pipeline safely

The scaffolder reuses one reviewed pipeline's topology and policies while
allocating new consumer, canary and watchdog identities. It writes **only local
repository files**, never touches PostgreSQL, JetStream or a running deployment.

Dry-run first:

```bash
python scripts/scaffold_event_pipeline.py \
  --pipeline-id order-picked-projection \
  --event-type order.picked
```

Write the reviewed, **disabled** scaffold:

```bash
python scripts/scaffold_event_pipeline.py \
  --pipeline-id order-picked-projection \
  --event-type order.picked \
  --write
```

This appends one registry entry without rewriting existing comments and
creates:

- `labs/postgres-inventory-kernel/event_pipeline_handlers/order_picked_projection.py` - an allowlisted handler that rejects the domain operation until implemented.
- `labs/postgres-inventory-kernel/tests/test_order_picked_projection_scaffold.py` - a guard that **requires disabled state** plus an explicitly skipped domain E2E test.

The registry includes the handler module and class; `bin/run-kernel-event-consumer`
now resolves this reviewed binding instead of editing a hardcoded two-projector
table. Both old pipelines have explicit bindings. Only built-in projector
modules and reviewed `event_pipeline_handlers.*` modules can be loaded.

Every scaffold is `enabled: false` and uses the strict `projection_gap_monitor: none`
mode: a new domain has **no known Position pending-gap buffer**. Do not copy
Inventory Position gap health to an unrelated projector.

Before enabling a new pipeline, implement the Inbox-transaction handler,
replace both scaffold tests with passing domain and real PostgreSQL/JetStream
E2E tests (ordering, duplicate redelivery, gap behavior, ACK after commit),
then change the registry flag as a separately reviewed rollout. The
scaffolder does **not** claim deployment readiness, automatically provision
JetStream or synthesize missing domain semantics.

Generation is exclusive: duplicate pipeline IDs or business subjects,
non-unique durables/Inbox names, invalid identifiers, and existing generated
files are rejected. CI verifies the deployment JSON Schema, cross-pipeline
identities, topology invariants, and scaffolder unit tests.

## Third concrete business pipeline: Inventory Transaction Index

The third pipeline exercises the scaffolder against an existing committed WMS
fact, `inventory.transaction.posted`. The event is emitted atomically from
`inventory_transactions` by the existing transactional Outbox trigger.

```text
Inventory Transaction posting
  -> PostgreSQL Outbox (inventory.transaction.posted)
  -> NATS JetStream WMS_EVENTS (exact business subject filter)
  -> inventory_transaction_index Inbox consumer
  -> inventory_transaction_index_projection read model
  -> JetStream ACK only after PostgreSQL commit
```

The `inventory-transaction-index` configuration uses generated unique durable,
Inbox, canary, and watchdog identities and an allowlisted Python handler.
It is `enabled: true` as a reviewed **desired state** after a real PostgreSQL
and JetStream E2E test, including lost-ACK redelivery without a second side
effect. No live deployment or provisioned JetStream consumer is implied.

The projection is a **search index** of posted transactions by tenant, time,
type and source reference. It is not a replacement for the immutable Inventory
Transaction ledger or an Event Store.

### Event Contract V2: explicit tenant boundary

As of migration `032_event_contract_v2_tenant_identity.sql`, newly emitted
`inventory.transaction.posted` events carry `schema_version: 2` and a
canonical `tenant_id` at the **top level of the envelope**, alongside
`event_id`, `type`, `aggregate_id`, and `data`:

```json
{
  "event_id": "0199a08e-0d00-7000-8000-000000000001",
  "tenant_id": "00000000-0000-0000-0000-000000000001",
  "type": "inventory.transaction.posted",
  "schema_version": 2,
  "aggregate_type": "InventoryTransaction",
  "aggregate_id": "0199a08e-0d00-7000-8000-000000000002",
  "aggregate_version": 1,
  "subject": "inventory-transaction/0199a08e-0d00-7000-8000-000000000002",
  "data": {
    "transaction_id": "0199a08e-0d00-7000-8000-000000000002",
    "transaction_type": "receipt",
    "source_reference": "ASN-123"
  }
}
```

This is an **illustrative subset**: actual envelopes additionally include
`source`, `occurred_at` and `recorded_at` (timezone-aware ISO 8601), etc.
The V2 parser rejects missing or noncanonical tenant UUIDs. V1 envelopes
must not silently gain a tenant claim, and existing V1 Outbox records are
**not rewritten**. Other event types remain V1.

The V2 Transaction Index projector reads the tenant from the envelope, never
SELECTs from `inventory_transactions` or `tenants`, and has **no source
table foreign keys**. Its immutable index key is
`(consumer_name, tenant_id, transaction_id)`; it verifies event/aggregate
consistency and processes the Inbox receipt and projection in one transaction.
A tenant-scoped invocation may supply `expected_tenant_id` to reject a
cross-tenant event before committing anything.

**V1 rollout bridge:** historical posting events still lacking `tenant_id`
may be drained using the co-located ledger lookup, with their Inbox metadata
marked `mode: v1_legacy`. This is an explicit fallback, not silent V1→V2
upcasting. A standalone remote consumer cannot use this fallback: drain or
replay historical V1 against the source before decoupling the index.

**Security boundary:** `tenant_id` is a routing/isolation identity, **not**
authentication or a signature. A producer or broker authorized to send
arbitrary messages could forge it. Cross-service deployment still requires
trusted publish credentials, broker ACLs, tenant-aware authorization for any
read API, and a deliberate tenant routing/partitioning policy. The shared
JetStream business subject does not itself enforce per-tenant delivery ACLs.

CI includes `test_inventory_transaction_index_scaffold.py` (domain integrity
and duplicate Inbox proof), `test_inventory_transaction_index_e2e.py`
(real JetStream plus ACK uncertainty) and a regression showing the reviewed
third registry entry matches `scaffold_event_pipeline.build_pipeline`.

## Tenant-scoped V2 routing and read authorization (opt-in)

The V2 Transaction Index now has a lab-tested, explicit tenant-bound subject:
`<prefix>.tenants.<canonical-tenant-uuid>.inventory.transaction.posted`.
An opted-in publisher verifies its tenant scope before network access, and a
tenant consumer verifies its exact JetStream durable filter and triple match
of subject, `Spotwo-Tenant-Id` header and envelope identity before Inbox
handling. Read queries require an authenticated-principal tenant grant and
`inventory.transactions.read` scope.

This **does not change the current shared routing in the three enabled
registry pipelines**. NATS broker-enforced per-tenant ACLs, tenant-scoped
Outbox claiming, per-tenant topology contracts and rollout/canary gates are
required before enabling tenant routing in production.

See [Tenant Routing and Authorization](../../labs/postgres-inventory-kernel/EVENT_PIPELINE_TENANT_ROUTING.md)
for the precise opt-in contract, negative tests and staged rollout gates.

## Tenant-scoped deployment registry, provisioning, and readiness

The root `tenant_deployments` collection holds reviewed tenant-specific
candidates, referencing the existing `inventory-transaction-index` parent
pipeline rather than duplicating its global stream and subject prefix.

The first candidate `transaction-index-pilot-a` is **disabled**. It stores
an exact tenant UUID, separate business/tenant-canary durable identities, an
isolated Inbox consumer name, and only the **names** of four NATS credential
environment variables. It contains no URLs, passwords, tokens or assumed
runtime status. The three existing shared business pipelines remain unchanged.

The opt-in tooling supports:

```bash
# Connect using TENANT_PILOT_PROVISIONER_NATS_URL from the environment:
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode plan

# Approved apply: creates ONLY missing exact-subject tenant durables on an
# already-existing shared stream; refuses drift or missing stream.
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode apply \
  --operator-id deployment-operator --approval-id reviewed-change-id

# Fail closed until enabled and topology + authenticated ACL + tenant canary
# all prove healthy (this command also needs scoped client credential envs).
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode readiness
```

A business durable must filter **exactly**
`<prefix>.tenants.<tenant_id>.inventory.transaction.posted`, and a separate
canary durable filters **exactly**
`<prefix>.tenants.<tenant_id>.health_check.ping`. An admin-only provisioner
never mutates streams or existing consumers and does not activate any publisher.
The readiness command makes real negative ACL requests using tenant-scoped
credentials and sends a bounded canary through PUB ACK, scoped durable pull
and ACK. Evidence must match the deployment identity and be recent.

Important: `--approval-id` records an operator-supplied reference in the
invocation, not an external approval-system attestation or persisted audit.
This candidate is not automatically enabled, and the authenticated NATS
fixture does not yet authorize tenant A's dedicated canary. The lab
deliberately proves that this missing permission results in **NOT_READY**,
rather than claiming deployment success. Production activation still requires
managed secrets/roles, authenticated canary privileges, a runtime rollout,
and an approval/audit integration.

See [Tenant Routing and Authorization](../../labs/postgres-inventory-kernel/EVENT_PIPELINE_TENANT_ROUTING.md)
for the full security and migration boundary.

## Tenant canary authorization and audited activation (lab)

The isolated authenticated broker fixture now grants **tenant A** only its
transaction subject and an independently filtered heartbeat subject. A
separate tenant-canary identity may access only the canary durable INFO,
NEXT and ACK control subjects; it cannot use the business durable.

The activation controller uses real credentials to verify denied cross-tenant
broker operations, exact tenant topology, and canary PUB ACK / pull / ACK.
A positive readiness assessment permits a single atomic PostgreSQL
`record_tenant_deployment_activation` receipt, with the exact tenant,
deployment, stream, business/canary durables and subjects, operator and
approval references, and fresh proof timestamps. Journal rows are
append-only, and approval replay is idempotent for the same identity.
Conflicting approval or a second activation is rejected.

```bash
# Requires reviewed enabled:true in the registry, real scoped NATS URLs,
# parent database URL and a separately approved operator/approval reference.
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode activate \
  --operator-id rollout-owner --approval-id change-ticket
```

**Safety:** the shipped `transaction-index-pilot-a` remains `enabled: false`.
The CLI refuses activation of disabled candidates. Unit/E2E tests enable a
local in-memory copy to prove readiness/activation against the ephemeral
authenticated NATS broker; the repository registry and production deployments
are not activated. An approval string is an *operator reference*, not proof
that a real external approval system authorized the change. The recorder
function is revoked from `PUBLIC` and requires a separately provisioned,
authorized DB role in production. Production activation requires live
managed credentials, trusted approval verification, rollout orchestration,
tenant traffic fencing, runtime SLIs and rollback.

## Tenant rollout waves, projection lag and guarded rollback (lab)

SQL migration `035_tenant_rollout_cutover.sql` adds an activation-receipt-
fenced per-deployment rollout state and an append-only action journal.
The controlled CLI accepts **explicit reviewed event IDs only**; it never
rewrites an entire tenant's backlog or switches newly inserted events
automatically. Only `inventory.transaction.posted` V2 rows that have
**never been claimed or attempted** may change from shared to tenant route.
The DB function atomically locks all selected Outbox rows, enforces
complete eligibility, stages the full batch or none, and records actions.
Old direct stage calls are blocked after a rollout begins.

```bash
# Only after the separate, reviewed activation receipt and an enabled pilot:
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode rollout-start \
  --activation-receipt <activation-uuid> \
  --operator-id rollout-owner --change-ref CR-101

# Each wave re-checks live broker ACL + scoped canary; refuses a second
# wave while the previous tenant rows are pending or not yet projected.
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode rollout-wave \
  --event-id <reviewed-event-uuid> \
  --operator-id rollout-owner --change-ref CR-101-wave1

# Read-only, including when the broker or publisher is unavailable.
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode rollout-status

# First pause future waves; then return ONLY never-attempted, unclaimed
# tenant-staged events to shared. Published/attempted rows remain pinned
# to the tenant route and require forward recovery, never blind replay.
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode rollout-pause \
  --operator-id rollout-owner --change-ref CR-101-pause
python bin/run-kernel-event-pipeline-tenant-deployment \
  --deployment transaction-index-pilot-a --mode rollout-revert \
  --event-id <unattempted-event-uuid> \
  --operator-id rollout-owner --change-ref CR-101-rollback
```

Monitoring reports tenant pending delivery, attempted/unpublished delivery,
published-but-not-projected count, and oldest projection lag. A second wave
requires zero pending and zero unprojected **live** tenant rows. The
projector's configured tenant Inbox identity is used, not just a matching
tenant ID. Rollout and rollback are atomic bounded batches (up to 100 event
IDs) with append-only operator/change reference evidence.

**No automatic runtime traffic routing:** activation and rollout begin do
not provision worker processes, intercept new producer inserts, replace
broker ACLs, switch shared durable configuration or activate the shipped
disabled pilot. A single event cannot safely move after a publish attempt
because a lost ACK makes broker delivery uncertain. The rollback API
therefore refuses attempted events, including ones whose lease has expired.
Partial forward recovery is an explicit operational decision. Publisher
scheduling, consumer health/SLOs, external approval authentication, broker
secrets, PostgreSQL restricted roles, and longer-lived replay/archive
reconciliation remain production rollout gates.

## Topology contract

The topology section declares:

### Stream

- exact subject set;
- storage type;
- retention policy;
- replica count;
- duplicate window.

### Business and canary durable consumers

- pull versus push mode;
- delivery policy;
- ACK policy;
- exact filter subject;
- `max_ack_pending`;
- `max_deliver`.

Both durables must remain explicit-ACK pull consumers. The business filter must stay within the publisher subject prefix **and must not match the synthetic canary subject**. For the Position projector the canonical business filter is `spotwo.wms.events.inventory.position.changed`. The canary durable is filtered only to `spotwo.wms.events.health_check.ping`.

This isolation matters because the Position projector accepts only `inventory.position.changed`; a broad business wildcard would also deliver synthetic canary traffic to the domain projector.

This is desired-state configuration, not a provisioning mechanism. The registry does not create, update, or repair JetStream resources. Both Position and Work State are enabled in the registry after lab proof, but this flag **does not establish that either runtime is deployed or ready**. The readiness gate still requires live component telemetry, watchdog receipts, and canary SLI; missing runtime signals remain NOT_READY.

## Bounded topology migration

For an intentional rollout, keep `topology` as the new steady-state target and describe only the old fields that differ:

```yaml
topology_migration:
  id: replicas-1-to-3
  valid_until: "2026-08-19T12:00:00Z"
  from_overrides:
    stream:
      replicas: 1
```

The runtime reconstructs and validates the complete source topology from those overrides.

While the deadline is active, exact source or exact target are acceptable. A third state is critical drift. After the deadline only target is accepted; a source still deployed is critical. If target is already live but the expired migration declaration remains in Git, topology becomes warning until the stale declaration is removed.

There is deliberately no stored `phase` value. Active versus expired is derived from `observed_at` and `valid_until`, so a stale string cannot extend a rollout indefinitely.

See `labs/postgres-inventory-kernel/EVENT_PIPELINE_TOPOLOGY_MIGRATION.md` for the rollout runbook.

## Validation

Run:

```bash
python scripts/validate_event_pipeline_deployments.py
```

or the full repository gate:

```bash
bin/check
```

Validation includes JSON Schema shape checks plus cross-field invariants such as unique pipeline IDs, warning/critical ordering, SLI freshness versus canary cadence, watchdog timeout versus canary timeout, stream subject coverage, business/canary subject isolation, pull-consumer mode, explicit ACK, and validity of any reconstructed migration source topology.

## Topology inspection

Compare the live broker with the steady-state target and any active migration contract:

```bash
bin/inspect-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

Exit codes are `0` ok, `1` warning, and `2` critical.

The inspector is read-only. It calls JetStream metadata APIs only and never creates, updates, deletes, publishes, fetches, ACKs, purges, or repairs resources.

Missing required streams or consumers are drift. Broker unavailability is reported as inspection unavailable rather than as a false match.

## Rollout preflight

Before an external provisioner mutates JetStream, build a deterministic dry-run plan:

```bash
bin/plan-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The planner combines the reviewed target, optional migration contract, and one live topology snapshot. Its decision is one of:

- `safe_to_apply` - the migration is active, live topology exactly matches the source, and the source-to-target delta affects exactly one JetStream resource;
- `no_changes` - the target is already deployed;
- `blocked` - do not begin the rollout from the observed state.

A `safe_to_apply` plan groups all changed fields for one resource into one `apply_target_resource` step, followed by exact-target verification, full event-pipeline readiness verification, and migration-contract cleanup. The planner itself is always dry-run and never performs that resource update.

If one migration changes more than one of `stream`, `business_consumer`, and `canary_consumer`, preflight blocks it. A sequential multi-resource rollout would create a hybrid topology that is neither the exact source nor the exact target accepted by the bounded migration contract. Split such work into separate reviewed migrations.

See `labs/postgres-inventory-kernel/EVENT_PIPELINE_TOPOLOGY_PREFLIGHT.md` for the preflight runbook.

## Authorized provisioning

After preflight returns `safe_to_apply`, one narrowly supported in-place migration can be executed with an explicit authorization tied to the reviewed migration ID:

```bash
bin/apply-kernel-event-pipeline-topology \
  --pipeline inventory-position-projection \
  --apply-migration <exact-topology-migration-id> \
  --tenant-id <uuid> \
  --pretty
```

The command re-runs preflight and requires full deployment readiness before mutation. Immediately before writing it re-reads the selected resource and requires the bounded source snapshot to still equal the preflight source, preventing a stale preflight from overwriting concurrent topology changes.

The initial automatic in-place capability is deliberately narrow:

- stream: `replicas`, `duplicate_window_seconds`;
- business consumer: `max_ack_pending`, `max_deliver`;
- canary consumer: `max_ack_pending`, `max_deliver`.

Unsupported topology changes are blocked. The provisioner never falls back to delete/recreate.

After mutation it requires exact target topology, a fresh tracked synthetic canary with status `ok`, and full deployment readiness `READY`. If a post-apply gate fails, it restores the complete source resource config captured immediately before mutation and then requires topology to match the declared source again.

A successful run only reports that the migration declaration can be cleaned up. It does not edit Git or remove `topology_migration` automatically.

See `labs/postgres-inventory-kernel/EVENT_PIPELINE_TOPOLOGY_PROVISIONING.md` and ADR 0049 for execution and rollback semantics.

## Crash-safe provisioning journal

Every mutating rollout is now journaled in PostgreSQL before the first JetStream write. The journal records the authorized pipeline/migration identity, one affected resource, the source topology snapshot, bounded field changes, an intent fingerprint, durable execution state, and a lease.

Only one non-terminal provisioning run may exist per pipeline. While another process owns a live lease, a second provisioner is blocked. After lease expiry, re-running the same command with the same migration ID claims the existing run ID and reconciles against live JetStream topology rather than starting a second rollout.

Recovery is deterministic:

- exact source live -> finalize the old run as rolled back/source restored;
- exact target live -> continue the missing target, canary, and readiness verification stages;
- rollback was started but target is still live -> restore the persisted source values and verify source;
- neither exact source nor exact target -> stop at `manual_intervention` without guessing or auto-repairing drift.

Lease loss is fail-stop. A process that loses ownership does not attempt a compensating mutation because another process may already be reconciling the run.

The same apply command is therefore both the normal executor and the crash-recovery entry point. JSON output includes `execution_journal` with run ID, state, attempt count, recovery count, and lease metadata.

See `labs/postgres-inventory-kernel/EVENT_PIPELINE_TOPOLOGY_CRASH_RECOVERY.md` and ADR 0050 for recovery and lease semantics.

## Operational readiness

For one configured pipeline:

```bash
bin/inspect-kernel-event-pipeline-readiness \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

The command resolves runtime endpoints from the environment-variable names in this registry and resolves the external watchdog JSON file from `runtime.watchdog_state_path_env`. Explicit CLI endpoint overrides exist for labs, but the logical identities, topology, migration intent, and policy still come only from this registry.

The readiness result is fail-closed:

- `READY` - configuration is enabled, topology contract is ok, and all required runtime signals are available and healthy.
- `DEGRADED` - at least one signal is warning and none is critical.
- `NOT_READY` - configuration is disabled/invalid, topology is unauthorized, a required signal is unavailable, or any signal is critical.

The readiness gate is an operational deployment decision. It does not mutate the WMS database, NATS stream, consumer, Inbox, projection, SLI history, or external watchdog state.

## Intentional changes

Change the registry and provisioning as one reviewed rollout. For a topology transition, merge the target plus a short migration deadline before changing the broker. Run rollout preflight before mutation. If the change is in the authorized in-place capability set, execute it with the exact reviewed migration ID. Once live topology matches target, a fresh canary is healthy, and event-pipeline readiness is healthy, remove `topology_migration` so steady-state exact matching is again the only accepted state.
