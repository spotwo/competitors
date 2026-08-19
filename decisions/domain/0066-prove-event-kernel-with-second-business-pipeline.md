# ADR 0066: Prove the event kernel with a second business pipeline

Status: Accepted
Date: 2026-08-19

## Context

The current event runtime has strong operational depth but almost all of that depth was developed around one business consumer: `inventory-position-projection`.

One successful pipeline does not prove that the architecture is generic. A design can look reusable while still carrying hidden assumptions from its first aggregate, event type, projection model, ordering rules, or health signals.

The next useful test is therefore not another reliability feature for Inventory Position. It is a second real business flow that must reuse the same event backbone without copying the runtime.

Warehouse Work is a useful second aggregate because its lifecycle already exists in the PostgreSQL kernel and changes through meaningful operational states such as `planned`, `released`, `assigned`, `in_progress`, `exception`, `completed`, and `cancelled`.

## Decision

Use Warehouse Work state transitions as the second business pipeline proof.

The candidate flow is:

```text
Warehouse Work command
        |
        v
PostgreSQL state transition
        |
        v
transactional Outbox
        |
        v
publisher -> NATS JetStream
        |
        v
shared Inbox consumer runtime
        |
        v
WarehouseWorkStateProjector
        |
        v
warehouse_work_state_projection
```

The new pipeline reuses the existing transactional Outbox, NATS transport conventions, Inbox transaction, consumer failure lane, malformed-delivery quarantine, deployment registry, and generic consumer runtime.

A registry-driven business-consumer launcher selects the handler from the pipeline id instead of introducing another pipeline-specific process loop.

## Separate event version from internal Work version

Do not use `warehouse_works.version` directly as the event aggregate version.

The internal Work version advances for every protected Work mutation. Some mutations do not change the externally meaningful Work state. For example, starting a later task can advance the Work version while the Work remains `in_progress`.

If that internal version were exposed as the state-event aggregate version, a consumer could observe apparent gaps even though no state event was missing.

Introduce a separate `domain_event_version` on Warehouse Work with these rules:

- it starts at zero;
- it advances exactly once when the Work state changes;
- it does not advance for same-state internal mutations;
- every emitted `warehouse.work.state.changed` event uses it as `aggregate_version`;
- the event payload still carries `previous_work_version` and `work_version` as internal mutation evidence.

This gives the event stream contiguous ordering semantics without weakening the internal optimistic/versioned mutation model.

Example:

```text
internal Work version:   0 -> 1 -> 2 -> 3 -> 4 -> 5
Work state event version 0 -> 1 -> 2 -> 3 ------> 4

state:
planned -> released -> assigned -> in_progress -> in_progress -> completed
```

The same-state mutation at internal version 4 is intentionally not a state event.

## Event contract

The second pipeline uses:

```text
event type:        warehouse.work.state.changed
aggregate type:    WarehouseWork
subject:           warehouse-work/<work-id>
NATS subject:      spotwo.wms.events.warehouse.work.state.changed
schema version:    1
```

The payload records at least:

- `work_id`;
- `tenant_id`;
- `warehouse_id`;
- `capability`;
- `domain_reference`;
- `previous_state`;
- `state`;
- `previous_work_version`;
- `work_version`.

The Outbox write is triggered by the same PostgreSQL transaction that changes the Work state, so a committed state transition cannot exist without its corresponding durable event record.

## Projection behavior

`WarehouseWorkStateProjector` reuses the shared Inbox transaction and materializes the latest ordered Work state.

The projection:

- requires the Inbox receipt in the same transaction;
- accepts the first event only at aggregate version `1` from `planned`;
- rejects an aggregate version gap;
- validates `previous_state` against the current projection state;
- treats the same event/version as a duplicate;
- rejects a different event claiming an already-owned version;
- preserves tenant, warehouse, capability, and domain-reference identity;
- records the internal Work version separately from the event aggregate version.

This deliberately exercises a different projection shape from Inventory Position quantity deltas.

## Deployment status

Register `warehouse-work-state-projection` in `operations/event-pipelines/registry.yml`, but keep it `enabled: false` for now.

The second pipeline has exposed a real genericity leak: composite event-pipeline health still contains Inventory Position-specific projection-gap telemetry. Enabling the new pipeline before that health contract is generalized would make a supposedly generic operational layer report another aggregate's health semantics.

The disabled registry entry is therefore intentional evidence, not an incomplete deployment.

The bounded lab runner may execute it only with an explicit `--allow-disabled` flag.

## Cross-pipeline identity rule

With more than one pipeline, uniqueness inside an individual pipeline is no longer enough.

Validate that all JetStream durable names and all logical Inbox consumer identities are globally unique across the deployment registry. This prevents two independently configured pipelines from accidentally sharing delivery state or Inbox idempotency identity.

## What this proves

This slice is successful when the same infrastructure can support both:

```text
InventoryPosition -> quantity projection
WarehouseWork     -> state projection
```

without duplicating transport, Inbox, retry, quarantine, or consumer-loop infrastructure.

It also succeeds by finding aggregate-specific assumptions. A generic architecture is not demonstrated by hiding those assumptions. It is demonstrated by making them explicit and removing or scoping them deliberately.

## What this does not prove

This ADR does not:

- declare the Warehouse Work projection a production read model;
- make database triggers the canonical way every domain event must be emitted;
- require every internal Work mutation to become a domain event;
- enable the second pipeline in deployed topology;
- prove that all current health and canary code is aggregate-neutral;
- promote the whole event kernel to a production architecture by itself.

## Follow-up

The next event-kernel step should generalize composite health so projection-specific signals are declared per pipeline rather than hard-coded for Inventory Position.

After that change, the Warehouse Work pipeline can be enabled and exercised through the same readiness, canary, topology, and recovery machinery as the Inventory Position pipeline. That is the stronger proof that the runtime has moved from one deep implementation to a reusable event-kernel pattern.
