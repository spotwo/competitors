# ADR 0013 - Inventory Domain Events, Outbox and EPCIS Boundary

Status: Proposed

Date: 2026-08-18

## Context

Inventory changes must be visible to WMS modules, automation, ERP integrations, analytics and sometimes trading partners. Direct synchronous callbacks inside an inventory commit create latency and partial-failure risks. At the same time, an internal inventory transaction record and an integration event have different responsibilities.

## Proposed decision

Separate three concepts:

```text
InventoryTransaction
  = internal durable inventory journal

DomainEvent
  = committed fact for Spotwo modules/integrations

EPCIS Event
  = standards-based supply-chain visibility representation when applicable
```

Publish Domain Events through a transactional outbox.

```text
Inventory commit
  |
  +-- ledger rows
  +-- position updates
  +-- outbox event rows
  |
 COMMIT
  |
Publisher
  |
  +-- webhook / Kafka / AMQP / other transport
```

## Candidate event envelope

```text
event_id
type
source
subject
occurred_at
recorded_at
aggregate_type
aggregate_id
aggregate_version
causation_id?
correlation_id?
actor?
warehouse_id?
schema_version
data
```

CloudEvents may be used as an interoperability envelope. Spotwo event semantics remain defined by the domain registry.

## Delivery rules

1. Outbox creation is atomic with the committed inventory change.
2. Publication is at-least-once.
3. `event_id` is stable across retries.
4. Consumers are idempotent.
5. Ordering is guaranteed only where an aggregate or inventory-key version/sequence is explicitly defined; there is no global event ordering guarantee.
6. `occurred_at` and `recorded_at` are distinct concepts.
7. Domain events carry immutable facts, not commands disguised in past tense.
8. Failure to deliver an external event does not roll back an already committed inventory transaction.
9. Replay authorization and data-retention rules are explicit because event archives may contain commercially sensitive inventory and actor data.

## EPCIS boundary

GS1 EPCIS is a visibility standard, not Spotwo's internal inventory ledger. Map suitable physical events such as receiving, shipping, movement, aggregation or relevant disposition changes when the required What/When/Where/Why context and identifiers are available.

Do not force internal reservation, allocation or low-level consistency events into EPCIS merely because they are domain events.

## Consequences

- Inventory commit latency is decoupled from network delivery.
- Internal modules can consume a stable event language.
- External partners can receive CloudEvents or EPCIS mappings without dictating internal storage design.
- Replays and duplicate delivery are safe by design.

## Evidence

See `events/inventory.yml`, CloudEvents evidence and GS1 EPCIS evidence.
