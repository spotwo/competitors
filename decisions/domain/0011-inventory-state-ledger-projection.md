# ADR 0011 - Inventory State, Ledger and Projection Model

Status: Proposed

Date: 2026-08-18

## Context

A WMS needs low-latency current inventory reads, strong auditability, reliable recovery, high write throughput and safe integration retries. A single mutable `quantity` row is fast but weak for audit and recovery. Full event sourcing for every WMS aggregate would add substantial complexity and is not required by the evidence.

Current vendor evidence shows a recurring separation between current on-hand views and inventory transaction history. Dynamics explicitly describes on-hand as a summarized view of inventory and warehouse transactions and provides a consistency tool that can rebuild on-hand structures from transaction data. Oracle and Infor expose inventory transaction/history records for warehouse activity.

## Proposed decision

Use a **hybrid synchronous ledger + materialized position** model.

```text
Command
  |
  v
Inventory Posting Service
  |
  +---- validate invariants
  +---- deduplicate command
  +---- lock/version affected positions
  |
  v
ONE DATABASE TRANSACTION
  |
  +---- append InventoryTransaction + Legs
  +---- update InventoryPosition rows
  +---- append Domain Event Outbox rows
  |
 COMMIT
  |
  +---- operational reads use InventoryPosition
  +---- audit/rebuild uses InventoryTransaction ledger
  +---- publisher drains Outbox
```

This is **not full event sourcing**. Inventory transactions are the durable journal for inventory state changes, but unrelated domain aggregates do not have to be reconstructed from domain events.

## Roles

### Inventory Position

Optimized current operational state keyed by inventory dimensions. It is the primary read model for WMS decisions such as availability, allocation, task execution and mobile UI.

Exact key dimensions, physical anchor semantics, serial membership, merge/split rules and the candidate PostgreSQL layout are defined in `decisions/domain/0014-inventory-key-stock-dimensions.md`.

### Inventory Transaction Ledger

Immutable journal of accepted physical, commitment and reconciliation deltas. It provides audit, traceability, consistency verification and rebuild input.

### Availability View

Derived projection answering a scoped question such as available-to-reserve, available-to-allocate or available-to-pick. It is not independently authoritative.

### Domain Event

Committed business fact published after the same transaction through an outbox. It is used for module integration, external notifications and automation, not as a substitute for the inventory ledger.

## Rules

1. No inventory mutation bypasses the Inventory Posting Service or an equivalent invariant-enforcing boundary.
2. Ledger append and all affected Inventory Position updates are atomic.
3. Posted ledger rows are immutable.
4. Inventory Position may be rebuilt or reconciled from durable transaction history plus an approved checkpoint strategy.
5. Availability is derived and disposable; it may be cached but must be reproducible.
6. Domain events are produced only for committed facts.
7. Outbox persistence is atomic with the ledger posting; network publication is not part of the database transaction.
8. Full event sourcing is not required for work, orders, shipments or other aggregates solely because inventory has a durable journal.
9. Transaction history archival may summarize old detail only under explicit traceability and reconciliation rules.
10. Current position version is a concurrency control and audit aid, not a replacement for ledger identity.
11. Inventory Position identity follows ADR 0014 and must not be expanded ad hoc by application features.

## Consequences

- Fast current reads do not require replaying history.
- Duplicate retries can be rejected before applying physical deltas twice.
- Corruption or drift can be detected by comparing positions against ledger-derived expectations.
- Integrations can receive reliable events without becoming part of inventory commit latency.
- The architecture remains simpler than full event sourcing while preserving high-value audit properties.

## Open questions

- Exact checkpoint interval and archival policy.
- Whether commitment journal rows share the same physical table as physical quantity rows.
- Whether high-contention positions require pessimistic row locks, optimistic retries, or configurable policies.
- The remaining stock-dimension questions explicitly listed in ADR 0014.

## Evidence

See `ledger/inventory.yml`, `ontology/inventory-transaction.yml`, `decisions/domain/0014-inventory-key-stock-dimensions.md`, and evidence records with subjects `inventory-ledger`, `inventory-position` and `inventory-key`.
