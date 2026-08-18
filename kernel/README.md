# Spotwo WMS Kernel

**Current reference contract: v0.1.0**

Status: **frozen reference specification**, not a production WMS release.

The canonical machine-readable contract is [`spec.yml`](spec.yml). ADR 0020 explains the compatibility meaning and non-goals.

## Architecture

```text
Capability / Business Need
          |
          v
   Planning / Decision
          |
    +-----+------+----------------+
    |            |                |
Availability  Reservation      Allocation
                               |
                               v
                         WarehouseWork
                               |
                         WarehouseTask
                               |
                   Assignment -> Execution
                                    |
                          +---------+---------+
                          |                   |
                      Exception       Confirmation Bridge
                          |                   |
                        Retry          +------+------+
                                       |             |
                                Inventory Posting  CountResult/etc
                                       |
                                       v
                              InventoryTransaction
                                       |
                              +--------+--------+
                              |                 |
                       InventoryPosition   immutable history
```

## Stable semantic boundaries

```text
InventoryPosition != InventoryTransaction
InventoryKey != SerialMembership
InventoryAnchor = Location XOR direct HandlingUnit
Availability != Reservation != Allocation != Assignment
InventoryCondition != InventoryHold != HoldPolicy != ActionEligibility
BusinessProcess != WarehouseWork != WarehouseTask
Allocation != WorkAssignment
TaskExecution != TaskConfirmation
TaskConfirmation != InventoryTransaction
TaskException != InventoryHold
MovementIntent != InventoryMovement
InventoryTransaction != DomainEvent != EPCISEvent
CountResult != InventoryTransaction
```

Accepted execution consumes commitments instead of returning executed quantity to availability.

## Executable proof

The reference implementation/falsification lab is PostgreSQL 18.4 under `labs/postgres-inventory-kernel/`.

```bash
bash bin/check
bash bin/check-kernel-lab
```

At v0.1 freeze:

- 7 ordered kernel migrations are part of the proof boundary;
- at least 43 functional/concurrency tests are required;
- 4 Golden Warehouse Scenarios are required;
- smoke performance/contention checks run with the PostgreSQL lab;
- preserved standard and 1M-position scale evidence lives under `labs/postgres-inventory-kernel/benchmarks/baselines/`.

## Golden Scenarios

1. Receive -> Putaway -> Reserve -> Allocate -> Pick -> Pack -> Ship.
2. Whole-HU relocation without rewriting contained InventoryPosition identity.
3. Short Pick -> Exception -> Retry -> residual completion.
4. Cycle Count -> observed variance -> explicit reconciliation.

The registry in `scenarios/golden.yml` is canonical and mapped to executable pytest functions.

## Version meaning

`0.1.x` may improve evidence, tests, diagnostics and implementation hardening without changing frozen semantic boundaries.

Breaking a frozen boundary requires a superseding ADR and a new reference-contract minor version, unless the old boundary is proven internally inconsistent or factually invalid.

The version does **not** freeze SQL tables/functions as a public API and does not imply production readiness.

## Next candidates

The machine manifest keeps the v0.2 backlog. Important next layers include executable outbox/event publication, FEFO/source selection, broader reservation/hold scopes, parallel Work graphs, automatic dispatch/resource qualification, cross-warehouse orchestration, tenant RLS, partitioning/archive, catch weight, owner/entitlement refinement, and optimization of extreme hot-position allocation contention.
