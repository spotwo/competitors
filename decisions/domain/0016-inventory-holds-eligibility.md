# ADR 0016 - Inventory Holds and Action Eligibility

Status: Proposed

## Context

ADR 0015 defines physical stock, Reservation, Allocation and derived Availability. The remaining gap is operational restriction: inventory can physically exist while being ineligible for one or more warehouse actions.

Vendor systems expose this through several mechanisms. Infor WMS supports holds on lot, location and license plate, permits multiple hold codes, and evaluates those codes during allocation/picking. Dynamics 365 inventory blocking prevents blocked on-hand inventory from being reserved without removing the physical stock.

A single overloaded `inventory_status = HOLD` cannot express all of these semantics without conflating physical condition, restriction reason, target scope and the action being restricted.

## Decision

Spotwo candidate semantics separate:

```text
Inventory Condition != Inventory Hold != Hold Policy != Action Eligibility
```

### Inventory Hold Policy

A Hold Policy is reusable master-data semantics describing which actions a hold blocks.

The v0.1 lab supports independent policy flags for:

```text
allocation
movement
picking
shipping
```

Counting remains allowed by default because a restriction on using inventory should not make that inventory invisible to inventory-control processes.

A policy can block several actions at once.

### Inventory Hold

An Inventory Hold is an applied instance of a Hold Policy with a reason, start time, optional expiration, and optional release.

Holds do not mutate physical quantity and do not become an Inventory Key dimension.

Multiple active holds may apply to the same stock. Restrictions compose by logical OR:

```text
eligible(action) =
  no active matching hold policy blocks(action)
```

Therefore one permissive hold cannot cancel another restrictive hold.

### v0.1 targets

The executable lab supports two target granularities:

```text
exact stock-dimension scope
exact InventoryPosition
```

The exact scope is the same non-anchor scope used by ADR 0015:

```text
warehouse
item
owner
inventory condition
lot?
stock scope?
attribute set?
stock segment?
```

A position hold further narrows that exact scope to one `InventoryPosition`.

Broad selectors such as an entire Location, Handling Unit or Lot across multiple otherwise-different stock scopes are deliberately deferred. Vendor evidence shows those selectors are important, but implementing them requires overlap-aware matching and locking rather than silently treating them as exact scope aliases.

### Availability

The current kernel Availability is available-to-commit/allocate. Positions blocked for allocation do not contribute free quantity:

```text
available =
  sum(free quantity on allocation-eligible positions)
  - coarse reservation remainder
```

The result is floored at zero. Applying a hold does not retroactively cancel an existing Reservation, so an existing reservation can temporarily exceed currently eligible free quantity.

New Reservation creation uses this same allocation-eligible capacity in v0.1. A future ADR may distinguish `available_to_reserve` from `available_to_allocate` for policies that intentionally allow commitment against temporarily restricted stock.

### Allocation

Any increase of `InventoryPosition.allocated_qty` must evaluate allocation eligibility under the same scope lock used by commitment operations.

A hold applied after an Allocation already committed does not retroactively release or rewrite that Allocation. Resolving already-allocated stock is a higher-level exception/replanning concern.

### Movement

Inventory movement checks movement eligibility independently from allocation eligibility.

A policy may therefore permit allocation but prohibit relocation, or prohibit allocation while allowing controlled movement to quarantine or inspection.

A same-warehouse full-HU relocation must not bypass movement holds on inventory contained by that HU. The lab checks the HU subtree before changing placement.

## Concurrency

Hold apply/release, Reservation and Allocation use the same transaction-scoped advisory lock derived from the exact stock-dimension scope.

This makes races deterministic at the semantic scope boundary:

```text
scope lock
  -> evaluate active holds / availability
  -> mutate hold, reservation or allocation
```

If a Reservation wins the lock first, a later hold may coexist with that already-committed Reservation. If an allocation-blocking hold wins first, a new Reservation or Allocation cannot consume the newly ineligible capacity.

Position row locks remain the second layer for exact-position quantity writes.

## Lifecycle

A hold is active when:

```text
starts_at <= now
AND released_at IS NULL
AND (expires_at IS NULL OR expires_at > now)
```

Expiration changes eligibility without deleting the hold record. Explicit release records a separate `hold_release` InventoryTransaction; the original hold remains audit history.

## Idempotency

Hold apply/release commands reuse the tenant-scoped InventoryTransaction idempotency contract:

```text
same key + same canonical payload
  -> replay original result

same key + different payload
  -> reject
```

## Invariants

1. Applying or releasing a hold never changes physical quantity.
2. Hold is not an Inventory Key dimension.
3. Multiple active holds compose restrictions by OR.
4. Allocation-blocked positions do not contribute to current allocation availability.
5. New allocation cannot increase `allocated_qty` on allocation-ineligible inventory.
6. Movement-blocked inventory cannot be moved through the quantity-movement function.
7. Full-HU relocation cannot bypass a movement hold on contained inventory.
8. Expired or explicitly released holds do not affect eligibility.
9. Existing Reservations and Allocations are not silently rewritten when a new hold is applied.
10. Hold apply/release is serialized with commitment writes on the exact stock scope and is retry-safe.

## Deliberate v0.1 boundary

This ADR does not yet implement:

- quantity-partial holds;
- wildcard/overlapping selectors such as all stock in a Location or HU;
- order-type exceptions that explicitly permit selected hold codes;
- automatic hold rules at receipt;
- hold escalation/review workflows;
- automatic reallocation of already-allocated stock after a hold;
- separate available-to-reserve versus available-to-allocate projections.

These require a broader policy selector or Warehouse Work layer and should not be hidden inside a boolean status field.

## Evidence / validation

Primary evidence records:

- `infor-inventory-holds-2026`
- `infor-held-inventory-eligibility-2026`
- `dynamics365-inventory-blocking-2026`

The executable model is implemented by `labs/postgres-inventory-kernel/sql/005_holds_eligibility.sql` and `tests/test_holds.py`.
