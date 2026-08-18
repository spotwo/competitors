# ADR 0015 - Inventory Commitment Engine

Status: Proposed

## Context

The inventory kernel already distinguishes physical stock, coarse reservation, exact allocation, and task assignment conceptually. ADR 0014 defines Inventory Position identity and the PostgreSQL lab proves semantic-key, movement, serial, and locking invariants.

The next boundary is commitment: how demand consumes warehouse availability before a concrete pick task exists.

## Decision

Spotwo candidate semantics separate four concepts:

```text
Availability != Reservation != Allocation != Assignment
```

### Availability

Availability is a derived projection. It is not an authoritative stock bucket and is not independently mutated.

For one exact stock-dimension scope in the v0.1 kernel lab:

```text
available =
  physical
  - exact-position reserved
  - exact-position allocated
  - coarse reservation remainder
```

Availability may become more contextual later, but this lab intentionally keeps one exact stock-dimension scope so concurrency semantics can be tested without wildcard-overlap ambiguity.

### Reservation

A Reservation is a demand commitment against an exact stock-dimension scope without selecting a Location, Handling Unit, or Inventory Position.

The v0.1 reservation scope is:

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

`Location` and `HandlingUnit` are intentionally absent.

Reservation state is represented by quantities:

```text
quantity = allocated_qty + released_qty + remaining_qty
```

A released reservation never reopens. If an allocation linked to a released reservation is later released, its quantity moves from reservation `allocated_qty` to `released_qty`, leaving `remaining_qty = 0`.

### Allocation

An Allocation binds demand to one concrete `InventoryPosition` and increments that position's `allocated_qty`.

An Allocation may reference a Reservation. When it does, converting reservation remainder into exact allocation does not change global availability:

```text
coarse reservation -Q
exact allocation    +Q
availability         0 delta
```

An allocation without a Reservation may consume only unreserved scope availability.

### Assignment

Assignment remains a Warehouse Work concern and is not part of this ADR. Allocation answers **which stock**; Assignment answers **who/what executes the work**.

## Concurrency

Reservation creation and unreserved allocation serialize on a transaction-scoped advisory lock derived from the exact stock-dimension scope.

Exact Inventory Position writes additionally use row locks.

The intended lock order is:

```text
scope advisory lock
  -> reservation row, when applicable
  -> allocation row, when applicable
  -> inventory position row
```

The scope lock protects against two independent reservations or unreserved allocations overcommitting aggregate stock distributed across multiple positions.

Hash collisions on the advisory key are acceptable because they reduce concurrency but do not weaken correctness.

## Idempotency

Reservation, allocation, and release commands reuse the tenant-scoped `InventoryTransaction` idempotency contract:

```text
same idempotency key + same canonical payload
  -> return original transaction

same idempotency key + different payload
  -> reject
```

Candidate object IDs and transaction IDs are not themselves the semantic idempotency payload.

## Invariants

1. Coarse Reservation never mutates `InventoryPosition.reserved_qty`.
2. Reservation creation cannot make scope availability negative.
3. Allocation cannot exceed exact-position free quantity.
4. Unreserved Allocation cannot consume coarse-reserved capacity.
5. A Reservation may be allocated across multiple positions with the same stock-dimension scope.
6. Reservation-to-Allocation conversion preserves global availability.
7. Releasing an Allocation linked to an active Reservation restores reservation remainder, not free capacity.
8. Releasing a closed Reservation-linked Allocation does not reopen that Reservation.
9. All commitment writes are retry-safe through request fingerprints.

## Deliberate v0.1 boundary

The lab does not yet model wildcard reservation scopes such as "any owner", "any lot", or "any condition". Those scopes overlap and require a broader policy/locking model.

It also does not yet model holds, FEFO source selection, reservation expiration, demand priority, or Warehouse Work creation. Those are subsequent layers.

## Evidence / validation

This ADR is exercised by `labs/postgres-inventory-kernel/sql/004_commitment_engine.sql` and `tests/test_commitment.py`.

It remains Proposed until the model is validated against broader vendor evidence and later golden warehouse scenarios.