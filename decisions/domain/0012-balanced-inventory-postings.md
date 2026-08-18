# ADR 0012 - Balanced Inventory Postings and Reversals

Status: Proposed

Date: 2026-08-18

## Context

Movement, posting changes, reservations, adjustments and reconciliation all affect inventory differently. Hiding them behind direct updates such as `stock.quantity -= q` makes conservation, audit, retries and reversals difficult to reason about.

## Proposed decision

Represent every accepted inventory change as a typed transaction containing one or more signed legs.

This is **double-entry-inspired quantity accounting**, not financial double-entry bookkeeping.

### Conserving movement

```text
Source Position       -10 EA
Destination Position  +10 EA
                       -----
Net physical delta       0 EA
```

### Condition transformation

```text
Available stock       -10 EA
Quarantine stock      +10 EA
                       -----
Net physical delta       0 EA
```

### Receipt

```text
Warehouse Position    +10 EA
External boundary       -
```

### Issue

```text
Warehouse Position    -10 EA
External boundary       -
```

### Adjustment

A non-conserving adjustment is allowed only with explicit reason, source authority and audit identity.

### Commitment

```text
Physical quantity     unchanged
Reserved/Allocated    +/- Q
Availability          recalculated
```

## Rules

1. Internal moves and pure dimension transformations conserve normalized physical quantity.
2. Receipts and issues cross the modeled warehouse boundary and therefore do not have to net to zero inside the warehouse ledger.
3. Reservations and allocations do not create or destroy physical stock.
4. Count reconciliation posts only the approved difference, referencing the accepted count evidence.
5. Posted transactions are immutable.
6. Corrections use compensating transactions, never UPDATE or DELETE of posted history.
7. Reversal references the original transaction and applies inverse semantic deltas, subject to current-state constraints.
8. UOM conversions must be explicit and deterministic before conservation checks.
9. Serial identity must remain exclusive across mutually exclusive physical positions.
10. One idempotency scope cannot successfully post the same logical transaction twice.
11. Multi-leg transactions either commit completely or not at all.
12. Actor, reason, source reference, occurred time and recorded time are audit fields, not optional logging decoration.

## Why not edit the old row?

Because business reality changes after posting. Inventory may already have moved, shipped or changed condition. A compensating transaction preserves both the original fact and the later correction.

## Consequences

- Movement and transformation invariants become mechanically testable.
- Inventory drift investigations have an explainable transaction chain.
- Retried commands cannot silently duplicate quantity.
- Reconciliation is a normal typed posting rather than a privileged database repair.
- Future financial integration can consume warehouse transactions without forcing the WMS ledger itself to become a general ledger.

## Evidence

See `ledger/inventory.yml`, Dynamics warehouse transaction evidence, SAP posting-change evidence, Oracle Inventory History and Infor Inventory Transactions.
