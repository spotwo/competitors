# Inventory Ledger

`ledger/*.yml` contains canonical candidate rules for posting inventory and commitment changes.

The ledger is a domain model, not a database migration and not a financial general ledger.

## Contract

- Posted inventory transactions are immutable.
- A transaction contains one or more semantic posting legs.
- Internal movement and pure inventory-dimension transformation conserve normalized physical quantity.
- Receipt and issue cross the modeled warehouse boundary.
- Reservation and allocation change commitment dimensions, not physical quantity.
- Adjustments and reconciliation are explicitly non-conserving and require reason/source authority.
- Corrections use compensating transactions instead of editing posted history.
- Posting, Inventory Position updates and Domain Event outbox insertion are one atomic commit boundary.
- Logical command retries are protected by idempotency identity.

## Authoring

When adding a transaction type:

1. State whether it affects physical quantity.
2. State whether physical quantity must be conserved inside the warehouse ledger.
3. Describe posting legs explicitly.
4. Add evidence when the semantics are based on a vendor or standard rather than a Spotwo proposal.
5. Add or update invariants if the transaction introduces a new correctness rule.
6. Run `bash bin/check`.

## Queries

```bash
python scripts/kernel.py ledger
python scripts/kernel.py ledger movement
python scripts/kernel.py ledger adjustment --json
python scripts/kernel.py invariants
```

See ADRs `0011`, `0012`, and `0013` under `decisions/domain/`.
