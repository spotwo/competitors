# Inventory Ledger Matrix

> Generated from `ledger/*.yml`. Do not edit by hand.

| Transaction type | Category | Physical | Conserves physical | Posting | Evidence |
|---|---|---|---|---|---:|
| Receipt | physical | yes | no | destination physical +Q | 1 |
| Issue | physical | yes | no | source physical -Q | 1 |
| Movement | physical | yes | yes | source physical -Q; destination physical +Q | 2 |
| Posting Change / Transformation | transformation | yes | yes | old context physical -Q; new context physical +Q | 1 |
| Adjustment In | reconciliation | yes | no | target physical +Q | 0 |
| Adjustment Out | reconciliation | yes | no | source physical -Q | 0 |
| Reservation | commitment | no | yes | scope reserved +Q | 1 |
| Reservation Release | commitment | no | yes | scope reserved -Q | 1 |
| Allocation | commitment | no | yes | source allocated +Q | 0 |
| Allocation Release | commitment | no | yes | source allocated -Q | 0 |
| Count Reconciliation | reconciliation | yes | no | position physical +/-Q with accepted count reference | 0 |
| Reversal | reversal | yes | no | inverse prior legs | 0 |

## Candidate invariants

| Invariant | Status | Statement |
|---|---|---|
| immutable-posted-history | adopted | Posted transactions and legs are immutable; corrections append new transactions. |
| movement-conservation | candidate | Internal movement and pure dimension transformation net to zero physical quantity for the same item and normalized unit of measure. |
| commitment-does-not-create-stock | adopted | Reservation and allocation never increase physical quantity. |
| available-is-derived | adopted | Availability is calculated from position and policy and is never an independently authoritative mutable bucket. |
| no-double-post | adopted | One logical command may post at most one successful transaction set for its idempotency scope. |
| atomic-position-ledger | candidate | Ledger rows and all affected Inventory Positions commit atomically or not at all. |
| explicit-adjustment-authority | candidate | Non-conserving adjustments require an explicit reason and source authority. |
| nonnegative-default | candidate | Physical reserved and allocated quantities do not become negative under the default warehouse policy. |
| serial-exclusivity | candidate | One serialized unit cannot be physically present in two mutually exclusive inventory positions simultaneously. |
