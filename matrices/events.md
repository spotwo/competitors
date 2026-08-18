# Inventory Domain Event Matrix

> Generated from `events/*.yml`. Do not edit by hand.

| Event type | Category | External visibility | Ledger relation | Evidence |
|---|---|---|---|---:|
| `inventory.transaction.posted` | inventory | no | Directly references transaction_id and affected inventory keys. | 0 |
| `inventory.position.changed` | inventory | no | Carries transaction_id plus before and after position version or deltas. | 0 |
| `inventory.reservation.created` | commitment | no | References reservation transaction and demand reference. | 0 |
| `inventory.reservation.released` | commitment | no | References release transaction and reservation identity. | 0 |
| `inventory.allocation.created` | commitment | no | References allocation transaction and source inventory key. | 0 |
| `inventory.allocation.released` | commitment | no | References release transaction and prior allocation identity. | 0 |
| `inventory.movement.confirmed` | movement | yes | References movement transaction with source and destination legs. | 1 |
| `inventory.adjustment.posted` | reconciliation | no | References adjustment transaction reason and authority. | 0 |
| `inventory.reconciliation.posted` | reconciliation | no | References reconciliation transaction and accepted evidence. | 0 |

## Event envelope

| Field | Required | Meaning |
|---|---|---|
| `event_id` | yes | Globally unique stable identity used for deduplication. |
| `type` | yes | Stable semantic event type such as inventory.movement.confirmed. |
| `source` | yes | Service or bounded context that produced the committed fact. |
| `subject` | yes | Canonical aggregate or business-object reference. |
| `occurred_at` | yes | Business time when the fact occurred. |
| `recorded_at` | yes | Time the fact was durably recorded. |
| `aggregate_type` | yes | Aggregate type for ordering and consumer routing. |
| `aggregate_id` | yes | Aggregate identity. |
| `aggregate_version` | yes | Monotonic version for per-aggregate ordering and stale-event detection. |
| `causation_id` | no | Command or prior event that directly caused this event. |
| `correlation_id` | no | End-to-end workflow or request correlation identity. |
| `actor` | no | Human system equipment or integration principal responsible for the action. |
| `warehouse_id` | no | Warehouse context for routing and tenancy. |
| `schema_version` | yes | Version of the event payload contract. |
| `data` | yes | Event-specific immutable fact payload. |

## External mappings

| Standard | Scope | Evidence |
|---|---|---:|
| CloudEvents | Transport envelope for internal and partner-facing domain-event delivery where supported. | 1 |
| GS1 EPCIS / CBV | External supply-chain visibility events for relevant physical object movements states and business steps. | 1 |
