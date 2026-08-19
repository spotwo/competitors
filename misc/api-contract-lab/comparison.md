# MISC API Contract Lab Comparison

> Generated from `misc/api-contract-lab/comparison.yml` and `scenarios.yml`. Do not edit manually.

This is a structural experiment, not a production benchmark or canonical API decision.

## Profile results

| Profile | Status | REST + OpenAPI | gRPC | Preferred | Margin |
|---|---|---:|---:|---|---:|
| `external_business_api` | `experiment-result` | 4.40 / 5 | 3.25 / 5 | `rest-openapi` | 1.15 |
| `internal_realtime_candidate` | `hypothesis` | 3.55 / 5 | 4.50 / 5 | `grpc` | 0.95 |

## Semantic operation parity

| Operation | REST | gRPC | Mutating | Idempotency |
|---|---|---|---|---|
| `get_location` | `GET /v1/locations/{location_id}` (`getLocation`) | `GetLocation` | no | `not-required` |
| `get_handling_unit` | `GET /v1/handling-units/{handling_unit_id}` (`getHandlingUnit`) | `GetHandlingUnit` | no | `not-required` |
| `list_inventory_positions` | `GET /v1/inventory/positions` (`listInventoryPositions`) | `ListInventoryPositions` | no | `not-required` |
| `create_task` | `POST /v1/tasks` (`createTask`) | `CreateTask` | yes | `required` |
| `get_task` | `GET /v1/tasks/{task_id}` (`getTask`) | `GetTask` | no | `not-required` |

## Shared model parity

| Model | Canonical fields in both contracts |
|---|---|
| `HandlingUnit` | `id`, `type`, `current_location_id` |
| `InventoryPosition` | `item_id`, `location_id`, `handling_unit_id`, `quantity` |
| `Location` | `id`, `code`, `type`, `parent_location_id` |
| `Task` | `id`, `kind`, `status`, `source_location_id`, `target_location_id`, `handling_unit_id`, `created_at` |

## Interpretation

For the external business API scope, REST/OpenAPI currently wins on HTTP/browser ubiquity, customer tooling, human debugging, agent tooling, and ordinary edge/proxy compatibility. gRPC remains stronger in the structural internal-realtime hypothesis because typed generated RPC contracts, wire efficiency, and bidirectional streaming receive more weight there.

The internal result is intentionally only a hypothesis. No production latency, throughput, CPU, memory, connection-scale, or failure-recovery benchmark is recorded by this lab.
