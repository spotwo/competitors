# Spotwo Technology Decision Matrix

> Generated from `decisions/technology/registry.yml`. Do not edit manually.

Competitor adoption and open-source popularity are evidence signals, not automatic authorization. Every decision is scoped and can be revisited when its explicit triggers become true.

## Summary

| Disposition | Count | Meaning |
|---|---:|---|
| `adopt` | 3 | Use as the current default inside the stated scope. |
| `trial` | 8 | Build or continue a bounded prototype before adoption. |
| `watch` | 5 | Keep as a researched option or architecture reference without committing runtime dependency. |
| `reject` | 1 | Do not use inside the stated scope; rejection does not extend beyond that scope. |

## Decisions

| Technology | Category | Disposition | Confidence | Evidence | Next action |
|---|---|---|---|---|---|
| **REST + OpenAPI** (`rest-openapi`) | `api-contract` | `adopt` | `high` | `repo:5 / oss:0 / market:4` | `continue` |
| **PostgreSQL** (`postgresql`) | `database` | `adopt` | `high` | `repo:2 / oss:0 / market:1` | `continue` |
| **NATS JetStream** (`nats-jetstream`) | `event-transport` | `adopt` | `high` | `repo:3 / oss:0 / market:0` | `continue` |
| **gRPC** (`grpc-internal`) | `api-contract` | `trial` | `medium` | `repo:3 / oss:0 / market:0` | `benchmark` |
| **Apache PLC4X** (`apache-plc4x`) | `industrial-connectivity` | `trial` | `medium` | `repo:0 / oss:1 / market:0` | `prototype` |
| **MQTT + Sparkplug** (`sparkplug-mqtt`) | `industrial-telemetry` | `trial` | `medium` | `repo:0 / oss:1 / market:0` | `prototype` |
| **GS1 EPCIS** (`gs1-epcis`) | `logistics-standard` | `trial` | `medium` | `repo:0 / oss:1 / market:0` | `prototype` |
| **Google OR-Tools** (`google-or-tools`) | `optimization` | `trial` | `medium` | `repo:0 / oss:1 / market:0` | `prototype` |
| **openTCS** (`opentcs`) | `robotics` | `trial` | `medium` | `repo:0 / oss:1 / market:0` | `prototype` |
| **VDA 5050** (`vda-5050`) | `robotics` | `trial` | `medium` | `repo:0 / oss:1 / market:0` | `prototype` |
| **Ocava** (`ocado-ocava`) | `simulation` | `trial` | `medium` | `repo:0 / oss:1 / market:0` | `prototype` |
| **Kubernetes** (`kubernetes-default`) | `deployment-orchestration` | `watch` | `medium` | `repo:2 / oss:0 / market:2` | `research` |
| **Redpanda / Kafka API** (`redpanda-kafka-secondary`) | `event-transport` | `watch` | `high` | `repo:2 / oss:0 / market:0` | `research` |
| **H3** (`uber-h3`) | `geospatial` | `watch` | `medium` | `repo:0 / oss:1 / market:0` | `research` |
| **Open-RMF** (`open-rmf`) | `robotics` | `watch` | `medium` | `repo:0 / oss:1 / market:0` | `research` |
| **OpenWMS** (`openwms`) | `warehouse-reference` | `watch` | `medium` | `repo:0 / oss:1 / market:0` | `research` |
| **Cloudflare Queues** (`cloudflare-queues-domain-events`) | `event-transport` | `reject` | `high` | `repo:2 / oss:0 / market:0` | `none` |

Full scope, rationale, evidence references, constraints, alternatives, next action text, and revisit triggers live in `decisions/technology/registry.yml`.
