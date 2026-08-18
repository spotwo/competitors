# Transport Selection Matrix

> Generated from `kernel/transports.yml`. Do not edit manually.

Profile: `spotwo-wms-operational-default`

## Weighted ranking

| Rank | Candidate | Disposition | Weighted score |
|---:|---|---|---:|
| 1 | NATS JetStream (`nats-jetstream`) | `canonical-default` | 4.80 / 5 |
| 2 | Redpanda / Kafka API (`redpanda-kafka`) | `secondary-streaming-profile` | 3.95 / 5 |
| 3 | Amazon SNS FIFO + SQS FIFO (`aws-sns-sqs-fifo`) | `aws-native-profile` | 3.40 / 5 |
| 4 | Cloudflare Queues (`cloudflare-queues`) | `cloudflare-worker-profile` | 2.75 / 5 |

## Criteria scores

| Criterion | Weight | NATS JetStream | Redpanda / Kafka API | Amazon SNS FIFO + SQS FIFO | Cloudflare Queues |
|---|---:|---:|---:|---:|---:|
| `hybrid_onprem_edge_fit` | 20% | 5 | 4 | 1 | 1 |
| `operational_simplicity` | 15% | 5 | 2 | 5 | 5 |
| `delivery_fit` | 15% | 5 | 5 | 5 | 4 |
| `aggregate_ordering_fit` | 10% | 4 | 5 | 5 | 1 |
| `fanout_and_horizontal_scale` | 10% | 5 | 5 | 5 | 2 |
| `replay_and_retention` | 10% | 4 | 5 | 2 | 2 |
| `cloud_and_vendor_portability` | 10% | 5 | 4 | 1 | 2 |
| `small_medium_cost_fit` | 10% | 5 | 2 | 4 | 5 |

## Decision

Canonical default: **`nats-jetstream`**.

Profile-specific exceptions:

- `high_volume_streaming_and_data_platform` -> `redpanda-kafka`
- `aws_native_managed_messaging` -> `aws-sns-sqs-fifo`
- `cloudflare_workers_async_jobs` -> `cloudflare-queues`

The ranking is a Spotwo WMS project-fit decision, not a universal broker benchmark. Broker-specific semantics remain outside the frozen kernel contract.
