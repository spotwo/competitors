# ADR 0023 - Domain Event Transport Selection

Status: Accepted

Date: 2026-08-18

## Context

ADR 0021 established the Transactional Outbox and durable Domain Event identity. ADR 0022 established a broker-neutral Publisher Runtime whose claim transaction commits before external network publication.

The kernel now needs a concrete default transport for the next production-like adapter without allowing that transport to become a hidden dependency of Inventory or Warehouse Work semantics.

The candidates evaluated for the operational WMS profile are:

- NATS JetStream;
- Redpanda using the Kafka API;
- Amazon SNS FIFO + SQS FIFO;
- Cloudflare Queues.

This is a project-fit decision, not a claim that one broker is universally superior.

## Decision

Choose **NATS JetStream as the canonical default Domain Event transport for the Spotwo operational WMS profile**.

Keep Redpanda/Kafka, AWS SNS/SQS FIFO, and Cloudflare Queues as supported profile targets behind the same `Transport` adapter boundary.

The machine-readable source of truth is `kernel/transports.yml`; `scripts/validate_transport_selection.py` recomputes the weighted ranking and rejects a declared default that is not the highest-scoring candidate or does not clear the configured decision margin.

## Why NATS JetStream is the default

### 1. Warehouse-local and hybrid deployment fit

The default WMS transport should be able to run close to warehouse execution systems rather than requiring every event to cross a public-cloud boundary.

NATS can be deployed as a compact server on-premises, in containers, VMs, cloud infrastructure, or edge hardware. Leaf Nodes are explicitly designed for edge/local traffic and can connect warehouse-local NATS systems to a central system.

JetStream can also operate on Leaf Nodes as an independent local island when hub/cloud connectivity is unavailable.

This is the strongest differentiator for the operational WMS profile.

Evidence:

- `nats-leafnodes-edge-2026`

### 2. Delivery semantics match the portable Outbox contract

JetStream provides acknowledged durable publication and at-least-once consumer delivery. It supports message deduplication through a publication ID and explicit consumer acknowledgements/redelivery.

These capabilities strengthen delivery but do not replace the portable Spotwo rules:

```text
stable event_id
+ at-least-once transport
+ consumer Inbox dedupe
+ aggregate_version
```

The kernel therefore does not depend on a broker-specific exactly-once claim.

Evidence:

- `nats-jetstream-delivery-2026`

### 3. Operational footprint is appropriate for a WMS default

A WMS installation may start as one warehouse with a small application estate and later grow into multiple buildings/sites.

The default broker should not force a Kafka-class data platform operational footprint before the workload needs one. NATS can start as a small deployment and expand to clustered/leaf-node topologies.

### 4. Multiple consumer views are natural

JetStream Streams and Consumers allow one durable event stream to support independent projections/integrations with their own acknowledgement state, filtering, replay position, and processing speed.

The recommended adapter profile is a durable pull consumer with explicit acknowledgement for scalable application-controlled processing.

## Weighted selection

`kernel/transports.yml` defines the operational WMS criteria and weights:

| Criterion | Weight |
|---|---:|
| hybrid/on-prem/edge fit | 20% |
| operational simplicity | 15% |
| delivery fit | 15% |
| aggregate ordering fit | 10% |
| fan-out and horizontal scale | 10% |
| replay and retention | 10% |
| cloud/vendor portability | 10% |
| small/medium cost fit | 10% |

The accepted weighted result is:

```text
NATS JetStream       4.75 / 5
Redpanda / Kafka     3.95 / 5
AWS SNS + SQS FIFO   3.40 / 5
Cloudflare Queues    2.75 / 5
```

The scores are project judgments supported by the evidence records, not vendor benchmark measurements.

## Transport mapping

The first NATS adapter SHOULD map the portable event contract as follows:

```text
canonical event_id
    -> envelope event_id
    -> Nats-Msg-Id publication dedup key

canonical event type/family
    -> stable NATS subject namespace

aggregate_type + aggregate_id
    -> envelope/header identity for consumer routing/version checks

aggregate_version
    -> envelope/header version signal
```

Do not create one subject per InventoryPosition or aggregate instance by default. Subject cardinality should describe stable event routing boundaries; aggregate identity remains event metadata unless a later measured design explicitly needs aggregate lanes.

## Ordering policy

The kernel continues to guarantee **no global delivery order**.

NATS stream sequence is not promoted into a WMS business-order contract, and multiple publisher/consumer workers may progress concurrently.

Consumers that materialize aggregate projections must use:

```text
aggregate_type
aggregate_id
aggregate_version
```

to reject stale facts or detect gaps.

If a capability later requires strict serial processing for one aggregate, that requirement belongs to the adapter/consumer design and must be executable-tested separately.

## Why not Redpanda/Kafka as the default

Redpanda is the stronger profile when the main requirement becomes a long-lived streaming/data platform:

- high partitioned throughput;
- long replay/retention;
- Kafka ecosystem compatibility;
- data engineering and analytical consumers;
- natural key-to-partition aggregate ordering.

It remains the preferred `high_volume_streaming_and_data_platform` exception profile.

For the operational default, its larger platform/partition/cluster footprint is unnecessary before those requirements exist.

Evidence:

- `redpanda-kafka-semantics-2026`

## Why not AWS SNS/SQS FIFO as the default

SNS FIFO + SQS FIFO is a strong managed profile for an AWS-native deployment. Message groups map naturally to aggregate identity, and FIFO topics/queues provide ordered delivery and service-side deduplication.

However, a managed AWS service cannot become the canonical warehouse-local transport without making WAN/AWS availability and vendor placement part of the operational WMS dependency graph.

It remains the preferred `aws_native_managed_messaging` exception profile.

Evidence:

- `aws-sns-sqs-fifo-2026`

## Why not Cloudflare Queues as the default

Cloudflare Queues is a strong fit for Workers-native asynchronous jobs and integrations. It provides at-least-once delivery, batching, retry/delay, explicit acknowledgement, pull consumers, and DLQs.

It does not guarantee publication-order delivery and its queue/consumer model is not as natural for a central replayable multi-consumer WMS event backbone.

It remains the preferred `cloudflare_workers_async_jobs` exception profile.

Evidence:

- `cloudflare-queues-semantics-2026`

## Kernel boundary remains unchanged

This ADR does **not** make NATS part of Inventory correctness.

The following remain mandatory:

1. Inventory/Work transactions write only authoritative state + Outbox inside PostgreSQL.
2. Publisher Runtime reads only committed Outbox records.
3. `Transport.publish()` is the only broker-specific boundary.
4. Stable `event_id` survives all retries.
5. Consumers remain idempotent even if a broker offers deduplication.
6. No consumer may depend on one global event order.
7. Broker subjects, topics, queues, partitions, message groups, retention, credentials, and retry policies belong to deployment/adapters.

## Consequences

### Positive

- there is one concrete default to implement and operate;
- the default works well from small local warehouse deployments to connected multi-site topologies;
- broker-specific guarantees do not leak into the kernel;
- Redpanda, AWS, and Cloudflare remain explicit alternatives instead of accidental forks;
- the decision is machine-verifiable and evidence-backed.

### Costs

- the project must operate NATS or choose a managed NATS provider for the default profile;
- strict per-aggregate ordering is not automatic under arbitrary horizontal consumption and needs explicit design where required;
- data-platform workloads may eventually justify a parallel or replacement Redpanda/Kafka adapter;
- broker selection must be revisited if deployment assumptions materially change.

## Next executable slice

Implement a real `NatsJetStreamTransport` adapter and CI integration against a pinned NATS server container.

The adapter test must prove at minimum:

1. publish acknowledgement before Outbox ACK;
2. stable `event_id` -> `Nats-Msg-Id` mapping;
3. retry of the same Outbox event does not create a second JetStream message within the configured dedupe window;
4. canonical envelope round-trips unchanged;
5. durable pull consumer + explicit ACK/redelivery behavior;
6. broker outage never rolls back or blocks the already-committed warehouse transaction.
