# ADR 0024 - NATS JetStream Transport Adapter

Status: Accepted

Date: 2026-08-18

## Context

ADR 0022 established the broker-neutral Publisher Runtime. ADR 0023 selected NATS JetStream as the canonical operational WMS transport profile while explicitly keeping broker semantics outside Inventory correctness.

The next slice must prove the decision against a real server. A fake transport cannot prove JetStream publication acknowledgement, `Nats-Msg-Id` deduplication, durable consumer acknowledgement, or redelivery.

## Decision

Implement `NatsJetStreamTransport` as the first concrete `Transport` adapter in the PostgreSQL Inventory Kernel lab.

The CI lab runs:

- NATS Server 2.14.5, pinned to the immutable multi-platform image digest `sha256:026a66a4497c6d7d3eed741781770099c48c755bf3a55b6950d76dd210596eb3`;
- `nats-py` 2.15.0;
- JetStream file storage in an isolated disposable Compose volume.

## Publication mapping

One claimed Outbox event maps to JetStream as follows:

```text
subject
  spotwo.wms.events.<canonical event_type>

payload
  canonical compact JSON envelope

Nats-Msg-Id
  canonical event_id

headers
  event type
  aggregate type
  aggregate id
  aggregate version
```

Aggregate identity remains metadata. The adapter does not create one subject per aggregate instance.

The target stream name and subject prefix are deployment configuration. Stream creation, retention, replicas, storage limits, duplicate window, credentials, and account topology remain deployment responsibilities rather than hidden adapter side effects.

## PUB ACK boundary

`NatsJetStreamTransport.publish()` returns only after JetStream sends a publication acknowledgement for the configured stream.

Only then may `PublisherRuntime` ACK the PostgreSQL Outbox row:

```text
JetStream PUB ACK
        |
        v
Transport.publish returns
        |
        v
PostgreSQL Outbox ACK
```

A connection failure, missing stream, subject mismatch, or publication timeout raises from the adapter. The runtime NACKs the Outbox claim and preserves the same event identity for retry.

The client keeps one connection for a publisher worker but disables opaque client reconnect loops. A broken connection fails back to the Outbox retry policy; the next publish attempt establishes a new connection. This bounds broker uncertainty inside one claim lease.

## Deduplicated retry

The adapter sets:

```text
Nats-Msg-Id = event_id
```

If JetStream accepted a message but PostgreSQL Outbox ACK persistence fails, a later Outbox attempt publishes the same stable event ID. Within the stream duplicate window, JetStream returns a duplicate PUB ACK pointing to the original stream sequence and does not append a second record.

This is transport-level protection, not the kernel's exactly-once guarantee. Consumers still require transactional Inbox deduplication because duplicates can occur outside the broker window or across other transports.

## Durable consumer proof

The integration lab creates a durable pull consumer with explicit acknowledgement. It proves:

1. the canonical JSON envelope and identity/version headers round-trip unchanged;
2. an unacknowledged message is redelivered after `ack_wait`;
3. the redelivery has the same stream sequence and incremented delivery count;
4. explicit ACK completes the consumer delivery.

## Warehouse transaction isolation

The integration lab commits an Inventory receipt and its Outbox events, then publishes through an unavailable NATS endpoint.

The broker failure NACKs the Outbox rows but does not roll back or rewrite:

- the committed Inventory Transaction;
- the Inventory Position quantity;
- the durable Outbox event identities.

NATS therefore remains downstream of the warehouse transaction boundary.

## Consequences

### Positive

- the canonical transport decision now has executable broker evidence;
- JetStream PUB ACK precedes PostgreSQL Outbox ACK;
- stable event retry maps to real server-side deduplication;
- durable pull ACK/redelivery semantics are verified in CI;
- broker outage remains isolated from committed warehouse work.

### Costs and limits

- the synchronous lab adapter owns an internal async runner and one NATS client per publisher worker;
- the adapter is intentionally single-threaded; horizontal scale uses multiple publisher workers;
- production authentication, TLS, account isolation, stream provisioning, clustering, Leaf Nodes, observability, backoff, and poison-message policy remain later deployment/runtime slices;
- JetStream deduplication supplements but never replaces consumer Inbox idempotency.
