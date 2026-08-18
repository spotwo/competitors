# ADR 0035: Quarantine malformed NATS JetStream deliveries before ACK

- Status: accepted
- Date: 2026-08-19
- Scope: PostgreSQL inventory kernel consumer path
- Depends on: ADR 0028, ADR 0032

## Context

The Inbox contract starts only after a Domain Event envelope has a trusted `event_id`. A JetStream delivery can fail earlier because its bytes are not UTF-8, its body is not JSON, its envelope is invalid, or its required transport headers disagree with the envelope.

Previously these failures were raised while the source adapter decoded the message. The runtime therefore had no delivery object to hand off durably and could not ACK safely. A permanently malformed message could be redelivered until the broker delivery limit was exhausted, while inventing an `event_id` would corrupt the Inbox identity model.

## Decision

Capture JetStream-owned delivery identity before parsing any application-controlled bytes or headers.

The malformed path is:

```text
JetStream fetch
  -> read stream / durable consumer / stream sequence / delivery metadata
  -> decode and validate payload, envelope, and headers
  -> on malformed input, persist a durable poison record
  -> commit PostgreSQL transaction
  -> ACK JetStream delivery
```

A malformed delivery never creates an Inbox receipt and never receives a synthetic `event_id`.

The poison record is keyed by:

```text
logical consumer_name
+ JetStream stream
+ JetStream durable consumer
+ JetStream stream sequence
```

This identity is transport-owned and exists even when every application header and payload byte is invalid.

## Evidence retained

The poison record retains:

- trusted JetStream identity and delivery metadata
- subject and headers as observed
- bounded failure code and bounded error text
- SHA-256 of the complete payload
- complete payload size
- a bounded 4 KiB payload preview plus a truncation flag
- first and latest delivery metadata
- first/last observation timestamps and observation count

The full malformed payload is deliberately not copied into PostgreSQL. The digest binds the record to the complete payload while the bounded preview prevents a poison message from turning quarantine into an unbounded database write.

## Idempotency and collisions

A redelivery of the same JetStream identity updates the observation count and latest delivery metadata instead of creating another poison row.

If the same transport identity is ever observed with a different subject, payload digest/size/preview, truncation state, or headers, capture fails closed. The runtime must not ACK that conflicting delivery.

## ACK rule

```text
poison capture committed -> ACK allowed
poison capture failed     -> ACK forbidden
ACK confirmation failed   -> safe redelivery and idempotent recapture
```

The poison quarantine is therefore the pre-`event_id` analogue of the valid-event failure lane, but the two stores must not be merged. Valid events are keyed by `(consumer_name, event_id)` and may later enter Inbox processing. Malformed deliveries have no trusted event identity and are terminal transport poison.

## Failure classification

The initial bounded codes are:

- `invalid_message_encoding`
- `invalid_message_json`
- `invalid_event_envelope`
- `invalid_transport_headers`

Raw exception text is evidence, not metric identity.

## Consequences

- permanently malformed deliveries no longer loop through the consumer path after durable quarantine and ACK
- no fabricated `event_id` can enter the Inbox or valid-event failure tables
- operators retain stable JetStream coordinates for forensic lookup
- the database receives bounded payload evidence rather than arbitrary message bodies
- loss of PostgreSQL quarantine durability intentionally leaves the broker delivery unacknowledged

## Non-goals

This slice does not add automatic repair, malformed-message replay, a generic multi-broker DLQ abstraction, poison-record deletion, or a synthetic Domain Event wrapper around malformed data.
