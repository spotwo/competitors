# ADR 0063: Trial signed CloudEvents webhooks for external events

Status: Accepted
Date: 2026-08-19

## Context

The Architecture Boundary Map deliberately left `external-async-integration` unresolved. Spotwo already uses NATS JetStream as the internal operational domain-event transport, but exposing internal subjects, streams, consumer identities, or retention settings to customers would couple a public contract to deployment topology.

The reference-coverage backlog therefore made this boundary the only `P0 + missing` architecture question and asked for standards, open-source, commercial, and executable evidence before selecting a public asynchronous shape.

## Evidence

The bounded research slice records primary evidence in `research/external-async-integration/evidence.yml`.

The evidence shows four distinct concerns that should not be collapsed:

1. CloudEvents standardizes an event information model and protocol bindings.
2. AsyncAPI describes message-driven APIs in a machine-readable, protocol-agnostic form.
3. Secure webhook delivery needs explicit signing, replay protection, duplicate handling, retry, and diagnostics semantics.
4. Commercial warehouse platforms expose heterogeneous asynchronous integration surfaces, so competitor transport choice is evidence rather than authority.

Open-source webhook gateways such as Svix and Convoy further show that reliable webhook delivery is a subsystem with persistent delivery state and workers rather than a direct HTTP request emitted from a domain transaction.

## Decision

Trial the following external asynchronous profile:

```text
Spotwo domain fact
    -> public projection
    -> CloudEvents 1.0 structured JSON
    -> signed HTTPS webhook
    -> customer endpoint
```

Describe the message-driven contract with AsyncAPI 3.1.0.

For the lab, use the Standard Webhooks signing shape:

- `webhook-id`
- `webhook-timestamp`
- `webhook-signature`
- HMAC-SHA256 over message id, timestamp, and the exact raw body

The CloudEvents `id` is stable across retries and is the consumer deduplication key in the trial. Delivery is explicitly duplicate-capable and at-least-once. No global ordering guarantee is assumed.

A successful HTTP 2xx acknowledges a delivery attempt. Network failures, HTTP 408, 425, 429, and 5xx responses are retryable in the lab classifier. Other 4xx responses are treated as terminal for the attempt profile rather than retried blindly.

## Boundary that remains unresolved

This ADR does **not** promote `external-async-integration` to a canonical architecture contract yet.

The repository has executable contract semantics, but it does not yet have the durable delivery service required to prove:

- transactional handoff from canonical domain facts to public delivery records;
- persistent retry scheduling and maximum delivery age;
- replay history and customer-visible diagnostics;
- endpoint disable/re-enable policy;
- durable pull-feed recovery and cursor semantics;
- rate limiting and backpressure;
- subscription authorization by tenant/site/event type;
- secret/key rotation under real requests;
- failure recovery across process and database restarts.

The architecture boundary therefore remains `unresolved` until a runtime delivery lab proves enough of those properties to justify a `trial` technology decision and a `candidate` boundary.

## Isolation rule

The public contract must never require knowledge of internal NATS JetStream subjects, stream names, durable consumer names, broker retention, or internal topology migrations.

CloudEvents identifiers and public event types belong to the external contract and must remain stable if Spotwo later changes the internal broker or delivery implementation.

## Consequences

This slice narrows the next executable work substantially. We no longer need to compare every asynchronous technology as if it occupied the same layer. The next lab should implement a durable webhook delivery journal behind this public contract and compare explicit replay through webhooks with a pull-feed recovery surface.

Adoption requires runtime evidence, not additional architecture prose.
