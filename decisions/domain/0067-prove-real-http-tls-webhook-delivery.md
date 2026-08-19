# ADR 0067: Prove real HTTP/TLS webhook delivery

Status: Accepted
Date: 2026-08-19

## Context

ADR 0063 selected a bounded public contract trial based on CloudEvents 1.0 structured JSON, signed HTTPS webhooks, and AsyncAPI 3.1. ADR 0064 then proved durable PostgreSQL delivery state, worker leases, retry, dead-letter, replay, and signing-secret version snapshots. ADR 0065 promoted the external asynchronous boundary to `candidate` while keeping signed CloudEvents webhooks at `trial`.

The remaining evidence gap was no longer the database state machine. The candidate had not actually crossed an HTTPS connection or exercised TLS and network failures against a customer-like endpoint.

## Decision

Add a real-socket HTTPS delivery adapter to the PostgreSQL kernel lab and exercise it against a local scripted customer endpoint.

The lab sends the exact structured CloudEvents JSON bytes with:

- `Content-Type: application/cloudevents+json`;
- `webhook-id` equal to the stable public CloudEvents id;
- `webhook-timestamp`;
- `webhook-signature` using the Standard Webhooks-style HMAC-SHA256 shape already selected by ADR 0063.

The adapter resolves secret material through an opaque `(secret_ref, version)` resolver boundary. Durable delivery rows continue to store only the reference and version, never secret bytes.

## Proven network behavior

The executable lab proves:

1. a trusted TLS endpoint receives the deterministic CloudEvents body and a verifiable signature;
2. an untrusted/self-signed certificate is rejected by the default trust store and becomes a retryable delivery failure;
3. slow customer responses are bounded by a client timeout and return the delivery to durable retry state;
4. abrupt connection reset/disconnect is retryable and does not lose the delivery intent;
5. HTTP `429` is classified as retryable and its `Retry-After` delta/date hint is parsed and bounded;
6. a versioned secret rotation changes the real HTTPS signature for an explicit replay while preserving the same public event id and payload;
7. unavailable secret material prevents network delivery and remains retryable;
8. one worker invocation claims at most one durable delivery, so local work admission remains bounded rather than reading an unbounded batch into memory.

## Retry-After boundary

The HTTP adapter captures and bounds `Retry-After`, but this ADR deliberately does **not** change the crash-safe PostgreSQL retry schedule from ADR 0064.

Persistently honoring a remote backpressure hint must be folded into the same atomic result-recording transaction before production adoption. A two-transaction "record result, then defer later" implementation would create a crash window and is therefore not introduced by this lab.

Until that atomic extension exists, the durable deterministic retry schedule remains authoritative and the parsed `Retry-After` value is evidence for the next rate-limit/backpressure slice.

## Security boundary

This is a local executable lab, not authorization to send arbitrary production webhooks.

Production delivery still requires:

- endpoint ownership verification and independently revocable subscription authorization;
- SSRF and DNS-rebinding controls for customer-supplied endpoint URLs;
- a real secret-manager implementation and credential lifecycle/revocation policy;
- certificate/hostname policy, TLS observability, and operator diagnostics;
- explicit redirect policy rather than silently following redirects;
- per-subscription and tenant rate limits across multiple workers.

## What remains before adoption

The candidate remains `trial`. The next evidence should cover atomic `Retry-After` scheduling, multi-worker rate limiting/backpressure, endpoint ownership and scoped authorization, real secret-manager integration, customer-visible delivery diagnostics, retention, metrics/SLOs, and the durable pull-feed recovery decision.

The public boundary remains independent from internal NATS subjects, streams, consumers, retention, and topology.

## Consequences

The candidate has now been proven across three distinct layers:

```text
public contract semantics
        +
durable PostgreSQL delivery state
        +
real HTTPS/TLS failure behavior
```

That materially reduces protocol risk without prematurely promoting the mechanism to `adopt`.
