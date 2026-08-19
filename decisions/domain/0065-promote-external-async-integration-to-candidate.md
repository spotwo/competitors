# ADR 0065: Promote external asynchronous integration to candidate

Status: Accepted
Date: 2026-08-19

## Context

The Architecture Boundary Map deliberately left `external-async-integration` unresolved until Spotwo had evidence for both the public contract shape and the delivery runtime behind it.

ADR 0063 narrowed the contract trial to CloudEvents 1.0 structured JSON delivered through signed HTTPS webhooks and described with AsyncAPI 3.1.0. The bounded contract lab proved stable event identity, Standard Webhooks-style HMAC-SHA256 signing, duplicate-capable at-least-once semantics, no global ordering assumption, and an explicit HTTP retry classifier.

ADR 0064 then added the missing executable runtime evidence. The PostgreSQL lab proves durable subscription and delivery state, worker lease exclusion, expired-lease recovery, stale-worker fencing, deterministic bounded retry, dead-letter state, explicit replay generations, subscription pause behavior, and signing-secret version snapshots.

That evidence satisfies the repository policy that executable evidence must exist before a runtime contract is promoted to `candidate`. It does not satisfy the higher bar for production adoption.

## Decision

Promote the `external-async-integration` architecture boundary from `unresolved` to `candidate`.

Register `signed-cloudevents-webhooks` as a scoped `trial` technology decision with this candidate profile:

```text
Spotwo domain fact
    -> public projection
    -> durable external delivery journal
    -> CloudEvents 1.0 structured JSON
    -> signed HTTPS webhook
    -> customer endpoint
```

Describe the public asynchronous contract with AsyncAPI 3.1.0. Keep the Standard Webhooks signing shape as the current trial profile for message identity, timestamp, signature metadata, and HMAC-SHA256 verification.

The public CloudEvents `id` remains stable across ordinary retries and explicit replay. Delivery remains explicitly at-least-once and duplicate-capable. No global ordering guarantee is introduced.

## Why candidate, not canonical

The repository has enough evidence to stop treating the mechanism as unknown, but it does not yet authorize production adoption.

The remaining production work includes:

- real HTTP/TLS failure and timeout tests against owned endpoints;
- endpoint ownership verification and subscription authorization by tenant, site, event type, and resource scope;
- secret-manager integration, rotation, revocation, and audit behavior under real requests;
- rate limiting, backpressure, delivery diagnostics, retention, observability, and SLOs;
- a public compatibility and deprecation policy for event types and payloads;
- a decision on whether durable pull-feed recovery is also required for customers that cannot rely on push delivery alone.

Until those properties are proven, the technology decision remains `trial` and the boundary remains `candidate`.

## Isolation rule

This promotion does not expose NATS JetStream as a customer contract.

Customer-visible event identity, event types, delivery generations, signing metadata, retry behavior, and replay remain independent from internal subjects, streams, durable consumer names, broker retention, and topology migrations.

The internal domain-event backbone may later become one source of facts projected into the external delivery journal, but changing the internal broker must not require changing the public asynchronous contract.

## Coverage interpretation

The external asynchronous reference-coverage record moves from `missing` to `partial`.

Standards, open-source implementation references, and executable evidence are now covered for the candidate question. Commercial evidence is still partial: Manhattan provides strong asynchronous acknowledgement and duplicate-processing evidence, while the broader WMS market remains heterogeneous and additional product-specific replay, authorization, and compatibility evidence would improve confidence before adoption.

## Consequences

Spotwo now has an explicit candidate public asynchronous boundary rather than an unresolved placeholder.

New customer-event work should target this candidate contract unless a separately reviewed use case requires another mechanism. Implementations must continue to preserve the public/internal isolation rule and must not describe this candidate as a production-adopted contract until a later ADR promotes the technology decision from `trial` to `adopt` and the architecture boundary from `candidate` to `canonical`.
