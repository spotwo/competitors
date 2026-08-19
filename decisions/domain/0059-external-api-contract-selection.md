# ADR 0059 - External API Contract Selection

Status: Accepted

Date: 2026-08-19

## Context

The technology decision registry kept REST + OpenAPI in `trial` until Spotwo had repository-owned evidence rather than relying only on competitor usage and general ecosystem familiarity.

PR #59 added an isolated contract lab under `misc/api-contract-lab/`. The lab models the same WMS semantics through two protocol shapes:

- REST + OpenAPI 3.1;
- gRPC + Protocol Buffers.

Both contracts expose the same five operations:

```text
GetLocation
GetHandlingUnit
ListInventoryPositions
CreateTask
GetTask
```

Both preserve the same shared models for Location, HandlingUnit, InventoryPosition, and Task. The lab also makes task creation idempotency explicit, maps equivalent error semantics, validates model/operation parity, and proves Python client/server code generation for both contract families.

The comparison is deliberately scoped. It does not ask which protocol is universally better.

## Evidence

The machine-readable structural comparison is `misc/api-contract-lab/comparison.yml`.

For the **external business API** profile the weighted result is:

```text
REST + OpenAPI   4.40 / 5
 gRPC            3.25 / 5
margin           1.15
```

This profile weights browser/HTTP ubiquity, customer tooling, human debugging, generated contracts, agent tooling, proxy compatibility, streaming, and wire efficiency.

For the **internal realtime candidate** profile the structural result is:

```text
gRPC             4.50 / 5
REST + OpenAPI   3.55 / 5
margin           0.95
```

The internal profile remains a hypothesis because the lab has not measured production latency, sustained throughput, CPU/memory cost, connection scale, proxy failure behavior, or offline recovery.

The dedicated code-generation CI successfully generated and compiled:

```text
OpenAPI 3.1
  -> Python client
  -> FastAPI server skeleton

gRPC
  -> Python messages
  -> client stub
  -> server servicer interface
```

Competitor evidence remains supporting evidence only. Verified repository research already shows REST APIs in products such as Logiwa IO, OpenBoxes, Oracle Warehouse Management Cloud, and ERPNext, but that market observation did not become authoritative until the Spotwo-owned lab existed.

## Decision

Adopt **REST + OpenAPI 3.1 as the canonical default external business API contract style for Spotwo**.

The adopted scope includes:

- customer and partner business integrations;
- browser and web clients;
- ordinary mobile client APIs;
- agent/tool-facing business APIs;
- warehouse-edge HTTP integrations where request/response semantics are appropriate.

This is a contract-style decision, not an implementation-framework decision. A Rails, Go, Python, Java, TypeScript, or another service may implement the contract as long as the external semantics remain compatible.

### Contract rules

The default external profile SHOULD follow these rules:

1. OpenAPI 3.1 is the machine-readable HTTP contract and must be kept executable through validation/code-generation checks.
2. Breaking external contract changes require an explicit API compatibility/versioning decision; implementation refactors alone must not force a public API version change.
3. Resource identifiers remain opaque strings at the protocol boundary unless a narrower domain contract explicitly requires otherwise.
4. Mutating operations that can be retried and can otherwise duplicate effects must expose an explicit idempotency mechanism. The lab uses `Idempotency-Key` for task creation.
5. Request correlation must be propagatable across the API boundary.
6. Errors must expose a stable machine-readable code separately from human-readable text; correlation identity should be returned when available.
7. REST/OpenAPI is not a replacement for asynchronous domain events. Long-running subscriptions, event fan-out, replay, and integration streams remain separate event-contract concerns.

### gRPC position

gRPC is **not rejected**.

Keep gRPC in `trial` for a separately scoped internal service-to-service or realtime/control profile. The structural lab gives it a strong result for typed contracts, code generation, wire efficiency, and bidirectional streaming, but runtime evidence is still missing.

Do not introduce gRPC merely to mirror every REST endpoint internally. A gRPC adoption must be justified by a concrete call path whose measured requirements benefit from its semantics.

## Consequences

The technology registry can promote `rest-openapi` from `trial` to `adopt` with high confidence for the external business API scope.

A separate `grpc-internal` decision remains `trial` with medium confidence until a realistic runtime benchmark exists.

The MISC contract lab remains non-production evidence. The exact example endpoints and schemas in that lab are not automatically the production Spotwo API. Future product API work should promote durable domain contracts into a canonical API area rather than treating `misc/` as runtime source code.

## Revisit when

Revisit the external default if one of these becomes true:

- customer integration requirements materially favor a different public protocol;
- measured payload/latency/connection constraints make HTTP/JSON unsuitable for an important external profile;
- browser/mobile/agent compatibility stops being a meaningful requirement for the target API;
- a new contract technology provides materially better interoperability without increasing customer operational burden.

Revisit gRPC separately when a real internal path requires high-frequency unary calls, client/server streaming, or bidirectional streaming and can be benchmarked against the REST baseline.
