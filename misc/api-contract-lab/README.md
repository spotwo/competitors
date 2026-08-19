# MISC API Contract Lab

This lab compares **REST + OpenAPI 3.1** with a semantically equivalent **gRPC / Protocol Buffers** contract for a deliberately small WMS surface.

It remains under `misc/` because the example contracts are experimental evidence rather than production API source code. The external contract-style result was later promoted into canonical ADR 0059, while the exact example endpoints and schemas here remain non-authoritative.

## Question

For Spotwo business integrations, mobile clients, agents, warehouse-edge deployments, and possible internal high-frequency calls, where should REST/OpenAPI and gRPC sit?

The lab does not ask which protocol is universally better. It evaluates two different scopes:

1. external business API;
2. internal realtime/service candidate.

## Shared WMS surface

Both contracts represent the same five operations:

```text
GetLocation
GetHandlingUnit
ListInventoryPositions
CreateTask
GetTask
```

The shared semantic models are:

```text
Location
HandlingUnit
InventoryPosition
Task
```

`CreateTask` is intentionally mutating and requires a caller-supplied idempotency key in both protocol shapes.

## Files

- `openapi.yaml` - OpenAPI 3.1 HTTP contract;
- `spotwo.proto` - equivalent gRPC contract;
- `scenarios.yml` - machine-readable semantic parity and error mapping;
- `comparison.yml` - weighted structural comparison;
- `comparison.md` - generated human-readable result;
- `requirements-openapi-client.txt` - isolated OpenAPI client-generator dependency;
- `requirements-openapi-server.txt` - isolated OpenAPI server-stub generator dependency;
- `requirements-grpc.txt` - isolated gRPC code-generator dependency.

## What is actually executable

The repository-level validator checks that both contracts continue to expose the same operations and shared model fields, that idempotency is explicit on task creation, and that comparison scores remain internally consistent.

The dedicated MISC CI additionally proves code generation:

```text
OpenAPI 3.1
  -> generated Python client
  -> generated FastAPI server skeleton

gRPC proto
  -> generated Python message classes
  -> generated client stub
  -> generated server servicer interface
```

All generated code is written to temporary CI directories and compiled. It is not committed and does not become a runtime dependency of Spotwo.

Local codegen smoke test:

```bash
bin/check-misc-api-contract-lab
```

## Current structural result

The weighted structural profile currently prefers:

```text
external business API        REST + OpenAPI
internal realtime candidate  gRPC
```

That is deliberately narrower than saying "REST everywhere" or "gRPC everywhere".

The external profile values browser/HTTP ubiquity, customer tooling, human debugging, agent tooling, and proxy compatibility. The internal profile gives more weight to wire efficiency, typed contracts, generated stubs, and bidirectional streaming.

## What this lab does not measure

No production latency, throughput, CPU, memory, connection-count, proxy behavior, or failure-recovery benchmark is claimed here. The scores in `comparison.yml` are structural engineering judgments, not measured performance numbers.

ADR 0059 promotes **REST + OpenAPI 3.1** to `adopt` only for the default external business API contract style. gRPC remains a separately scoped `trial` for internal realtime/service paths until a realistic runtime benchmark exists. This lab stays as preserved evidence and must not be treated as the production Spotwo API definition.
