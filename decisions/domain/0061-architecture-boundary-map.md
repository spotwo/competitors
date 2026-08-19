# ADR 0061: Canonical architecture boundary map

Status: **Accepted**

Date: 2026-08-19

## Context

Spotwo now has technology decisions for PostgreSQL, NATS JetStream, REST/OpenAPI, and several bounded trials such as Sparkplug, PLC4X, VDA 5050, EPCIS, OR-Tools, and Ocava.

Selecting technologies without first fixing architecture boundaries would create a serious risk: a protocol, library, vendor API, or external standard could accidentally define Spotwo domain semantics and state ownership.

The research RFC therefore makes the Architecture Boundary Map the first execution step before deeper market/standards convergence.

## Decision

Create `architecture/boundaries.yml` as the canonical machine-readable map of major Spotwo architecture boundaries.

Every boundary must define:

- participants on each side;
- classification and direction;
- canonical, candidate, or unresolved contract status;
- interaction style;
- commands, events, and queries that cross it;
- explicit authoritative state ownership;
- ownership leaks that are forbidden;
- retry, idempotency, ordering, and offline behavior;
- trust boundary, authentication, and authorization expectations;
- compatibility/versioning rules;
- simulation expectations;
- relevant standards;
- repository, technology-decision, and open-source evidence;
- explicit research gaps.

The initial map contains ten boundaries:

1. external business API;
2. internal domain-event backbone;
3. external asynchronous integration;
4. Spotwo Edge control and synchronization;
5. device and machine telemetry;
6. PLC and machine control;
7. AMR/AGV fleet control;
8. supply-chain traceability and visibility;
9. optimization/solver boundary;
10. real versus simulated execution.

## Contract status semantics

`canonical` means the contract mechanism is adopted for that exact boundary and must cite an adopted technology decision.

`candidate` means the boundary and ownership are accepted, while the implementation mechanism remains under a bounded trial. A candidate must cite a trial or already-adopted technology decision.

`unresolved` means the boundary is known but the mechanism is intentionally unselected. An unresolved boundary must not pretend to have a primary technology decision.

This distinction is important: architecture can be clear before a technology is chosen.

## Initial canonical contracts

Two boundaries are canonical immediately because repository-owned decisions and executable evidence already exist:

- external business API -> REST + OpenAPI 3.1;
- internal domain-event backbone -> broker-neutral Spotwo domain events transported by NATS JetStream.

The first map intentionally leaves external asynchronous integration and Edge management/control unresolved rather than inferring that internal NATS topology or public REST endpoints should automatically own those responsibilities.

## Boundary ownership principles

The following invariants apply across the map:

- external standards never become the internal Spotwo domain model merely because Spotwo supports them;
- transports move facts and commands but do not become the source of truth for business state;
- adapters isolate protocol/vendor semantics from canonical Spotwo concepts;
- physical devices, PLCs, robots, and safety controllers retain authority over physical and safety state appropriate to their layer;
- Spotwo owns warehouse intent and business state only where its bounded context is authoritative;
- unresolved boundaries stay visibly unresolved instead of being filled with plausible guesses.

## Generated view and query interface

`matrices/spotwo-architecture-boundaries.md` is generated from the registry for human review.

`bin/query-boundary` provides read-only machine access by boundary id, classification, contract status, technology, standard, or research gap, with table, JSON, and NDJSON output.

This lets the next research phase ask bounded questions such as:

```bash
bin/query-boundary --status unresolved
bin/query-boundary --technology sparkplug
bin/query-boundary --standard "VDA 5050"
bin/query-boundary --has-gaps --format json
```

## Validation

Repository validation checks:

- JSON Schema conformance;
- unique boundary IDs;
- cross-references to technology decisions and open-source projects;
- repository evidence paths;
- canonical contracts reference `adopt` decisions;
- candidate contracts reference `trial` or `adopt` decisions;
- unresolved contracts do not declare a primary technology decision;
- rejected technology decisions are never cited as supporting boundary evidence;
- each boundary declares state ownership and at least one command/event/query flow.

The generated map is also checked for freshness and query semantics have unit tests.

## Consequences

Positive:

- technology research now has an architecture skeleton to attach to;
- state ownership becomes reviewable before implementation;
- unknowns become a bounded research backlog;
- standards and vendor protocols can remain adapters/projections instead of distorting the internal model;
- future build/embed/adapter decisions can be made per boundary.

Costs:

- the map must evolve as evidence changes;
- some boundaries are deliberately incomplete;
- implementation work may reveal that one boundary needs to be split into several narrower contracts.

That refinement is expected. The map is a canonical architecture hypothesis with validated ownership, not a claim that the first decomposition is final forever.
