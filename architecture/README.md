# Spotwo Architecture Boundaries

`architecture/boundaries.yml` is the canonical machine-readable boundary map.

The map answers a different question from the technology decision registry. A technology decision says whether Spotwo adopts, trials, watches, or rejects a technology inside a scope. A boundary record says **who talks to whom, who owns state, what may cross the boundary, and what reliability/security/versioning rules apply**.

## Contract statuses

- `canonical` - the mechanism is adopted for this exact boundary and cites an adopted technology decision;
- `candidate` - the boundary/ownership is accepted, while the implementation mechanism remains under trial;
- `unresolved` - the boundary is known but the mechanism is intentionally not selected yet.

Unresolved is a valid architecture state. It is better than converting a plausible guess into a hidden dependency.

## Querying boundaries

```bash
bin/query-boundary
bin/query-boundary --status unresolved
bin/query-boundary --technology nats
bin/query-boundary --technology sparkplug
bin/query-boundary --standard "VDA 5050"
bin/query-boundary --classification industrial-adapter-boundary
bin/query-boundary --has-gaps --format json
bin/query-boundary --gap enrollment --format ndjson
```

`--technology` searches only the contract mechanism and explicit technology/open-source references. It does not treat arbitrary research-gap prose as evidence that a technology belongs to the boundary.

## Reference coverage backlog

`architecture/coverage-gaps.yml` is the canonical machine-readable backlog for the **Reference Coverage Gaps** step of the research strategy.

It does not select technologies. It records whether each boundary has enough evidence across four separate lenses:

- `standards`;
- `open_source`;
- `commercial`;
- `executable`.

Each lens is `required`, `useful`, or `not-required` and has a status of `covered`, `partial`, `missing`, or `not-required`.

Evidence and research targets are structurally separate. A target such as `AutoStore`, `open62541`, or `AsyncAPI` means "research this next". It does not mean the repository has verified a claim about that target.

Query the backlog with:

```bash
bin/query-coverage
bin/query-coverage --priority P0
bin/query-coverage --overall missing
bin/query-coverage --lens executable --lens-status missing
bin/query-coverage --target open62541
bin/query-coverage --gap reconnect --format json
bin/query-coverage --contract-status unresolved --format ndjson
```

The initial P0 research queue is deliberately bounded to external asynchronous integration, Spotwo Edge control/synchronization, PLC/machine control, and AMR/AGV fleet control.

## Human-readable views

`matrices/spotwo-architecture-boundaries.md` is generated from the boundary registry:

```bash
python scripts/generate_architecture_boundary_map.py
```

`matrices/spotwo-architecture-reference-coverage.md` is generated from the coverage backlog:

```bash
python scripts/generate_architecture_coverage.py
```

The repository validator checks that both generated views are current.

## Evidence and ownership rules

Each boundary records:

- source and destination participants;
- classification;
- canonical/candidate/unresolved contract;
- commands, events, and queries;
- authoritative state owners;
- forbidden ownership leaks;
- retry, idempotency, ordering, and offline behavior;
- trust boundary, authentication, and authorization;
- compatibility/versioning;
- simulation expectations;
- standards;
- technology/open-source/repository evidence;
- research gaps.

Technology and standard names do not gain ownership merely by appearing in this map. Spotwo domain semantics remain independent from transports, generated DTOs, external standards, vendor SDKs, fieldbus addresses, robot protocol messages, and solver-native objects.

## Next research use

Use the coverage backlog to drive the next evidence passes instead of collecting references indiscriminately. Start with P0 gaps, attach verified evidence to the exact boundary and lens it supports, and only then revisit technology decisions or contract status when the evidence justifies a change.
