# Spotwo Architecture Boundaries

`architecture/boundaries.yml` is the canonical machine-readable boundary map.

The map answers a different question from the technology decision registry. A technology decision says whether Spotwo adopts, trials, watches, or rejects a technology inside a scope. A boundary record says **who talks to whom, who owns state, what may cross the boundary, and what reliability/security/versioning rules apply**.

## Contract statuses

- `canonical` - the mechanism is adopted for this exact boundary and cites an adopted technology decision;
- `candidate` - the boundary/ownership is accepted, while the implementation mechanism remains under trial;
- `unresolved` - the boundary is known but the mechanism is intentionally not selected yet.

Unresolved is a valid architecture state. It is better than converting a plausible guess into a hidden dependency.

## Querying

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

## Human-readable view

`matrices/spotwo-architecture-boundaries.md` is generated from the registry:

```bash
python scripts/generate_architecture_boundary_map.py
```

The repository validator checks that the generated view is current.

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

The immediate next use of this registry is **Reference Coverage Gaps**. Query unresolved boundaries and explicit gaps, then attach market, standards, and implementation evidence to those exact architecture questions instead of growing an undifferentiated technology catalog.
