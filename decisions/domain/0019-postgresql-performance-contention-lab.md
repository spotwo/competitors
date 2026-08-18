# ADR 0019: PostgreSQL Performance and Contention Lab

- Status: Proposed
- Date: 2026-08-18

## Context

The executable kernel now covers InventoryPosition identity, immutable inventory postings, Reservation/Allocation/Consumption, Holds/Eligibility, Warehouse Work execution, and end-to-end Golden Warehouse Scenarios.

Correctness under small deterministic tests does not answer the next class of architecture questions:

- does source selection continue to use appropriate indexes as InventoryPosition cardinality grows?
- what is the contention cost of one hot pick face?
- do scope-level advisory locks serialize more work than intended?
- does deterministic row-lock ordering continue to prevent movement deadlocks under opposite-direction load?
- where should future work prefer row locks, optimistic version checks, advisory locks, or Serializable retry?
- which metrics are stable enough to become merge gates and which must remain observational benchmarks?

A WMS kernel should not make these choices only from vendor descriptions or intuition. They need reproducible PostgreSQL evidence.

## Decision

Add a first-class PostgreSQL Performance & Contention Lab next to the functional kernel lab.

The lab has three profiles:

```text
smoke     20k positions / 16 workers
standard 100k positions / 32 workers
scale      1M positions / 100 workers
```

Profile values are defaults, not product capacity claims.

### PR/merge gate

Every normal `bin/check-kernel-lab` run executes the `smoke` profile after functional tests.

The smoke gate checks deterministic properties rather than fixed timing:

1. critical InventoryPosition source lookup remains index-backed;
2. concurrent allocation cannot exceed physical capacity;
3. final materialized allocation equals successful committed allocations;
4. hot allocation contention produces no deadlock;
5. opposite-direction movement contention produces no deadlock;
6. movement conserves total physical quantity.

### Observational metrics

The same run records, but does not gate on:

- seed time;
- planner time;
- executor time;
- shared buffer hits/reads;
- index names and plan node types;
- throughput operations/second;
- p50/p95/p99/max operation latency;
- PostgreSQL deadlock counters.

These values are machine-dependent and should become regression thresholds only on a controlled, stable runner or through relative comparison to an equivalent baseline run.

### Scale profiles

`standard` and `scale` are deliberate experiments rather than required checks on every PR. They use the identical schema/functions and benchmark code as the smoke profile, but increase cardinality and concurrency.

The scale profile is specifically intended to expose:

```text
1M InventoryPositions
100 concurrent workers
hot exact-position allocation
opposite-direction movement on one semantic stock scope
```

This is an architecture probe, not a claim that a production Spotwo deployment is limited to or guaranteed to sustain those numbers.

## Benchmark dataset

The harness seeds deterministic synthetic warehouse data directly in PostgreSQL using `generate_series`, then runs `ANALYZE` before query-plan measurement.

Inventory positions vary by Item and Location while retaining a shared tenant/warehouse/owner/condition model. This provides enough cardinality for planner behavior without introducing application-layer fixture cost into the benchmark.

## Source-selection plan

The first protected query is an exact stock-scope candidate lookup against `inventory_positions`.

The harness captures:

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
```

and requires at least one index-backed plan node in the smoke dataset.

The exact index name is reported rather than hard-coded as the permanent domain contract so future index redesign remains possible.

## Hot allocation contention

Workers race to allocate one InventoryPosition where demand is intentionally larger than physical capacity.

Expected invariant:

```text
physical = N
workers  > N

successes          = N
final allocated_qty = N
rejected            = workers - N
deadlocks            = 0
```

This exercises the actual kernel allocation function rather than a benchmark-only SQL shortcut.

## Movement contention

Workers alternate source and destination across the same two positions.

The existing movement primitive:

1. acquires the semantic stock-scope advisory lock;
2. obtains position locks in deterministic ID order;
3. validates stock identity/eligibility;
4. posts balanced physical legs.

The benchmark intentionally creates opposite-direction pressure and verifies:

```text
deadlocks = 0
physical(A) + physical(B) remains constant
```

This makes any cost of scope-level serialization visible in throughput/latency while continuing to protect correctness.

## Locking policy implications

This ADR does **not** permanently choose advisory locks for every future WMS write.

Current exact-scope advisory locking remains the candidate policy while performance evidence is collected. A later ADR may narrow or replace that policy if controlled benchmarks show unacceptable contention and an alternative preserves the same invariants.

Potential comparison candidates include:

- row locking only;
- optimistic `version` compare-and-swap with bounded retry;
- finer advisory-lock keys;
- Serializable transactions with application retry;
- workload-specific combinations.

## CI policy

Hosted CI is used for the deterministic smoke profile, not for absolute capacity certification.

A result such as:

```text
p95 = 27 ms
```

on one hosted runner is telemetry, not a contractual SLO.

A result such as:

```text
Seq Scan replaced the expected index-backed candidate lookup
```

or:

```text
allocation race overcommitted physical stock
```

is a merge-blocking regression.

## Consequences

### Positive

- performance decisions become evidence-driven;
- contention behavior is reproducible with the same kernel code as functional CI;
- planner regressions are visible early;
- locking correctness is tested under real concurrent sessions;
- large-scale experiments do not make ordinary PR CI expensive or flaky.

### Costs

- kernel CI performs additional dataset seeding and concurrent work;
- benchmark results need interpretation rather than a single universal TPS number;
- controlled hardware will still be required before production SLO/capacity claims.

## Follow-up questions

The lab should guide, not pre-answer, the next optimization decisions:

1. Is the existing `(tenant, warehouse, item, condition)` InventoryPosition index sufficient for exact source selection at 1M+ positions?
2. Does the semantic-scope advisory lock create an unacceptable hot-pick-face serialization point?
3. Should Reservation and Allocation use the same lock granularity under all workloads?
4. Should transaction/ledger history be partitioned before 10M+ rows?
5. Which read projections need covering/partial indexes?
6. When should position/history tables be vacuum/analyze tuned independently?
7. Should production performance regression CI run on dedicated hardware rather than GitHub-hosted runners?
