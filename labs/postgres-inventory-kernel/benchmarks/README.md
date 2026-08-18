# PostgreSQL Kernel Performance & Contention Lab

This directory measures the executable WMS kernel under realistic PostgreSQL contention without turning machine-specific latency into a brittle merge gate.

## Profiles

| Profile | InventoryPositions | Workers | Transfer iterations / worker | Purpose |
|---|---:|---:|---:|---|
| `smoke` | 20,000 | 16 | 4 | PR/merge correctness + plan-shape gate |
| `standard` | 100,000 | 32 | 10 | developer performance baseline |
| `scale` | 1,000,000 | 100 | 20 | deliberate scale/contention experiment |

The profile values can be overridden with CLI flags.

## Run

The normal kernel command runs the `smoke` profile after the functional test suite:

```bash
bash bin/check-kernel-lab
```

Run a larger profile against the same ephemeral PostgreSQL container:

```bash
KERNEL_LAB_PERF_PROFILE=standard bash bin/check-kernel-lab
KERNEL_LAB_PERF_PROFILE=scale bash bin/check-kernel-lab
```

Disable performance smoke when you only want functional kernel tests:

```bash
KERNEL_LAB_PERF_PROFILE=off bash bin/check-kernel-lab
```

The benchmark can also be called directly when `KERNEL_LAB_DATABASE_URL` already points at a migrated kernel database:

```bash
python labs/postgres-inventory-kernel/benchmarks/run.py --profile smoke
python labs/postgres-inventory-kernel/benchmarks/run.py --profile scale --workers 120
```

## What is measured

### Candidate source lookup

The harness seeds realistic `InventoryPosition` cardinality, runs `ANALYZE`, then captures:

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
```

for an exact stock-scope source-selection query. The report records planner/executor time, node types, index names, shared-buffer hits and reads.

The PR smoke gate requires an index-backed candidate lookup, but does **not** enforce a fixed millisecond threshold because GitHub hosted runners are not stable benchmark hardware.

### Hot allocation contention

Multiple clients concurrently allocate the same hot `InventoryPosition` while demand exceeds physical capacity. The gate proves:

```text
successful allocations == physical capacity
final allocated_qty      == physical capacity
deadlocks                 == 0
```

The report also records throughput and p50/p95/p99 latency.

### Opposite-direction movement contention

Clients repeatedly transfer quantity across the same pair of positions in opposite directions. This stresses the kernel's scope advisory lock plus deterministic position-lock ordering.

The gate proves:

```text
deadlocks = 0
source + target physical quantity is conserved
```

Latency/TPS are reported as observational baseline telemetry.

## Output

The harness prints one machine-readable line:

```text
KERNEL_PERF_REPORT={...json...}
```

This is intentionally log-friendly so CI systems can persist or compare reports later without changing the benchmark contract.

## Why no absolute latency gate?

Absolute timing depends on runner CPU, noisy neighbors, filesystem/cache state, container startup and PostgreSQL configuration. The merge gate therefore protects deterministic properties:

- critical lookup remains index-backed;
- capacity cannot be overcommitted under concurrency;
- movement conserves physical quantity;
- deterministic lock ordering does not deadlock.

Performance numbers are still captured so regressions can later be compared on controlled hardware or against a stable baseline runner.
