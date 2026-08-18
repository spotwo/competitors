from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os
import statistics
import time
import uuid

import psycopg

DATABASE_URL = os.getenv(
    "KERNEL_LAB_DATABASE_URL",
    "postgresql://postgres:postgres@127.0.0.1:55432/kernel_lab",
)

TENANT = uuid.UUID("90000000-0000-0000-0000-000000000001")
WAREHOUSE = uuid.UUID("90000000-0000-0000-0000-000000000010")
OWNER = uuid.UUID("90000000-0000-0000-0000-000000002001")
CONDITION = uuid.UUID("90000000-0000-0000-0000-000000003001")

PROFILES = {
    "smoke": {"positions": 20_000, "workers": 16, "transfer_iterations": 4},
    "standard": {"positions": 100_000, "workers": 32, "transfer_iterations": 10},
    "scale": {"positions": 1_000_000, "workers": 100, "transfer_iterations": 20},
}


def connect(*, autocommit: bool = False) -> psycopg.Connection:
    conn = psycopg.connect(DATABASE_URL, autocommit=autocommit)
    conn.execute("SET statement_timeout = '30s'")
    conn.execute("SET lock_timeout = '10s'")
    return conn


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def latency_summary(values: list[float]) -> dict[str, float]:
    return {
        "p50_ms": round(percentile(values, 0.50), 3),
        "p95_ms": round(percentile(values, 0.95), 3),
        "p99_ms": round(percentile(values, 0.99), 3),
        "mean_ms": round(statistics.fmean(values), 3) if values else 0.0,
        "max_ms": round(max(values), 3) if values else 0.0,
    }


def seed_dataset(position_count: int) -> dict[str, object]:
    started = time.perf_counter()
    item_count = max(10, min(1000, math.isqrt(position_count)))
    location_count = math.ceil(position_count / item_count)

    with connect(autocommit=True) as conn:
        conn.execute(
            """
            TRUNCATE TABLE
              kernel_lab.warehouse_task_confirmations,
              kernel_lab.warehouse_task_exceptions,
              kernel_lab.warehouse_task_executions,
              kernel_lab.warehouse_work_assignments,
              kernel_lab.warehouse_work_command_receipts,
              kernel_lab.warehouse_tasks,
              kernel_lab.warehouse_works,
              kernel_lab.inventory_count_results,
              kernel_lab.inventory_holds,
              kernel_lab.inventory_hold_policies,
              kernel_lab.inventory_allocations,
              kernel_lab.inventory_reservations,
              kernel_lab.inventory_transaction_legs,
              kernel_lab.inventory_transactions,
              kernel_lab.inventory_serial_memberships,
              kernel_lab.handling_unit_movements,
              kernel_lab.inventory_positions,
              kernel_lab.serials,
              kernel_lab.handling_units,
              kernel_lab.inventory_attribute_sets,
              kernel_lab.stock_segments,
              kernel_lab.stock_scopes,
              kernel_lab.lots,
              kernel_lab.locations,
              kernel_lab.inventory_conditions,
              kernel_lab.owners,
              kernel_lab.items,
              kernel_lab.warehouses,
              kernel_lab.tenants
            CASCADE
            """
        )
        conn.execute(
            "INSERT INTO kernel_lab.tenants (id, name) VALUES (%s, 'Benchmark Tenant')",
            (TENANT,),
        )
        conn.execute(
            "INSERT INTO kernel_lab.warehouses (id, tenant_id, code) VALUES (%s, %s, 'BENCH')",
            (WAREHOUSE, TENANT),
        )
        conn.execute(
            "INSERT INTO kernel_lab.owners (id, tenant_id, code) VALUES (%s, %s, 'OWNER')",
            (OWNER, TENANT),
        )
        conn.execute(
            "INSERT INTO kernel_lab.inventory_conditions (id, tenant_id, code) VALUES (%s, %s, 'NORMAL')",
            (CONDITION, TENANT),
        )
        conn.execute(
            """
            INSERT INTO kernel_lab.items (id, tenant_id, sku, exact_serial_tracking)
            SELECT
              ('91000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid,
              %s,
              'SKU-' || lpad(g::text, 6, '0'),
              false
            FROM generate_series(1, %s) AS g
            """,
            (TENANT, item_count),
        )
        conn.execute(
            """
            INSERT INTO kernel_lab.locations (id, tenant_id, warehouse_id, code)
            SELECT
              ('92000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid,
              %s,
              %s,
              'L-' || lpad(g::text, 6, '0')
            FROM generate_series(1, %s) AS g
            """,
            (TENANT, WAREHOUSE, location_count),
        )
        conn.execute(
            """
            INSERT INTO kernel_lab.inventory_positions (
              id, tenant_id, warehouse_id, location_id,
              item_id, owner_id, inventory_condition_id, physical_qty
            )
            SELECT
              ('93000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid,
              %s,
              %s,
              ('92000000-0000-0000-0000-' ||
                lpad(to_hex((((g - 1) / %s) %% %s) + 1), 12, '0'))::uuid,
              ('91000000-0000-0000-0000-' ||
                lpad(to_hex(((g - 1) %% %s) + 1), 12, '0'))::uuid,
              %s,
              %s,
              100
            FROM generate_series(1, %s) AS g
            """,
            (
                TENANT,
                WAREHOUSE,
                item_count,
                location_count,
                item_count,
                OWNER,
                CONDITION,
                position_count,
            ),
        )
        conn.execute("ANALYZE kernel_lab.inventory_positions")

    return {
        "positions": position_count,
        "items": item_count,
        "locations": location_count,
        "seed_seconds": round(time.perf_counter() - started, 3),
    }


def first_item_id() -> uuid.UUID:
    return uuid.UUID("91000000-0000-0000-0000-000000000001")


def collect_plan() -> dict[str, object]:
    with connect() as conn:
        row = conn.execute(
            """
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT id, physical_qty, allocated_qty
            FROM kernel_lab.inventory_positions
            WHERE tenant_id = %s
              AND warehouse_id = %s
              AND item_id = %s
              AND owner_id = %s
              AND inventory_condition_id = %s
              AND lot_id IS NULL
              AND stock_scope_id IS NULL
              AND attribute_set_id IS NULL
              AND stock_segment_id IS NULL
              AND physical_qty > allocated_qty
            ORDER BY location_id NULLS LAST, id
            LIMIT 20
            """,
            (TENANT, WAREHOUSE, first_item_id(), OWNER, CONDITION),
        ).fetchone()[0]

    document = row[0]
    root = document["Plan"]
    index_names: set[str] = set()
    node_types: list[str] = []
    shared_hit = 0
    shared_read = 0

    def walk(node: dict[str, object]) -> None:
        nonlocal shared_hit, shared_read
        node_types.append(str(node.get("Node Type", "")))
        if node.get("Index Name"):
            index_names.add(str(node["Index Name"]))
        shared_hit += int(node.get("Shared Hit Blocks", 0) or 0)
        shared_read += int(node.get("Shared Read Blocks", 0) or 0)
        for child in node.get("Plans", []) or []:
            walk(child)

    walk(root)
    return {
        "execution_ms": round(float(document.get("Execution Time", 0.0)), 3),
        "planning_ms": round(float(document.get("Planning Time", 0.0)), 3),
        "shared_hit_blocks": shared_hit,
        "shared_read_blocks": shared_read,
        "index_names": sorted(index_names),
        "node_types": node_types,
    }


def benchmark_hot_allocation(workers: int) -> dict[str, object]:
    capacity = max(1, workers // 2)
    hot_position = uuid.UUID("93000000-0000-0000-0000-000000000001")
    with connect(autocommit=True) as conn:
        conn.execute("DELETE FROM kernel_lab.inventory_transaction_legs")
        conn.execute("DELETE FROM kernel_lab.inventory_transactions")
        conn.execute(
            """
            UPDATE kernel_lab.inventory_positions
            SET physical_qty = %s, allocated_qty = 0, reserved_qty = 0, version = 0
            WHERE id = %s
            """,
            (capacity, hot_position),
        )

    def attempt(worker: int) -> tuple[str, float]:
        started = time.perf_counter()
        try:
            with connect(autocommit=True) as conn:
                conn.execute(
                    "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 1)",
                    (uuid.uuid4(), TENANT, f"bench:allocation:{worker}", hot_position),
                )
            return "success", (time.perf_counter() - started) * 1000
        except psycopg.errors.CheckViolation:
            return "rejected", (time.perf_counter() - started) * 1000
        except psycopg.errors.DeadlockDetected:
            return "deadlock", (time.perf_counter() - started) * 1000

    wall_started = time.perf_counter()
    results: list[tuple[str, float]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(attempt, worker) for worker in range(workers)]
        for future in as_completed(futures):
            results.append(future.result())
    wall_seconds = time.perf_counter() - wall_started

    statuses = [status for status, _ in results]
    latencies = [latency for _, latency in results]
    with connect() as conn:
        allocated = conn.execute(
            "SELECT allocated_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (hot_position,),
        ).fetchone()[0]

    return {
        "workers": workers,
        "capacity": capacity,
        "successes": statuses.count("success"),
        "rejected": statuses.count("rejected"),
        "deadlocks": statuses.count("deadlock"),
        "final_allocated_qty": float(allocated),
        "wall_seconds": round(wall_seconds, 3),
        "throughput_ops_s": round(len(results) / wall_seconds, 2),
        **latency_summary(latencies),
    }


def benchmark_opposite_transfers(workers: int, iterations: int) -> dict[str, object]:
    item = first_item_id()
    with connect(autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT id
            FROM kernel_lab.inventory_positions
            WHERE tenant_id = %s AND item_id = %s
            ORDER BY location_id, id
            LIMIT 2
            """,
            (TENANT, item),
        ).fetchall()
        if len(rows) != 2:
            raise RuntimeError("benchmark dataset needs two positions for the hot item")
        left, right = rows[0][0], rows[1][0]
        conn.execute("DELETE FROM kernel_lab.inventory_transaction_legs")
        conn.execute("DELETE FROM kernel_lab.inventory_transactions")
        conn.execute(
            """
            UPDATE kernel_lab.inventory_positions
            SET physical_qty = 100000, allocated_qty = 0, reserved_qty = 0
            WHERE id IN (%s, %s)
            """,
            (left, right),
        )
        deadlocks_before = conn.execute(
            "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()"
        ).fetchone()[0]

    def worker(worker_id: int) -> tuple[int, list[float]]:
        local_deadlocks = 0
        local_latencies: list[float] = []
        for iteration in range(iterations):
            source, target = (left, right) if (worker_id + iteration) % 2 == 0 else (right, left)
            started = time.perf_counter()
            try:
                with connect(autocommit=True) as conn:
                    conn.execute(
                        "SELECT kernel_lab.transfer_inventory_quantity(%s, %s, %s, %s, %s, 1)",
                        (
                            uuid.uuid4(),
                            TENANT,
                            f"bench:move:{worker_id}:{iteration}",
                            source,
                            target,
                        ),
                    )
            except psycopg.errors.DeadlockDetected:
                local_deadlocks += 1
            local_latencies.append((time.perf_counter() - started) * 1000)
        return local_deadlocks, local_latencies

    wall_started = time.perf_counter()
    deadlocks = 0
    latencies: list[float] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, worker_id) for worker_id in range(workers)]
        for future in as_completed(futures):
            local_deadlocks, local_latencies = future.result()
            deadlocks += local_deadlocks
            latencies.extend(local_latencies)
    wall_seconds = time.perf_counter() - wall_started

    with connect() as conn:
        total = conn.execute(
            "SELECT sum(physical_qty) FROM kernel_lab.inventory_positions WHERE id IN (%s, %s)",
            (left, right),
        ).fetchone()[0]
        deadlocks_after = conn.execute(
            "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()"
        ).fetchone()[0]

    operations = workers * iterations
    return {
        "workers": workers,
        "iterations_per_worker": iterations,
        "operations": operations,
        "deadlocks_seen_by_clients": deadlocks,
        "deadlocks_pg_stat_delta": int(deadlocks_after - deadlocks_before),
        "conserved_physical_qty": float(total),
        "wall_seconds": round(wall_seconds, 3),
        "throughput_ops_s": round(operations / wall_seconds, 2),
        **latency_summary(latencies),
    }


def enforce_smoke_gates(report: dict[str, object]) -> None:
    plan = report["candidate_plan"]
    allocation = report["hot_allocation"]
    transfers = report["opposite_transfers"]

    if not plan["index_names"]:
        raise SystemExit("performance smoke failed: candidate source lookup used no index")
    if allocation["deadlocks"] != 0:
        raise SystemExit("performance smoke failed: allocation contention produced a deadlock")
    if allocation["successes"] != allocation["capacity"]:
        raise SystemExit("performance smoke failed: allocation race did not consume exact capacity")
    if allocation["final_allocated_qty"] != float(allocation["capacity"]):
        raise SystemExit("performance smoke failed: final allocated quantity is inconsistent")
    if transfers["deadlocks_seen_by_clients"] != 0 or transfers["deadlocks_pg_stat_delta"] != 0:
        raise SystemExit("performance smoke failed: deterministic movement locking deadlocked")
    if transfers["conserved_physical_qty"] != 200000.0:
        raise SystemExit("performance smoke failed: opposite transfers violated conservation")


def main() -> int:
    parser = argparse.ArgumentParser(description="Spotwo PostgreSQL kernel performance/contention lab")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="smoke")
    parser.add_argument("--positions", type=int, help="override profile position count")
    parser.add_argument("--workers", type=int, help="override profile worker count")
    parser.add_argument("--transfer-iterations", type=int, help="override transfer iterations per worker")
    args = parser.parse_args()

    profile = dict(PROFILES[args.profile])
    if args.positions is not None:
        profile["positions"] = args.positions
    if args.workers is not None:
        profile["workers"] = args.workers
    if args.transfer_iterations is not None:
        profile["transfer_iterations"] = args.transfer_iterations

    report: dict[str, object] = {
        "profile": args.profile,
        "configuration": profile,
    }
    with connect() as conn:
        report["postgresql_version"] = conn.execute("SHOW server_version").fetchone()[0]

    report["dataset"] = seed_dataset(int(profile["positions"]))
    report["candidate_plan"] = collect_plan()
    report["hot_allocation"] = benchmark_hot_allocation(int(profile["workers"]))
    report["opposite_transfers"] = benchmark_opposite_transfers(
        int(profile["workers"]), int(profile["transfer_iterations"])
    )

    if args.profile == "smoke":
        enforce_smoke_gates(report)

    print("KERNEL_PERF_REPORT=" + json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
