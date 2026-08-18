from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import threading

import psycopg

import conftest as lab


def create_work(
    conn: psycopg.Connection,
    *,
    capability: str = "picking",
    domain_reference: str = "ORDER-1",
    priority: int = 50,
    tasks: list[dict] | None = None,
):
    work_id = lab.new_id()
    conn.execute(
        """
        INSERT INTO kernel_lab.warehouse_works (
          id, tenant_id, warehouse_id, capability, domain_reference, priority
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (work_id, lab.TENANT, lab.WAREHOUSE, capability, domain_reference, priority),
    )

    specs = tasks or [{"operation": "pick"}]
    task_ids: list = []
    for sequence, spec in enumerate(specs, start=1):
        task_id = lab.new_id()
        task_ids.append(task_id)
        conn.execute(
            """
            INSERT INTO kernel_lab.warehouse_tasks (
              id, tenant_id, work_id, sequence, operation,
              planned_quantity, uom, domain_payload,
              requires_domain_confirmation, confirmation_requirements
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb)
            """,
            (
                task_id,
                lab.TENANT,
                work_id,
                sequence,
                spec.get("operation", f"step-{sequence}"),
                spec.get("planned_quantity"),
                spec.get("uom"),
                spec.get("domain_payload", "{}"),
                spec.get("requires_domain_confirmation", False),
                spec.get("confirmation_requirements", "{}"),
            ),
        )
    return work_id, task_ids


def release(conn, work_id, *, key="release-work"):
    return conn.execute(
        "SELECT kernel_lab.release_warehouse_work(%s, %s, %s)",
        (lab.TENANT, key, work_id),
    ).fetchone()[0]


def claim(
    conn,
    work_id,
    *,
    resource_id="worker-1",
    resource_kind="human",
    method="claim",
    key="claim-work",
    assignment_id=None,
):
    assignment_id = assignment_id or lab.new_id()
    result = conn.execute(
        """
        SELECT kernel_lab.claim_warehouse_work(%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            assignment_id,
            lab.TENANT,
            key,
            work_id,
            resource_kind,
            resource_id,
            method,
        ),
    ).fetchone()[0]
    return assignment_id, result


def start(
    conn,
    task_id,
    *,
    resource_id="worker-1",
    resource_kind="human",
    channel="rf",
    key="start-task",
    execution_id=None,
):
    execution_id = execution_id or lab.new_id()
    result = conn.execute(
        """
        SELECT kernel_lab.start_warehouse_task(%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            execution_id,
            lab.TENANT,
            key,
            task_id,
            resource_kind,
            resource_id,
            channel,
        ),
    ).fetchone()[0]
    return execution_id, result


def confirm(
    conn,
    task_id,
    *,
    resource_id="worker-1",
    resource_kind="human",
    outcome="confirmed",
    actual_quantity=None,
    domain_result_reference=None,
    data="{}",
    key="confirm-task",
    confirmation_id=None,
):
    confirmation_id = confirmation_id or lab.new_id()
    result = conn.execute(
        """
        SELECT kernel_lab.confirm_warehouse_task(
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
        )
        """,
        (
            confirmation_id,
            lab.TENANT,
            key,
            task_id,
            resource_kind,
            resource_id,
            outcome,
            actual_quantity,
            domain_result_reference,
            data,
        ),
    ).fetchone()[0]
    return confirmation_id, result


def raise_exception(
    conn,
    task_id,
    *,
    code="SHORT",
    details='{"reason":"missing stock"}',
    key="raise-exception",
    exception_id=None,
):
    exception_id = exception_id or lab.new_id()
    result = conn.execute(
        """
        SELECT kernel_lab.raise_warehouse_task_exception(
          %s, %s, %s, %s, 'human', 'worker-1', %s, %s::jsonb
        )
        """,
        (exception_id, lab.TENANT, key, task_id, code, details),
    ).fetchone()[0]
    return exception_id, result


def test_release_makes_only_first_task_ready():
    with lab.connect() as conn:
        work_id, task_ids = create_work(
            conn,
            tasks=[
                {"operation": "pick"},
                {"operation": "move"},
                {"operation": "put"},
            ],
        )
        assert release(conn, work_id) == work_id
        work_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (work_id,)
        ).fetchone()[0]
        task_states = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_tasks WHERE work_id = %s ORDER BY sequence",
            (work_id,),
        ).fetchall()

    assert work_state == "released"
    assert [row[0] for row in task_states] == ["ready", "pending", "pending"]
    assert len(task_ids) == 3


def test_released_work_queue_ranks_priority():
    with lab.connect() as conn:
        low_id, _ = create_work(conn, domain_reference="LOW", priority=10)
        high_id, _ = create_work(conn, domain_reference="HIGH", priority=90)
        release(conn, low_id, key="release-low")
        release(conn, high_id, key="release-high")
        rows = conn.execute(
            """
            SELECT id, queue_rank
            FROM kernel_lab.warehouse_work_queue
            WHERE tenant_id = %s AND warehouse_id = %s
            ORDER BY queue_rank
            """,
            (lab.TENANT, lab.WAREHOUSE),
        ).fetchall()

    assert rows[0] == (high_id, 1)
    assert rows[1] == (low_id, 2)


def test_concurrent_claim_allows_only_one_resource():
    with lab.connect() as conn:
        work_id, _ = create_work(conn)
        release(conn, work_id)

    barrier = threading.Barrier(2)

    def attempt(resource_id: str):
        try:
            with lab.connect() as conn:
                barrier.wait()
                assignment_id, _ = claim(
                    conn,
                    work_id,
                    resource_id=resource_id,
                    key=f"claim-{resource_id}",
                )
                return ("ok", assignment_id, resource_id)
        except psycopg.errors.CheckViolation:
            return ("rejected", None, resource_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ["worker-a", "worker-b"]))

    assert sorted(result[0] for result in results) == ["ok", "rejected"]

    with lab.connect() as conn:
        active = conn.execute(
            """
            SELECT resource_id
            FROM kernel_lab.warehouse_work_assignments
            WHERE work_id = %s AND state = 'active'
            """,
            (work_id,),
        ).fetchall()
        state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (work_id,)
        ).fetchone()[0]

    assert len(active) == 1
    assert state == "assigned"


def test_only_assigned_resource_can_start_task_and_channel_is_orthogonal():
    with lab.connect() as conn:
        work_id, task_ids = create_work(conn)
        release(conn, work_id)
        claim(conn, work_id, resource_id="worker-1")

    with lab.connect(autocommit=True) as conn:
        try:
            start(conn, task_ids[0], resource_id="worker-2", key="wrong-worker")
            assert False, "unassigned resource must not start task"
        except psycopg.errors.CheckViolation:
            pass

    with lab.connect() as conn:
        execution_id, result = start(
            conn,
            task_ids[0],
            resource_id="worker-1",
            channel="voice",
            key="right-worker",
        )
        channel, task_state, work_state = conn.execute(
            """
            SELECT e.channel, t.state, w.state
            FROM kernel_lab.warehouse_task_executions e
            JOIN kernel_lab.warehouse_tasks t ON t.id = e.task_id
            JOIN kernel_lab.warehouse_works w ON w.id = t.work_id
            WHERE e.id = %s
            """,
            (execution_id,),
        ).fetchone()

    assert result == execution_id
    assert channel == "voice"
    assert task_state == "in_progress"
    assert work_state == "in_progress"


def test_task_sequence_cannot_skip_and_confirmation_advances_next_task():
    with lab.connect() as conn:
        work_id, task_ids = create_work(
            conn,
            tasks=[{"operation": "pick"}, {"operation": "put"}],
        )
        release(conn, work_id)
        claim(conn, work_id)

    with lab.connect(autocommit=True) as conn:
        try:
            start(conn, task_ids[1], key="skip-task")
            assert False, "later task must not start before predecessor"
        except psycopg.errors.CheckViolation:
            pass

    with lab.connect() as conn:
        start(conn, task_ids[0], key="start-first")
        confirm(conn, task_ids[0], key="confirm-first")
        states = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_tasks WHERE work_id = %s ORDER BY sequence",
            (work_id,),
        ).fetchall()
        work_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (work_id,)
        ).fetchone()[0]

    assert [row[0] for row in states] == ["completed", "ready"]
    assert work_state == "in_progress"


def test_final_confirmation_completes_work_and_assignment():
    with lab.connect() as conn:
        work_id, task_ids = create_work(conn)
        release(conn, work_id)
        assignment_id, _ = claim(conn, work_id)
        start(conn, task_ids[0])
        confirm(conn, task_ids[0], actual_quantity=Decimal("4"))
        work_state, assignment_state = conn.execute(
            """
            SELECT w.state, a.state
            FROM kernel_lab.warehouse_works w
            JOIN kernel_lab.warehouse_work_assignments a ON a.work_id = w.id
            WHERE w.id = %s AND a.id = %s
            """,
            (work_id, assignment_id),
        ).fetchone()

    assert work_state == "completed"
    assert assignment_state == "completed"


def test_domain_result_reference_is_required_when_task_contract_demands_it():
    with lab.connect() as conn:
        work_id, task_ids = create_work(
            conn,
            tasks=[
                {
                    "operation": "move-stock",
                    "planned_quantity": 5,
                    "uom": "EA",
                    "requires_domain_confirmation": True,
                }
            ],
        )
        release(conn, work_id)
        claim(conn, work_id)
        start(conn, task_ids[0])

    with lab.connect(autocommit=True) as conn:
        try:
            confirm(conn, task_ids[0], key="missing-domain-result")
            assert False, "domain-affecting task must require result reference"
        except psycopg.errors.CheckViolation:
            pass

    with lab.connect() as conn:
        confirmation_id, _ = confirm(
            conn,
            task_ids[0],
            key="with-domain-result",
            actual_quantity=5,
            domain_result_reference="inventory-transaction:tx-123",
        )
        stored = conn.execute(
            """
            SELECT domain_result_reference
            FROM kernel_lab.warehouse_task_confirmations
            WHERE id = %s
            """,
            (confirmation_id,),
        ).fetchone()[0]

    assert stored == "inventory-transaction:tx-123"


def test_exception_resume_creates_new_execution_attempt():
    with lab.connect() as conn:
        work_id, task_ids = create_work(conn)
        release(conn, work_id)
        claim(conn, work_id)
        first_execution, _ = start(conn, task_ids[0], key="start-attempt-1")
        exception_id, _ = raise_exception(conn, task_ids[0])

        states = conn.execute(
            """
            SELECT w.state, t.state, e.state, x.state
            FROM kernel_lab.warehouse_works w
            JOIN kernel_lab.warehouse_tasks t ON t.work_id = w.id
            JOIN kernel_lab.warehouse_task_executions e ON e.task_id = t.id
            JOIN kernel_lab.warehouse_task_exceptions x ON x.execution_id = e.id
            WHERE w.id = %s AND e.id = %s AND x.id = %s
            """,
            (work_id, first_execution, exception_id),
        ).fetchone()
        assert states == ("exception", "exception", "blocked", "open")

        conn.execute(
            "SELECT kernel_lab.resolve_warehouse_task_exception(%s, %s, %s, 'resume')",
            (lab.TENANT, "resume-exception", exception_id),
        )
        work_state, task_state, old_execution_state, exception_state = conn.execute(
            """
            SELECT w.state, t.state, e.state, x.state
            FROM kernel_lab.warehouse_works w
            JOIN kernel_lab.warehouse_tasks t ON t.work_id = w.id
            JOIN kernel_lab.warehouse_task_executions e ON e.task_id = t.id
            JOIN kernel_lab.warehouse_task_exceptions x ON x.execution_id = e.id
            WHERE w.id = %s AND e.id = %s AND x.id = %s
            """,
            (work_id, first_execution, exception_id),
        ).fetchone()
        assert (work_state, task_state, old_execution_state, exception_state) == (
            "assigned",
            "ready",
            "aborted",
            "resolved",
        )

        second_execution, _ = start(conn, task_ids[0], key="start-attempt-2")
        confirm(conn, task_ids[0], key="confirm-attempt-2")
        attempts = conn.execute(
            """
            SELECT id, state
            FROM kernel_lab.warehouse_task_executions
            WHERE task_id = %s
            ORDER BY started_at, id
            """,
            (task_ids[0],),
        ).fetchall()
        final_work_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (work_id,)
        ).fetchone()[0]

    assert first_execution != second_execution
    assert {state for _, state in attempts} == {"aborted", "completed"}
    assert final_work_state == "completed"


def test_cancel_before_start_releases_assignment_and_remaining_tasks():
    with lab.connect() as conn:
        work_id, _ = create_work(
            conn,
            tasks=[{"operation": "pick"}, {"operation": "put"}],
        )
        release(conn, work_id)
        assignment_id, _ = claim(conn, work_id)
        result = conn.execute(
            "SELECT kernel_lab.cancel_warehouse_work(%s, %s, %s, %s)",
            (lab.TENANT, "cancel-work", work_id, "order cancelled"),
        ).fetchone()[0]
        states = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_tasks WHERE work_id = %s ORDER BY sequence",
            (work_id,),
        ).fetchall()
        work_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (work_id,)
        ).fetchone()[0]
        assignment_state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_work_assignments WHERE id = %s",
            (assignment_id,),
        ).fetchone()[0]

    assert result == work_id
    assert work_state == "cancelled"
    assert assignment_state == "released"
    assert [row[0] for row in states] == ["cancelled", "cancelled"]


def test_cancel_after_execution_started_is_rejected():
    with lab.connect() as conn:
        work_id, task_ids = create_work(conn)
        release(conn, work_id)
        claim(conn, work_id)
        start(conn, task_ids[0])

    with lab.connect(autocommit=True) as conn:
        try:
            conn.execute(
                "SELECT kernel_lab.cancel_warehouse_work(%s, %s, %s, %s)",
                (lab.TENANT, "late-cancel", work_id, "too late"),
            )
            assert False, "started work requires capability-specific compensation"
        except psycopg.errors.CheckViolation:
            pass

    with lab.connect() as conn:
        state = conn.execute(
            "SELECT state FROM kernel_lab.warehouse_works WHERE id = %s", (work_id,)
        ).fetchone()[0]
    assert state == "in_progress"


def test_work_command_idempotency_replays_exact_payload_and_rejects_collision():
    with lab.connect() as conn:
        work_a, _ = create_work(conn, domain_reference="A")
        work_b, _ = create_work(conn, domain_reference="B")
        first = release(conn, work_a, key="release-retry")
        replay = release(conn, work_a, key="release-retry")
        assert first == replay == work_a

    with lab.connect(autocommit=True) as conn:
        try:
            release(conn, work_b, key="release-retry")
            assert False, "same key with different command payload must be rejected"
        except psycopg.errors.UniqueViolation:
            pass
