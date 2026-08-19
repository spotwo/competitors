from __future__ import annotations

import psycopg
import pytest

import conftest as lab
import test_work as work
from consumer_runtime import ConsumedEvent, PostgresInboxStore
from work_state_projection import WarehouseWorkStateProjector


CONSUMER_NAME = "warehouse_work_state_projection"


def load_work_events(work_id) -> list[ConsumedEvent]:
    with lab.connect() as conn:
        rows = conn.execute(
            """
            SELECT
              event_id,
              event_type,
              source,
              subject,
              occurred_at,
              recorded_at,
              aggregate_type,
              aggregate_id,
              aggregate_version,
              schema_version,
              data
            FROM kernel_lab.domain_event_outbox
            WHERE aggregate_type = 'WarehouseWork'
              AND aggregate_id = %s
            ORDER BY aggregate_version
            """,
            (str(work_id),),
        ).fetchall()

    return [
        ConsumedEvent.from_envelope(
            {
                "event_id": str(row[0]),
                "type": row[1],
                "source": row[2],
                "subject": row[3],
                "occurred_at": row[4].isoformat(),
                "recorded_at": row[5].isoformat(),
                "aggregate_type": row[6],
                "aggregate_id": row[7],
                "aggregate_version": row[8],
                "schema_version": row[9],
                "data": row[10],
            }
        )
        for row in rows
    ]


def build_two_task_work():
    with lab.connect() as conn:
        work_id, task_ids = work.create_work(
            conn,
            capability="picking",
            domain_reference="ORDER-WORK-EVENTS",
            tasks=[{"operation": "pick"}, {"operation": "put"}],
        )
        work.release(conn, work_id, key="work-events:release")
        work.claim(conn, work_id, key="work-events:claim")
        work.start(conn, task_ids[0], key="work-events:start-1")
        work.confirm(conn, task_ids[0], key="work-events:confirm-1")
        work.start(conn, task_ids[1], key="work-events:start-2")
        work.confirm(conn, task_ids[1], key="work-events:confirm-2")
    return work_id


def process_event(event: ConsumedEvent) -> bool:
    return PostgresInboxStore(lab.DATABASE_URL).process_once(
        consumer_name=CONSUMER_NAME,
        event=event,
        delivery_metadata={
            "transport": "test",
            "transport_message_id": str(event.event_id),
            "subject": f"spotwo.wms.events.{event.event_type}",
            "delivery_count": 1,
        },
        handler=WarehouseWorkStateProjector(consumer_name=CONSUMER_NAME),
    )


def test_work_state_transitions_emit_contiguous_domain_events():
    work_id = build_two_task_work()
    events = load_work_events(work_id)

    assert [event.event_type for event in events] == [
        "warehouse.work.state.changed",
    ] * 4
    assert [event.aggregate_version for event in events] == [1, 2, 3, 4]
    assert [
        (event.data["previous_state"], event.data["state"]) for event in events
    ] == [
        ("planned", "released"),
        ("released", "assigned"),
        ("assigned", "in_progress"),
        ("in_progress", "completed"),
    ]
    assert [event.data["work_version"] for event in events] == [1, 2, 3, 5]

    with lab.connect() as conn:
        internal_version, event_version = conn.execute(
            """
            SELECT version, domain_event_version
            FROM kernel_lab.warehouse_works
            WHERE id = %s
            """,
            (work_id,),
        ).fetchone()

    assert internal_version == 5
    assert event_version == 4


def test_work_projector_reuses_inbox_transaction_and_reaches_final_state():
    work_id = build_two_task_work()
    events = load_work_events(work_id)

    assert all(process_event(event) for event in events)
    assert process_event(events[-1]) is False

    with lab.connect() as conn:
        projection = conn.execute(
            """
            SELECT
              tenant_id,
              warehouse_id,
              capability,
              domain_reference,
              state,
              aggregate_version,
              work_version,
              last_event_id
            FROM kernel_lab.warehouse_work_state_projection
            WHERE consumer_name = %s AND work_id = %s
            """,
            (CONSUMER_NAME, work_id),
        ).fetchone()
        receipt_count = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
            """,
            (CONSUMER_NAME,),
        ).fetchone()[0]

    assert projection == (
        lab.TENANT,
        lab.WAREHOUSE,
        "picking",
        "ORDER-WORK-EVENTS",
        "completed",
        4,
        5,
        events[-1].event_id,
    )
    assert receipt_count == 4


def test_projection_gap_rolls_back_inbox_receipt_before_retry():
    work_id = build_two_task_work()
    events = load_work_events(work_id)

    assert process_event(events[0]) is True
    with pytest.raises(
        psycopg.errors.CheckViolation,
        match="aggregate version gap",
    ):
        process_event(events[2])

    with lab.connect() as conn:
        gap_receipt = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, events[2].event_id),
        ).fetchone()[0]
    assert gap_receipt == 0

    assert process_event(events[1]) is True
    assert process_event(events[2]) is True

    with lab.connect() as conn:
        version = conn.execute(
            """
            SELECT aggregate_version
            FROM kernel_lab.warehouse_work_state_projection
            WHERE consumer_name = %s AND work_id = %s
            """,
            (CONSUMER_NAME, work_id),
        ).fetchone()[0]
    assert version == 3


def test_direct_state_change_without_internal_version_advance_is_rejected():
    with lab.connect() as conn:
        work_id, _ = work.create_work(conn)

    with lab.connect(autocommit=True) as conn:
        with pytest.raises(
            psycopg.errors.CheckViolation,
            match="advance internal version exactly once",
        ):
            conn.execute(
                """
                UPDATE kernel_lab.warehouse_works
                SET state = 'released'
                WHERE id = %s
                """,
                (work_id,),
            )
