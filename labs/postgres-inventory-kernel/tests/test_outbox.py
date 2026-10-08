from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import psycopg
import pytest

import conftest as lab


def event_types(conn: psycopg.Connection) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            "SELECT event_type FROM kernel_lab.domain_event_outbox ORDER BY recorded_at, event_id"
        ).fetchall()
    ]


def claim(conn: psycopg.Connection, worker: str, limit: int = 100, lease: int = 30):
    return conn.execute(
        "SELECT * FROM kernel_lab.claim_domain_events(%s, %s, %s)",
        (worker, limit, lease),
    ).fetchall()


def enqueue_test_event(conn: psycopg.Connection, *, key: str, value: int = 1):
    return conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s, %s, 'test.event.created', %s,
          'TestAggregate', %s, 1,
          jsonb_build_object('value', %s)
        )
        """,
        (lab.TENANT, key, f"test/{key}", key, value),
    ).fetchone()[0]


def test_receipt_persists_transaction_and_position_events_atomically():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=0)
        transaction_id = lab.new_id()

        conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 5, 'ASN-OUTBOX-1')",
            (transaction_id, lab.TENANT, "outbox:receipt", position_id),
        )

        rows = conn.execute(
            """
            SELECT event_type, aggregate_type, aggregate_id, aggregate_version, data
            FROM kernel_lab.domain_event_outbox
            ORDER BY event_type
            """
        ).fetchall()

        assert [row[0] for row in rows] == [
            "inventory.position.changed",
            "inventory.transaction.posted",
        ]
        position_event = rows[0]
        assert position_event[1:4] == (
            "InventoryPosition",
            str(position_id),
            1,
        )
        assert Decimal(str(position_event[4]["physical_delta"])) == Decimal("5")
        assert position_event[4]["transaction_id"] == str(transaction_id)



def test_claim_envelope_preserves_v1_and_adds_tenant_only_to_posting_v2():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=0)
        transaction_id = lab.new_id()
        conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 5, 'ASN-EVENT-V2')",
            (transaction_id, lab.TENANT, "outbox:tenant-v2", position_id),
        )
        enqueue_test_event(conn, key="outbox:legacy-v1")
        conn.commit()

        envelopes = {
            row[3]: row[7]
            for row in conn.execute(
                "SELECT * FROM kernel_lab.claim_domain_events(%s, %s, %s)",
                ("test-v2-claim", 10, 30),
            ).fetchall()
        }

    assert set(envelopes) == {
        "inventory.transaction.posted",
        "inventory.position.changed",
        "test.event.created",
    }
    posted = envelopes["inventory.transaction.posted"]
    assert posted["schema_version"] == 2
    assert posted["tenant_id"] == str(lab.TENANT)
    assert posted["aggregate_id"] == str(transaction_id)
    for event_type in ("inventory.position.changed", "test.event.created"):
        v1 = envelopes[event_type]
        assert v1["schema_version"] == 1
        assert "tenant_id" not in v1



def test_failed_posting_rolls_back_transaction_and_outbox():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=1)
        conn.commit()

        failed_transaction_id = lab.new_id()
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 2)",
                (
                    failed_transaction_id,
                    lab.TENANT,
                    "outbox:failed-allocation",
                    position_id,
                ),
            )
        conn.rollback()

        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.inventory_transactions WHERE id = %s",
            (failed_transaction_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_outbox
            WHERE data ->> 'transaction_id' = %s
            """,
            (str(failed_transaction_id),),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_idempotency_keys
            WHERE data ->> 'transaction_id' = %s
            """,
            (str(failed_transaction_id),),
        ).fetchone()[0] == 0


def test_exact_posting_retry_does_not_duplicate_events():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=0)
        first_transaction_id = lab.new_id()
        retry_transaction_id = lab.new_id()

        first = conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 7, 'ASN-OUTBOX-2')",
            (first_transaction_id, lab.TENANT, "outbox:retry", position_id),
        ).fetchone()[0]
        retry = conn.execute(
            "SELECT kernel_lab.post_inventory_receipt(%s, %s, %s, %s, 7, 'ASN-OUTBOX-2')",
            (retry_transaction_id, lab.TENANT, "outbox:retry", position_id),
        ).fetchone()[0]

        assert first == first_transaction_id
        assert retry == first_transaction_id
        assert conn.execute(
            "SELECT physical_qty FROM kernel_lab.inventory_positions WHERE id = %s",
            (position_id,),
        ).fetchone()[0] == Decimal("7")
        assert event_types(conn).count("inventory.transaction.posted") == 1
        assert event_types(conn).count("inventory.position.changed") == 1


def test_event_dedup_key_is_payload_bound_and_returns_stable_event_id():
    with lab.connect() as conn:
        first = enqueue_test_event(conn, key="outbox:dedup", value=1)
        retry = enqueue_test_event(conn, key="outbox:dedup", value=1)
        assert retry == first

        with pytest.raises(psycopg.errors.UniqueViolation):
            enqueue_test_event(conn, key="outbox:dedup", value=2)
        conn.rollback()


def test_movement_emits_one_movement_event_and_two_position_events():
    with lab.connect() as conn:
        source_id = lab.insert_position(conn, location_id=lab.LOCATION_A, physical_qty=10)
        target_id = lab.insert_position(conn, location_id=lab.LOCATION_B, physical_qty=0)
        transaction_id = lab.new_id()

        conn.execute(
            "SELECT kernel_lab.transfer_inventory_quantity(%s, %s, %s, %s, %s, 4)",
            (
                transaction_id,
                lab.TENANT,
                "outbox:movement",
                source_id,
                target_id,
            ),
        )

        types = event_types(conn)
        assert types.count("inventory.transaction.posted") == 1
        assert types.count("inventory.movement.confirmed") == 1
        assert types.count("inventory.position.changed") == 2

        position_events = conn.execute(
            """
            SELECT aggregate_id, aggregate_version, data ->> 'physical_delta'
            FROM kernel_lab.domain_event_outbox
            WHERE event_type = 'inventory.position.changed'
            ORDER BY aggregate_id
            """
        ).fetchall()
        assert {row[0] for row in position_events} == {str(source_id), str(target_id)}
        assert {row[1] for row in position_events} == {1}
        assert {Decimal(row[2]) for row in position_events} == {Decimal("-4"), Decimal("4")}


def test_commitment_events_match_domain_registry_without_lying_for_low_level_allocation():
    with lab.connect() as conn:
        position_id = lab.insert_position(conn, physical_qty=20)
        reservation_id = lab.new_id()
        allocation_id = lab.new_id()

        conn.execute(
            """
            SELECT kernel_lab.create_inventory_reservation(
              %s, %s, %s, %s, 'ORDER-OUTBOX', %s, %s, %s, %s, 8
            )
            """,
            (
                reservation_id,
                lab.new_id(),
                lab.TENANT,
                "outbox:reservation",
                lab.WAREHOUSE,
                lab.ITEM,
                lab.OWNER,
                lab.CONDITION,
            ),
        )
        conn.execute(
            """
            SELECT kernel_lab.allocate_inventory_commitment(
              %s, %s, %s, %s, 'ORDER-OUTBOX', %s, 8, %s
            )
            """,
            (
                allocation_id,
                lab.new_id(),
                lab.TENANT,
                "outbox:allocation",
                position_id,
                reservation_id,
            ),
        )
        conn.execute(
            "SELECT kernel_lab.release_inventory_allocation(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, "outbox:allocation-release", allocation_id),
        )
        conn.execute(
            "SELECT kernel_lab.release_inventory_reservation(%s, %s, %s, %s)",
            (lab.new_id(), lab.TENANT, "outbox:reservation-release", reservation_id),
        )

        types = set(event_types(conn))
        assert {
            "inventory.reservation.created",
            "inventory.reservation.released",
            "inventory.allocation.created",
            "inventory.allocation.released",
        } <= types

        other_position = lab.insert_position(
            conn, location_id=lab.LOCATION_B, physical_qty=3
        )
        conn.execute(
            "SELECT kernel_lab.allocate_inventory_position(%s, %s, %s, %s, 1)",
            (lab.new_id(), lab.TENANT, "outbox:low-level-allocation", other_position),
        )
        low_level_created = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_outbox
            WHERE event_type = 'inventory.allocation.created'
              AND aggregate_id = %s
            """,
            (str(other_position),),
        ).fetchone()[0]
        assert low_level_created == 0


def test_parallel_publishers_claim_disjoint_event_sets():
    with lab.connect() as conn:
        for index in range(10):
            enqueue_test_event(conn, key=f"outbox:seed:{index}", value=index)
        conn.commit()

    def worker(name: str):
        with lab.connect() as conn:
            rows = claim(conn, name, limit=5, lease=30)
            conn.commit()
            return {row[0] for row in rows}

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(worker, ["publisher-a", "publisher-b"]))

    assert len(first) == 5
    assert len(second) == 5
    assert first.isdisjoint(second)


def test_ack_nack_lease_and_consumer_deduplication():
    with lab.connect() as conn:
        event_id = enqueue_test_event(conn, key="outbox:delivery")
        conn.commit()

        first_claim = claim(conn, "publisher-a", limit=1, lease=30)[0]
        first_token = first_claim[1]
        assert first_claim[0] == event_id

        assert conn.execute(
            "SELECT kernel_lab.ack_domain_event(%s, %s)",
            (event_id, lab.new_id()),
        ).fetchone()[0] is False
        assert conn.execute(
            "SELECT kernel_lab.nack_domain_event(%s, %s, 'stale worker', 60)",
            (event_id, lab.new_id()),
        ).fetchone()[0] is False
        assert conn.execute(
            "SELECT kernel_lab.nack_domain_event(%s, %s, 'broker unavailable', 60)",
            (event_id, first_token),
        ).fetchone()[0] is True
        conn.commit()

        assert claim(conn, "publisher-b", limit=100, lease=30) == []

        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET available_at = clock_timestamp() - interval '1 second'
            WHERE event_id = %s
            """,
            (event_id,),
        )
        second_claim = claim(conn, "publisher-b", limit=1, lease=30)[0]
        second_token = second_claim[1]
        assert second_claim[0] == event_id
        assert second_token != first_token

        assert conn.execute(
            "SELECT kernel_lab.ack_domain_event(%s, %s)",
            (event_id, first_token),
        ).fetchone()[0] is False
        assert conn.execute(
            "SELECT kernel_lab.ack_domain_event(%s, %s)",
            (event_id, second_token),
        ).fetchone()[0] is True
        assert conn.execute(
            "SELECT kernel_lab.ack_domain_event(%s, %s)",
            (event_id, second_token),
        ).fetchone()[0] is True

        attempt_count, last_error, published = conn.execute(
            """
            SELECT attempt_count, last_error, published_at IS NOT NULL
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert attempt_count == 2
        assert last_error is None
        assert published is True

        assert conn.execute(
            "SELECT kernel_lab.try_record_domain_event_receipt('availability-projector', %s)",
            (event_id,),
        ).fetchone()[0] is True
        assert conn.execute(
            "SELECT kernel_lab.try_record_domain_event_receipt('availability-projector', %s)",
            (event_id,),
        ).fetchone()[0] is False
        assert conn.execute(
            "SELECT kernel_lab.try_record_domain_event_receipt('audit-projector', %s)",
            (event_id,),
        ).fetchone()[0] is True


def test_quarantine_is_lease_fenced_and_operator_replay_is_audited():
    with lab.connect() as conn:
        event_id = enqueue_test_event(conn, key="outbox:quarantine")
        conn.commit()

        claimed = claim(conn, "publisher-a", limit=1, lease=30)[0]
        claim_token = claimed[1]
        attempt_count = claimed[2]
        assert attempt_count == 1

        assert conn.execute(
            """
            SELECT kernel_lab.quarantine_domain_event(
              %s, %s, 'ValueError: invalid envelope', 'attempt budget exhausted'
            )
            """,
            (event_id, lab.new_id()),
        ).fetchone()[0] is False
        assert conn.execute(
            """
            SELECT kernel_lab.quarantine_domain_event(
              %s, %s, 'ValueError: invalid envelope', 'attempt budget exhausted'
            )
            """,
            (event_id, claim_token),
        ).fetchone()[0] is True
        conn.commit()

        assert claim(conn, "publisher-b", limit=1, lease=30) == []

        with pytest.raises(psycopg.errors.CheckViolation, match="operator id is required"):
            conn.execute(
                "SELECT kernel_lab.replay_quarantined_domain_event(%s, '', 'fixed')",
                (event_id,),
            )
        conn.rollback()

        assert conn.execute(
            """
            SELECT kernel_lab.replay_quarantined_domain_event(
              %s, 'on-call@example.com', 'producer schema fixed'
            )
            """,
            (event_id,),
        ).fetchone()[0] is True
        assert conn.execute(
            """
            SELECT kernel_lab.replay_quarantined_domain_event(
              %s, 'on-call@example.com', 'duplicate request'
            )
            """,
            (event_id,),
        ).fetchone()[0] is False

        state = conn.execute(
            """
            SELECT
              attempt_count,
              last_error,
              last_failed_at,
              quarantined_at,
              quarantine_reason
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert state == (0, None, None, None, None)

        audit = conn.execute(
            """
            SELECT
              action,
              operator_id,
              reason,
              previous_attempt_count,
              previous_last_error,
              previous_quarantine_reason
            FROM kernel_lab.domain_event_outbox_operator_actions
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()
        assert audit == (
            "replay",
            "on-call@example.com",
            "producer schema fixed",
            1,
            "ValueError: invalid envelope",
            "attempt budget exhausted",
        )

        replayed = claim(conn, "publisher-b", limit=1, lease=30)[0]
        assert replayed[0] == event_id
        assert replayed[2] == 1
