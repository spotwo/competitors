from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.types.json import Jsonb

import conftest as lab
from consumer_runtime import MalformedDeliveryEvidence
from malformed_delivery import PostgresNatsMalformedDeliveryQuarantine
from malformed_delivery_retention import (
    MAX_ARCHIVE_BATCH_SIZE,
    MalformedDeliveryArchiveBatch,
    MalformedDeliveryRetentionPolicy,
    PostgresMalformedDeliveryArchiveStore,
)

CONSUMER_NAME = "inventory-position-quantity-projector"
STREAM = "WMS_EVENTS"
DURABLE = "POSITION_PROJECTOR"
SUBJECT = "spotwo.wms.events.inventory.position.changed"


def insert_poison(
    conn: psycopg.Connection,
    *,
    stream_sequence: int,
    last_seen_at: datetime,
    consumer_name: str = CONSUMER_NAME,
    observation_count: int = 1,
    failure_code: str = "invalid_event_envelope",
):
    first_seen_at = last_seen_at - timedelta(hours=1)
    payload = b"bad"
    payload_sha256 = f"{stream_sequence:064x}"
    headers = {"Nats-Msg-Id": f"poison-{stream_sequence}"}
    first_metadata = {
        "transport": "nats-jetstream",
        "stream": STREAM,
        "consumer": DURABLE,
        "stream_sequence": stream_sequence,
        "subject": SUBJECT,
        "delivery_count": 1,
        "marker": "first",
    }
    last_metadata = first_metadata | {
        "delivery_count": observation_count,
        "marker": "last",
    }
    conn.execute(
        """
        INSERT INTO kernel_lab.nats_jetstream_consumer_poison_deliveries (
          consumer_name,
          stream,
          durable_consumer,
          stream_sequence,
          subject,
          failure_code,
          payload_sha256,
          payload_size,
          payload_preview,
          payload_truncated,
          headers,
          first_delivery_metadata,
          last_delivery_metadata,
          last_error,
          observation_count,
          first_seen_at,
          last_seen_at,
          quarantined_at
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, false,
          %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            consumer_name,
            STREAM,
            DURABLE,
            stream_sequence,
            SUBJECT,
            failure_code,
            payload_sha256,
            len(payload),
            payload,
            Jsonb(headers),
            Jsonb(first_metadata),
            Jsonb(last_metadata),
            "invalid envelope for retention test",
            observation_count,
            first_seen_at,
            last_seen_at,
            first_seen_at,
        ),
    )
    return {
        "consumer_name": consumer_name,
        "stream": STREAM,
        "durable_consumer": DURABLE,
        "stream_sequence": stream_sequence,
        "subject": SUBJECT,
        "failure_code": failure_code,
        "payload_sha256": payload_sha256,
        "payload_size": len(payload),
        "payload_preview": payload,
        "payload_truncated": False,
        "headers": headers,
        "first_delivery_metadata": first_metadata,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
    }


def archive_store() -> PostgresMalformedDeliveryArchiveStore:
    return PostgresMalformedDeliveryArchiveStore(lab.DATABASE_URL)


def test_archive_moves_only_inactive_poison_evidence_for_one_consumer():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        old = insert_poison(
            conn,
            stream_sequence=101,
            last_seen_at=cutoff - timedelta(days=1),
            observation_count=3,
        )
        recent = insert_poison(
            conn,
            stream_sequence=102,
            last_seen_at=cutoff + timedelta(minutes=1),
            observation_count=5,
        )
        other = insert_poison(
            conn,
            consumer_name="availability-projector",
            stream_sequence=103,
            last_seen_at=cutoff - timedelta(days=2),
        )
        original = conn.execute(
            """
            SELECT to_jsonb(d)
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries AS d
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (CONSUMER_NAME, old["stream_sequence"]),
        ).fetchone()[0]
        conn.commit()

    result = archive_store().archive_batch(
        consumer_name=CONSUMER_NAME,
        before=cutoff,
        batch_size=100,
    )

    assert result.consumer_name == CONSUMER_NAME
    assert result.archive_run_id is not None
    assert result.archived_delivery_count == 1

    with lab.connect() as conn:
        archived = conn.execute(
            """
            SELECT to_jsonb(a) - 'archived_at' - 'archive_run_id'
            FROM kernel_lab.nats_jetstream_consumer_poison_delivery_archive AS a
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (CONSUMER_NAME, old["stream_sequence"]),
        ).fetchone()[0]
        assert archived == original

        live_sequences = {
            row[0]
            for row in conn.execute(
                """
                SELECT stream_sequence
                FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
                WHERE consumer_name = %s
                """,
                (CONSUMER_NAME,),
            ).fetchall()
        }
        assert live_sequences == {recent["stream_sequence"]}
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (other["consumer_name"], other["stream_sequence"]),
        ).fetchone()[0] == 1

    empty = archive_store().archive_batch(
        consumer_name=CONSUMER_NAME,
        before=cutoff,
        batch_size=100,
    )
    assert empty.archived_delivery_count == 0
    assert empty.archive_run_id is None

    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.nats_jetstream_consumer_poison_delivery_archive
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (CONSUMER_NAME, old["stream_sequence"]),
        ).fetchone()[0] == 1


def test_archived_identity_reactivates_with_cumulative_observation_history():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        original = insert_poison(
            conn,
            stream_sequence=201,
            last_seen_at=cutoff - timedelta(days=1),
            observation_count=2,
        )
        conn.commit()

    archive_store().archive_batch(
        consumer_name=CONSUMER_NAME,
        before=cutoff,
        batch_size=1,
    )

    evidence = MalformedDeliveryEvidence(
        failure_code=original["failure_code"],
        error="same archived poison observed again",
        payload_sha256=original["payload_sha256"],
        payload_size=original["payload_size"],
        payload_preview=original["payload_preview"],
        payload_truncated=False,
        headers=original["headers"],
    )
    current_metadata = {
        "transport": "nats-jetstream",
        "stream": STREAM,
        "consumer": DURABLE,
        "stream_sequence": original["stream_sequence"],
        "subject": SUBJECT,
        "delivery_count": 3,
        "marker": "reactivated",
    }

    disposition = PostgresNatsMalformedDeliveryQuarantine(
        lab.DATABASE_URL
    ).capture(
        consumer_name=CONSUMER_NAME,
        evidence=evidence,
        delivery_metadata=current_metadata,
    )

    assert disposition.created is False
    assert disposition.observation_count == 3
    assert disposition.failure_code == original["failure_code"]

    with lab.connect() as conn:
        live = conn.execute(
            """
            SELECT observation_count, first_seen_at, quarantined_at,
                   first_delivery_metadata, last_delivery_metadata
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (CONSUMER_NAME, original["stream_sequence"]),
        ).fetchone()
        assert live[0] == 3
        assert live[1] == original["first_seen_at"]
        assert live[2] == original["first_seen_at"]
        assert live[3]["marker"] == "first"
        assert live[4]["marker"] == "reactivated"

        archived = conn.execute(
            """
            SELECT observation_count, last_delivery_metadata
            FROM kernel_lab.nats_jetstream_consumer_poison_delivery_archive
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (CONSUMER_NAME, original["stream_sequence"]),
        ).fetchone()
        assert archived[0] == 2
        assert archived[1]["marker"] == "last"


def test_archived_identity_collision_still_fails_closed():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        original = insert_poison(
            conn,
            stream_sequence=301,
            last_seen_at=cutoff - timedelta(days=1),
        )
        conn.commit()

    archive_store().archive_batch(
        consumer_name=CONSUMER_NAME,
        before=cutoff,
        batch_size=1,
    )

    conflicting = MalformedDeliveryEvidence(
        failure_code=original["failure_code"],
        error="conflicting poison payload",
        payload_sha256=f"{999:064x}",
        payload_size=4,
        payload_preview=b"evil",
        payload_truncated=False,
        headers=original["headers"],
    )
    metadata = {
        "transport": "nats-jetstream",
        "stream": STREAM,
        "consumer": DURABLE,
        "stream_sequence": original["stream_sequence"],
        "subject": SUBJECT,
        "delivery_count": 2,
    }

    with pytest.raises(
        psycopg.errors.UniqueViolation,
        match="archived JetStream delivery identity belongs to different poison evidence",
    ):
        PostgresNatsMalformedDeliveryQuarantine(lab.DATABASE_URL).capture(
            consumer_name=CONSUMER_NAME,
            evidence=conflicting,
            delivery_metadata=metadata,
        )

    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (CONSUMER_NAME, original["stream_sequence"]),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.nats_jetstream_consumer_poison_delivery_archive
            WHERE consumer_name = %s AND stream_sequence = %s
            """,
            (CONSUMER_NAME, original["stream_sequence"]),
        ).fetchone()[0] == 1


def test_parallel_archive_batches_are_bounded_and_disjoint():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        for ordinal in range(1, 9):
            insert_poison(
                conn,
                stream_sequence=400 + ordinal,
                last_seen_at=cutoff - timedelta(days=ordinal),
            )
        conn.commit()

    def archive_three(_worker: int) -> MalformedDeliveryArchiveBatch:
        return archive_store().archive_batch(
            consumer_name=CONSUMER_NAME,
            before=cutoff,
            batch_size=3,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(archive_three, range(2)))

    assert first.archived_delivery_count == 3
    assert second.archived_delivery_count == 3
    assert first.archive_run_id != second.archive_run_id

    final_batch = archive_three(3)
    empty_batch = archive_three(4)
    assert final_batch.archived_delivery_count == 2
    assert final_batch.archive_run_id is not None
    assert empty_batch.archived_delivery_count == 0
    assert empty_batch.archive_run_id is None

    with lab.connect() as conn:
        run_counts = dict(
            conn.execute(
                """
                SELECT archive_run_id, count(*)
                FROM kernel_lab.nats_jetstream_consumer_poison_delivery_archive
                GROUP BY archive_run_id
                """
            ).fetchall()
        )
        assert run_counts == {
            first.archive_run_id: 3,
            second.archive_run_id: 3,
            final_batch.archive_run_id: 2,
        }
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = %s
            """,
            (CONSUMER_NAME,),
        ).fetchone()[0] == 0


def test_retention_policy_and_database_inputs_are_bounded():
    observed_at = datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc)
    policy = MalformedDeliveryRetentionPolicy(retention_days=30, batch_size=500)
    assert policy.cutoff(observed_at=observed_at) == observed_at - timedelta(days=30)

    with pytest.raises(ValueError, match="retention_days"):
        MalformedDeliveryRetentionPolicy(retention_days=0)
    with pytest.raises(ValueError, match="retention_days"):
        MalformedDeliveryRetentionPolicy(retention_days=36_501)
    with pytest.raises(ValueError, match="batch_size"):
        MalformedDeliveryRetentionPolicy(batch_size=0)
    with pytest.raises(ValueError, match="batch_size"):
        MalformedDeliveryRetentionPolicy(batch_size=MAX_ARCHIVE_BATCH_SIZE + 1)
    with pytest.raises(ValueError, match="timezone"):
        policy.cutoff(observed_at=datetime(2026, 8, 19, 0, 0))
    with pytest.raises(ValueError, match="consumer_name"):
        archive_store().archive_batch(
            consumer_name=" ",
            before=observed_at,
        )
    with pytest.raises(ValueError, match="timezone"):
        archive_store().archive_batch(
            consumer_name=CONSUMER_NAME,
            before=datetime(2026, 8, 19, 0, 0),
        )
    with pytest.raises(ValueError, match="batch_size"):
        archive_store().archive_batch(
            consumer_name=CONSUMER_NAME,
            before=observed_at,
            batch_size=MAX_ARCHIVE_BATCH_SIZE + 1,
        )

    with pytest.raises(
        psycopg.errors.CheckViolation,
        match="archive cutoff cannot be in the future",
    ):
        archive_store().archive_batch(
            consumer_name=CONSUMER_NAME,
            before=datetime.now(timezone.utc) + timedelta(days=1),
            batch_size=1,
        )
