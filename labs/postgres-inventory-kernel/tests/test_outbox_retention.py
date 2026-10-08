from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

import conftest as lab
from outbox_retention import (
    MAX_ARCHIVE_BATCH_SIZE,
    OutboxArchiveBatch,
    OutboxRetentionPolicy,
    PostgresOutboxArchiveStore,
)


def enqueue_event(
    conn: psycopg.Connection,
    *,
    key: str,
    ordinal: int,
):
    return conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s,
          %s,
          'inventory.retention.test',
          %s,
          'InventoryPosition',
          %s,
          %s,
          jsonb_build_object('ordinal', %s)
        )
        """,
        (
            lab.TENANT,
            key,
            f"inventory-position/retention-{ordinal}",
            f"retention-{ordinal}",
            ordinal,
            ordinal,
        ),
    ).fetchone()[0]


def mark_published(
    conn: psycopg.Connection,
    event_id,
    *,
    published_at: datetime,
) -> None:
    conn.execute(
        """
        UPDATE kernel_lab.domain_event_outbox
        SET published_at = %s,
            claimed_by = NULL,
            claim_token = NULL,
            claimed_until = NULL,
            quarantined_at = NULL,
            quarantine_reason = NULL
        WHERE event_id = %s
        """,
        (published_at, event_id),
    )


def test_archive_moves_only_old_published_events_and_preserves_identity():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)

        old_id = enqueue_event(conn, key="retention:old", ordinal=1)
        recent_id = enqueue_event(conn, key="retention:recent", ordinal=2)
        ready_id = enqueue_event(conn, key="retention:ready", ordinal=3)
        delayed_id = enqueue_event(conn, key="retention:delayed", ordinal=4)
        leased_id = enqueue_event(conn, key="retention:leased", ordinal=5)
        quarantined_id = enqueue_event(conn, key="retention:quarantined", ordinal=6)

        mark_published(
            conn,
            old_id,
            published_at=cutoff - timedelta(days=1),
        )
        mark_published(
            conn,
            recent_id,
            published_at=cutoff + timedelta(days=1),
        )
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET available_at = %s + interval '1 hour'
            WHERE event_id = %s
            """,
            (observed_at, delayed_id),
        )
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET claimed_by = 'publisher-a',
                claim_token = %s,
                claimed_until = %s + interval '1 hour',
                attempt_count = 1
            WHERE event_id = %s
            """,
            (lab.new_id(), observed_at, leased_id),
        )
        conn.execute(
            """
            UPDATE kernel_lab.domain_event_outbox
            SET attempt_count = 5,
                last_error = 'schema rejected',
                last_failed_at = %s,
                quarantined_at = %s,
                quarantine_reason = 'attempt budget exhausted'
            WHERE event_id = %s
            """,
            (observed_at, observed_at, quarantined_id),
        )
        original = conn.execute(
            """
            SELECT to_jsonb(o)
            FROM kernel_lab.domain_event_outbox o
            WHERE event_id = %s
            """,
            (old_id,),
        ).fetchone()[0]
        conn.commit()

    result = PostgresOutboxArchiveStore(lab.DATABASE_URL).archive_batch(
        before=cutoff,
        batch_size=100,
    )

    assert result.archive_run_id is not None
    assert result.archived_event_count == 1
    assert result.archived_operator_action_count == 0

    with lab.connect() as conn:
        archived = conn.execute(
            """
            SELECT to_jsonb(a) - 'archived_at' - 'archive_run_id'
            FROM kernel_lab.domain_event_outbox_archive a
            WHERE event_id = %s
            """,
            (old_id,),
        ).fetchone()[0]
        assert archived == original

        live_ids = {
            row[0]
            for row in conn.execute(
                "SELECT event_id FROM kernel_lab.domain_event_outbox"
            ).fetchall()
        }
        assert live_ids == {
            recent_id,
            ready_id,
            delayed_id,
            leased_id,
            quarantined_id,
        }
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.domain_event_idempotency_keys"
        ).fetchone()[0] == 6

        retry_id = enqueue_event(conn, key="retention:old", ordinal=1)
        assert retry_id == old_id
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (old_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT kernel_lab.ack_domain_event(%s, %s)",
            (old_id, lab.new_id()),
        ).fetchone()[0] is True

        with pytest.raises(psycopg.errors.UniqueViolation):
            enqueue_event(conn, key="retention:old", ordinal=7)
        conn.rollback()


def test_archive_preserves_operator_replay_audit_with_the_event():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        event_id = enqueue_event(conn, key="retention:audit", ordinal=1)
        action_id = conn.execute(
            """
            INSERT INTO kernel_lab.domain_event_outbox_operator_actions (
              event_id,
              action,
              operator_id,
              reason,
              previous_attempt_count,
              previous_last_error,
              previous_quarantine_reason,
              acted_at
            ) VALUES (
              %s,
              'replay',
              'on-call@example.com',
              'producer schema repaired',
              5,
              'schema rejected',
              'attempt budget exhausted',
              %s - interval '35 days'
            )
            RETURNING action_id
            """,
            (event_id, observed_at),
        ).fetchone()[0]
        mark_published(
            conn,
            event_id,
            published_at=cutoff - timedelta(days=1),
        )
        conn.commit()

    result = PostgresOutboxArchiveStore(lab.DATABASE_URL).archive_batch(
        before=cutoff,
        batch_size=1,
    )

    assert result.archived_event_count == 1
    assert result.archived_operator_action_count == 1

    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_outbox_operator_actions
            WHERE action_id = %s
            """,
            (action_id,),
        ).fetchone()[0] == 0
        archived_action = conn.execute(
            """
            SELECT
              event_id,
              operator_id,
              reason,
              previous_attempt_count,
              previous_last_error,
              previous_quarantine_reason,
              archive_run_id
            FROM kernel_lab.domain_event_outbox_operator_action_archive
            WHERE action_id = %s
            """,
            (action_id,),
        ).fetchone()
        assert archived_action == (
            event_id,
            "on-call@example.com",
            "producer schema repaired",
            5,
            "schema rejected",
            "attempt budget exhausted",
            result.archive_run_id,
        )


def test_parallel_archive_batches_are_bounded_and_disjoint():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        for ordinal in range(1, 9):
            event_id = enqueue_event(
                conn,
                key=f"retention:batch:{ordinal}",
                ordinal=ordinal,
            )
            mark_published(
                conn,
                event_id,
                published_at=cutoff - timedelta(days=ordinal),
            )
        conn.commit()

    def archive_three(_worker: int) -> OutboxArchiveBatch:
        return PostgresOutboxArchiveStore(lab.DATABASE_URL).archive_batch(
            before=cutoff,
            batch_size=3,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(archive_three, range(2)))

    assert first.archived_event_count == 3
    assert second.archived_event_count == 3
    assert first.archive_run_id != second.archive_run_id

    with lab.connect() as conn:
        run_counts = dict(
            conn.execute(
                """
                SELECT archive_run_id, count(*)
                FROM kernel_lab.domain_event_outbox_archive
                GROUP BY archive_run_id
                """
            ).fetchall()
        )
        assert run_counts == {
            first.archive_run_id: 3,
            second.archive_run_id: 3,
        }
        assert conn.execute(
            "SELECT count(*) FROM kernel_lab.domain_event_outbox"
        ).fetchone()[0] == 2

    final_batch = archive_three(3)
    empty_batch = archive_three(4)
    assert final_batch.archived_event_count == 2
    assert final_batch.archive_run_id is not None
    assert empty_batch.archived_event_count == 0
    assert empty_batch.archive_run_id is None


def test_archive_copy_failure_rolls_back_live_row_deletion():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        event_id = enqueue_event(conn, key="retention:collision", ordinal=1)
        mark_published(
            conn,
            event_id,
            published_at=cutoff - timedelta(days=1),
        )
        conn.execute(
            """
            INSERT INTO kernel_lab.domain_event_outbox_archive
            SELECT (jsonb_populate_record(
              NULL::kernel_lab.domain_event_outbox_archive,
              to_jsonb(o) || jsonb_build_object(
                'archived_at', clock_timestamp(),
                'archive_run_id', uuidv7()
              )
            )).*
            FROM kernel_lab.domain_event_outbox o
            WHERE o.event_id = %s
            """,
            (event_id,),
        )
        conn.commit()

    with pytest.raises(psycopg.errors.UniqueViolation):
        PostgresOutboxArchiveStore(lab.DATABASE_URL).archive_batch(
            before=cutoff,
            batch_size=1,
        )

    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_outbox
            WHERE event_id = %s
            """,
            (event_id,),
        ).fetchone()[0] == 1


def test_retention_policy_and_archive_inputs_are_bounded():
    observed_at = datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc)
    policy = OutboxRetentionPolicy(retention_days=30, batch_size=500)
    assert policy.cutoff(observed_at=observed_at) == observed_at - timedelta(days=30)

    with pytest.raises(ValueError, match="retention_days"):
        OutboxRetentionPolicy(retention_days=0)
    with pytest.raises(ValueError, match="batch_size"):
        OutboxRetentionPolicy(batch_size=MAX_ARCHIVE_BATCH_SIZE + 1)
    with pytest.raises(ValueError, match="timezone"):
        policy.cutoff(observed_at=datetime(2026, 8, 18, 18, 0))
    with pytest.raises(ValueError, match="archive_run_id"):
        OutboxArchiveBatch(
            cutoff=observed_at,
            archived_at=observed_at,
            archive_run_id=lab.new_id(),
            archived_event_count=0,
            archived_operator_action_count=0,
        )

    store = PostgresOutboxArchiveStore(lab.DATABASE_URL)
    with pytest.raises(ValueError, match="timezone"):
        store.archive_batch(before=datetime(2026, 8, 18, 18, 0))
    with pytest.raises(ValueError, match="batch_size"):
        store.archive_batch(before=observed_at, batch_size=0)


def test_database_rejects_future_cutoff_and_unbounded_batch():
    with lab.connect() as conn:
        with pytest.raises(psycopg.errors.CheckViolation, match="future"):
            conn.execute(
                """
                SELECT *
                FROM kernel_lab.archive_published_domain_events(
                  statement_timestamp() + interval '1 day',
                  1
                )
                """
            )
        conn.rollback()

        with pytest.raises(psycopg.errors.CheckViolation, match="between 1 and 1000"):
            conn.execute(
                """
                SELECT *
                FROM kernel_lab.archive_published_domain_events(
                  statement_timestamp() - interval '30 days',
                  1001
                )
                """
            )
        conn.rollback()
