from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.types.json import Jsonb

import conftest as lab
from consumer_failure import PostgresConsumerFailureStore
from consumer_failure_retention import (
    MAX_ARCHIVE_BATCH_SIZE,
    ConsumerFailureArchiveBatch,
    ConsumerFailureRetentionPolicy,
    PostgresConsumerFailureArchiveStore,
)

CONSUMER_NAME = "inventory-position-quantity-projector"


def insert_failure(
    conn: psycopg.Connection,
    *,
    status: str,
    state_at: datetime,
    consumer_name: str = CONSUMER_NAME,
    with_inbox: bool = False,
    action_count: int = 0,
):
    event_id = lab.new_id()
    resolved = status == "resolved"
    quarantined = status == "quarantined"
    retryable = not quarantined
    envelope = {
        "event_id": str(event_id),
        "type": "inventory.position.changed",
        "data": {"retention_test": True},
    }
    conn.execute(
        """
        INSERT INTO kernel_lab.domain_event_consumer_failures (
          consumer_name,
          event_id,
          envelope,
          delivery_metadata,
          status,
          failure_code,
          retryable,
          attempt_count,
          available_at,
          last_error,
          first_failed_at,
          last_failed_at,
          quarantined_at,
          quarantine_reason,
          resolved_at,
          resolution
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, 2, %s,
          'handler failed before repair',
          %s - interval '1 day',
          %s - interval '1 hour',
          %s, %s, %s, %s
        )
        """,
        (
            consumer_name,
            event_id,
            Jsonb(envelope),
            Jsonb({"transport": "test", "delivery_count": 1}),
            status,
            "invalid_event_contract" if quarantined else "handler_failure",
            retryable,
            state_at,
            state_at,
            state_at,
            state_at if quarantined else None,
            "terminal:invalid_event_contract" if quarantined else None,
            state_at if resolved else None,
            "applied" if resolved else None,
        ),
    )

    if with_inbox:
        conn.execute(
            """
            INSERT INTO kernel_lab.domain_event_inbox (
              consumer_name, event_id, first_seen_at, metadata
            ) VALUES (%s, %s, %s, %s)
            """,
            (
                consumer_name,
                event_id,
                state_at - timedelta(seconds=1),
                Jsonb({"effect": "committed"}),
            ),
        )

    replay_ids = []
    for ordinal in range(1, action_count + 1):
        replay_id = lab.new_id()
        replay_ids.append(replay_id)
        conn.execute(
            """
            INSERT INTO kernel_lab.domain_event_consumer_failure_actions (
              replay_id,
              consumer_name,
              event_id,
              action,
              operator_id,
              reason,
              previous_attempt_count,
              previous_failure_code,
              previous_retryable,
              previous_last_error,
              previous_quarantine_reason,
              acted_at
            ) VALUES (
              %s, %s, %s, 'replay', %s, %s, 3,
              'handler_failure', true, 'database unavailable',
              'attempt_limit_exhausted:handler_failure', %s
            )
            """,
            (
                replay_id,
                consumer_name,
                event_id,
                f"operator-{ordinal}@example.com",
                f"handler repaired {ordinal}",
                state_at - timedelta(hours=ordinal),
            ),
        )

    return event_id, replay_ids


def archive_store() -> PostgresConsumerFailureArchiveStore:
    return PostgresConsumerFailureArchiveStore(lab.DATABASE_URL)


def test_archive_moves_only_old_resolved_failures_with_inbox_evidence():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        old_id, replay_ids = insert_failure(
            conn,
            status="resolved",
            state_at=cutoff - timedelta(days=1),
            with_inbox=True,
            action_count=2,
        )
        recent_id, _ = insert_failure(
            conn,
            status="resolved",
            state_at=cutoff + timedelta(days=1),
            with_inbox=True,
        )
        deferred_id, _ = insert_failure(
            conn,
            status="deferred",
            state_at=cutoff - timedelta(days=2),
        )
        quarantined_id, _ = insert_failure(
            conn,
            status="quarantined",
            state_at=cutoff - timedelta(days=3),
        )
        orphan_id, _ = insert_failure(
            conn,
            status="resolved",
            state_at=cutoff - timedelta(days=4),
            with_inbox=False,
        )
        other_consumer_id, _ = insert_failure(
            conn,
            consumer_name="availability-projector",
            status="resolved",
            state_at=cutoff - timedelta(days=5),
            with_inbox=True,
        )
        original_failure = conn.execute(
            """
            SELECT to_jsonb(f)
            FROM kernel_lab.domain_event_consumer_failures AS f
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, old_id),
        ).fetchone()[0]
        original_actions = dict(
            conn.execute(
                """
                SELECT replay_id, to_jsonb(a)
                FROM kernel_lab.domain_event_consumer_failure_actions AS a
                WHERE consumer_name = %s AND event_id = %s
                """,
                (CONSUMER_NAME, old_id),
            ).fetchall()
        )
        conn.commit()

    result = archive_store().archive_batch(
        consumer_name=CONSUMER_NAME,
        before=cutoff,
        batch_size=100,
    )

    assert result.consumer_name == CONSUMER_NAME
    assert result.archive_run_id is not None
    assert result.archived_failure_count == 1
    assert result.archived_action_count == 2

    with lab.connect() as conn:
        archived_failure = conn.execute(
            """
            SELECT to_jsonb(a) - 'archived_at' - 'archive_run_id'
            FROM kernel_lab.domain_event_consumer_failure_archive AS a
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, old_id),
        ).fetchone()[0]
        assert archived_failure == original_failure

        archived_actions = dict(
            conn.execute(
                """
                SELECT
                  replay_id,
                  to_jsonb(a) - 'archived_at' - 'archive_run_id'
                FROM kernel_lab.domain_event_consumer_failure_action_archive AS a
                WHERE consumer_name = %s AND event_id = %s
                """,
                (CONSUMER_NAME, old_id),
            ).fetchall()
        )
        assert archived_actions == original_actions
        assert set(archived_actions) == set(replay_ids)

        live_ids = {
            row[0]
            for row in conn.execute(
                """
                SELECT event_id
                FROM kernel_lab.domain_event_consumer_failures
                WHERE consumer_name = %s
                """,
                (CONSUMER_NAME,),
            ).fetchall()
        }
        assert live_ids == {
            recent_id,
            deferred_id,
            quarantined_id,
            orphan_id,
        }
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_consumer_failures
            WHERE consumer_name = 'availability-projector'
              AND event_id = %s
            """,
            (other_consumer_id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            """
            SELECT metadata
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, old_id),
        ).fetchone()[0] == {"effect": "committed"}

        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            conn.execute(
                """
                DELETE FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s AND event_id = %s
                """,
                (CONSUMER_NAME, old_id),
            )
        conn.rollback()


def test_archived_replay_identity_remains_payload_bound():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        event_id, replay_ids = insert_failure(
            conn,
            status="resolved",
            state_at=cutoff - timedelta(days=1),
            with_inbox=True,
            action_count=1,
        )
        conn.commit()

    archive_store().archive_batch(
        consumer_name=CONSUMER_NAME,
        before=cutoff,
        batch_size=1,
    )
    failure_store = PostgresConsumerFailureStore(lab.DATABASE_URL)

    duplicate = failure_store.replay(
        replay_id=replay_ids[0],
        consumer_name=CONSUMER_NAME,
        event_id=event_id,
        operator_id="operator-1@example.com",
        reason="handler repaired 1",
    )
    assert duplicate.outcome == "duplicate"

    with pytest.raises(
        psycopg.errors.UniqueViolation,
        match="replay identity belongs to another request",
    ):
        failure_store.replay(
            replay_id=replay_ids[0],
            consumer_name=CONSUMER_NAME,
            event_id=event_id,
            operator_id="operator-1@example.com",
            reason="changed replay request",
        )

    not_quarantined = failure_store.replay(
        replay_id=lab.new_id(),
        consumer_name=CONSUMER_NAME,
        event_id=event_id,
        operator_id="operator-1@example.com",
        reason="new request against resolved history",
    )
    assert not_quarantined.outcome == "not_quarantined"


def test_parallel_archive_batches_are_bounded_disjoint_and_keep_inbox():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        for ordinal in range(1, 9):
            insert_failure(
                conn,
                status="resolved",
                state_at=cutoff - timedelta(days=ordinal),
                with_inbox=True,
            )
        conn.commit()

    def archive_three(_worker: int) -> ConsumerFailureArchiveBatch:
        return archive_store().archive_batch(
            consumer_name=CONSUMER_NAME,
            before=cutoff,
            batch_size=3,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(archive_three, range(2)))

    assert first.archived_failure_count == 3
    assert second.archived_failure_count == 3
    assert first.archive_run_id != second.archive_run_id

    final_batch = archive_three(3)
    empty_batch = archive_three(4)
    assert final_batch.archived_failure_count == 2
    assert final_batch.archive_run_id is not None
    assert empty_batch.archived_failure_count == 0
    assert empty_batch.archive_run_id is None

    with lab.connect() as conn:
        run_counts = dict(
            conn.execute(
                """
                SELECT archive_run_id, count(*)
                FROM kernel_lab.domain_event_consumer_failure_archive
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
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s
            """,
            (CONSUMER_NAME,),
        ).fetchone()[0] == 8


def test_action_archive_conflict_rolls_back_parent_copy_and_live_deletes():
    with lab.connect() as conn:
        observed_at = conn.execute("SELECT statement_timestamp()").fetchone()[0]
        cutoff = observed_at - timedelta(days=30)
        event_id, replay_ids = insert_failure(
            conn,
            status="resolved",
            state_at=cutoff - timedelta(days=1),
            with_inbox=True,
            action_count=1,
        )
        collision_event_id, _ = insert_failure(
            conn,
            consumer_name="archive-collision-owner",
            status="resolved",
            state_at=cutoff - timedelta(days=2),
            with_inbox=True,
        )
        collision_run_id = conn.execute(
            """
            INSERT INTO kernel_lab.domain_event_consumer_failure_archive
            SELECT f.*, clock_timestamp(), uuidv7()
            FROM kernel_lab.domain_event_consumer_failures AS f
            WHERE f.consumer_name = %s AND f.event_id = %s
            RETURNING archive_run_id
            """,
            ("archive-collision-owner", collision_event_id),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO kernel_lab.domain_event_consumer_failure_action_archive (
              replay_id,
              consumer_name,
              event_id,
              action,
              operator_id,
              reason,
              previous_attempt_count,
              previous_failure_code,
              previous_retryable,
              previous_last_error,
              previous_quarantine_reason,
              acted_at,
              archived_at,
              archive_run_id
            ) VALUES (
              %s, %s, %s, 'replay', 'collision@example.com',
              'inject archive identity collision', 1, 'handler_failure', true,
              'prior failure', 'attempt_limit_exhausted:handler_failure',
              clock_timestamp(), clock_timestamp(), %s
            )
            """,
            (
                replay_ids[0],
                "archive-collision-owner",
                collision_event_id,
                collision_run_id,
            ),
        )
        conn.commit()

    with pytest.raises(
        psycopg.errors.UniqueViolation,
        match="replay identity exists in live and archive",
    ):
        PostgresConsumerFailureStore(lab.DATABASE_URL).replay(
            replay_id=replay_ids[0],
            consumer_name=CONSUMER_NAME,
            event_id=event_id,
            operator_id="operator-1@example.com",
            reason="handler repaired 1",
        )

    with pytest.raises(psycopg.errors.UniqueViolation):
        archive_store().archive_batch(
            consumer_name=CONSUMER_NAME,
            before=cutoff,
            batch_size=1,
        )

    with lab.connect() as conn:
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_consumer_failure_archive
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event_id),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_consumer_failures
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event_id),
        ).fetchone()[0] == 1
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_consumer_failure_actions
            WHERE replay_id = %s
            """,
            (replay_ids[0],),
        ).fetchone()[0] == 1
        assert conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = %s AND event_id = %s
            """,
            (CONSUMER_NAME, event_id),
        ).fetchone()[0] == 1


def test_retention_policy_and_database_inputs_are_bounded():
    observed_at = datetime(2026, 8, 18, 18, 0, tzinfo=timezone.utc)
    policy = ConsumerFailureRetentionPolicy(retention_days=30, batch_size=500)
    assert policy.cutoff(observed_at=observed_at) == observed_at - timedelta(days=30)

    with pytest.raises(ValueError, match="retention_days"):
        ConsumerFailureRetentionPolicy(retention_days=0)
    with pytest.raises(ValueError, match="batch_size"):
        ConsumerFailureRetentionPolicy(batch_size=MAX_ARCHIVE_BATCH_SIZE + 1)
    with pytest.raises(ValueError, match="timezone"):
        policy.cutoff(observed_at=datetime(2026, 8, 18, 18, 0))
    with pytest.raises(ValueError, match="archive_run_id"):
        ConsumerFailureArchiveBatch(
            consumer_name=CONSUMER_NAME,
            cutoff=observed_at,
            archived_at=observed_at,
            archive_run_id=lab.new_id(),
            archived_failure_count=0,
            archived_action_count=0,
        )

    store = archive_store()
    with pytest.raises(ValueError, match="consumer_name"):
        store.archive_batch(
            consumer_name=" ",
            before=observed_at,
        )
    with pytest.raises(ValueError, match="timezone"):
        store.archive_batch(
            consumer_name=CONSUMER_NAME,
            before=datetime(2026, 8, 18, 18, 0),
        )
    with pytest.raises(ValueError, match="batch_size"):
        store.archive_batch(
            consumer_name=CONSUMER_NAME,
            before=observed_at,
            batch_size=0,
        )

    with lab.connect() as conn:
        with pytest.raises(psycopg.errors.CheckViolation, match="consumer name"):
            conn.execute(
                """
                SELECT *
                FROM kernel_lab.archive_resolved_domain_event_consumer_failures(
                  ' ',
                  statement_timestamp() - interval '30 days',
                  1
                )
                """
            )
        conn.rollback()

        with pytest.raises(psycopg.errors.CheckViolation, match="cutoff"):
            conn.execute(
                """
                SELECT *
                FROM kernel_lab.archive_resolved_domain_event_consumer_failures(
                  %s,
                  NULL,
                  1
                )
                """,
                (CONSUMER_NAME,),
            )
        conn.rollback()

        with pytest.raises(psycopg.errors.CheckViolation, match="future"):
            conn.execute(
                """
                SELECT *
                FROM kernel_lab.archive_resolved_domain_event_consumer_failures(
                  %s,
                  statement_timestamp() + interval '1 day',
                  1
                )
                """,
                (CONSUMER_NAME,),
            )
        conn.rollback()

        with pytest.raises(psycopg.errors.CheckViolation, match="between 1 and 1000"):
            conn.execute(
                """
                SELECT *
                FROM kernel_lab.archive_resolved_domain_event_consumer_failures(
                  %s,
                  statement_timestamp() - interval '30 days',
                  1001
                )
                """,
                (CONSUMER_NAME,),
            )
        conn.rollback()
