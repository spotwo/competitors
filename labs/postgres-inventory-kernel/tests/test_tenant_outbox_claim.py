"""PostgreSQL tenant delivery ownership: disjoint claims, lease fencing and retries."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

import conftest as lab
from publisher_runtime import PostgresOutboxStore, PostgresTenantOutboxStore


OTHER_TENANT = UUID("00000000-0000-0000-0000-000000000002")


def enqueue_posted(conn, tenant_id, *, suffix: str, version: int = 2):
    aggregate_id = uuid4()
    event_id = conn.execute(
        """
        SELECT kernel_lab.enqueue_domain_event(
          %s, %s, 'inventory.transaction.posted', %s,
          'InventoryTransaction', %s, 1,
          jsonb_build_object(
            'transaction_id', %s::uuid, 'transaction_type', 'receipt',
            'source_reference', %s::text
          ),
          clock_timestamp(), NULL, NULL, NULL, NULL, %s
        )
        """,
        (
            tenant_id, f"routing:{suffix}", f"inventory-transaction/{aggregate_id}",
            str(aggregate_id), aggregate_id, suffix, version,
        ),
    ).fetchone()[0]
    return event_id


def test_scoped_claims_never_steal_unstaged_legacy_or_other_tenants():
    with lab.connect() as conn:
        conn.execute(
            "INSERT INTO kernel_lab.tenants(id, name) VALUES (%s, 'Other Tenant')",
            (OTHER_TENANT,),
        )
        target_id = enqueue_posted(conn, lab.TENANT, suffix="tenant-a")
        foreign_id = enqueue_posted(conn, OTHER_TENANT, suffix="tenant-b")
        legacy_id = enqueue_posted(conn, lab.TENANT, suffix="legacy", version=1)
        conn.commit()

        assert conn.execute(
            "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
            (target_id, OTHER_TENANT, "reviewer"),
        ).fetchone()[0] is False
        assert conn.execute(
            "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
            (legacy_id, lab.TENANT, "reviewer"),
        ).fetchone()[0] is False
        assert conn.execute(
            "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
            (target_id, lab.TENANT, "reviewer"),
        ).fetchone()[0] is True
        assert conn.execute(
            "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
            (target_id, lab.TENANT, "reviewer"),
        ).fetchone()[0] is False
        conn.commit()

    scoped = PostgresTenantOutboxStore(lab.DATABASE_URL, tenant_id=lab.TENANT)
    foreign = PostgresTenantOutboxStore(lab.DATABASE_URL, tenant_id=OTHER_TENANT)
    global_store = PostgresOutboxStore(lab.DATABASE_URL)

    target = scoped.claim(worker_id="tenant-a-publisher", limit=10, lease_seconds=30)
    assert [x.event_id for x in target] == [target_id]
    assert target[0].event_type == "inventory.transaction.posted"
    assert target[0].envelope["tenant_id"] == str(lab.TENANT)
    assert foreign.claim(worker_id="tenant-b", limit=10, lease_seconds=30) == []

    shared = global_store.claim(worker_id="legacy-publisher", limit=10, lease_seconds=30)
    assert {x.event_id for x in shared} == {foreign_id, legacy_id}
    assert all(x.event_id != target_id for x in shared)
    assert all(global_store.ack(event_id=x.event_id, claim_token=x.claim_token) for x in shared)

    with lab.connect() as conn:
        route, published, attempt, action = conn.execute(
            """
            SELECT o.delivery_route, o.published_at, o.attempt_count, a.operator_id
            FROM kernel_lab.domain_event_outbox o
            JOIN kernel_lab.domain_event_outbox_route_actions a ON a.event_id = o.event_id
            WHERE o.event_id = %s
            """,
            (target_id,),
        ).fetchone()
    assert route == "tenant"
    assert published is None
    assert attempt == 1
    assert action == "reviewer"
    assert scoped.ack(event_id=target_id, claim_token=target[0].claim_token) is True


def test_staging_refuses_active_or_expired_claim_even_before_ack():
    with lab.connect() as conn:
        event_id = enqueue_posted(conn, lab.TENANT, suffix="leased")
        conn.commit()

    shared = PostgresOutboxStore(lab.DATABASE_URL)
    claimed = shared.claim(worker_id="inflight", limit=10, lease_seconds=30)
    assert len(claimed) == 1
    assert claimed[0].event_id == event_id
    with lab.connect() as conn:
        assert conn.execute(
            "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
            (event_id, lab.TENANT, "operator"),
        ).fetchone()[0] is False
        # Even an expired lease remains owned until release or a new claim.
        conn.execute(
            "UPDATE kernel_lab.domain_event_outbox SET claimed_until = clock_timestamp() - interval '1 second' WHERE event_id = %s",
            (event_id,),
        )
        assert conn.execute(
            "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
            (event_id, lab.TENANT, "operator"),
        ).fetchone()[0] is False
    assert shared.nack(
        event_id=event_id, claim_token=claimed[0].claim_token,
        error="simulate broker outage", retry_after_seconds=0,
    ) is False
    assert PostgresTenantOutboxStore(
        lab.DATABASE_URL, tenant_id=lab.TENANT
    ).claim(worker_id="scoped", limit=10, lease_seconds=30) == []


def test_tenant_claim_retains_retry_and_quarantine_contract():
    with lab.connect() as conn:
        event_id = enqueue_posted(conn, lab.TENANT, suffix="retry")
        conn.execute(
            "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
            (event_id, lab.TENANT, "reviewer"),
        )
        conn.commit()

    store = PostgresTenantOutboxStore(lab.DATABASE_URL, tenant_id=lab.TENANT)
    first = store.claim(worker_id="scoped-worker", limit=1, lease_seconds=30)[0]
    assert store.nack(
        event_id=event_id, claim_token=first.claim_token,
        error="temporary NATS failure", retry_after_seconds=0,
    )
    again = store.claim(worker_id="scoped-worker", limit=1, lease_seconds=30)[0]
    assert again.event_id == event_id
    assert again.attempt_count == 2
    assert again.claim_token != first.claim_token
    assert store.ack(event_id=event_id, claim_token=first.claim_token) is False
    assert store.ack(event_id=event_id, claim_token=again.claim_token) is True
    assert store.claim(worker_id="scoped-worker", limit=1, lease_seconds=30) == []
