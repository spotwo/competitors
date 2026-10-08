"""Live tenant rollout proof: activation fence, one-route claims and safe revert."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest

import conftest as lab
import test_nats_authenticated_acl as auth_lab
import test_tenant_event_deployment_e2e as deployment_lab
import test_tenant_outbox_claim as outbox_lab
from publisher_runtime import PostgresOutboxStore, PostgresTenantOutboxStore
from tenant_rollout import PostgresTenantRolloutStore, TenantRolloutController
from tenant_event_deployment import TenantTopologyProvisioner
from tenant_deployment_activation import TenantActivationController


def binding():
    original = deployment_lab.lab_binding()
    return replace(
        original,
        deployment=replace(
            original.deployment,
            inbox_consumer_name=f"authenticated-{auth_lab.DURABLE_A}",
        ),
    )


def controller():
    return TenantRolloutController(
        database_url=lab.DATABASE_URL,
        provisioner_nats_url=auth_lab.ADMIN_URL,
        publisher_nats_url=auth_lab.user_url("lab_tenant_a_pub", "lab-a-pub"),
        consumer_nats_url=auth_lab.user_url("lab_tenant_a_sub", "lab-a-sub"),
        canary_consumer_nats_url=auth_lab.user_url("lab_tenant_a_canary", "lab-a-canary"),
    )


def activated_rollout():
    bound = binding()
    with asyncio.Runner() as runner:
        runner.run(deployment_lab.reset_stream(create=True))
    provisioner = TenantTopologyProvisioner(auth_lab.ADMIN_URL)
    assert provisioner.provision(
        bound, operator_id="topology-operator", approval_id="topology-approval"
    ).status == "ready"
    activation = TenantActivationController(
        database_url=lab.DATABASE_URL,
        provisioner_nats_url=auth_lab.ADMIN_URL,
        publisher_nats_url=auth_lab.user_url("lab_tenant_a_pub", "lab-a-pub"),
        consumer_nats_url=auth_lab.user_url("lab_tenant_a_sub", "lab-a-sub"),
        canary_consumer_nats_url=auth_lab.user_url("lab_tenant_a_canary", "lab-a-canary"),
    ).activate(bound, operator_id="rollout-operator", approval_id="ACT-301")
    return bound, activation


def teardown():
    with asyncio.Runner() as runner:
        runner.run(deployment_lab.reset_stream(create=False))


def enqueue(*, suffix: str):
    with lab.connect() as conn:
        event_id = outbox_lab.enqueue_posted(conn, lab.TENANT, suffix=suffix)
        conn.commit()
    return event_id


def test_cutover_requires_receipt_live_readiness_and_bounded_virgin_events():
    bound, activation = activated_rollout()
    try:
        store = PostgresTenantRolloutStore(lab.DATABASE_URL)
        assert store.snapshot(bound).phase == "not_started"
        with pytest.raises(psycopg.errors.CheckViolation, match="receipt/tenant mismatch"):
            controller().start(
                bound, activation_receipt=uuid4(), operator_id="operator",
                change_ref="CUTOVER-001",
            )

        generation = controller().start(
            bound, activation_receipt=activation.receipt_id, operator_id="operator",
            change_ref="CUTOVER-001",
        )
        assert generation is not None
        first = enqueue(suffix="cutover-virgin")
        second = enqueue(suffix="cutover-other")
        with pytest.raises(psycopg.errors.CheckViolation, match="entirely eligible"):
            store.stage(
                bound, event_ids=(first, uuid4()), operator_id="operator",
                change_ref="WAVE-INVALID",
            )
        with lab.connect() as conn:
            assert conn.execute(
                "SELECT delivery_route FROM kernel_lab.domain_event_outbox WHERE event_id=%s",
                (first,),
            ).fetchone()[0] == "shared"

        assert controller().stage(
            bound, event_ids=(first,), operator_id="operator",
            change_ref="WAVE-1",
        ) == 1
        assert store.snapshot(bound).tenant_pending == 1
        with pytest.raises(ValueError, match="pending deliveries"):
            controller().stage(
                bound, event_ids=(second,), operator_id="operator",
                change_ref="WAVE-2",
            )

        # Shared workers only see the other, unstaged event.
        shared = PostgresOutboxStore(lab.DATABASE_URL).claim(
            worker_id="shared-worker", limit=10, lease_seconds=30
        )
        assert [x.event_id for x in shared] == [second]
        tenant = PostgresTenantOutboxStore(lab.DATABASE_URL, tenant_id=lab.TENANT).claim(
            worker_id="tenant-worker", limit=10, lease_seconds=30
        )
        assert [x.event_id for x in tenant] == [first]
        with pytest.raises(psycopg.errors.CheckViolation, match="audited cutover API"):
            with lab.connect() as conn:
                conn.execute(
                    "SELECT kernel_lab.stage_tenant_event_route(%s,%s,%s)",
                    (second, lab.TENANT, "operator"),
                )

        with lab.connect() as conn:
            actions = conn.execute(
                "SELECT action, event_id FROM kernel_lab.tenant_deployment_rollout_journal "
                "WHERE deployment_id=%s ORDER BY recorded_at, action",
                (bound.deployment.deployment_id,),
            ).fetchall()
            assert {action for action, _ in actions} == {"started", "staged"}
    finally:
        teardown()


def test_pause_and_all_or_nothing_revert_only_never_attempted_events():
    bound, activation = activated_rollout()
    try:
        store = PostgresTenantRolloutStore(lab.DATABASE_URL)
        controller().start(
            bound, activation_receipt=activation.receipt_id,
            operator_id="owner", change_ref="CUTOVER-02",
        )
        first = enqueue(suffix="rollback-claimed")
        second = enqueue(suffix="rollback-virgin")
        assert store.stage(
            bound, event_ids=(first, second), operator_id="owner",
            change_ref="WAVE-1",
        ) == 2
        attempted = PostgresTenantOutboxStore(
            lab.DATABASE_URL, tenant_id=lab.TENANT
        ).claim(worker_id="tenant-publisher", limit=1, lease_seconds=30)
        assert [x.event_id for x in attempted] == [first]
        assert store.pause(bound, operator_id="owner", change_ref="PAUSE-1")
        assert not store.pause(bound, operator_id="owner", change_ref="PAUSE-1")
        with pytest.raises(psycopg.errors.CheckViolation, match="attempted"):
            store.rollback(
                bound, event_ids=(first, second), operator_id="owner",
                change_ref="ROLLBACK-BAD",
            )
        assert store.rollback(
            bound, event_ids=(second,), operator_id="owner",
            change_ref="ROLLBACK-SAFE",
        ) == 1
        with lab.connect() as conn:
            routes = dict(conn.execute(
                "SELECT event_id, delivery_route FROM kernel_lab.domain_event_outbox"
            ).fetchall())
            assert routes[first] == "tenant"
            assert routes[second] == "shared"
            events = conn.execute(
                "SELECT action FROM kernel_lab.tenant_deployment_rollout_journal "
                "ORDER BY recorded_at"
            ).fetchall()
            assert "reverted" in [row[0] for row in events]
        with pytest.raises(psycopg.errors.CheckViolation, match="not active"):
            store.stage(
                bound, event_ids=(second,), operator_id="owner", change_ref="AFTER-PAUSE"
            )
        with pytest.raises(psycopg.errors.CheckViolation, match="append-only"):
            with lab.connect() as conn:
                conn.execute("DELETE FROM kernel_lab.tenant_deployment_rollout_journal")
    finally:
        teardown()


def test_real_jetstream_delivery_and_projection_lag_prevent_wrong_route_replay():
    bound, activation = activated_rollout()
    try:
        store = PostgresTenantRolloutStore(lab.DATABASE_URL)
        controller().start(
            bound, activation_receipt=activation.receipt_id,
            operator_id="owner", change_ref="CUTOVER-03",
        )
        event = enqueue(suffix="nats-cutover-1")
        next_event = enqueue(suffix="nats-cutover-2")
        assert controller().stage(
            bound, event_ids=(event,), operator_id="owner", change_ref="WAVE-1",
        ) == 1
        assert auth_lab.publish_scoped(
            tenant_id=lab.TENANT, username="lab_tenant_a_pub", password="lab-a-pub"
        ).published == 1
        snap = store.snapshot(bound)
        assert snap.tenant_pending == 0
        assert snap.published_unprojected == 1
        assert not snap.safe_for_next_wave
        with pytest.raises(ValueError, match="projection lag"):
            controller().stage(
                bound, event_ids=(next_event,), operator_id="owner", change_ref="WAVE-2"
            )

        delivered = auth_lab.consume_scoped(
            tenant_id=lab.TENANT, durable=auth_lab.DURABLE_A,
            username="lab_tenant_a_sub", password="lab-a-sub"
        )
        assert delivered.acknowledged == 1
        assert store.snapshot(bound).safe_for_next_wave
        assert controller().stage(
            bound, event_ids=(next_event,), operator_id="owner", change_ref="WAVE-2"
        ) == 1
        assert store.pause(bound, operator_id="owner", change_ref="PAUSE-1")
        with pytest.raises(psycopg.errors.CheckViolation, match="attempted"):
            store.rollback(
                bound, event_ids=(event,), operator_id="owner", change_ref="UNSAFE-REPLAY"
            )
        assert store.rollback(
            bound, event_ids=(next_event,), operator_id="owner",
            change_ref="SAFE-REVERT"
        ) == 1
        with lab.connect() as conn:
            result = conn.execute(
                "SELECT published_at, delivery_route FROM kernel_lab.domain_event_outbox "
                "WHERE event_id=%s", (event,),
            ).fetchone()
            assert result[0] is not None
            assert result[1] == "tenant"
            assert conn.execute(
                "SELECT delivery_route FROM kernel_lab.domain_event_outbox WHERE event_id=%s",
                (next_event,),
            ).fetchone()[0] == "shared"
    finally:
        teardown()
