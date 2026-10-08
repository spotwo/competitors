"""Real authenticated NATS broker ACL proof, not just in-process route checks.

A separate NATS lab server has explicit publisher, subscriber and provisioner
credentials. These are public, local-only fixtures and must NEVER be deployed.
"""

from __future__ import annotations

import asyncio
import os
from urllib.parse import urlsplit

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig

import conftest as lab
import test_tenant_outbox_claim as scoped_lab
from consumer_runtime import InboxConsumerRuntime, PostgresInboxStore
from event_pipeline_handlers.inventory_transaction_index import InventoryTransactionIndexProjector
from event_tenant_routing import TenantEventRoute
from nats_consumer import NatsJetStreamPullSource
from nats_transport import NatsJetStreamTransport
from publisher_runtime import PostgresTenantOutboxStore, PublisherRuntime
import test_nats_consumer as nats_lab


STREAM = "WMS_TENANT_AUTH"
DURABLE_A = "TENANT_A_AUTH"
DURABLE_B = "TENANT_B_AUTH"
PREFIX = "spotwo.wms.auth"
ADMIN_URL = os.getenv(
    "KERNEL_LAB_NATS_AUTH_URL",
    "nats://lab_admin:lab-admin-only@127.0.0.1:54223",
)


def user_url(user: str, password: str) -> str:
    parts = urlsplit(ADMIN_URL)
    return f"nats://{user}:{password}@{parts.hostname}:{parts.port or 4222}"


async def provision(route_a: TenantEventRoute, route_b: TenantEventRoute):
    admin = await nats.connect(
        servers=[ADMIN_URL], allow_reconnect=False, connect_timeout=2
    )
    js = admin.jetstream()
    await js.add_stream(StreamConfig(
        name=STREAM,
        subjects=[f"{PREFIX}.>"],
        storage=StorageType.FILE,
    ))
    for durable, route in ((DURABLE_A, route_a), (DURABLE_B, route_b)):
        await js.add_consumer(STREAM, ConsumerConfig(
            durable_name=durable,
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=2,
            max_deliver=3,
            filter_subject=route.subject,
        ))
    return admin


async def negative_broker_permissions(url: str, route_b: TenantEventRoute) -> list[str]:
    errors: list[str] = []

    async def on_error(exc: Exception):
        errors.append(str(exc))

    client = await nats.connect(
        servers=[url], error_cb=on_error, allow_reconnect=False, connect_timeout=2
    )
    try:
        # Test the BROKER without using any route validation in our transport.
        await client.publish(route_b.subject, b"forbidden broker-level cross-tenant publish")
        await client.flush()
        await client.subscribe(route_b.subject)
        await client.flush()
        await client.publish(f"$JS.API.CONSUMER.INFO.{STREAM}.{DURABLE_B}", b"{}")
        await client.publish(f"$JS.API.CONSUMER.DELETE.{STREAM}.{DURABLE_A}", b"{}")
        await client.flush()
        await asyncio.sleep(0.3)
        return errors
    finally:
        await client.close()


def publish_scoped(*, tenant_id, username: str, password: str):
    route = TenantEventRoute(PREFIX, tenant_id)
    with NatsJetStreamTransport(
        server_url=user_url(username, password),
        stream_name=STREAM,
        subject_prefix=PREFIX,
        tenant_route=route,
    ) as transport:
        return PublisherRuntime(
            store=PostgresTenantOutboxStore(
                lab.DATABASE_URL, tenant_id=tenant_id
            ),
            transport=transport,
            worker_id=f"authenticated-{username}",
            batch_size=5,
            lease_seconds=30,
            retry_policy=nats_lab.NO_JITTER_RETRY,
        ).run_once()


def consume_scoped(*, tenant_id, durable: str, username: str, password: str):
    route = TenantEventRoute(PREFIX, tenant_id)
    consumer_name = f"authenticated-{durable}"
    with NatsJetStreamPullSource(
        server_url=user_url(username, password),
        stream_name=STREAM,
        durable_name=durable,
        tenant_route=route,
        fetch_timeout_seconds=2,
    ) as source:
        runtime = InboxConsumerRuntime(
            source=source,
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=consumer_name,
            handler=InventoryTransactionIndexProjector(
                consumer_name=consumer_name,
                expected_tenant_id=tenant_id,
            ),
        )
        cycle = runtime.run_once()
        assert cycle.applied == 1
        assert cycle.acknowledged == 1
        assert runtime.run_once().received == 0
        return cycle


def test_authenticated_acl_and_tenant_end_to_end():
    tenant_a, tenant_b = lab.TENANT, scoped_lab.OTHER_TENANT
    route_a, route_b = TenantEventRoute(PREFIX, tenant_a), TenantEventRoute(PREFIX, tenant_b)

    with lab.connect() as conn:
        conn.execute(
            "INSERT INTO kernel_lab.tenants(id, name) VALUES (%s, 'Auth Tenant B')",
            (tenant_b,),
        )
        event_a = scoped_lab.enqueue_posted(conn, tenant_a, suffix="acl-a")
        event_b = scoped_lab.enqueue_posted(conn, tenant_b, suffix="acl-b")
        for event_id, tenant in ((event_a, tenant_a), (event_b, tenant_b)):
            assert conn.execute(
                "SELECT kernel_lab.stage_tenant_event_route(%s, %s, %s)",
                (event_id, tenant, "lab-provisioner"),
            ).fetchone()[0]
        conn.commit()

    with asyncio.Runner() as runner:
        admin = runner.run(provision(route_a, route_b))
        try:
            assert publish_scoped(
                tenant_id=tenant_a, username="lab_tenant_a_pub", password="lab-a-pub"
            ).published == 1
            assert publish_scoped(
                tenant_id=tenant_b, username="lab_tenant_b_pub", password="lab-b-pub"
            ).published == 1

            # Server-denied publishes, subscribes and JetStream control APIs
            # are captured from the real NATS protocol - no app-side guard.
            errors = runner.run(negative_broker_permissions(
                user_url("lab_tenant_a_pub", "lab-a-pub"), route_b
            ))
            assert any("permissions violation for publish" in item.lower() for item in errors), errors

            sub_errors = runner.run(negative_broker_permissions(
                user_url("lab_tenant_a_sub", "lab-a-sub"), route_b
            ))
            assert any("permissions violation for subscription" in item.lower() for item in sub_errors), sub_errors
            assert any("$js.api.consumer.info" in item.lower() and "permissions violation" in item.lower()
                       for item in sub_errors), sub_errors
            assert any("$js.api.consumer.delete" in item.lower() and "permissions violation" in item.lower()
                       for item in sub_errors), sub_errors

            assert consume_scoped(
                tenant_id=tenant_a, durable=DURABLE_A,
                username="lab_tenant_a_sub", password="lab-a-sub",
            ).event_id == event_a
            assert consume_scoped(
                tenant_id=tenant_b, durable=DURABLE_B,
                username="lab_tenant_b_sub", password="lab-b-sub",
            ).event_id == event_b

            stream_info = runner.run(admin.jetstream().stream_info(STREAM))
            assert stream_info.state.messages == 2
            with lab.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT tenant_id, count(*)
                    FROM kernel_lab.inventory_transaction_index_projection
                    GROUP BY tenant_id
                    """
                ).fetchall()
                assert dict(rows) == {tenant_a: 1, tenant_b: 1}
        finally:
            runner.run(admin.jetstream().delete_stream(STREAM))
            runner.run(admin.close())
