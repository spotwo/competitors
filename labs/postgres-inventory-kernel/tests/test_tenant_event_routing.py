"""Tenant routing is a reviewed opt-in path, not an implicit V1 rollout."""

from __future__ import annotations

import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

import conftest as lab
import test_inventory_transaction_index_scaffold as index_lab
import test_nats_consumer as nats_lab
from consumer_runtime import ConsumedEvent, InboxConsumerRuntime, PostgresInboxStore
from event_pipeline_handlers.inventory_transaction_index import InventoryTransactionIndexProjector
from event_tenant_routing import TENANT_HEADER, TenantEventRoute
from inventory_transaction_index_reader import (
    READ_SCOPE,
    VerifiedPrincipal,
    list_authorized_transactions,
)
from nats_consumer import NatsJetStreamPullSource
from nats_transport import NatsJetStreamTransport, NATS_MESSAGE_ID_HEADER
from publisher_runtime import ClaimedEvent


def routed_event(*, tenant_id: UUID, transaction_id: UUID | None = None) -> ConsumedEvent:
    transaction_id = transaction_id or uuid4()
    original = index_lab.load_transaction_event(index_lab.create_receipt())
    envelope = {
        **original.envelope,
        "event_id": str(uuid4()),
        "tenant_id": str(tenant_id),
        "aggregate_id": str(transaction_id),
        "subject": f"inventory-transaction/{transaction_id}",
        "data": {
            "transaction_id": str(transaction_id),
            "transaction_type": "receipt",
            "source_reference": "TENANT-ROUTED",
        },
    }
    return ConsumedEvent.from_envelope(envelope)


def claim_for(event: ConsumedEvent) -> ClaimedEvent:
    return ClaimedEvent(
        event_id=event.event_id,
        claim_token=uuid4(),
        attempt_count=1,
        event_type=event.event_type,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        aggregate_version=event.aggregate_version,
        envelope=event.envelope,
    )


def test_tenant_route_is_deterministic_and_rejects_mixed_identity():
    tenant = uuid4()
    foreign = uuid4()
    route = TenantEventRoute("spotwo.wms.events", tenant)
    event = routed_event(tenant_id=tenant)
    assert route.subject == f"spotwo.wms.events.tenants.{tenant}.inventory.transaction.posted"
    route.validate_delivery(
        subject=route.subject, headers={TENANT_HEADER: str(tenant)}, envelope=event.envelope
    )
    with pytest.raises(ValueError, match="delivery subject"):
        route.validate_delivery(
            subject=f"spotwo.wms.events.tenants.{foreign}.inventory.transaction.posted",
            headers={TENANT_HEADER: str(tenant)},
            envelope=event.envelope,
        )
    with pytest.raises(ValueError, match="tenant header"):
        route.validate_delivery(
            subject=route.subject, headers={TENANT_HEADER: str(foreign)},
            envelope=event.envelope,
        )
    with pytest.raises(ValueError, match="outside the authorized"):
        route.validate_delivery(
            subject=route.subject, headers={TENANT_HEADER: str(tenant)},
            envelope={**event.envelope, "tenant_id": str(foreign)},
        )
    with pytest.raises(ValueError, match="Event Contract V2"):
        route.validate_envelope({**event.envelope, "schema_version": 1})


def test_publisher_denies_cross_tenant_or_legacy_before_connecting():
    tenant = uuid4()
    other_tenant = uuid4()
    prefix = "spotwo.wms.routing"
    event = routed_event(tenant_id=tenant)
    with NatsJetStreamTransport(
        server_url="nats://127.0.0.1:1",
        stream_name="NOT_PROVISIONED",
        subject_prefix=prefix,
        tenant_route=TenantEventRoute(prefix, tenant),
    ) as transport:
        assert transport.subject_for(claim_for(event)) == (
            f"{prefix}.tenants.{tenant}.inventory.transaction.posted"
        )
        with pytest.raises(ValueError, match="outside the authorized"):
            transport.publish(claim_for(routed_event(tenant_id=other_tenant)))
        with pytest.raises(ValueError, match="Event Contract V2"):
            legacy = replace(event, schema_version=1, tenant_id=None)
            transport.publish(claim_for(legacy))


def test_durable_filter_must_equal_exact_tenant_route():
    prefix = "spotwo.wms.events"
    tenant = uuid4()
    route = TenantEventRoute(prefix, tenant)
    from nats.js.api import AckPolicy
    from types import SimpleNamespace

    broad = SimpleNamespace(
        durable_name="TENANT_A",
        ack_policy=AckPolicy.EXPLICIT,
        deliver_subject=None,
        filter_subject=f"{prefix}.>",
        filter_subjects=None,
    )
    with pytest.raises(ValueError, match="exact authorized filter"):
        NatsJetStreamPullSource._validate_consumer_config(
            broad, expected_durable_name="TENANT_A",
            expected_filter_subject=route.subject,
        )
    broad.filter_subject = route.subject
    NatsJetStreamPullSource._validate_consumer_config(
        broad, expected_durable_name="TENANT_A",
        expected_filter_subject=route.subject,
    )


def test_real_jetstream_exact_filter_isolates_two_tenants_and_index_read_authz():
    suffix = uuid4().hex.upper()
    stream = f"WMS_TENANT_{suffix}"
    durable = f"TENANT_{suffix}"
    prefix = f"spotwo.wms.tenanttest.{suffix.lower()}"
    tenant_a = uuid4()
    tenant_b = uuid4()
    route_a = TenantEventRoute(prefix, tenant_a)
    route_b = TenantEventRoute(prefix, tenant_b)
    event_a = routed_event(tenant_id=tenant_a)
    event_b = routed_event(tenant_id=tenant_b)

    probe = nats_lab.ConsumerProbe(nats_lab.NATS_URL)
    probe.provision(
        stream_name=stream,
        subject_prefix=prefix,
        durable_name=durable,
        filter_subject=route_a.subject,
    )
    source = NatsJetStreamPullSource(
        server_url=nats_lab.NATS_URL,
        stream_name=stream,
        durable_name=durable,
        client_name=f"tenant-client-{suffix}",
        fetch_timeout_seconds=0.2,
        tenant_route=route_a,
    )
    try:
        # Publish B first. A's consumer must not see it even if earlier in stream.
        for route, event in ((route_b, event_b), (route_a, event_a)):
            with NatsJetStreamTransport(
                server_url=nats_lab.NATS_URL,
                stream_name=stream,
                subject_prefix=prefix,
                client_name=f"publisher-{route.tenant_id}",
                tenant_route=route,
            ) as transport:
                assert transport.publish(claim_for(event)).transport_message_id

        consumer_name = f"index-{suffix.lower()}"
        runtime = InboxConsumerRuntime(
            source=source,
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=consumer_name,
            handler=InventoryTransactionIndexProjector(
                consumer_name=consumer_name,
                expected_tenant_id=tenant_a,
            ),
        )
        cycle = runtime.run_once()
        assert cycle.event_id == event_a.event_id
        assert cycle.applied == 1 and cycle.acknowledged == 1
        assert runtime.run_once().received == 0
        assert probe.consumer_info(
            stream_name=stream, durable_name=durable
        ).num_ack_pending == 0

        principal_a = VerifiedPrincipal(
            subject="authenticated-user-a",
            permitted_tenants=frozenset({tenant_a}),
            scopes=frozenset({READ_SCOPE}),
        )
        with lab.connect() as conn:
            rows = list_authorized_transactions(
                conn, principal=principal_a, tenant_id=tenant_a,
                consumer_name=consumer_name,
            )
            assert len(rows) == 1 and rows[0][0] == UUID(event_a.aggregate_id)
            with pytest.raises(PermissionError, match="tenant is not authorized"):
                list_authorized_transactions(
                    conn, principal=principal_a, tenant_id=tenant_b,
                    consumer_name=consumer_name,
                )
            with pytest.raises(PermissionError, match="read scope"):
                list_authorized_transactions(
                    conn,
                    principal=replace(principal_a, scopes=frozenset()),
                    tenant_id=tenant_a, consumer_name=consumer_name,
                )
            with pytest.raises(PermissionError, match="verified principal"):
                list_authorized_transactions(
                    conn, principal=None, tenant_id=tenant_a,
                    consumer_name=consumer_name,
                )
            assert conn.execute(
                "SELECT count(*) FROM kernel_lab.domain_event_inbox WHERE consumer_name = %s",
                (consumer_name,),
            ).fetchone()[0] == 1
    finally:
        source.close()
        probe.close(stream)


def test_forged_tenant_message_is_quarantined_before_inbox_handling():
    suffix = uuid4().hex.upper()
    stream = f"WMS_FORGE_{suffix}"
    durable = f"FORGE_{suffix}"
    prefix = f"spotwo.wms.forge.{suffix.lower()}"
    tenant_a = uuid4()
    tenant_b = uuid4()
    route_a = TenantEventRoute(prefix, tenant_a)
    forged = routed_event(tenant_id=tenant_b)
    probe = nats_lab.ConsumerProbe(nats_lab.NATS_URL)
    probe.provision(
        stream_name=stream, subject_prefix=prefix, durable_name=durable,
        filter_subject=route_a.subject,
    )
    source = NatsJetStreamPullSource(
        server_url=nats_lab.NATS_URL,
        stream_name=stream,
        durable_name=durable,
        tenant_route=route_a,
    )
    try:
        payload = json.dumps(forged.envelope).encode()
        headers = {
            NATS_MESSAGE_ID_HEADER: str(forged.event_id),
            "Spotwo-Event-Type": forged.event_type,
            "Spotwo-Aggregate-Type": forged.aggregate_type,
            "Spotwo-Aggregate-Id": forged.aggregate_id,
            "Spotwo-Aggregate-Version": str(forged.aggregate_version),
            TENANT_HEADER: str(tenant_b),
        }
        probe._runner.run(
            probe._jetstream.publish(
                route_a.subject, payload, headers=headers, stream=stream
            )
        )
        received = source.fetch_one()
        assert received is not None
        assert received.envelope is None
        assert received.malformed.failure_code == "invalid_tenant_route"
        with lab.connect() as conn:
            assert conn.execute(
                "SELECT count(*) FROM kernel_lab.domain_event_inbox"
            ).fetchone()[0] == 0
    finally:
        source.close()
        probe.close(stream)
