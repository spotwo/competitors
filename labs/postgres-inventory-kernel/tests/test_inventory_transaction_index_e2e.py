"""Third pipeline: real PostgreSQL Outbox -> JetStream -> Inbox -> transaction index."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

import conftest as lab
import test_inventory_transaction_index_scaffold as index_lab
import test_nats_consumer as nats_lab
from consumer_runtime import InboxConsumerRuntime, PostgresInboxStore
from event_pipeline_handlers.inventory_transaction_index import InventoryTransactionIndexProjector
from nats_consumer import NatsJetStreamPullSource
from nats_transport import NatsJetStreamTransport
from publisher_runtime import PostgresOutboxStore, PublisherRuntime


def test_transaction_index_jetstream_ack_uncertainty_and_second_posting():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_TXINDEX_{suffix}"
    durable_name = f"TXINDEX_{suffix}"
    consumer_name = f"transaction-index-{suffix.lower()}"
    prefix = f"spotwo.wms.txindex.{suffix.lower()}"

    probe = nats_lab.ConsumerProbe(nats_lab.NATS_URL)
    probe.provision(
        stream_name=stream_name,
        subject_prefix=prefix,
        durable_name=durable_name,
        filter_subject=f"{prefix}.inventory.transaction.posted",
    )
    source = NatsJetStreamPullSource(
        server_url=nats_lab.NATS_URL,
        stream_name=stream_name,
        durable_name=durable_name,
        client_name=f"txindex-projector-{suffix}",
        fetch_timeout_seconds=2,
    )

    def publish_outbox():
        with NatsJetStreamTransport(
            server_url=nats_lab.NATS_URL,
            stream_name=stream_name,
            subject_prefix=prefix,
            client_name=f"txindex-publisher-{suffix}",
        ) as transport:
            return PublisherRuntime(
                store=PostgresOutboxStore(lab.DATABASE_URL),
                transport=transport,
                worker_id=f"txindex-publisher-{suffix}",
                batch_size=10,
                lease_seconds=30,
                retry_policy=nats_lab.NO_JITTER_RETRY,
            ).run_once()

    try:
        first_id = index_lab.create_receipt(
            key="txindex:e2e:first", reference="ASN-TX-E2E-1"
        )
        first_event = index_lab.load_transaction_event(first_id)
        published = publish_outbox()
        # The source also emits InventoryPosition.changed; only the posting
        # reaches the third consumer because its durable uses an exact filter.
        assert published.published == 2
        assert published.failed == 0

        runtime = InboxConsumerRuntime(
            source=nats_lab.FailFirstAckSource(source),
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=consumer_name,
            handler=InventoryTransactionIndexProjector(consumer_name=consumer_name),
        )
        with pytest.raises(OSError, match="before JetStream ACK"):
            runtime.run_once()

        with lab.connect() as conn:
            assert conn.execute(
                """
                SELECT event_id
                FROM kernel_lab.inventory_transaction_index_projection
                WHERE consumer_name = %s AND transaction_id = %s
                """,
                (consumer_name, first_id),
            ).fetchone()[0] == first_event.event_id
            assert conn.execute(
                """
                SELECT count(*) FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                """,
                (consumer_name,),
            ).fetchone()[0] == 1

        time.sleep(0.7)
        redelivery = runtime.run_once()
        assert redelivery.event_id == first_event.event_id
        assert redelivery.duplicate == 1
        assert redelivery.applied == 0
        assert redelivery.acknowledged == 1

        second_id = index_lab.create_receipt(
            key="txindex:e2e:second", reference="ASN-TX-E2E-2",
            location_id=lab.LOCATION_B,
        )
        second_event = index_lab.load_transaction_event(second_id)
        published = publish_outbox()
        assert published.published == 2
        assert published.failed == 0

        consumed = runtime.run_once()
        assert consumed.event_id == second_event.event_id
        assert consumed.applied == 1
        assert consumed.duplicate == 0
        assert consumed.acknowledged == 1

        with lab.connect() as conn:
            indexed = conn.execute(
                """
                SELECT transaction_id, event_id, source_reference
                FROM kernel_lab.inventory_transaction_index_projection
                WHERE consumer_name = %s
                ORDER BY occurred_at, transaction_id
                """,
                (consumer_name,),
            ).fetchall()
            receipts = conn.execute(
                """
                SELECT count(*)
                FROM kernel_lab.domain_event_inbox
                WHERE consumer_name = %s
                """,
                (consumer_name,),
            ).fetchone()[0]

        assert set(indexed) == {
            (first_id, first_event.event_id, "ASN-TX-E2E-1"),
            (second_id, second_event.event_id, "ASN-TX-E2E-2"),
        }
        assert receipts == 2
        assert probe.consumer_info(
            stream_name=stream_name,
            durable_name=durable_name,
        ).num_ack_pending == 0
    finally:
        source.close()
        probe.close(stream_name)
