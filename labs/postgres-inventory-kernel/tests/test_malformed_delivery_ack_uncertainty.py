from __future__ import annotations

from hashlib import sha256

import pytest

import conftest as lab
from consumer_runtime import (
    InboxConsumerRuntime,
    InboxDeliveryMetadata,
    MalformedDeliveryEvidence,
)
from malformed_delivery import PostgresNatsMalformedDeliveryQuarantine


def metadata(*, delivery_count: int) -> InboxDeliveryMetadata:
    return InboxDeliveryMetadata(
        transport="nats-jetstream",
        transport_message_id="POISON_ACK:23",
        subject="spotwo.wms.poison.ack",
        delivery_count=delivery_count,
        stream="POISON_ACK",
        consumer="POISON_ACK_DURABLE",
        stream_sequence=23,
        consumer_sequence=23 + delivery_count - 1,
        pending_count=0,
    )


def poison_evidence() -> MalformedDeliveryEvidence:
    payload = b"{broken-json"
    return MalformedDeliveryEvidence(
        failure_code="invalid_message_json",
        error="JetStream message must contain a UTF-8 JSON envelope",
        payload_sha256=sha256(payload).hexdigest(),
        payload_size=len(payload),
        payload_preview=payload,
        payload_truncated=False,
        headers={},
    )


class Delivery:
    envelope = None
    malformed = poison_evidence()

    def __init__(self, *, delivery_count: int, fail_ack: bool):
        self.metadata = metadata(delivery_count=delivery_count)
        self.fail_ack = fail_ack
        self.acknowledged = False

    def ack(self):
        if self.fail_ack:
            raise OSError("connection lost before poison ACK confirmation")
        self.acknowledged = True


class Source:
    def __init__(self, delivery: Delivery):
        self.delivery = delivery

    def fetch_one(self):
        return self.delivery


def test_ack_uncertainty_redelivery_recaptures_same_poison_identity_without_inbox():
    quarantine = PostgresNatsMalformedDeliveryQuarantine(lab.DATABASE_URL)
    first_delivery = Delivery(delivery_count=1, fail_ack=True)
    first_runtime = InboxConsumerRuntime(
        source=Source(first_delivery),
        store=None,
        consumer_name="availability-projector",
        handler=lambda _event, _transaction: None,
        malformed_lane=quarantine,
    )

    with pytest.raises(OSError, match="before poison ACK confirmation"):
        first_runtime.run_once()

    with lab.connect() as conn:
        first_row = conn.execute(
            """
            SELECT observation_count,
                   first_delivery_metadata ->> 'delivery_count',
                   last_delivery_metadata ->> 'delivery_count'
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = 'availability-projector'
              AND stream = 'POISON_ACK'
              AND durable_consumer = 'POISON_ACK_DURABLE'
              AND stream_sequence = 23
            """
        ).fetchone()
    assert first_row == (1, "1", "1")

    redelivery = Delivery(delivery_count=2, fail_ack=False)
    second_runtime = InboxConsumerRuntime(
        source=Source(redelivery),
        store=None,
        consumer_name="availability-projector",
        handler=lambda _event, _transaction: None,
        malformed_lane=quarantine,
    )
    result = second_runtime.run_once()

    assert redelivery.acknowledged is True
    assert result.event_id is None
    assert result.delivery_count == 2
    assert result.malformed == 1
    assert result.quarantined == 1
    assert result.acknowledged == 1

    with lab.connect() as conn:
        final_row = conn.execute(
            """
            SELECT observation_count,
                   first_delivery_metadata ->> 'delivery_count',
                   last_delivery_metadata ->> 'delivery_count'
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = 'availability-projector'
              AND stream = 'POISON_ACK'
              AND durable_consumer = 'POISON_ACK_DURABLE'
              AND stream_sequence = 23
            """
        ).fetchone()
        poison_count = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.nats_jetstream_consumer_poison_deliveries
            WHERE consumer_name = 'availability-projector'
            """
        ).fetchone()[0]
        inbox_count = conn.execute(
            """
            SELECT count(*)
            FROM kernel_lab.domain_event_inbox
            WHERE consumer_name = 'availability-projector'
            """
        ).fetchone()[0]

    assert final_row == (2, "1", "2")
    assert poison_count == 1
    assert inbox_count == 0
