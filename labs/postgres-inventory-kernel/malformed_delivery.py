from __future__ import annotations

from typing import Any, Mapping

import psycopg
from psycopg.types.json import Jsonb

from consumer_runtime import (
    MalformedDeliveryDisposition,
    MalformedDeliveryEvidence,
)


def _required_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    if value != value.strip():
        raise ValueError(f"{field} cannot contain surrounding whitespace")
    return value


class PostgresNatsMalformedDeliveryQuarantine:
    """Persists poison delivery evidence keyed only by trusted JetStream identity."""

    def __init__(self, database_url: str):
        self.database_url = database_url

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url)
        conn.execute("SET statement_timeout = '30s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    def capture(
        self,
        *,
        consumer_name: str,
        evidence: MalformedDeliveryEvidence,
        delivery_metadata: Mapping[str, Any],
    ) -> MalformedDeliveryDisposition:
        _required_name(consumer_name, "consumer_name")
        metadata = dict(delivery_metadata)
        if metadata.get("transport") != "nats-jetstream":
            raise ValueError("malformed delivery quarantine requires nats-jetstream metadata")

        stream = metadata.get("stream")
        durable_consumer = metadata.get("consumer")
        stream_sequence = metadata.get("stream_sequence")
        subject = metadata.get("subject")
        _required_name(stream, "stream")
        _required_name(durable_consumer, "consumer")
        _required_name(subject, "subject")
        if (
            isinstance(stream_sequence, bool)
            or not isinstance(stream_sequence, int)
            or stream_sequence < 1
        ):
            raise ValueError("stream_sequence must be positive")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT created, observation_count, persisted_failure_code
                FROM kernel_lab.capture_nats_jetstream_consumer_poison_delivery(
                  %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s
                )
                """,
                (
                    consumer_name,
                    stream,
                    durable_consumer,
                    stream_sequence,
                    subject,
                    evidence.failure_code,
                    evidence.error,
                    evidence.payload_sha256,
                    evidence.payload_size,
                    evidence.payload_preview,
                    evidence.payload_truncated,
                    Jsonb(dict(evidence.headers)),
                    Jsonb(metadata),
                ),
            ).fetchone()

        if row is None:
            raise RuntimeError("malformed delivery capture returned no result")
        return MalformedDeliveryDisposition(
            failure_code=row[2],
            observation_count=row[1],
            created=row[0],
        )
