from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError
import psycopg
import pytest

import conftest as lab
from consumer_failure import (
    ConsumerRetryPolicy,
    DurableConsumerFailureLane,
    PostgresConsumerFailureStore,
)
from consumer_runtime import ConsumedEvent, InboxConsumerRuntime, PostgresInboxStore
from event_pipeline_canary import (
    CANARY_EVENT_TYPE,
    EventPipelineCanaryHandler,
    EventPipelineCanaryRunSnapshot,
    EventPipelineCanarySloPolicy,
    NatsJetStreamCanaryAckObserver,
    PostgresEventPipelineCanaryStore,
)
from event_pipeline_canary_sli import PostgresEventPipelineCanarySliStore
from event_pipeline_fault_drill import (
    verify_canary_fault,
    verify_control_plane_fault,
    with_recovery,
)
from nats_consumer import NatsJetStreamPullSource
from nats_transport import NatsJetStreamTransport
from publisher_runtime import PostgresOutboxStore, PublisherRuntime, RetryPolicy

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")
FAULT_TIMEOUT_SECONDS = 0.05


@dataclass(frozen=True)
class FaultScope:
    stream: str
    durable: str
    consumer: str
    subject_prefix: str

    @property
    def filter_subject(self) -> str:
        return f"{self.subject_prefix}.{CANARY_EVENT_TYPE}"


def _new_scope() -> FaultScope:
    suffix = uuid4().hex[:12]
    return FaultScope(
        stream=f"WMS_FAULT_{suffix.upper()}",
        durable=f"FAULT_{suffix.upper()}",
        consumer=f"fault_canary_{suffix}",
        subject_prefix=f"spotwo.wms.fault.{suffix}",
    )


class FaultNatsLab:
    def __init__(self, server_url: str):
        self._runner = asyncio.Runner()
        self._client = self._runner.run(
            nats.connect(servers=[server_url], allow_reconnect=False, connect_timeout=2)
        )
        self._jetstream = self._client.jetstream()
        self._streams: set[str] = set()

    def provision(self, scope: FaultScope, *, misconfigured: bool = False) -> None:
        self._runner.run(self._provision(scope, misconfigured=misconfigured))
        self._streams.add(scope.stream)

    async def _provision(self, scope: FaultScope, *, misconfigured: bool) -> None:
        await self._delete_if_present(scope.stream)
        await self._jetstream.add_stream(
            StreamConfig(
                name=scope.stream,
                subjects=[f"{scope.subject_prefix}.>"],
                storage=StorageType.FILE,
                duplicate_window=120,
            )
        )
        filter_subject = (
            f"{scope.subject_prefix}.inventory.changed"
            if misconfigured
            else scope.filter_subject
        )
        await self._jetstream.add_consumer(
            scope.stream,
            ConsumerConfig(
                durable_name=scope.durable,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=5,
                max_deliver=3,
                max_ack_pending=10,
                filter_subject=filter_subject,
            ),
        )

    async def _delete_if_present(self, stream: str) -> None:
        try:
            await self._jetstream.delete_stream(stream)
        except NotFoundError:
            pass

    def close(self) -> None:
        try:
            for stream in tuple(self._streams):
                self._runner.run(self._delete_if_present(stream))
            self._runner.run(self._client.close())
        finally:
            self._runner.close()

    def __enter__(self) -> "FaultNatsLab":
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()


def _policy() -> EventPipelineCanarySloPolicy:
    return EventPipelineCanarySloPolicy(
        warning_publish_seconds=60,
        critical_publish_seconds=120,
        warning_delivery_seconds=60,
        critical_delivery_seconds=120,
        warning_projection_seconds=60,
        critical_projection_seconds=120,
        warning_end_to_end_seconds=60,
        critical_end_to_end_seconds=120,
        clock_skew_tolerance_seconds=60,
    )


def _start_tracked(
    store: PostgresEventPipelineCanarySliStore,
    scope: FaultScope,
    *,
    timeout_seconds: float,
):
    return store.start_tracked(
        tenant_id=lab.TENANT,
        stream_name=scope.stream,
        durable_name=scope.durable,
        consumer_name=scope.consumer,
        timeout_seconds=timeout_seconds,
    )


def _publish_all(
    scope: FaultScope,
    *,
    server_url: str = NATS_URL,
    connect_timeout_seconds: float = 2,
):
    with NatsJetStreamTransport(
        server_url=server_url,
        stream_name=scope.stream,
        subject_prefix=scope.subject_prefix,
        client_name=f"fault-publisher-{uuid4().hex[:8]}",
        connect_timeout_seconds=connect_timeout_seconds,
        publish_timeout_seconds=max(connect_timeout_seconds, 0.1),
    ) as transport:
        return PublisherRuntime(
            store=PostgresOutboxStore(lab.DATABASE_URL),
            transport=transport,
            worker_id=f"fault-publisher-{uuid4().hex[:8]}",
            batch_size=100,
            lease_seconds=30,
            retry_policy=RetryPolicy(
                base_delay_seconds=1,
                max_delay_seconds=1,
                jitter_ratio=0,
                max_attempts=3,
            ),
        ).run_once()


def _consume_until_event(
    scope: FaultScope,
    target_event_id: UUID,
    *,
    handler=None,
    failure_lane=None,
):
    handler = handler or EventPipelineCanaryHandler(consumer_name=scope.consumer)
    with NatsJetStreamPullSource(
        server_url=NATS_URL,
        stream_name=scope.stream,
        durable_name=scope.durable,
        client_name=f"fault-consumer-{uuid4().hex[:8]}",
        fetch_timeout_seconds=1,
    ) as source:
        runtime = InboxConsumerRuntime(
            source=source,
            store=PostgresInboxStore(lab.DATABASE_URL),
            consumer_name=scope.consumer,
            handler=handler,
            failure_lane=failure_lane,
        )
        for _attempt in range(10):
            result = runtime.run_once()
            if result.event_id == target_event_id:
                return result
    raise AssertionError(f"target event {target_event_id} was not consumed")


def _report(identity, scope: FaultScope, *, timed_out: bool):
    observed_at = datetime.now(timezone.utc)
    database = PostgresEventPipelineCanaryStore(lab.DATABASE_URL).snapshot(
        canary_id=identity.canary_id,
        consumer_name=scope.consumer,
    )
    with NatsJetStreamCanaryAckObserver(
        server_url=NATS_URL,
        stream_name=scope.stream,
        durable_name=scope.durable,
        expected_filter_subject=scope.filter_subject,
        connect_timeout_seconds=1,
    ) as observer:
        ack = observer.snapshot(observed_at=observed_at)
    snapshot = EventPipelineCanaryRunSnapshot(
        observed_at=observed_at,
        expected_stream_name=scope.stream,
        expected_durable_name=scope.durable,
        expected_consumer_name=scope.consumer,
        expected_filter_subject=scope.filter_subject,
        database=database,
        ack=ack,
    )
    return _policy().evaluate(snapshot, timed_out=timed_out)


def _expire_fault_deadline() -> None:
    time.sleep(FAULT_TIMEOUT_SECONDS + 0.03)


def _record_fault_and_assert_history(
    store: PostgresEventPipelineCanarySliStore,
    scope: FaultScope,
    report,
    expected_code: str,
) -> None:
    store.record(report)
    short = store.snapshot(
        stream_name=scope.stream,
        durable_name=scope.durable,
        consumer_name=scope.consumer,
    ).window(300)
    assert short.total_runs == 1
    assert short.failed_runs == 1
    assert short.successful_runs == 0
    assert short.failure_codes == {expected_code: 1}
    assert short.availability_burn_rate is not None
    assert short.availability_burn_rate > 14.4


def _run_recovery(
    store: PostgresEventPipelineCanarySliStore,
    scope: FaultScope,
):
    identity = _start_tracked(store, scope, timeout_seconds=2)
    publish = _publish_all(scope)
    assert publish.published >= 1
    result = _consume_until_event(scope, identity.event_id)
    assert result.applied == 1
    assert result.acknowledged == 1
    report = _report(identity, scope, timed_out=False)
    assert report.snapshot.complete is True
    assert report.status == "ok"
    assert report.alerts == ()
    store.record(report)
    return report


def _assert_recovery_history(
    store: PostgresEventPipelineCanarySliStore,
    scope: FaultScope,
    expected_code: str,
) -> None:
    short = store.snapshot(
        stream_name=scope.stream,
        durable_name=scope.durable,
        consumer_name=scope.consumer,
    ).window(300)
    assert short.total_runs == 2
    assert short.failed_runs == 1
    assert short.successful_runs == 1
    assert short.failure_codes == {expected_code: 1}
    assert short.consecutive_failures == 0
    assert short.availability_burn_rate is not None
    assert short.availability_burn_rate > 14.4


def _assert_fault_recovered(
    result,
    recovery_report,
    *,
    historical_failure_retained: bool = True,
) -> None:
    final = with_recovery(
        result,
        recovery_report=recovery_report,
        historical_failure_retained=historical_failure_retained,
    )
    assert final.passed is True


def test_fault_drill_publisher_stopped_detects_publish_timeout_and_recovers():
    scope = _new_scope()
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    with FaultNatsLab(NATS_URL) as broker:
        broker.provision(scope)
        identity = _start_tracked(store, scope, timeout_seconds=FAULT_TIMEOUT_SECONDS)
        _expire_fault_deadline()
        fault_report = _report(identity, scope, timed_out=True)
        result = verify_canary_fault("publisher_stopped", fault_report)
        assert result.fault_detected is True
        _record_fault_and_assert_history(store, scope, fault_report, "canary_publish_timeout")

        recovery_report = _run_recovery(store, scope)
        _assert_recovery_history(store, scope, "canary_publish_timeout")
        _assert_fault_recovered(result, recovery_report)


def test_fault_drill_nats_unavailable_detects_outbox_failure_and_recovers():
    scope = _new_scope()
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    with FaultNatsLab(NATS_URL) as broker:
        broker.provision(scope)
        identity = _start_tracked(store, scope, timeout_seconds=FAULT_TIMEOUT_SECONDS)

        publish = _publish_all(
            scope,
            server_url="nats://127.0.0.1:1",
            connect_timeout_seconds=0.1,
        )
        assert publish.claimed == 1
        assert publish.failed == 1
        assert publish.published == 0
        with lab.connect() as conn:
            row = conn.execute(
                """
                SELECT published_at, quarantined_at, last_error,
                       available_at, last_failed_at
                FROM kernel_lab.domain_event_outbox
                WHERE event_id = %s
                """,
                (identity.event_id,),
            ).fetchone()
        assert row is not None
        assert row[0] is None
        assert row[1] is None
        assert row[2]
        assert row[3] is not None
        assert row[4] is not None

        _expire_fault_deadline()
        fault_report = _report(identity, scope, timed_out=True)
        result = verify_canary_fault(
            "nats_unavailable",
            fault_report,
            secondary_code="outbox_publish_failed",
        )
        assert result.fault_detected is True
        _record_fault_and_assert_history(store, scope, fault_report, "canary_publish_timeout")

        recovery_report = _run_recovery(store, scope)
        _assert_recovery_history(store, scope, "canary_publish_timeout")
        _assert_fault_recovered(result, recovery_report)


def test_fault_drill_consumer_stopped_detects_delivery_timeout_and_recovers():
    scope = _new_scope()
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    with FaultNatsLab(NATS_URL) as broker:
        broker.provision(scope)
        identity = _start_tracked(store, scope, timeout_seconds=FAULT_TIMEOUT_SECONDS)
        assert _publish_all(scope).published == 1

        _expire_fault_deadline()
        fault_report = _report(identity, scope, timed_out=True)
        result = verify_canary_fault("consumer_stopped", fault_report)
        assert result.fault_detected is True
        _record_fault_and_assert_history(store, scope, fault_report, "canary_delivery_timeout")

        recovery_report = _run_recovery(store, scope)
        _assert_recovery_history(store, scope, "canary_delivery_timeout")
        _assert_fault_recovered(result, recovery_report)


def test_fault_drill_consumer_misconfiguration_is_detected_before_timeout_and_recovers():
    scope = _new_scope()
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    with FaultNatsLab(NATS_URL) as broker:
        broker.provision(scope, misconfigured=True)
        identity = _start_tracked(store, scope, timeout_seconds=2)

        fault_report = _report(identity, scope, timed_out=False)
        result = verify_canary_fault("consumer_misconfigured", fault_report)
        assert result.fault_detected is True
        _record_fault_and_assert_history(
            store,
            scope,
            fault_report,
            "canary_consumer_configuration_invalid",
        )

        broker.provision(scope, misconfigured=False)
        recovery_report = _run_recovery(store, scope)
        _assert_recovery_history(store, scope, "canary_consumer_configuration_invalid")
        _assert_fault_recovered(result, recovery_report)


def test_fault_drill_handler_failure_preserves_secondary_failure_evidence_and_recovers():
    scope = _new_scope()
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    failure_lane = DurableConsumerFailureLane(
        store=PostgresConsumerFailureStore(lab.DATABASE_URL),
        retry_policy=ConsumerRetryPolicy(
            base_delay_seconds=1,
            max_delay_seconds=1,
            jitter_ratio=0,
            max_attempts=3,
        ),
    )

    def broken_handler(_event, _transaction) -> None:
        raise RuntimeError("fault drill injected handler failure")

    with FaultNatsLab(NATS_URL) as broker:
        broker.provision(scope)
        identity = _start_tracked(store, scope, timeout_seconds=FAULT_TIMEOUT_SECONDS)
        assert _publish_all(scope).published == 1
        consume = _consume_until_event(
            scope,
            identity.event_id,
            handler=broken_handler,
            failure_lane=failure_lane,
        )
        assert consume.failure_code == "handler_failure"
        assert consume.deferred == 1
        assert consume.acknowledged == 1

        with lab.connect() as conn:
            failure = conn.execute(
                """
                SELECT status, failure_code
                FROM kernel_lab.domain_event_consumer_failures
                WHERE consumer_name = %s AND event_id = %s
                """,
                (scope.consumer, identity.event_id),
            ).fetchone()
        assert failure == ("deferred", "handler_failure")

        _expire_fault_deadline()
        fault_report = _report(identity, scope, timed_out=True)
        result = verify_canary_fault(
            "inbox_handler_failure",
            fault_report,
            secondary_code="handler_failure",
        )
        assert result.fault_detected is True
        _record_fault_and_assert_history(store, scope, fault_report, "canary_delivery_timeout")

        recovery_report = _run_recovery(store, scope)
        _assert_recovery_history(store, scope, "canary_delivery_timeout")
        _assert_fault_recovered(result, recovery_report)


def test_fault_drill_ack_confirmation_failure_is_distinct_and_recovers():
    scope = _new_scope()
    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    with FaultNatsLab(NATS_URL) as broker:
        broker.provision(scope)
        identity = _start_tracked(store, scope, timeout_seconds=FAULT_TIMEOUT_SECONDS)
        assert _publish_all(scope).published == 1

        source = NatsJetStreamPullSource(
            server_url=NATS_URL,
            stream_name=scope.stream,
            durable_name=scope.durable,
            client_name=f"fault-no-ack-{uuid4().hex[:8]}",
            fetch_timeout_seconds=1,
        )
        try:
            delivery = source.fetch_one()
            assert delivery is not None
            assert delivery.malformed is None
            assert delivery.envelope is not None
            event = ConsumedEvent.from_envelope(delivery.envelope)
            assert event.event_id == identity.event_id
            applied = PostgresInboxStore(lab.DATABASE_URL).process_once(
                consumer_name=scope.consumer,
                event=event,
                delivery_metadata=delivery.metadata.to_dict(),
                handler=EventPipelineCanaryHandler(consumer_name=scope.consumer),
            )
            assert applied is True

            _expire_fault_deadline()
            fault_report = _report(identity, scope, timed_out=True)
            assert fault_report.snapshot.database.projected_at is not None
            assert fault_report.snapshot.ack_confirmed is False
            result = verify_canary_fault("ack_confirmation_failure", fault_report)
            assert result.fault_detected is True
            _record_fault_and_assert_history(store, scope, fault_report, "canary_ack_timeout")
            delivery.ack()
        finally:
            source.close()

        recovery_report = _run_recovery(store, scope)
        _assert_recovery_history(store, scope, "canary_ack_timeout")
        _assert_fault_recovered(result, recovery_report)


def test_fault_drill_postgres_unavailable_is_an_explicit_control_plane_dependency():
    scope = _new_scope()
    bad_store = PostgresEventPipelineCanarySliStore(
        "postgresql://postgres:postgres@127.0.0.1:1/kernel_lab?connect_timeout=1"
    )
    with pytest.raises(psycopg.OperationalError):
        _start_tracked(bad_store, scope, timeout_seconds=FAULT_TIMEOUT_SECONDS)

    result = verify_control_plane_fault(
        "postgres_unavailable",
        secondary_code="postgres_probe_store_unavailable",
    )
    assert result.fault_detected is True

    store = PostgresEventPipelineCanarySliStore(lab.DATABASE_URL)
    with FaultNatsLab(NATS_URL) as broker:
        broker.provision(scope)
        recovery_report = _run_recovery(store, scope)
        short = store.snapshot(
            stream_name=scope.stream,
            durable_name=scope.durable,
            consumer_name=scope.consumer,
        ).window(300)
        assert short.total_runs == 1
        assert short.successful_runs == 1
        assert short.failed_runs == 0
        _assert_fault_recovered(
            result,
            recovery_report,
            historical_failure_retained=False,
        )
