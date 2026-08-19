from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from uuid import uuid4

import nats
import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

import conftest as lab
from event_pipeline_readiness import EventPipelineDeploymentRegistry
from event_pipeline_topology import TopologyResourceSnapshot
from event_pipeline_topology_crash_safe_provisioner import (
    CrashSafeAuthorizedEventPipelineTopologyProvisioner,
    RecoverableNatsJetStreamTopologyMutator,
)
from event_pipeline_topology_preflight import EventPipelineTopologyPreflightPlanner, TopologyPlanChange
from event_pipeline_topology_provisioning_journal import (
    PostgresTopologyProvisioningJournal,
    TopologyProvisioningJournalError,
)

NATS_URL = os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")


@dataclass(frozen=True)
class StatusReport:
    status: str
    code: str

    def to_dict(self):
        return {"status": self.status, "code": self.code}


def deployment_spec(*, stream: str, business: str, canary: str, prefix: str):
    data = {
        "version": 1,
        "pipelines": [
            {
                "id": "inventory-position-projection",
                "enabled": True,
                "runtime": {
                    "database_url_env": "DATABASE_URL",
                    "nats_url_env": "NATS_URL",
                    "watchdog_state_path_env": "WATCHDOG_STATE_PATH",
                },
                "transport": {"stream": stream, "subject_prefix": prefix},
                "consumer": {
                    "durable": business,
                    "inbox_consumer_name": "position_projection",
                    "malformed_lookback_seconds": 300,
                },
                "canary": {
                    "durable": canary,
                    "consumer_name": "event_pipeline_canary",
                    "cadence_seconds": 60,
                    "timeout_seconds": 30,
                },
                "slo": {
                    "availability_target": 0.999,
                    "latency_target_ratio": 0.99,
                    "latency_target_seconds": 5,
                    "stale_after_seconds": 180,
                    "warning_consecutive_failures": 2,
                    "critical_consecutive_failures": 3,
                    "warning_slow_burn_rate": 3.0,
                    "critical_fast_burn_rate": 14.4,
                },
                "watchdog": {
                    "start_grace_seconds": 30,
                    "warning_missing_executions": 1,
                    "critical_missing_executions": 2,
                    "warning_scheduler_lag_seconds": 15,
                    "critical_scheduler_lag_seconds": 60,
                    "execution_timeout_seconds": 45,
                },
            }
        ],
    }
    return EventPipelineDeploymentRegistry.from_mapping(data).pipelines[0]


def topology(prefix: str):
    return {
        "stream": {
            "subjects": [f"{prefix}.>"],
            "storage": "file",
            "retention": "limits",
            "replicas": 1,
            "duplicate_window_seconds": 120,
        },
        "business_consumer": {
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": f"{prefix}.inventory.position.changed",
            "max_ack_pending": 20,
            "max_deliver": 3,
        },
        "canary_consumer": {
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": f"{prefix}.health_check.ping",
            "max_ack_pending": 10,
            "max_deliver": 3,
        },
    }


def migration():
    return {
        "id": "business-ack-window-10-to-20",
        "valid_until": "2099-01-01T00:00:00Z",
        "from_overrides": {"business_consumer": {"max_ack_pending": 10}},
    }


def test_journal_prevents_two_live_owners_for_one_pipeline():
    snapshot = TopologyResourceSnapshot(
        "business_consumer",
        "POSITION_PROJECTOR",
        True,
        {
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": "spotwo.wms.events.inventory.position.changed",
            "max_ack_pending": 10,
            "max_deliver": 3,
        },
    )
    change = TopologyPlanChange(
        resource="business_consumer",
        field="max_ack_pending",
        current=10,
        target=20,
    )
    owner_a = PostgresTopologyProvisioningJournal(lab.DATABASE_URL, owner_id="owner-a", lease_seconds=120)
    owner_b = PostgresTopologyProvisioningJournal(lab.DATABASE_URL, owner_id="owner-b", lease_seconds=120)

    run = owner_a.start(
        pipeline_id="inventory-position-projection",
        migration_id=migration()["id"],
        resource="business_consumer",
        source_snapshot=snapshot,
        changes=(change,),
    )

    with pytest.raises(TopologyProvisioningJournalError) as caught:
        owner_b.claim_active(
            pipeline_id="inventory-position-projection",
            migration_id=migration()["id"],
        )

    assert caught.value.code == "topology_provisioning_lease_held"
    assert owner_a.get(run.run_id).state == "prepared"


class LiveTopology:
    def __init__(self, server_url: str):
        self.runner = asyncio.Runner()
        self.client = self.runner.run(
            nats.connect(servers=[server_url], allow_reconnect=False, connect_timeout=2)
        )
        self.js = self.client.jetstream()

    def provision(self, *, stream: str, prefix: str, business: str, canary: str):
        self.runner.run(self._provision(stream=stream, prefix=prefix, business=business, canary=canary))

    async def _provision(self, *, stream: str, prefix: str, business: str, canary: str):
        try:
            await self.js.delete_stream(stream)
        except NotFoundError:
            pass
        await self.js.add_stream(
            StreamConfig(
                name=stream,
                subjects=[f"{prefix}.>"],
                storage=StorageType.FILE,
                retention=RetentionPolicy.LIMITS,
                num_replicas=1,
                duplicate_window=120,
            )
        )
        await self.js.add_consumer(
            stream,
            ConsumerConfig(
                durable_name=business,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                filter_subject=f"{prefix}.inventory.position.changed",
                max_ack_pending=10,
                max_deliver=3,
            ),
        )
        await self.js.add_consumer(
            stream,
            ConsumerConfig(
                durable_name=canary,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                filter_subject=f"{prefix}.health_check.ping",
                max_ack_pending=10,
                max_deliver=3,
            ),
        )

    def business_max_ack_pending(self, stream: str, durable: str) -> int:
        info = self.runner.run(self.js.consumer_info(stream, durable))
        return int(info.config.max_ack_pending)

    def close(self, stream: str):
        try:
            self.runner.run(self.js.delete_stream(stream))
        except NotFoundError:
            pass
        self.runner.run(self.client.close())
        self.runner.close()


def _crashed_target_case():
    suffix = uuid4().hex[:10].upper()
    stream = f"WMS_CRASH_{suffix}"
    prefix = f"spotwo.wms.crash.{suffix.lower()}"
    business = f"POSITION_{suffix}"
    canary = f"CANARY_{suffix}"
    spec = deployment_spec(stream=stream, business=business, canary=canary, prefix=prefix)
    target = topology(prefix)
    live = LiveTopology(NATS_URL)
    live.provision(stream=stream, prefix=prefix, business=business, canary=canary)

    planner = EventPipelineTopologyPreflightPlanner(NATS_URL)
    preflight = planner.plan(spec, target_topology=target, migration_mapping=migration())
    assert preflight.decision == "safe_to_apply"
    resource = preflight.changes[0].resource
    source = next(item for item in preflight.contract.target.resources if item.resource == resource)

    owner_a = PostgresTopologyProvisioningJournal(lab.DATABASE_URL, owner_id="crashed-owner", lease_seconds=120)
    run = owner_a.start(
        pipeline_id=spec.pipeline_id,
        migration_id=migration()["id"],
        resource=resource,
        source_snapshot=source,
        changes=preflight.changes,
    )
    owner_a.transition(run.run_id, "applying")
    RecoverableNatsJetStreamTopologyMutator(NATS_URL).apply(
        spec=spec,
        resource=resource,
        changes=preflight.changes,
        expected_source=source,
    )
    assert live.business_max_ack_pending(stream, business) == 20

    with lab.connect(autocommit=True) as conn:
        conn.execute(
            """
            UPDATE kernel_lab.event_pipeline_topology_provisioning_runs
            SET lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE run_id = %s
            """,
            (run.run_id,),
        )
    return live, spec, target, run.run_id


def test_crashed_apply_with_target_live_is_reclaimed_and_completed():
    live, spec, target, run_id = _crashed_target_case()
    try:
        owner_b = PostgresTopologyProvisioningJournal(lab.DATABASE_URL, owner_id="recovery-owner", lease_seconds=120)
        report = CrashSafeAuthorizedEventPipelineTopologyProvisioner(
            NATS_URL,
            journal=owner_b,
        ).execute(
            spec,
            target_topology=target,
            migration_mapping=migration(),
            authorized_migration_id=migration()["id"],
            readiness_verifier=lambda: StatusReport("ready", "ready"),
            canary_verifier=lambda: StatusReport("ok", "fresh_canary"),
        )

        stored = owner_b.get(run_id)
        assert report.status == "succeeded"
        assert report.code == "topology_target_recovered_and_verified"
        assert stored.state == "completed"
        assert stored.recovery_count == 1
        assert stored.attempt_count == 2
        assert live.business_max_ack_pending(spec.transport.stream, spec.consumer.durable) == 20
    finally:
        live.close(spec.transport.stream)


def test_crashed_apply_with_failed_recovery_canary_rolls_back_source():
    live, spec, target, run_id = _crashed_target_case()
    try:
        owner_b = PostgresTopologyProvisioningJournal(lab.DATABASE_URL, owner_id="rollback-owner", lease_seconds=120)
        report = CrashSafeAuthorizedEventPipelineTopologyProvisioner(
            NATS_URL,
            journal=owner_b,
        ).execute(
            spec,
            target_topology=target,
            migration_mapping=migration(),
            authorized_migration_id=migration()["id"],
            readiness_verifier=lambda: StatusReport("ready", "ready"),
            canary_verifier=lambda: StatusReport("critical", "forced_failure"),
        )

        stored = owner_b.get(run_id)
        assert report.status == "rolled_back"
        assert report.code == "topology_postapply_canary_not_ok"
        assert stored.state == "rolled_back"
        assert live.business_max_ack_pending(spec.transport.stream, spec.consumer.durable) == 10
    finally:
        live.close(spec.transport.stream)
