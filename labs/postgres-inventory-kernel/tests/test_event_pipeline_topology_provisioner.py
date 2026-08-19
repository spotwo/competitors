from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

import nats
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import NotFoundError

import conftest as lab
from event_pipeline_readiness import EventPipelineDeploymentRegistry
from event_pipeline_topology import (
    EventPipelineTopologyReport,
    TopologyDrift,
    TopologyResourceSnapshot,
)
from event_pipeline_topology_migration import (
    TopologyContractReport,
    TopologyMigrationConfig,
)
from event_pipeline_topology_preflight import (
    EventPipelineTopologyPreflightReport,
    TopologyPlanChange,
    TopologyPlanStep,
)
from event_pipeline_topology_provisioner import (
    AuthorizedEventPipelineTopologyProvisioner,
)

NATS_URL = (
    lab.os.getenv("KERNEL_LAB_NATS_URL", "nats://127.0.0.1:54222")
    if hasattr(lab, "os")
    else "nats://127.0.0.1:54222"
)


@dataclass(frozen=True)
class StatusReport:
    status: str
    code: str

    def to_dict(self):
        return {"status": self.status, "code": self.code}


def deployment_spec(
    *,
    stream_name: str = "WMS_EVENTS",
    business_durable: str = "POSITION_PROJECTOR",
    canary_durable: str = "EVENT_PIPELINE_CANARY",
    subject_prefix: str = "spotwo.wms.events",
):
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
                "transport": {
                    "stream": stream_name,
                    "subject_prefix": subject_prefix,
                },
                "consumer": {
                    "durable": business_durable,
                    "inbox_consumer_name": "position_projection",
                    "malformed_lookback_seconds": 300,
                },
                "canary": {
                    "durable": canary_durable,
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


def topology(
    *,
    subject_prefix: str = "spotwo.wms.events",
    business_max_ack_pending: int = 20,
):
    return {
        "stream": {
            "subjects": [f"{subject_prefix}.>"],
            "storage": "file",
            "retention": "limits",
            "replicas": 1,
            "duplicate_window_seconds": 120,
        },
        "business_consumer": {
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": f"{subject_prefix}.inventory.position.changed",
            "max_ack_pending": business_max_ack_pending,
            "max_deliver": 3,
        },
        "canary_consumer": {
            "delivery_mode": "pull",
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "filter_subject": f"{subject_prefix}.health_check.ping",
            "max_ack_pending": 10,
            "max_deliver": 3,
        },
    }


def migration():
    return {
        "id": "business-ack-window-10-to-20",
        "valid_until": "2099-01-01T00:00:00Z",
        "from_overrides": {
            "business_consumer": {"max_ack_pending": 10},
        },
    }


def safe_preflight(spec, *, field: str = "max_ack_pending"):
    observed_at = datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc)
    source_resources = (
        TopologyResourceSnapshot(
            "stream",
            spec.transport.stream,
            True,
            {
                "subjects": ("spotwo.wms.events.>",),
                "storage": "file",
                "retention": "limits",
                "replicas": 1,
                "duplicate_window_seconds": 120.0,
            },
        ),
        TopologyResourceSnapshot(
            "business_consumer",
            spec.consumer.durable,
            True,
            {
                "delivery_mode": "pull",
                "deliver_policy": "all",
                "ack_policy": "explicit",
                "filter_subject": "spotwo.wms.events.inventory.position.changed",
                "max_ack_pending": 10,
                "max_deliver": 3,
            },
        ),
        TopologyResourceSnapshot(
            "canary_consumer",
            spec.canary.durable,
            True,
            {
                "delivery_mode": "pull",
                "deliver_policy": "all",
                "ack_policy": "explicit",
                "filter_subject": "spotwo.wms.events.health_check.ping",
                "max_ack_pending": 10,
                "max_deliver": 3,
            },
        ),
    )
    target_value = 20 if field == "max_ack_pending" else "changed"
    current_value = 10 if field == "max_ack_pending" else "current"
    drift = (
        TopologyDrift(
            "business_consumer",
            field,
            f"business_consumer_{field}_mismatch",
            target_value,
            current_value,
        ),
    )
    target_report = EventPipelineTopologyReport(
        observed_at=observed_at,
        stream_name=spec.transport.stream,
        status="critical",
        resources=source_resources,
        drift=drift,
    )
    migration_config = TopologyMigrationConfig.from_mapping(migration())
    contract = TopologyContractReport(
        observed_at=observed_at,
        status="ok",
        code="topology_migration_source_accepted",
        matched="source",
        target=target_report,
        source_drift=(),
        migration=migration_config,
        phase="active",
    )
    change = TopologyPlanChange(
        resource="business_consumer",
        field=field,
        current=current_value,
        target=target_value,
    )
    return EventPipelineTopologyPreflightReport(
        observed_at=observed_at,
        pipeline_id=spec.pipeline_id,
        status="ok",
        decision="safe_to_apply",
        code="topology_migration_ready",
        contract=contract,
        changes=(change,),
        steps=(
            TopologyPlanStep(
                order=1,
                code="apply_target_resource",
                resource="business_consumer",
                fields=(field,),
                requires_atomic_resource_update=True,
            ),
        ),
    )


class FixedPlanner:
    def __init__(self, report):
        self.report = report

    def plan(self, _spec, **_kwargs):
        return self.report


class NoopMutator:
    def __init__(self, source_snapshot):
        self.source_snapshot = source_snapshot
        self.rollback_calls = 0

    def apply(self, **_kwargs):
        return self.source_snapshot, object()

    def rollback(self, **_kwargs):
        self.rollback_calls += 1


class SequencedInspector:
    def __init__(self, reports):
        self.reports = list(reports)

    def inspect(self, *_args, **_kwargs):
        return self.reports.pop(0)


def target_contract(preflight):
    return TopologyContractReport(
        observed_at=preflight.observed_at,
        status="ok",
        code=None,
        matched="target",
        target=EventPipelineTopologyReport(
            observed_at=preflight.observed_at,
            stream_name=preflight.contract.target.stream_name,
            status="ok",
            resources=preflight.contract.target.resources,
            drift=(),
        ),
        source_drift=preflight.contract.source_drift,
        migration=preflight.contract.migration,
        phase="active",
    )


def source_contract(preflight):
    return preflight.contract


def test_authorization_must_match_reviewed_migration_id():
    spec = deployment_spec()
    preflight = safe_preflight(spec)
    provisioner = AuthorizedEventPipelineTopologyProvisioner(
        "nats://unused",
        preflight_planner=FixedPlanner(preflight),
        contract_inspector=SequencedInspector([]),
        mutator=NoopMutator(preflight.contract.target.resources[1]),
    )

    report = provisioner.execute(
        spec,
        target_topology=topology(),
        migration_mapping=migration(),
        authorized_migration_id="different-migration",
        readiness_verifier=lambda: StatusReport("ready", "ready"),
        canary_verifier=lambda: StatusReport("ok", "ok"),
        observed_at=preflight.observed_at,
    )

    assert report.status == "blocked"
    assert report.code == "topology_migration_authorization_mismatch"
    assert report.mutation is None


def test_unsupported_in_place_field_is_blocked_before_readiness_or_mutation():
    spec = deployment_spec()
    preflight = safe_preflight(spec, field="filter_subject")
    provisioner = AuthorizedEventPipelineTopologyProvisioner(
        "nats://unused",
        preflight_planner=FixedPlanner(preflight),
        contract_inspector=SequencedInspector([]),
        mutator=NoopMutator(preflight.contract.target.resources[1]),
    )

    report = provisioner.execute(
        spec,
        target_topology=topology(),
        migration_mapping=migration(),
        authorized_migration_id=migration()["id"],
        readiness_verifier=lambda: (_ for _ in ()).throw(AssertionError("must not run")),
        canary_verifier=lambda: (_ for _ in ()).throw(AssertionError("must not run")),
        observed_at=preflight.observed_at,
    )

    assert report.status == "blocked"
    assert report.code == "topology_mutation_not_supported"


def test_preapply_readiness_must_be_ready():
    spec = deployment_spec()
    preflight = safe_preflight(spec)
    provisioner = AuthorizedEventPipelineTopologyProvisioner(
        "nats://unused",
        preflight_planner=FixedPlanner(preflight),
        contract_inspector=SequencedInspector([]),
        mutator=NoopMutator(preflight.contract.target.resources[1]),
    )

    report = provisioner.execute(
        spec,
        target_topology=topology(),
        migration_mapping=migration(),
        authorized_migration_id=migration()["id"],
        readiness_verifier=lambda: StatusReport("degraded", "sli_warning"),
        canary_verifier=lambda: StatusReport("ok", "ok"),
        observed_at=preflight.observed_at,
    )

    assert report.status == "blocked"
    assert report.code == "topology_preapply_readiness_not_ready"
    assert report.mutation is None


def test_success_requires_target_canary_and_postapply_readiness():
    spec = deployment_spec()
    preflight = safe_preflight(spec)
    mutator = NoopMutator(preflight.contract.target.resources[1])
    readiness_reports = iter(
        [StatusReport("ready", "before"), StatusReport("ready", "after")]
    )
    provisioner = AuthorizedEventPipelineTopologyProvisioner(
        "nats://unused",
        preflight_planner=FixedPlanner(preflight),
        contract_inspector=SequencedInspector([target_contract(preflight)]),
        mutator=mutator,
    )

    report = provisioner.execute(
        spec,
        target_topology=topology(),
        migration_mapping=migration(),
        authorized_migration_id=migration()["id"],
        readiness_verifier=lambda: next(readiness_reports),
        canary_verifier=lambda: StatusReport("ok", "fresh_canary"),
        observed_at=preflight.observed_at,
    )

    assert report.status == "succeeded"
    assert report.code == "topology_target_applied_and_verified"
    assert report.mutation.target_verified is True
    assert report.canary_after.status == "ok"
    assert report.readiness_after.status == "ready"
    assert report.rollback.attempted is False
    assert report.to_dict()["cleanup"]["remove_topology_migration_contract"] is True


def test_postapply_canary_failure_rolls_back_and_verifies_source():
    spec = deployment_spec()
    preflight = safe_preflight(spec)
    mutator = NoopMutator(preflight.contract.target.resources[1])
    provisioner = AuthorizedEventPipelineTopologyProvisioner(
        "nats://unused",
        preflight_planner=FixedPlanner(preflight),
        contract_inspector=SequencedInspector(
            [target_contract(preflight), source_contract(preflight)]
        ),
        mutator=mutator,
    )

    report = provisioner.execute(
        spec,
        target_topology=topology(),
        migration_mapping=migration(),
        authorized_migration_id=migration()["id"],
        readiness_verifier=lambda: StatusReport("ready", "ready"),
        canary_verifier=lambda: StatusReport("critical", "canary_failure"),
        observed_at=preflight.observed_at,
    )

    assert report.status == "rolled_back"
    assert report.code == "topology_postapply_canary_not_ok"
    assert report.rollback.restored is True
    assert report.rollback.source_verified is True
    assert mutator.rollback_calls == 1


class LiveTopology:
    def __init__(self, server_url: str):
        self._runner = asyncio.Runner()
        self._client = self._runner.run(
            nats.connect(servers=[server_url], allow_reconnect=False, connect_timeout=2)
        )
        self._jetstream = self._client.jetstream()

    def provision(
        self,
        *,
        stream_name: str,
        subject_prefix: str,
        business_durable: str,
        canary_durable: str,
        business_max_ack_pending: int = 10,
    ):
        self._runner.run(
            self._provision(
                stream_name=stream_name,
                subject_prefix=subject_prefix,
                business_durable=business_durable,
                canary_durable=canary_durable,
                business_max_ack_pending=business_max_ack_pending,
            )
        )

    def business_max_ack_pending(self, stream_name: str, durable: str) -> int:
        info = self._runner.run(self._jetstream.consumer_info(stream_name, durable))
        return int(info.config.max_ack_pending)

    def close(self, stream_name: str):
        try:
            self._runner.run(self._delete_if_present(stream_name))
            self._runner.run(self._client.close())
        finally:
            self._runner.close()

    async def _provision(
        self,
        *,
        stream_name: str,
        subject_prefix: str,
        business_durable: str,
        canary_durable: str,
        business_max_ack_pending: int,
    ):
        await self._delete_if_present(stream_name)
        await self._jetstream.add_stream(
            StreamConfig(
                name=stream_name,
                subjects=[f"{subject_prefix}.>"],
                storage=StorageType.FILE,
                retention=RetentionPolicy.LIMITS,
                num_replicas=1,
                duplicate_window=120,
            )
        )
        await self._jetstream.add_consumer(
            stream_name,
            ConsumerConfig(
                durable_name=business_durable,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                filter_subject=f"{subject_prefix}.inventory.position.changed",
                max_ack_pending=business_max_ack_pending,
                max_deliver=3,
            ),
        )
        await self._jetstream.add_consumer(
            stream_name,
            ConsumerConfig(
                durable_name=canary_durable,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                filter_subject=f"{subject_prefix}.health_check.ping",
                max_ack_pending=10,
                max_deliver=3,
            ),
        )

    async def _delete_if_present(self, stream_name: str):
        try:
            await self._jetstream.delete_stream(stream_name)
        except NotFoundError:
            pass


def _live_case():
    suffix = uuid4().hex.upper()
    stream_name = f"WMS_PROVISION_{suffix}"
    subject_prefix = f"spotwo.wms.provision.{suffix.lower()}"
    business_durable = f"POSITION_{suffix}"
    canary_durable = f"CANARY_{suffix}"
    spec = deployment_spec(
        stream_name=stream_name,
        business_durable=business_durable,
        canary_durable=canary_durable,
        subject_prefix=subject_prefix,
    )
    target = topology(
        subject_prefix=subject_prefix,
        business_max_ack_pending=20,
    )
    return stream_name, subject_prefix, business_durable, canary_durable, spec, target


def test_live_authorized_consumer_update_reaches_exact_target():
    (
        stream_name,
        subject_prefix,
        business_durable,
        canary_durable,
        spec,
        target,
    ) = _live_case()
    live = LiveTopology(NATS_URL)
    live.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        business_durable=business_durable,
        canary_durable=canary_durable,
    )

    try:
        report = AuthorizedEventPipelineTopologyProvisioner(NATS_URL).execute(
            spec,
            target_topology=target,
            migration_mapping=migration(),
            authorized_migration_id=migration()["id"],
            readiness_verifier=lambda: StatusReport("ready", "lab"),
            canary_verifier=lambda: StatusReport("ok", "lab"),
        )

        assert report.status == "succeeded"
        assert report.mutation.resource == "business_consumer"
        assert report.mutation.fields == ("max_ack_pending",)
        assert live.business_max_ack_pending(stream_name, business_durable) == 20
    finally:
        live.close(stream_name)


def test_live_failed_postapply_verification_restores_source_consumer_config():
    (
        stream_name,
        subject_prefix,
        business_durable,
        canary_durable,
        spec,
        target,
    ) = _live_case()
    live = LiveTopology(NATS_URL)
    live.provision(
        stream_name=stream_name,
        subject_prefix=subject_prefix,
        business_durable=business_durable,
        canary_durable=canary_durable,
    )

    try:
        report = AuthorizedEventPipelineTopologyProvisioner(NATS_URL).execute(
            spec,
            target_topology=target,
            migration_mapping=migration(),
            authorized_migration_id=migration()["id"],
            readiness_verifier=lambda: StatusReport("ready", "lab"),
            canary_verifier=lambda: StatusReport("critical", "forced_failure"),
        )

        assert report.status == "rolled_back"
        assert report.rollback.restored is True
        assert report.rollback.source_verified is True
        assert live.business_max_ack_pending(stream_name, business_durable) == 10
    finally:
        live.close(stream_name)
