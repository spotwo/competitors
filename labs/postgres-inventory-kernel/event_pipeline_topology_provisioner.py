from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Protocol

import nats
from nats.aio.client import Client as NatsClient
from nats.js.errors import NotFoundError

from event_pipeline_topology import TopologyResourceSnapshot
from event_pipeline_topology_migration import TopologyContractInspector, TopologyMigrationConfig
from event_pipeline_topology_preflight import (
    EventPipelineTopologyPreflightPlanner,
    EventPipelineTopologyPreflightReport,
    TopologyPlanChange,
)

ProvisioningStatus = Literal["succeeded", "blocked", "rolled_back", "failed"]
ResourceKind = Literal["stream", "business_consumer", "canary_consumer"]

SUPPORTED_MUTATION_FIELDS: Mapping[str, frozenset[str]] = {
    "stream": frozenset({"replicas", "duplicate_window_seconds"}),
    "business_consumer": frozenset({"max_ack_pending", "max_deliver"}),
    "canary_consumer": frozenset({"max_ack_pending", "max_deliver"}),
}


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value)).lower()


def _seconds(value: Any) -> float | None:
    if value is None:
        return None
    total_seconds = getattr(value, "total_seconds", None)
    return float(total_seconds()) if callable(total_seconds) else float(value)


def _resource_name(spec: Any, resource: ResourceKind) -> str:
    if resource == "stream":
        return spec.transport.stream
    if resource == "business_consumer":
        return spec.consumer.durable
    return spec.canary.durable


def _resource_snapshot(
    resource: ResourceKind,
    *,
    name: str,
    info: Any,
) -> TopologyResourceSnapshot:
    if info is None:
        return TopologyResourceSnapshot(resource, name, False, {})

    config = info.config
    if resource == "stream":
        fields = {
            "subjects": tuple(sorted(config.subjects or ())),
            "storage": _enum_value(config.storage),
            "retention": _enum_value(config.retention),
            "replicas": int(config.num_replicas),
            "duplicate_window_seconds": _seconds(config.duplicate_window),
        }
    else:
        fields = {
            "delivery_mode": "pull" if config.deliver_subject is None else "push",
            "deliver_policy": _enum_value(config.deliver_policy),
            "ack_policy": _enum_value(config.ack_policy),
            "filter_subject": config.filter_subject,
            "max_ack_pending": int(config.max_ack_pending),
            "max_deliver": int(config.max_deliver),
        }
    return TopologyResourceSnapshot(resource, name, True, fields)


def _snapshot_matches(
    actual: TopologyResourceSnapshot,
    expected: TopologyResourceSnapshot,
) -> bool:
    if (
        actual.resource != expected.resource
        or actual.name != expected.name
        or actual.exists != expected.exists
    ):
        return False
    if not actual.exists:
        return True
    if actual.fields.keys() != expected.fields.keys():
        return False
    for field, expected_value in expected.fields.items():
        actual_value = actual.fields[field]
        if (
            field == "duplicate_window_seconds"
            and actual_value is not None
            and expected_value is not None
            and abs(float(actual_value) - float(expected_value)) <= 1e-6
        ):
            continue
        if actual_value != expected_value:
            return False
    return True


def _target_value(change: TopologyPlanChange) -> int | float:
    if change.field in ("replicas", "max_ack_pending", "max_deliver"):
        return int(change.target)
    if change.field == "duplicate_window_seconds":
        return float(change.target)
    raise ValueError(f"unsupported topology mutation field: {change.resource}.{change.field}")


def _status(report: Any) -> str | None:
    return getattr(report, "status", None)


class ReadinessVerifier(Protocol):
    def __call__(self) -> Any: ...


class CanaryVerifier(Protocol):
    def __call__(self) -> Any: ...


@dataclass(frozen=True)
class MutationReceipt:
    resource: ResourceKind
    fields: tuple[str, ...]
    source: TopologyResourceSnapshot
    target_verified: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource": self.resource,
            "fields": list(self.fields),
            "source": self.source.to_dict(),
            "target_verified": self.target_verified,
        }


@dataclass(frozen=True)
class RollbackReceipt:
    attempted: bool
    restored: bool
    source_verified: bool
    code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempted": self.attempted,
            "restored": self.restored,
            "source_verified": self.source_verified,
            "code": self.code,
        }


@dataclass(frozen=True)
class AuthorizedTopologyProvisioningReport:
    observed_at: datetime
    pipeline_id: str
    status: ProvisioningStatus
    code: str
    authorized_migration_id: str
    preflight: EventPipelineTopologyPreflightReport
    readiness_before: Any | None
    mutation: MutationReceipt | None
    canary_after: Any | None
    readiness_after: Any | None
    rollback: RollbackReceipt

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        _required_name(self.pipeline_id, "pipeline_id")
        _required_name(self.code, "code")
        _required_name(self.authorized_migration_id, "authorized_migration_id")
        if self.status == "succeeded":
            if self.mutation is None or not self.mutation.target_verified:
                raise ValueError("successful provisioning requires verified target mutation")
            if self.rollback.attempted:
                raise ValueError("successful provisioning cannot include rollback")
        if self.status == "rolled_back" and not self.rollback.attempted:
            raise ValueError("rolled-back provisioning requires rollback attempt")

    def to_dict(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at.isoformat(),
            "pipeline": self.pipeline_id,
            "status": self.status,
            "code": self.code,
            "authorization": {
                "migration_id": self.authorized_migration_id,
                "automatic_recreate": False,
                "single_resource_only": True,
            },
            "preflight": self.preflight.to_dict(),
            "readiness_before": (
                self.readiness_before.to_dict() if self.readiness_before is not None else None
            ),
            "mutation": self.mutation.to_dict() if self.mutation is not None else None,
            "canary_after": self.canary_after.to_dict() if self.canary_after is not None else None,
            "readiness_after": (
                self.readiness_after.to_dict() if self.readiness_after is not None else None
            ),
            "rollback": self.rollback.to_dict(),
            "cleanup": {
                "remove_topology_migration_contract": self.status == "succeeded",
            },
        }


class TopologyMutationError(RuntimeError):
    def __init__(
        self,
        code: str,
        *,
        source_config: Any | None = None,
        source_snapshot: TopologyResourceSnapshot | None = None,
    ):
        super().__init__(code)
        self.code = code
        self.source_config = source_config
        self.source_snapshot = source_snapshot


class NatsJetStreamTopologyMutator:
    """Apply one guarded in-place update and retain the exact rollback config."""

    def __init__(
        self,
        server_url: str,
        *,
        client_name: str = "spotwo-wms-topology-provisioner",
        connect_timeout_seconds: float = 2.0,
    ):
        self.server_url = _required_name(server_url, "server_url")
        self.client_name = _required_name(client_name, "client_name")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        self.connect_timeout_seconds = float(connect_timeout_seconds)

    def apply(
        self,
        *,
        spec: Any,
        resource: ResourceKind,
        changes: tuple[TopologyPlanChange, ...],
        expected_source: TopologyResourceSnapshot,
    ) -> tuple[TopologyResourceSnapshot, Any]:
        runner = asyncio.Runner()
        try:
            return runner.run(
                self._apply(
                    spec=spec,
                    resource=resource,
                    changes=changes,
                    expected_source=expected_source,
                )
            )
        finally:
            runner.close()

    def rollback(self, *, spec: Any, resource: ResourceKind, source_config: Any) -> None:
        runner = asyncio.Runner()
        try:
            runner.run(
                self._rollback(
                    spec=spec,
                    resource=resource,
                    source_config=source_config,
                )
            )
        finally:
            runner.close()

    async def _connect(self) -> NatsClient:
        return await asyncio.wait_for(
            nats.connect(
                servers=[self.server_url],
                name=self.client_name,
                allow_reconnect=False,
                connect_timeout=self.connect_timeout_seconds,
                reconnect_time_wait=0,
                max_reconnect_attempts=1,
            ),
            timeout=self.connect_timeout_seconds,
        )

    async def _read_info(self, jetstream: Any, spec: Any, resource: ResourceKind) -> Any:
        try:
            if resource == "stream":
                return await jetstream.stream_info(spec.transport.stream)
            durable = (
                spec.consumer.durable
                if resource == "business_consumer"
                else spec.canary.durable
            )
            return await jetstream.consumer_info(spec.transport.stream, durable)
        except NotFoundError:
            return None

    async def _write_config(
        self,
        jetstream: Any,
        spec: Any,
        resource: ResourceKind,
        config: Any,
    ) -> None:
        if resource == "stream":
            await jetstream.update_stream(config)
            return
        # nats-py uses the durable consumer create endpoint as create-or-update.
        # The provisioner reaches this call only after proving the durable exists.
        await jetstream.add_consumer(spec.transport.stream, config)

    @staticmethod
    def _apply_changes(
        resource: ResourceKind,
        config: Any,
        changes: tuple[TopologyPlanChange, ...],
    ) -> None:
        for change in changes:
            if change.resource != resource:
                raise ValueError("mutation changes must target exactly one resource")
            if change.field not in SUPPORTED_MUTATION_FIELDS[resource]:
                raise ValueError(
                    f"unsupported topology mutation field: {resource}.{change.field}"
                )
            value = _target_value(change)
            if resource == "stream":
                if change.field == "replicas":
                    config.num_replicas = value
                else:
                    config.duplicate_window = value
            elif change.field == "max_ack_pending":
                config.max_ack_pending = value
            else:
                config.max_deliver = value

    async def _apply(
        self,
        *,
        spec: Any,
        resource: ResourceKind,
        changes: tuple[TopologyPlanChange, ...],
        expected_source: TopologyResourceSnapshot,
    ) -> tuple[TopologyResourceSnapshot, Any]:
        client: NatsClient | None = None
        try:
            client = await self._connect()
            jetstream = client.jetstream()
            info = await self._read_info(jetstream, spec, resource)
            current = _resource_snapshot(
                resource,
                name=_resource_name(spec, resource),
                info=info,
            )
            if not _snapshot_matches(current, expected_source):
                raise TopologyMutationError(
                    "topology_source_changed_before_apply",
                    source_snapshot=current,
                )
            if info is None:
                raise TopologyMutationError(
                    "topology_source_resource_missing",
                    source_snapshot=current,
                )

            source_config = deepcopy(info.config)
            target_config = deepcopy(info.config)
            self._apply_changes(resource, target_config, changes)
            try:
                await self._write_config(jetstream, spec, resource, target_config)
            except Exception as exc:
                raise TopologyMutationError(
                    "topology_mutation_failed",
                    source_config=source_config,
                    source_snapshot=current,
                ) from exc
            return current, source_config
        finally:
            if client is not None and not client.is_closed:
                try:
                    await client.close()
                except Exception:
                    pass

    async def _rollback(
        self,
        *,
        spec: Any,
        resource: ResourceKind,
        source_config: Any,
    ) -> None:
        client: NatsClient | None = None
        try:
            client = await self._connect()
            await self._write_config(
                client.jetstream(),
                spec,
                resource,
                deepcopy(source_config),
            )
        finally:
            if client is not None and not client.is_closed:
                try:
                    await client.close()
                except Exception:
                    pass


class AuthorizedEventPipelineTopologyProvisioner:
    """Apply one preflight-authorized topology mutation with verification and rollback."""

    def __init__(
        self,
        server_url: str,
        *,
        preflight_planner: EventPipelineTopologyPreflightPlanner | None = None,
        contract_inspector: TopologyContractInspector | None = None,
        mutator: NatsJetStreamTopologyMutator | None = None,
    ):
        self.server_url = _required_name(server_url, "server_url")
        self.preflight_planner = preflight_planner or EventPipelineTopologyPreflightPlanner(
            server_url
        )
        self.contract_inspector = contract_inspector or TopologyContractInspector(server_url)
        self.mutator = mutator or NatsJetStreamTopologyMutator(server_url)

    @staticmethod
    def _terminal(
        *,
        observed_at: datetime,
        spec: Any,
        status: ProvisioningStatus,
        code: str,
        migration_id: str,
        preflight: EventPipelineTopologyPreflightReport,
        readiness_before: Any | None = None,
        mutation: MutationReceipt | None = None,
        canary_after: Any | None = None,
        readiness_after: Any | None = None,
        rollback: RollbackReceipt | None = None,
    ) -> AuthorizedTopologyProvisioningReport:
        return AuthorizedTopologyProvisioningReport(
            observed_at=observed_at,
            pipeline_id=spec.pipeline_id,
            status=status,
            code=code,
            authorized_migration_id=migration_id,
            preflight=preflight,
            readiness_before=readiness_before,
            mutation=mutation,
            canary_after=canary_after,
            readiness_after=readiness_after,
            rollback=rollback or RollbackReceipt(False, False, False, None),
        )

    def execute(
        self,
        spec: Any,
        *,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        authorized_migration_id: str,
        readiness_verifier: ReadinessVerifier,
        canary_verifier: CanaryVerifier,
        observed_at: datetime | None = None,
    ) -> AuthorizedTopologyProvisioningReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        migration_id = _required_name(authorized_migration_id, "authorized_migration_id")
        preflight = self.preflight_planner.plan(
            spec,
            target_topology=target_topology,
            migration_mapping=migration_mapping,
            observed_at=observed_at,
        )

        if migration_mapping is None:
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_migration_required",
                migration_id=migration_id,
                preflight=preflight,
            )

        migration = TopologyMigrationConfig.from_mapping(migration_mapping)
        if migration.migration_id != migration_id:
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_migration_authorization_mismatch",
                migration_id=migration_id,
                preflight=preflight,
            )
        if preflight.decision != "safe_to_apply":
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code=preflight.code,
                migration_id=migration_id,
                preflight=preflight,
            )

        resources = tuple(dict.fromkeys(change.resource for change in preflight.changes))
        if len(resources) != 1:
            raise RuntimeError("safe preflight must contain exactly one resource")
        resource = resources[0]
        if any(
            change.field not in SUPPORTED_MUTATION_FIELDS.get(resource, frozenset())
            for change in preflight.changes
        ):
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_mutation_not_supported",
                migration_id=migration_id,
                preflight=preflight,
            )

        readiness_before = readiness_verifier()
        if _status(readiness_before) != "ready":
            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="blocked",
                code="topology_preapply_readiness_not_ready",
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
            )

        source_snapshot = next(
            item for item in preflight.contract.target.resources if item.resource == resource
        )
        source_config: Any | None = None
        mutation: MutationReceipt | None = None
        canary_after: Any | None = None
        readiness_after: Any | None = None

        try:
            actual_source, source_config = self.mutator.apply(
                spec=spec,
                resource=resource,
                changes=preflight.changes,
                expected_source=source_snapshot,
            )
            mutation = MutationReceipt(
                resource=resource,
                fields=tuple(change.field for change in preflight.changes),
                source=actual_source,
                target_verified=False,
            )

            contract_after = self.contract_inspector.inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
            )
            if contract_after.matched != "target" or contract_after.target.status != "ok":
                raise RuntimeError("topology_target_verification_failed")
            mutation = MutationReceipt(
                resource=resource,
                fields=mutation.fields,
                source=actual_source,
                target_verified=True,
            )

            canary_after = canary_verifier()
            if _status(canary_after) != "ok":
                raise RuntimeError("topology_postapply_canary_not_ok")

            readiness_after = readiness_verifier()
            if _status(readiness_after) != "ready":
                raise RuntimeError("topology_postapply_readiness_not_ready")

            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="succeeded",
                code="topology_target_applied_and_verified",
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
                mutation=mutation,
                canary_after=canary_after,
                readiness_after=readiness_after,
            )
        except Exception as exc:
            if isinstance(exc, TopologyMutationError):
                source_config = source_config or exc.source_config
                failure_code = exc.code
            else:
                failure_code = str(exc)
            if failure_code not in {
                "topology_source_changed_before_apply",
                "topology_source_resource_missing",
                "topology_target_verification_failed",
                "topology_postapply_canary_not_ok",
                "topology_postapply_readiness_not_ready",
            }:
                failure_code = "topology_mutation_failed"

            if source_config is None:
                return self._terminal(
                    observed_at=observed_at,
                    spec=spec,
                    status="failed",
                    code=failure_code,
                    migration_id=migration_id,
                    preflight=preflight,
                    readiness_before=readiness_before,
                    mutation=mutation,
                    canary_after=canary_after,
                    readiness_after=readiness_after,
                )

            try:
                self.mutator.rollback(
                    spec=spec,
                    resource=resource,
                    source_config=source_config,
                )
            except Exception:
                return self._terminal(
                    observed_at=observed_at,
                    spec=spec,
                    status="failed",
                    code="topology_rollback_failed",
                    migration_id=migration_id,
                    preflight=preflight,
                    readiness_before=readiness_before,
                    mutation=mutation,
                    canary_after=canary_after,
                    readiness_after=readiness_after,
                    rollback=RollbackReceipt(
                        True, False, False, "topology_rollback_failed"
                    ),
                )

            try:
                rollback_contract = self.contract_inspector.inspect(
                    spec,
                    target_topology=target_topology,
                    migration_mapping=migration_mapping,
                )
                source_verified = rollback_contract.matched == "source"
            except Exception:
                source_verified = False

            if not source_verified:
                return self._terminal(
                    observed_at=observed_at,
                    spec=spec,
                    status="failed",
                    code="topology_rollback_verification_failed",
                    migration_id=migration_id,
                    preflight=preflight,
                    readiness_before=readiness_before,
                    mutation=mutation,
                    canary_after=canary_after,
                    readiness_after=readiness_after,
                    rollback=RollbackReceipt(
                        True,
                        True,
                        False,
                        "topology_rollback_verification_failed",
                    ),
                )

            return self._terminal(
                observed_at=observed_at,
                spec=spec,
                status="rolled_back",
                code=failure_code,
                migration_id=migration_id,
                preflight=preflight,
                readiness_before=readiness_before,
                mutation=mutation,
                canary_after=canary_after,
                readiness_after=readiness_after,
                rollback=RollbackReceipt(True, True, True, None),
            )
