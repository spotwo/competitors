"""Opt-in per-tenant deployment contract, broker provisioning and live readiness.

Does NOT migrate shared publishers, deploy services, grant DB roles or manage
production broker credentials. The broker proof uses real NATS permissions.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from uuid import UUID, uuid5
from typing import Any, Mapping

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError

from event_tenant_routing import TenantEventRoute, tenant_uuid

_ENV = re.compile(r"^[A-Z][A-Z0-9_]*$")
_ID = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def _name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value.strip() != value:
        raise ValueError(f"{field} must be a nonblank normalized string")
    return value


def _env(value: Any, field: str) -> str:
    if not _ENV.fullmatch(_name(value, field)):
        raise ValueError(f"{field} must be an uppercase environment variable name")
    return value


@dataclass(frozen=True)
class TenantDeployment:
    deployment_id: str
    pipeline_id: str
    tenant_id: UUID
    enabled: bool
    business_durable: str
    inbox_consumer_name: str
    canary_durable: str
    provisioner_nats_url_env: str
    publisher_nats_url_env: str
    consumer_nats_url_env: str
    canary_consumer_nats_url_env: str

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "TenantDeployment":
        if not isinstance(data, Mapping):
            raise ValueError("tenant deployment must be an object")
        if not isinstance(data.get("enabled"), bool):
            raise ValueError("tenant deployment enabled must be boolean")
        identifier = _name(data.get("id"), "tenant deployment id")
        if not _ID.fullmatch(identifier):
            raise ValueError("tenant deployment id must be kebab-case")
        kwargs = {
            field: _env(data.get(field), field) for field in (
                "provisioner_nats_url_env", "publisher_nats_url_env",
                "consumer_nats_url_env", "canary_consumer_nats_url_env",
            )
        }
        result = cls(
            deployment_id=identifier,
            pipeline_id=_name(data.get("pipeline_id"), "pipeline_id"),
            tenant_id=tenant_uuid(data.get("tenant_id")),
            enabled=data["enabled"],
            business_durable=_name(data.get("business_durable"), "business_durable"),
            inbox_consumer_name=_name(data.get("inbox_consumer_name"), "inbox_consumer_name"),
            canary_durable=_name(data.get("canary_durable"), "canary_durable"),
            **kwargs,
        )
        if result.business_durable == result.canary_durable:
            raise ValueError("tenant canary and business durables must differ")
        if len(set(kwargs.values())) != len(kwargs):
            raise ValueError("tenant NATS credential env names must be distinct")
        return result

    def binding(self, registry: Any) -> "TenantDeploymentBinding":
        parent = registry.get(self.pipeline_id)
        if self.pipeline_id != "inventory-transaction-index":
            raise ValueError("tenant deployment currently supports only transaction index V2")
        return TenantDeploymentBinding(self, parent)


@dataclass(frozen=True)
class TenantDeploymentBinding:
    deployment: TenantDeployment
    parent: Any

    @property
    def stream(self) -> str:
        return self.parent.transport.stream

    @property
    def route(self) -> TenantEventRoute:
        return TenantEventRoute(self.parent.transport.subject_prefix, self.deployment.tenant_id)

    @property
    def canary_subject(self) -> str:
        return (
            f"{self.parent.transport.subject_prefix}.tenants."
            f"{self.deployment.tenant_id}.health_check.ping"
        )

    def durables(self) -> tuple[tuple[str, str], ...]:
        return (
            (self.deployment.business_durable, self.route.subject),
            (self.deployment.canary_durable, self.canary_subject),
        )


def load_tenant_deployments(data: Mapping[str, Any], registry: Any) -> tuple[TenantDeploymentBinding, ...]:
    raw = data.get("tenant_deployments", [])
    if not isinstance(raw, list):
        raise ValueError("tenant_deployments must be an array")
    bindings = tuple(TenantDeployment.from_mapping(item).binding(registry) for item in raw)
    existing_durables = {d for item in registry.pipelines for d in
                         (item.consumer.durable, item.canary.durable)}
    existing_inboxes = {name for item in registry.pipelines for name in
                        (item.consumer.inbox_consumer_name, item.canary.consumer_name)}
    ids: set[str] = set()
    tenant_pairs: set[tuple[str, UUID]] = set()
    for item in bindings:
        spec = item.deployment
        if spec.deployment_id in ids:
            raise ValueError("tenant deployment ids must be unique")
        ids.add(spec.deployment_id)
        pair = (spec.pipeline_id, spec.tenant_id)
        if pair in tenant_pairs:
            raise ValueError("tenant pipeline identities must be unique")
        tenant_pairs.add(pair)
        for durable, _ in item.durables():
            if durable in existing_durables:
                raise ValueError("tenant JetStream durables must be globally unique")
            existing_durables.add(durable)
        if spec.inbox_consumer_name in existing_inboxes:
            raise ValueError("tenant Inbox consumer identities must be globally unique")
        existing_inboxes.add(spec.inbox_consumer_name)
    return bindings


def _consumer_matches(config: Any, expected: str, durable: str) -> bool:
    return (
        config.durable_name == durable
        and config.filter_subject == expected
        and not getattr(config, "filter_subjects", None)
        and config.ack_policy == AckPolicy.EXPLICIT
        and config.deliver_subject is None
        and config.deliver_policy == DeliverPolicy.ALL
        and config.max_ack_pending == 10
        and config.max_deliver == 3
    )


@dataclass(frozen=True)
class TenantTopologyPlan:
    status: str
    code: str | None
    missing_durables: tuple[str, ...]
    stream: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status, "code": self.code,
            "missing_durables": list(self.missing_durables), "stream": self.stream,
        }


class TenantTopologyProvisioner:
    """Create ONLY missing exact-filter tenant durables on an EXISTING stream.

    No stream mutation, no change to any existing durable, no silent repair of
    drift. An explicit operator approval is necessary for mutation.
    """

    def __init__(self, admin_nats_url: str):
        self.admin_nats_url = _name(admin_nats_url, "admin_nats_url")

    async def _plan(self, js: Any, binding: TenantDeploymentBinding) -> TenantTopologyPlan:
        try:
            stream = await js.stream_info(binding.stream)
        except NotFoundError:
            return TenantTopologyPlan("blocked", "stream_missing", (), binding.stream)
        # Existing stream topology must explicitly cover tenant route and canary.
        if f"{binding.parent.transport.subject_prefix}.>" not in stream.config.subjects:
            return TenantTopologyPlan("blocked", "tenant_subjects_not_covered", (), binding.stream)
        missing: list[str] = []
        for durable, subject in binding.durables():
            try:
                info = await js.consumer_info(binding.stream, durable)
            except NotFoundError:
                missing.append(durable)
                continue
            if not _consumer_matches(info.config, subject, durable):
                return TenantTopologyPlan("blocked", f"durable_drift:{durable}", (), binding.stream)
        return TenantTopologyPlan(
            "ready" if not missing else "provisionable",
            None if not missing else "tenant_durables_missing",
            tuple(missing),
            binding.stream,
        )

    async def _with_js(self, fn: Any) -> Any:
        client = await nats.connect(
            servers=[self.admin_nats_url], allow_reconnect=False, connect_timeout=2
        )
        try:
            return await fn(client.jetstream())
        finally:
            await client.close()

    def plan(self, binding: TenantDeploymentBinding) -> TenantTopologyPlan:
        with asyncio.Runner() as runner:
            return runner.run(self._with_js(lambda js: self._plan(js, binding)))

    def provision(
        self,
        binding: TenantDeploymentBinding,
        *,
        operator_id: str,
        approval_id: str,
    ) -> TenantTopologyPlan:
        _name(operator_id, "operator_id")
        _name(approval_id, "approval_id")
        with asyncio.Runner() as runner:
            return runner.run(self._with_js(
                lambda js: self._provision(js, binding)
            ))

    async def _provision(self, js: Any, binding: TenantDeploymentBinding) -> TenantTopologyPlan:
        initial = await self._plan(js, binding)
        if initial.status == "blocked":
            raise ValueError(f"tenant provisioning refused: {initial.code}")
        created: list[str] = []
        try:
            for durable, subject in binding.durables():
                if durable not in initial.missing_durables:
                    continue
                await js.add_consumer(
                    binding.stream,
                    ConsumerConfig(
                        durable_name=durable,
                        filter_subject=subject,
                        deliver_policy=DeliverPolicy.ALL,
                        ack_policy=AckPolicy.EXPLICIT,
                        max_ack_pending=10,
                        max_deliver=3,
                        ack_wait=2,
                    ),
                )
                created.append(durable)
            verified = await self._plan(js, binding)
            if verified.status != "ready":
                raise ValueError(f"tenant topology not ready: {verified.code}")
            return verified
        except Exception:
            for durable in reversed(created):
                try:
                    await js.delete_consumer(binding.stream, durable)
                except Exception:
                    pass  # Recovery requires explicit operator inspection.
            raise


@dataclass(frozen=True)
class TenantReadinessReport:
    status: str
    codes: tuple[str, ...]
    deployment_id: str
    tenant_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status, "codes": list(self.codes),
            "deployment_id": self.deployment_id, "tenant_id": self.tenant_id,
        }


class TenantReadinessGate:
    """Fail-closed: topology alone never implies tenant operational readiness."""

    def assess(
        self,
        binding: TenantDeploymentBinding,
        *,
        topology: TenantTopologyPlan | None,
        acl_proof: Mapping[str, Any] | None,
        canary_proof: Mapping[str, Any] | None,
    ) -> TenantReadinessReport:
        spec = binding.deployment
        codes: list[str] = []
        if not spec.enabled:
            codes.append("deployment_disabled")
        if topology is None or topology.status != "ready" or topology.stream != binding.stream:
            codes.append("tenant_topology_not_ready")
        # Trusted evidence must be generated by the authenticated broker probe,
        # not inferred from NATS subject strings or registry env variable names.
        if not isinstance(acl_proof, Mapping) or not (
            acl_proof.get("verified") is True
            and acl_proof.get("tenant_id") == str(spec.tenant_id)
            and acl_proof.get("stream") == binding.stream
            and acl_proof.get("durable") == spec.business_durable
        ):
            codes.append("tenant_broker_acl_unverified")
        if not isinstance(canary_proof, Mapping) or not (
            canary_proof.get("verified") is True
            and canary_proof.get("tenant_id") == str(spec.tenant_id)
            and canary_proof.get("stream") == binding.stream
            and canary_proof.get("durable") == spec.canary_durable
            and canary_proof.get("subject") == binding.canary_subject
        ):
            codes.append("tenant_canary_unverified")
        return TenantReadinessReport(
            status="ready" if not codes else "not_ready",
            codes=tuple(codes),
            deployment_id=spec.deployment_id,
            tenant_id=str(spec.tenant_id),
        )
