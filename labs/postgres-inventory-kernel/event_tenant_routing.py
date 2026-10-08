"""Opt-in tenant-scoped business routing contract; not broker authorization."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping
from uuid import UUID

TENANT_HEADER = "Spotwo-Tenant-Id"
ROUTED_EVENT_TYPE = "inventory.transaction.posted"
ROUTED_SCHEMA_VERSION = 2


def tenant_uuid(value: Any) -> UUID:
    if not isinstance(value, str):
        raise ValueError("tenant_id must be a canonical lowercase UUID string")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError("tenant_id must be a canonical lowercase UUID string") from exc
    if str(parsed) != value:
        raise ValueError("tenant_id must be a canonical lowercase UUID string")
    return parsed


@dataclass(frozen=True)
class TenantEventRoute:
    subject_prefix: str
    tenant_id: UUID
    event_type: str = ROUTED_EVENT_TYPE

    def __post_init__(self) -> None:
        # Subject syntax is a wire contract, not a dependency on the NATS
        # client library; the deployment registry is validated without nats-py.
        prefix = self.subject_prefix
        if (
            not isinstance(prefix, str)
            or not prefix
            or prefix.strip() != prefix
            or any(re.fullmatch(r"[A-Za-z0-9_-]+", part) is None
                   for part in prefix.split("."))
        ):
            raise ValueError("subject_prefix must use normalized literal NATS tokens")
        if not isinstance(self.tenant_id, UUID):
            raise ValueError("tenant_id must be a UUID")
        if self.event_type != ROUTED_EVENT_TYPE:
            raise ValueError("only V2 inventory.transaction.posted supports scoped routing")

    @property
    def subject(self) -> str:
        return f"{self.subject_prefix}.tenants.{self.tenant_id}.{self.event_type}"

    def validate_envelope(self, envelope: Mapping[str, Any]) -> None:
        if envelope.get("schema_version") != ROUTED_SCHEMA_VERSION:
            raise ValueError("tenant routing requires Event Contract V2")
        if envelope.get("type") != self.event_type:
            raise ValueError("event type is not authorized for this tenant route")
        if tenant_uuid(envelope.get("tenant_id")) != self.tenant_id:
            raise ValueError("event tenant_id is outside the authorized tenant route")

    def validate_delivery(
        self,
        *,
        subject: str,
        headers: Mapping[str, str],
        envelope: Mapping[str, Any],
    ) -> None:
        if subject != self.subject:
            raise ValueError("JetStream delivery subject differs from tenant route")
        if headers.get(TENANT_HEADER) != str(self.tenant_id):
            raise ValueError("JetStream tenant header differs from authorized scope")
        self.validate_envelope(envelope)
