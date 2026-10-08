"""Authenticated-call-site authorization gate for Transaction Index read queries.

The caller MUST supply a principal produced by a verified authentication layer.
This module enforces per-tenant and per-scope access, not authentication itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

READ_SCOPE = "inventory.transactions.read"


@dataclass(frozen=True)
class VerifiedPrincipal:
    subject: str
    permitted_tenants: frozenset[UUID]
    scopes: frozenset[str]

    def __post_init__(self) -> None:
        if not self.subject or self.subject != self.subject.strip():
            raise ValueError("authenticated subject is required")
        if not isinstance(self.permitted_tenants, frozenset) or any(
            not isinstance(item, UUID) for item in self.permitted_tenants
        ):
            raise ValueError("permitted_tenants must be an explicit UUID set")
        if not isinstance(self.scopes, frozenset) or any(
            not isinstance(item, str) for item in self.scopes
        ):
            raise ValueError("scopes must be an explicit string set")


def list_authorized_transactions(
    transaction: Any,
    *,
    principal: VerifiedPrincipal,
    tenant_id: UUID,
    consumer_name: str,
    limit: int = 100,
) -> list[tuple[Any, ...]]:
    """Query only the allowed tenant; deny before executing any SQL."""
    if not isinstance(principal, VerifiedPrincipal):
        raise PermissionError("verified principal is required")
    if READ_SCOPE not in principal.scopes:
        raise PermissionError("transaction index read scope is required")
    if not isinstance(tenant_id, UUID) or tenant_id not in principal.permitted_tenants:
        raise PermissionError("tenant is not authorized for transaction index reads")
    if not consumer_name or consumer_name != consumer_name.strip():
        raise ValueError("consumer_name is required")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")

    return transaction.execute(
        """
        SELECT transaction_id, transaction_type, source_reference, occurred_at
        FROM kernel_lab.inventory_transaction_index_projection
        WHERE consumer_name = %s AND tenant_id = %s
        ORDER BY occurred_at DESC, transaction_id DESC
        LIMIT %s
        """,
        (consumer_name, tenant_id, limit),
    ).fetchall()
