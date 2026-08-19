from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from event_pipeline_topology import TopologyResourceSnapshot
from event_pipeline_topology_preflight import TopologyPlanChange

TERMINAL_STATES = frozenset({"completed", "rolled_back", "failed", "manual_intervention"})
ACTIVE_STATES = frozenset(
    {
        "prepared",
        "applying",
        "applied",
        "target_verified",
        "canary_verified",
        "readiness_verified",
        "rollback_started",
        "source_verified",
    }
)
ALL_STATES = ACTIVE_STATES | TERMINAL_STATES

_ALLOWED_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "prepared": frozenset({"applying", "target_verified", "rollback_started", "failed", "manual_intervention"}),
    "applying": frozenset({"applied", "target_verified", "rollback_started", "failed", "manual_intervention"}),
    "applied": frozenset({"target_verified", "rollback_started", "failed", "manual_intervention"}),
    "target_verified": frozenset({"canary_verified", "rollback_started", "failed", "manual_intervention"}),
    "canary_verified": frozenset({"readiness_verified", "rollback_started", "failed", "manual_intervention"}),
    "readiness_verified": frozenset({"completed", "rollback_started", "failed", "manual_intervention"}),
    "rollback_started": frozenset({"source_verified", "failed", "manual_intervention"}),
    "source_verified": frozenset({"rolled_back", "failed", "manual_intervention"}),
}


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _snapshot_mapping(snapshot: TopologyResourceSnapshot) -> dict[str, Any]:
    return snapshot.to_dict()


def _snapshot_from_mapping(value: Mapping[str, Any]) -> TopologyResourceSnapshot:
    fields = dict(value.get("fields") or {})
    if isinstance(fields.get("subjects"), list):
        fields["subjects"] = tuple(fields["subjects"])
    return TopologyResourceSnapshot(
        resource=_required_name(value.get("resource"), "source_snapshot.resource"),
        name=_required_name(value.get("name"), "source_snapshot.name"),
        exists=bool(value.get("exists")),
        fields=fields,
    )


def _change_mapping(change: TopologyPlanChange) -> dict[str, Any]:
    return {
        "resource": change.resource,
        "field": change.field,
        "current": change.current,
        "target": change.target,
    }


def _fingerprint(
    *,
    pipeline_id: str,
    migration_id: str,
    resource: str,
    source_snapshot: Mapping[str, Any],
    changes: Iterable[Mapping[str, Any]],
) -> str:
    payload = {
        "pipeline": pipeline_id,
        "migration": migration_id,
        "resource": resource,
        "source_snapshot": source_snapshot,
        "changes": list(changes),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class TopologyProvisioningJournalError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class TopologyProvisioningJournalRun:
    run_id: UUID
    pipeline_id: str
    migration_id: str
    resource: str
    state: str
    state_code: str | None
    source_snapshot: TopologyResourceSnapshot
    changes: tuple[Mapping[str, Any], ...]
    intent_fingerprint: str
    lease_owner: str | None
    lease_expires_at: datetime | None
    attempt_count: int
    recovery_count: int
    started_at: datetime
    updated_at: datetime
    recovered: bool = False

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def changed_fields(self) -> tuple[str, ...]:
        return tuple(str(item["field"]) for item in self.changes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "pipeline": self.pipeline_id,
            "migration_id": self.migration_id,
            "resource": self.resource,
            "state": self.state,
            "state_code": self.state_code,
            "source_snapshot": self.source_snapshot.to_dict(),
            "changes": [dict(item) for item in self.changes],
            "intent_fingerprint": self.intent_fingerprint,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at.isoformat() if self.lease_expires_at else None,
            "attempt_count": self.attempt_count,
            "recovery_count": self.recovery_count,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "recovered": self.recovered,
        }


class PostgresTopologyProvisioningJournal:
    """Persist topology rollout intent and lease ownership for crash-safe reconciliation."""

    def __init__(
        self,
        database_url: str,
        *,
        owner_id: str,
        lease_seconds: float = 120.0,
    ):
        self.database_url = _required_name(database_url, "database_url")
        self.owner_id = _required_name(owner_id, "owner_id")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.lease_seconds = float(lease_seconds)

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    @staticmethod
    def _row_to_run(row: Mapping[str, Any], *, recovered: bool = False) -> TopologyProvisioningJournalRun:
        return TopologyProvisioningJournalRun(
            run_id=row["run_id"],
            pipeline_id=row["pipeline_id"],
            migration_id=row["migration_id"],
            resource=row["resource"],
            state=row["state"],
            state_code=row["state_code"],
            source_snapshot=_snapshot_from_mapping(row["source_snapshot"]),
            changes=tuple(dict(item) for item in row["changes"]),
            intent_fingerprint=row["intent_fingerprint"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            attempt_count=int(row["attempt_count"]),
            recovery_count=int(row["recovery_count"]),
            started_at=row["started_at"],
            updated_at=row["updated_at"],
            recovered=recovered,
        )

    def claim_active(self, *, pipeline_id: str, migration_id: str) -> TopologyProvisioningJournalRun | None:
        pipeline_id = _required_name(pipeline_id, "pipeline_id")
        migration_id = _required_name(migration_id, "migration_id")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT r.*, r.lease_expires_at <= clock_timestamp() AS lease_expired
                FROM kernel_lab.event_pipeline_topology_provisioning_runs AS r
                WHERE r.pipeline_id = %s
                  AND r.state NOT IN ('completed', 'rolled_back', 'failed', 'manual_intervention')
                FOR UPDATE
                """,
                (pipeline_id,),
            ).fetchone()
            if row is None:
                return None
            if row["migration_id"] != migration_id:
                raise TopologyProvisioningJournalError("topology_provisioning_active_migration_conflict")
            if row["lease_owner"] != self.owner_id and not row["lease_expired"]:
                raise TopologyProvisioningJournalError("topology_provisioning_lease_held")

            updated = conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_provisioning_runs
                SET lease_owner = %s,
                    lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                    attempt_count = attempt_count + CASE WHEN lease_owner = %s THEN 0 ELSE 1 END,
                    recovery_count = recovery_count + CASE WHEN lease_owner = %s THEN 0 ELSE 1 END,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                RETURNING *
                """,
                (
                    self.owner_id,
                    self.lease_seconds,
                    self.owner_id,
                    self.owner_id,
                    row["run_id"],
                ),
            ).fetchone()
            return self._row_to_run(updated, recovered=row["lease_owner"] != self.owner_id)

    def start(
        self,
        *,
        pipeline_id: str,
        migration_id: str,
        resource: str,
        source_snapshot: TopologyResourceSnapshot,
        changes: tuple[TopologyPlanChange, ...],
    ) -> TopologyProvisioningJournalRun:
        pipeline_id = _required_name(pipeline_id, "pipeline_id")
        migration_id = _required_name(migration_id, "migration_id")
        resource = _required_name(resource, "resource")
        if resource not in {"stream", "business_consumer", "canary_consumer"}:
            raise ValueError("resource is not supported")
        source_mapping = _snapshot_mapping(source_snapshot)
        change_mappings = tuple(_change_mapping(item) for item in changes)
        intent_fingerprint = _fingerprint(
            pipeline_id=pipeline_id,
            migration_id=migration_id,
            resource=resource,
            source_snapshot=source_mapping,
            changes=change_mappings,
        )
        run_id = uuid4()
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    INSERT INTO kernel_lab.event_pipeline_topology_provisioning_runs (
                      run_id,
                      pipeline_id,
                      migration_id,
                      resource,
                      state,
                      source_snapshot,
                      changes,
                      intent_fingerprint,
                      lease_owner,
                      lease_expires_at
                    ) VALUES (
                      %s, %s, %s, %s, 'prepared', %s::jsonb, %s::jsonb, %s, %s,
                      clock_timestamp() + (%s * interval '1 second')
                    )
                    RETURNING *
                    """,
                    (
                        run_id,
                        pipeline_id,
                        migration_id,
                        resource,
                        json.dumps(source_mapping),
                        json.dumps(change_mappings),
                        intent_fingerprint,
                        self.owner_id,
                        self.lease_seconds,
                    ),
                ).fetchone()
                return self._row_to_run(row)
        except psycopg.errors.UniqueViolation:
            existing = self.claim_active(pipeline_id=pipeline_id, migration_id=migration_id)
            if existing is None:
                raise TopologyProvisioningJournalError("topology_provisioning_start_race")
            if existing.intent_fingerprint != intent_fingerprint:
                raise TopologyProvisioningJournalError("topology_provisioning_intent_mismatch")
            return existing

    def renew(self, run_id: UUID) -> TopologyProvisioningJournalRun:
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_provisioning_runs
                SET lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                  AND lease_owner = %s
                  AND state NOT IN ('completed', 'rolled_back', 'failed', 'manual_intervention')
                  AND lease_expires_at > clock_timestamp()
                RETURNING *
                """,
                (self.lease_seconds, run_id, self.owner_id),
            ).fetchone()
            if row is None:
                raise TopologyProvisioningJournalError("topology_provisioning_lease_lost")
            return self._row_to_run(row)

    def transition(
        self,
        run_id: UUID,
        state: str,
        *,
        code: str | None = None,
    ) -> TopologyProvisioningJournalRun:
        if state not in ALL_STATES:
            raise ValueError(f"unknown provisioning journal state: {state}")
        with self._connect() as conn:
            current = conn.execute(
                """
                SELECT *, lease_expires_at <= clock_timestamp() AS lease_expired
                FROM kernel_lab.event_pipeline_topology_provisioning_runs
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run_id,),
            ).fetchone()
            if current is None:
                raise TopologyProvisioningJournalError("topology_provisioning_run_missing")
            if current["state"] in TERMINAL_STATES:
                raise TopologyProvisioningJournalError("topology_provisioning_run_terminal")
            if current["lease_owner"] != self.owner_id or current["lease_expired"]:
                raise TopologyProvisioningJournalError("topology_provisioning_lease_lost")
            if state not in _ALLOWED_TRANSITIONS[current["state"]]:
                raise TopologyProvisioningJournalError("topology_provisioning_invalid_state_transition")

            terminal = state in TERMINAL_STATES
            row = conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_provisioning_runs
                SET state = %s,
                    state_code = %s,
                    lease_owner = CASE WHEN %s THEN NULL ELSE lease_owner END,
                    lease_expires_at = CASE
                      WHEN %s THEN NULL
                      ELSE clock_timestamp() + (%s * interval '1 second')
                    END,
                    updated_at = clock_timestamp()
                WHERE run_id = %s
                RETURNING *
                """,
                (state, code, terminal, terminal, self.lease_seconds, run_id),
            ).fetchone()
            return self._row_to_run(row)

    def get(self, run_id: UUID) -> TopologyProvisioningJournalRun | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM kernel_lab.event_pipeline_topology_provisioning_runs
                WHERE run_id = %s
                """,
                (run_id,),
            ).fetchone()
            return None if row is None else self._row_to_run(row)
