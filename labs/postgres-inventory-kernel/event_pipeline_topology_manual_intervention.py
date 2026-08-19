from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from event_pipeline_topology_crash_safe_provisioner import RecoverableNatsJetStreamTopologyMutator
from event_pipeline_topology_migration import TopologyContractInspector, TopologyMigrationConfig
from event_pipeline_topology_provisioning_journal import PostgresTopologyProvisioningJournal, TopologyProvisioningJournalRun

InterventionAction = Literal["restore_source", "accept_target", "abort"]
InterventionState = Literal[
    "detected", "proposed", "confirmed", "executing",
    "resolved_source", "resolved_target", "aborted", "failed",
]
InterventionHealth = Literal["clear", "warning", "critical"]

ACTIVE_STATES = frozenset({"detected", "proposed", "confirmed", "executing"})
TERMINAL_STATES = frozenset({"resolved_source", "resolved_target", "aborted", "failed"})
ACTIONS = frozenset({"restore_source", "accept_target", "abort"})


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (UUID, datetime)):
        return str(value) if isinstance(value, UUID) else value.isoformat()
    return value


def _fingerprint(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _status(report: Any) -> str | None:
    return getattr(report, "status", None)


class TopologyManualInterventionError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class TopologyManualIntervention:
    intervention_id: UUID
    run_id: UUID
    pipeline_id: str
    migration_id: str
    resource: str
    state: InterventionState
    evidence: Mapping[str, Any]
    evidence_fingerprint: str
    proposed_action: InterventionAction | None
    proposed_by: str | None
    proposed_at: datetime | None
    confirmed_by: str | None
    confirmed_at: datetime | None
    resolution_code: str | None
    created_at: datetime
    updated_at: datetime
    state_changed_at: datetime

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def to_dict(self) -> dict[str, Any]:
        return {
            "intervention_id": str(self.intervention_id),
            "run_id": str(self.run_id),
            "pipeline": self.pipeline_id,
            "migration_id": self.migration_id,
            "resource": self.resource,
            "state": self.state,
            "terminal": self.terminal,
            "evidence": _jsonable(self.evidence),
            "evidence_fingerprint": self.evidence_fingerprint,
            "proposed_action": self.proposed_action,
            "proposed_by": self.proposed_by,
            "proposed_at": self.proposed_at.isoformat() if self.proposed_at else None,
            "confirmed_by": self.confirmed_by,
            "confirmed_at": self.confirmed_at.isoformat() if self.confirmed_at else None,
            "resolution_code": self.resolution_code,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "state_changed_at": self.state_changed_at.isoformat(),
        }


class PostgresTopologyManualInterventionStore:
    """Persist explicit decisions without reopening a terminal provisioning run."""

    def __init__(self, database_url: str):
        self.database_url = _required_name(database_url, "database_url")

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    @staticmethod
    def _row(row: Mapping[str, Any]) -> TopologyManualIntervention:
        return TopologyManualIntervention(
            intervention_id=row["intervention_id"], run_id=row["run_id"],
            pipeline_id=row["pipeline_id"], migration_id=row["migration_id"],
            resource=row["resource"], state=row["state"], evidence=dict(row["evidence"]),
            evidence_fingerprint=row["evidence_fingerprint"], proposed_action=row["proposed_action"],
            proposed_by=row["proposed_by"], proposed_at=row["proposed_at"],
            confirmed_by=row["confirmed_by"], confirmed_at=row["confirmed_at"],
            resolution_code=row["resolution_code"], created_at=row["created_at"],
            updated_at=row["updated_at"], state_changed_at=row["state_changed_at"],
        )

    def get(self, intervention_id: UUID) -> TopologyManualIntervention | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kernel_lab.event_pipeline_topology_interventions WHERE intervention_id = %s",
                (intervention_id,),
            ).fetchone()
        return None if row is None else self._row(row)

    def latest_for_pipeline(self, pipeline_id: str) -> TopologyManualIntervention | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kernel_lab.event_pipeline_topology_interventions WHERE pipeline_id = %s ORDER BY created_at DESC LIMIT 1",
                (_required_name(pipeline_id, "pipeline_id"),),
            ).fetchone()
        return None if row is None else self._row(row)

    def latest_for_run(self, run_id: UUID) -> TopologyManualIntervention | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kernel_lab.event_pipeline_topology_interventions WHERE run_id = %s ORDER BY created_at DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        return None if row is None else self._row(row)

    def active_for_run(self, run_id: UUID) -> TopologyManualIntervention | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM kernel_lab.event_pipeline_topology_interventions
                WHERE run_id = %s AND state IN ('detected', 'proposed', 'confirmed', 'executing')
                ORDER BY created_at DESC LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return None if row is None else self._row(row)

    def manual_run_for_pipeline(self, pipeline_id: str) -> UUID | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT run_id FROM kernel_lab.event_pipeline_topology_provisioning_runs
                WHERE pipeline_id = %s AND state = 'manual_intervention'
                ORDER BY started_at DESC LIMIT 1
                """,
                (_required_name(pipeline_id, "pipeline_id"),),
            ).fetchone()
        return None if row is None else row["run_id"]

    def create_detected(self, *, run: TopologyProvisioningJournalRun, evidence: Mapping[str, Any], evidence_fingerprint: str) -> TopologyManualIntervention:
        existing = self.active_for_run(run.run_id)
        if existing is not None:
            return existing
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    INSERT INTO kernel_lab.event_pipeline_topology_interventions (
                      intervention_id, run_id, pipeline_id, migration_id, resource,
                      state, evidence, evidence_fingerprint
                    ) VALUES (%s, %s, %s, %s, %s, 'detected', %s::jsonb, %s)
                    RETURNING *
                    """,
                    (uuid4(), run.run_id, run.pipeline_id, run.migration_id, run.resource,
                     json.dumps(_jsonable(evidence)), evidence_fingerprint),
                ).fetchone()
                return self._row(row)
        except psycopg.errors.UniqueViolation:
            existing = self.active_for_run(run.run_id)
            if existing is None:
                raise TopologyManualInterventionError("topology_manual_intervention_detection_race")
            return existing

    def propose(self, intervention_id: UUID, *, action: InterventionAction, operator: str) -> TopologyManualIntervention:
        if action not in ACTIONS:
            raise ValueError("unsupported topology manual intervention action")
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_interventions
                SET state = 'proposed', proposed_action = %s, proposed_by = %s, proposed_at = clock_timestamp()
                WHERE intervention_id = %s AND state = 'detected' RETURNING *
                """,
                (action, _required_name(operator, "operator"), intervention_id),
            ).fetchone()
        if row is None:
            raise TopologyManualInterventionError("topology_manual_intervention_not_detected")
        return self._row(row)

    def confirm(self, intervention_id: UUID, *, operator: str) -> TopologyManualIntervention:
        operator = _required_name(operator, "operator")
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM kernel_lab.event_pipeline_topology_interventions WHERE intervention_id = %s FOR UPDATE",
                (intervention_id,),
            ).fetchone()
            if current is None:
                raise TopologyManualInterventionError("topology_manual_intervention_missing")
            if current["state"] != "proposed":
                raise TopologyManualInterventionError("topology_manual_intervention_not_proposed")
            if current["proposed_by"] == operator:
                raise TopologyManualInterventionError("topology_manual_intervention_second_operator_required")
            row = conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_interventions
                SET state = 'confirmed', confirmed_by = %s, confirmed_at = clock_timestamp()
                WHERE intervention_id = %s AND state = 'proposed' RETURNING *
                """,
                (operator, intervention_id),
            ).fetchone()
        if row is None:
            raise TopologyManualInterventionError("topology_manual_intervention_confirmation_race")
        return self._row(row)

    def mark_executing(self, intervention_id: UUID) -> TopologyManualIntervention:
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_interventions SET state = 'executing'
                WHERE intervention_id = %s AND state = 'confirmed' RETURNING *
                """,
                (intervention_id,),
            ).fetchone()
        if row is None:
            raise TopologyManualInterventionError("topology_manual_intervention_not_confirmed")
        return self._row(row)

    def finish(self, intervention_id: UUID, *, state: Literal["resolved_source", "resolved_target", "aborted", "failed"], code: str) -> TopologyManualIntervention:
        with self._connect() as conn:
            row = conn.execute(
                """
                UPDATE kernel_lab.event_pipeline_topology_interventions SET state = %s, resolution_code = %s
                WHERE intervention_id = %s AND state = 'executing' RETURNING *
                """,
                (state, _required_name(code, "resolution_code"), intervention_id),
            ).fetchone()
        if row is None:
            raise TopologyManualInterventionError("topology_manual_intervention_not_executing")
        return self._row(row)


@dataclass(frozen=True)
class TopologyManualInterventionReport:
    status: str
    code: str
    intervention: TopologyManualIntervention | None
    mutates_topology: bool

    @property
    def successful(self) -> bool:
        return self.status in {"detected", "proposed", "confirmed", "resolved", "aborted", "idle"}

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status, "code": self.code, "mutates_topology": self.mutates_topology,
                "intervention": self.intervention.to_dict() if self.intervention else None}


class ExplicitTopologyManualInterventionController:
    """Require an explicit proposal and an independent confirmation before emergency resolution."""

    def __init__(self, *, database_url: str, nats_url: str, contract_inspector: Any | None = None, mutator: Any | None = None):
        self.database_url = _required_name(database_url, "database_url")
        self.nats_url = _required_name(nats_url, "nats_url")
        self.store = PostgresTopologyManualInterventionStore(database_url)
        self.journal = PostgresTopologyProvisioningJournal(database_url, owner_id="manual-intervention-observer")
        self.contract_inspector = contract_inspector or TopologyContractInspector(nats_url)
        self.mutator = mutator or RecoverableNatsJetStreamTopologyMutator(nats_url)

    def _run(self, run_id: UUID) -> TopologyProvisioningJournalRun:
        run = self.journal.get(run_id)
        if run is None:
            raise TopologyManualInterventionError("topology_manual_intervention_provisioning_run_missing")
        if run.state != "manual_intervention":
            raise TopologyManualInterventionError("topology_manual_intervention_provisioning_run_not_manual")
        return run

    @staticmethod
    def _validate_migration(run: TopologyProvisioningJournalRun, migration_mapping: Mapping[str, Any] | None) -> None:
        if migration_mapping is None:
            raise TopologyManualInterventionError("topology_manual_intervention_migration_contract_missing")
        if TopologyMigrationConfig.from_mapping(migration_mapping).migration_id != run.migration_id:
            raise TopologyManualInterventionError("topology_manual_intervention_migration_contract_mismatch")

    @staticmethod
    def _evidence_payload(*, run: TopologyProvisioningJournalRun, target_topology: Mapping[str, Any], migration_mapping: Mapping[str, Any], contract: Any) -> dict[str, Any]:
        return {
            "run_id": str(run.run_id), "pipeline": run.pipeline_id, "migration_id": run.migration_id,
            "resource": run.resource, "state_code": run.state_code, "intent_fingerprint": run.intent_fingerprint,
            "source_snapshot": run.source_snapshot.to_dict(), "changes": [dict(item) for item in run.changes],
            "target_topology": _jsonable(target_topology), "migration": _jsonable(migration_mapping),
            "live": {"matched": contract.matched, "target_status": contract.target.status,
                     "resources": [item.to_dict() for item in contract.target.resources]},
        }

    def detect(self, spec: Any, *, target_topology: Mapping[str, Any], migration_mapping: Mapping[str, Any] | None, observed_at: datetime | None = None) -> TopologyManualInterventionReport:
        run_id = self.store.manual_run_for_pipeline(spec.pipeline_id)
        if run_id is None:
            return TopologyManualInterventionReport("idle", "topology_manual_intervention_not_required", None, False)
        run = self._run(run_id)
        latest = self.store.latest_for_run(run_id)
        if latest is not None and latest.state in {"resolved_source", "resolved_target", "aborted"}:
            return TopologyManualInterventionReport("idle", "topology_manual_intervention_already_closed", latest, False)
        active = self.store.active_for_run(run_id)
        if active is not None:
            return TopologyManualInterventionReport("detected", "topology_manual_intervention_already_active", active, False)
        self._validate_migration(run, migration_mapping)
        try:
            contract = self.contract_inspector.inspect(spec, target_topology=target_topology,
                                                       migration_mapping=migration_mapping, observed_at=observed_at)
        except Exception as exc:
            raise TopologyManualInterventionError("topology_manual_intervention_evidence_unavailable") from exc
        assert migration_mapping is not None
        payload = self._evidence_payload(run=run, target_topology=target_topology,
                                         migration_mapping=migration_mapping, contract=contract)
        evidence = {"observed_at": (observed_at or datetime.now(timezone.utc)).isoformat(), "snapshot": payload}
        intervention = self.store.create_detected(run=run, evidence=evidence, evidence_fingerprint=_fingerprint(payload))
        return TopologyManualInterventionReport("detected", "topology_manual_intervention_detected", intervention, False)

    def propose(self, intervention_id: UUID, *, action: InterventionAction, operator: str) -> TopologyManualInterventionReport:
        value = self.store.propose(intervention_id, action=action, operator=operator)
        return TopologyManualInterventionReport("proposed", "topology_manual_intervention_proposed", value, False)

    def confirm(self, intervention_id: UUID, *, operator: str) -> TopologyManualInterventionReport:
        value = self.store.confirm(intervention_id, operator=operator)
        return TopologyManualInterventionReport("confirmed", "topology_manual_intervention_confirmed", value, False)

    def execute(self, intervention_id: UUID, spec: Any, *, target_topology: Mapping[str, Any], migration_mapping: Mapping[str, Any] | None, canary_verifier: Any, readiness_verifier: Any, observed_at: datetime | None = None) -> TopologyManualInterventionReport:
        intervention = self.store.get(intervention_id)
        if intervention is None:
            raise TopologyManualInterventionError("topology_manual_intervention_missing")
        if intervention.state != "confirmed":
            raise TopologyManualInterventionError("topology_manual_intervention_not_confirmed")
        run = self._run(intervention.run_id)
        self._validate_migration(run, migration_mapping)
        try:
            contract = self.contract_inspector.inspect(spec, target_topology=target_topology,
                                                       migration_mapping=migration_mapping, observed_at=observed_at)
        except Exception as exc:
            raise TopologyManualInterventionError("topology_manual_intervention_evidence_unavailable") from exc
        assert migration_mapping is not None
        payload = self._evidence_payload(run=run, target_topology=target_topology,
                                         migration_mapping=migration_mapping, contract=contract)
        if _fingerprint(payload) != intervention.evidence_fingerprint:
            self.store.mark_executing(intervention_id)
            failed = self.store.finish(intervention_id, state="failed", code="topology_manual_intervention_evidence_stale")
            return TopologyManualInterventionReport("failed", "topology_manual_intervention_evidence_stale", failed, False)

        executing = self.store.mark_executing(intervention_id)
        action = executing.proposed_action
        if action == "abort":
            closed = self.store.finish(intervention_id, state="aborted", code="topology_manual_intervention_aborted")
            return TopologyManualInterventionReport("aborted", "topology_manual_intervention_aborted", closed, False)
        if action == "accept_target":
            if contract.matched != "target" or contract.target.status != "ok":
                failed = self.store.finish(intervention_id, state="failed", code="topology_manual_intervention_target_not_exact")
                return TopologyManualInterventionReport("failed", "topology_manual_intervention_target_not_exact", failed, False)
            return self._verify(intervention_id, "resolved_target", "topology_manual_intervention_target_accepted",
                                canary_verifier, readiness_verifier, False)
        if action != "restore_source":
            failed = self.store.finish(intervention_id, state="failed", code="topology_manual_intervention_action_invalid")
            return TopologyManualInterventionReport("failed", "topology_manual_intervention_action_invalid", failed, False)

        try:
            self.mutator.rollback_to_snapshot(spec=spec, resource=run.resource, source_snapshot=run.source_snapshot)
        except Exception:
            failed = self.store.finish(intervention_id, state="failed", code="topology_manual_intervention_restore_source_failed")
            return TopologyManualInterventionReport("failed", "topology_manual_intervention_restore_source_failed", failed, True)
        try:
            restored = self.contract_inspector.inspect(spec, target_topology=target_topology,
                                                       migration_mapping=migration_mapping)
        except Exception:
            restored = None
        if restored is None or restored.matched != "source":
            failed = self.store.finish(intervention_id, state="failed", code="topology_manual_intervention_source_verification_failed")
            return TopologyManualInterventionReport("failed", "topology_manual_intervention_source_verification_failed", failed, True)
        return self._verify(intervention_id, "resolved_source", "topology_manual_intervention_source_restored",
                            canary_verifier, readiness_verifier, True)

    def _verify(self, intervention_id: UUID, target_state: Literal["resolved_source", "resolved_target"],
                success_code: str, canary_verifier: Any, readiness_verifier: Any,
                mutates_topology: bool) -> TopologyManualInterventionReport:
        try:
            if _status(canary_verifier()) != "ok":
                code = "topology_manual_intervention_canary_not_ok"
                failed = self.store.finish(intervention_id, state="failed", code=code)
                return TopologyManualInterventionReport("failed", code, failed, mutates_topology)
            if _status(readiness_verifier()) != "ready":
                code = "topology_manual_intervention_readiness_not_ready"
                failed = self.store.finish(intervention_id, state="failed", code=code)
                return TopologyManualInterventionReport("failed", code, failed, mutates_topology)
        except Exception:
            code = "topology_manual_intervention_verification_unavailable"
            failed = self.store.finish(intervention_id, state="failed", code=code)
            return TopologyManualInterventionReport("failed", code, failed, mutates_topology)
        closed = self.store.finish(intervention_id, state=target_state, code=success_code)
        return TopologyManualInterventionReport("resolved", success_code, closed, mutates_topology)


@dataclass(frozen=True)
class TopologyManualInterventionHealthReport:
    pipeline_id: str
    status: InterventionHealth
    code: str | None
    intervention: TopologyManualIntervention | None

    def to_dict(self) -> dict[str, Any]:
        return {"pipeline": self.pipeline_id, "status": self.status, "code": self.code,
                "collection": {"read_only": True},
                "intervention": self.intervention.to_dict() if self.intervention else None}


def inspect_manual_intervention_health(database_url: str, *, pipeline_id: str) -> TopologyManualInterventionHealthReport:
    pipeline_id = _required_name(pipeline_id, "pipeline_id")
    latest = PostgresTopologyManualInterventionStore(database_url).latest_for_pipeline(pipeline_id)
    if latest is None:
        return TopologyManualInterventionHealthReport(pipeline_id, "clear", None, None)
    if latest.state in ACTIVE_STATES:
        return TopologyManualInterventionHealthReport(pipeline_id, "critical", "topology_manual_intervention_action_required", latest)
    if latest.state in {"resolved_source", "resolved_target"}:
        return TopologyManualInterventionHealthReport(pipeline_id, "warning", "topology_manual_intervention_resolved_cleanup_pending", latest)
    if latest.state == "aborted":
        return TopologyManualInterventionHealthReport(pipeline_id, "critical", "topology_manual_intervention_aborted_attention", latest)
    return TopologyManualInterventionHealthReport(pipeline_id, "critical", "topology_manual_intervention_failed", latest)


def render_prometheus(report: TopologyManualInterventionHealthReport) -> str:
    pipeline = report.pipeline_id.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    status_value = {"clear": 0, "warning": 1, "critical": 2}[report.status]
    lines = [
        "# HELP spotwo_wms_event_pipeline_topology_manual_intervention_health Manual intervention health: 0 clear, 1 warning, 2 critical.",
        "# TYPE spotwo_wms_event_pipeline_topology_manual_intervention_health gauge",
        f'spotwo_wms_event_pipeline_topology_manual_intervention_health{{pipeline="{pipeline}"}} {status_value}',
        "# HELP spotwo_wms_event_pipeline_topology_manual_intervention_state Latest bounded intervention state.",
        "# TYPE spotwo_wms_event_pipeline_topology_manual_intervention_state gauge",
    ]
    if report.intervention is not None:
        lines.append(f'spotwo_wms_event_pipeline_topology_manual_intervention_state{{pipeline="{pipeline}",state="{report.intervention.state}"}} 1')
    return "\n".join(lines) + "\n"
