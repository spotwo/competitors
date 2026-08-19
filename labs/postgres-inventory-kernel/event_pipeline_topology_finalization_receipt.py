from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from event_pipeline_topology_config import DeploymentTopologyConfig
from event_pipeline_topology_finalization import TopologyMigrationFinalizationInspector
from event_pipeline_topology_manual_intervention import PostgresTopologyManualInterventionStore
from event_pipeline_topology_migration import TopologyContractInspector, TopologyMigrationConfig
from event_pipeline_topology_provisioning_health import PostgresTopologyProvisioningHealthStore

ReceiptLineage = Literal["completed", "resolved_target"]
IssueStatus = Literal["issued", "blocked"]
VerificationStatus = Literal["closed", "blocked"]


def _required_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a normalized non-empty string")
    return value


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def canonical_fingerprint(value: Any) -> str:
    encoded = json.dumps(
        _jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def target_contract_payload(spec: Any, target_topology: Mapping[str, Any]) -> dict[str, Any]:
    expectation = DeploymentTopologyConfig.from_mapping(target_topology).expectation(spec)
    return {
        "pipeline": spec.pipeline_id,
        "stream": {
            "name": expectation.stream_name,
            "subjects": list(sorted(expectation.stream.subjects)),
            "storage": expectation.stream.storage,
            "retention": expectation.stream.retention,
            "replicas": expectation.stream.replicas,
            "duplicate_window_seconds": float(expectation.stream.duplicate_window_seconds),
        },
        "business_consumer": {
            "name": expectation.business_consumer.durable_name,
            "delivery_mode": expectation.business_consumer.delivery_mode,
            "deliver_policy": expectation.business_consumer.deliver_policy,
            "ack_policy": expectation.business_consumer.ack_policy,
            "filter_subject": expectation.business_consumer.filter_subject,
            "max_ack_pending": expectation.business_consumer.max_ack_pending,
            "max_deliver": expectation.business_consumer.max_deliver,
        },
        "canary_consumer": {
            "name": expectation.canary_consumer.durable_name,
            "delivery_mode": expectation.canary_consumer.delivery_mode,
            "deliver_policy": expectation.canary_consumer.deliver_policy,
            "ack_policy": expectation.canary_consumer.ack_policy,
            "filter_subject": expectation.canary_consumer.filter_subject,
            "max_ack_pending": expectation.canary_consumer.max_ack_pending,
            "max_deliver": expectation.canary_consumer.max_deliver,
        },
    }


def live_contract_payload(spec: Any, contract: Any) -> dict[str, Any]:
    resources = {item.resource: item for item in contract.target.resources}
    expected_resources = ("stream", "business_consumer", "canary_consumer")
    if tuple(resources) != expected_resources:
        raise ValueError("live topology resources must use canonical resource order")
    payload: dict[str, Any] = {"pipeline": spec.pipeline_id}
    for resource_name in expected_resources:
        resource = resources[resource_name]
        if not resource.exists:
            raise ValueError(f"{resource_name} must exist for finalization evidence")
        fields = _jsonable(resource.fields)
        payload[resource_name] = {"name": resource.name, **fields}
    return payload


@dataclass(frozen=True)
class TopologyFinalizationReceipt:
    receipt_id: UUID
    pipeline_id: str
    migration_id: str
    run_id: UUID
    lineage: ReceiptLineage
    target_topology: Mapping[str, Any]
    target_fingerprint: str
    migration_contract: Mapping[str, Any]
    migration_fingerprint: str
    live_topology: Mapping[str, Any]
    live_fingerprint: str
    readiness_status: str
    finalization_code: str
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": str(self.receipt_id),
            "pipeline": self.pipeline_id,
            "migration_id": self.migration_id,
            "run_id": str(self.run_id),
            "lineage": self.lineage,
            "target_topology": _jsonable(self.target_topology),
            "target_fingerprint": self.target_fingerprint,
            "migration_contract": _jsonable(self.migration_contract),
            "migration_fingerprint": self.migration_fingerprint,
            "live_topology": _jsonable(self.live_topology),
            "live_fingerprint": self.live_fingerprint,
            "readiness_status": self.readiness_status,
            "finalization_code": self.finalization_code,
            "created_at": self.created_at.isoformat(),
            "immutable": True,
        }


@dataclass(frozen=True)
class TopologyFinalizationVerification:
    verification_id: UUID
    receipt_id: UUID
    status: VerificationStatus
    code: str
    registry_target_fingerprint: str
    live_fingerprint: str | None
    readiness_status: str | None
    evidence: Mapping[str, Any]
    verified_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "verification_id": str(self.verification_id),
            "receipt_id": str(self.receipt_id),
            "status": self.status,
            "code": self.code,
            "registry_target_fingerprint": self.registry_target_fingerprint,
            "live_fingerprint": self.live_fingerprint,
            "readiness_status": self.readiness_status,
            "evidence": _jsonable(self.evidence),
            "verified_at": self.verified_at.isoformat(),
            "append_only": True,
        }


class PostgresTopologyFinalizationReceiptStore:
    """Persist immutable pre-merge receipts and append-only post-merge verification evidence."""

    def __init__(self, database_url: str):
        self.database_url = _required_name(database_url, "database_url")

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        conn.execute("SET statement_timeout = '8s'")
        conn.execute("SET lock_timeout = '5s'")
        return conn

    @staticmethod
    def _receipt(row: Mapping[str, Any]) -> TopologyFinalizationReceipt:
        return TopologyFinalizationReceipt(
            receipt_id=row["receipt_id"],
            pipeline_id=row["pipeline_id"],
            migration_id=row["migration_id"],
            run_id=row["run_id"],
            lineage=row["lineage"],
            target_topology=dict(row["target_topology"]),
            target_fingerprint=row["target_fingerprint"],
            migration_contract=dict(row["migration_contract"]),
            migration_fingerprint=row["migration_fingerprint"],
            live_topology=dict(row["live_topology"]),
            live_fingerprint=row["live_fingerprint"],
            readiness_status=row["readiness_status"],
            finalization_code=row["finalization_code"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _verification(row: Mapping[str, Any]) -> TopologyFinalizationVerification:
        return TopologyFinalizationVerification(
            verification_id=row["verification_id"],
            receipt_id=row["receipt_id"],
            status=row["status"],
            code=row["code"],
            registry_target_fingerprint=row["registry_target_fingerprint"],
            live_fingerprint=row["live_fingerprint"],
            readiness_status=row["readiness_status"],
            evidence=dict(row["evidence"]),
            verified_at=row["verified_at"],
        )

    def get_receipt(self, receipt_id: UUID) -> TopologyFinalizationReceipt | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM kernel_lab.event_pipeline_topology_finalization_receipts WHERE receipt_id = %s",
                (receipt_id,),
            ).fetchone()
        return None if row is None else self._receipt(row)

    def receipt_for_migration(
        self, pipeline_id: str, migration_id: str
    ) -> TopologyFinalizationReceipt | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM kernel_lab.event_pipeline_topology_finalization_receipts
                WHERE pipeline_id = %s AND migration_id = %s
                """,
                (
                    _required_name(pipeline_id, "pipeline_id"),
                    _required_name(migration_id, "migration_id"),
                ),
            ).fetchone()
        return None if row is None else self._receipt(row)

    def closed_verification(
        self, receipt_id: UUID
    ) -> TopologyFinalizationVerification | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM kernel_lab.event_pipeline_topology_finalization_verifications
                WHERE receipt_id = %s AND status = 'closed'
                ORDER BY verified_at DESC LIMIT 1
                """,
                (receipt_id,),
            ).fetchone()
        return None if row is None else self._verification(row)

    def create_receipt(
        self,
        *,
        pipeline_id: str,
        migration_id: str,
        run_id: UUID,
        lineage: ReceiptLineage,
        target_topology: Mapping[str, Any],
        target_fingerprint: str,
        migration_contract: Mapping[str, Any],
        migration_fingerprint: str,
        live_topology: Mapping[str, Any],
        live_fingerprint: str,
        readiness_status: str,
        finalization_code: str,
    ) -> TopologyFinalizationReceipt:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    INSERT INTO kernel_lab.event_pipeline_topology_finalization_receipts (
                      receipt_id, pipeline_id, migration_id, run_id, lineage,
                      target_topology, target_fingerprint,
                      migration_contract, migration_fingerprint,
                      live_topology, live_fingerprint,
                      readiness_status, finalization_code
                    ) VALUES (
                      %s, %s, %s, %s, %s,
                      %s::jsonb, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s
                    ) RETURNING *
                    """,
                    (
                        uuid4(),
                        pipeline_id,
                        migration_id,
                        run_id,
                        lineage,
                        json.dumps(_jsonable(target_topology)),
                        target_fingerprint,
                        json.dumps(_jsonable(migration_contract)),
                        migration_fingerprint,
                        json.dumps(_jsonable(live_topology)),
                        live_fingerprint,
                        readiness_status,
                        finalization_code,
                    ),
                ).fetchone()
            return self._receipt(row)
        except psycopg.errors.UniqueViolation:
            existing = self.receipt_for_migration(pipeline_id, migration_id)
            if existing is None:
                raise
            return existing

    def record_verification(
        self,
        *,
        receipt_id: UUID,
        status: VerificationStatus,
        code: str,
        registry_target_fingerprint: str,
        live_fingerprint: str | None,
        readiness_status: str | None,
        evidence: Mapping[str, Any],
    ) -> TopologyFinalizationVerification:
        if status == "closed":
            existing = self.closed_verification(receipt_id)
            if existing is not None:
                return existing
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    INSERT INTO kernel_lab.event_pipeline_topology_finalization_verifications (
                      verification_id, receipt_id, status, code,
                      registry_target_fingerprint, live_fingerprint,
                      readiness_status, evidence
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    RETURNING *
                    """,
                    (
                        uuid4(),
                        receipt_id,
                        status,
                        code,
                        registry_target_fingerprint,
                        live_fingerprint,
                        readiness_status,
                        json.dumps(_jsonable(evidence)),
                    ),
                ).fetchone()
            return self._verification(row)
        except psycopg.errors.UniqueViolation:
            existing = self.closed_verification(receipt_id)
            if existing is None:
                raise
            return existing

    def provisioning_identity(self, run_id: UUID) -> tuple[str, str] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT migration_id, state
                FROM kernel_lab.event_pipeline_topology_provisioning_runs
                WHERE run_id = %s
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return row["migration_id"], row["state"]


@dataclass(frozen=True)
class TopologyFinalizationReceiptIssueReport:
    status: IssueStatus
    code: str
    receipt: TopologyFinalizationReceipt | None

    @property
    def successful(self) -> bool:
        return self.status == "issued"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "code": self.code,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "side_effects": {
                "git": False,
                "jetstream": False,
                "postgres_receipt": self.successful,
            },
        }


@dataclass(frozen=True)
class TopologyPostMergeVerificationReport:
    status: VerificationStatus
    code: str
    receipt: TopologyFinalizationReceipt | None
    verification: TopologyFinalizationVerification | None

    @property
    def closed(self) -> bool:
        return self.status == "closed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "code": self.code,
            "migration_closed": self.closed,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "verification": self.verification.to_dict() if self.verification else None,
            "side_effects": {
                "git": False,
                "jetstream": False,
                "postgres_verification": self.verification is not None,
            },
        }


class TopologyFinalizationReceiptController:
    """Bridge the pre-merge finalization proof to an exact post-merge verification."""

    def __init__(
        self,
        *,
        database_url: str,
        nats_url: str | None,
        finalization_inspector: Any | None = None,
        contract_inspector: Any | None = None,
        readiness_collector: Any | None = None,
    ):
        self.database_url = _required_name(database_url, "database_url")
        self.nats_url = nats_url
        self.store = PostgresTopologyFinalizationReceiptStore(database_url)
        self.finalization_inspector = finalization_inspector
        self.contract_inspector = contract_inspector
        self.readiness_collector = readiness_collector

    def _contract_inspector(self) -> Any:
        if self.contract_inspector is not None:
            return self.contract_inspector
        if not self.nats_url:
            raise RuntimeError("nats_url is required")
        return TopologyContractInspector(self.nats_url)

    def _readiness_collector(self) -> Any:
        if self.readiness_collector is not None:
            return self.readiness_collector
        from event_pipeline_topology_readiness import (
            TopologyAwareEventPipelineReadinessCollector,
        )

        return TopologyAwareEventPipelineReadinessCollector()

    def _finalization_inspector(self) -> Any:
        if self.finalization_inspector is not None:
            return self.finalization_inspector
        return TopologyMigrationFinalizationInspector(
            database_url=self.database_url,
            nats_url=self.nats_url,
        )

    def _lineage_matches(self, receipt: TopologyFinalizationReceipt) -> bool:
        identity = self.store.provisioning_identity(receipt.run_id)
        if identity is None:
            return False
        migration_id, state = identity
        if migration_id != receipt.migration_id:
            return False
        if receipt.lineage == "completed":
            return state == "completed"
        if state != "manual_intervention":
            return False
        intervention = PostgresTopologyManualInterventionStore(
            self.database_url
        ).latest_for_run(receipt.run_id)
        return intervention is not None and intervention.state == "resolved_target"

    def issue(
        self,
        spec: Any,
        *,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        watchdog_state: Mapping[str, Any] | None,
        observed_at: datetime | None = None,
    ) -> TopologyFinalizationReceiptIssueReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        if migration_mapping is None:
            return TopologyFinalizationReceiptIssueReport(
                "blocked", "topology_finalization_receipt_migration_missing", None
            )
        migration = TopologyMigrationConfig.from_mapping(migration_mapping)
        existing = self.store.receipt_for_migration(
            spec.pipeline_id, migration.migration_id
        )

        try:
            finalization = self._finalization_inspector().inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                watchdog_state=watchdog_state,
                observed_at=observed_at,
            )
        except Exception:
            return TopologyFinalizationReceiptIssueReport(
                "blocked", "topology_finalization_receipt_gate_unavailable", existing
            )
        if not finalization.safe_to_remove_topology_migration:
            return TopologyFinalizationReceiptIssueReport(
                "blocked", finalization.code, existing
            )

        provisioning = PostgresTopologyProvisioningHealthStore(
            self.database_url
        ).snapshot(
            pipeline_id=spec.pipeline_id,
            observed_at=observed_at,
        )
        terminal = provisioning.latest_terminal
        if terminal is None or terminal.migration_id != migration.migration_id:
            return TopologyFinalizationReceiptIssueReport(
                "blocked", "topology_finalization_receipt_lineage_missing", existing
            )
        lineage = finalization.lineage
        if lineage not in ("completed", "resolved_target"):
            return TopologyFinalizationReceiptIssueReport(
                "blocked", "topology_finalization_receipt_lineage_invalid", existing
            )

        try:
            contract = self._contract_inspector().inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=migration_mapping,
                observed_at=observed_at,
            )
        except Exception:
            return TopologyFinalizationReceiptIssueReport(
                "blocked",
                "topology_finalization_receipt_live_evidence_unavailable",
                existing,
            )
        if contract.matched != "target" or contract.target.status != "ok":
            return TopologyFinalizationReceiptIssueReport(
                "blocked", "topology_finalization_receipt_live_target_changed", existing
            )

        target_payload = target_contract_payload(spec, target_topology)
        live_payload = live_contract_payload(spec, contract)
        target_fingerprint = canonical_fingerprint(target_payload)
        live_fingerprint = canonical_fingerprint(live_payload)
        if live_fingerprint != target_fingerprint:
            return TopologyFinalizationReceiptIssueReport(
                "blocked", "topology_finalization_receipt_fingerprint_mismatch", existing
            )
        migration_fingerprint = canonical_fingerprint(migration_mapping)

        if existing is not None:
            same = (
                existing.run_id == terminal.run_id
                and existing.lineage == lineage
                and existing.target_fingerprint == target_fingerprint
                and existing.migration_fingerprint == migration_fingerprint
                and existing.live_fingerprint == live_fingerprint
            )
            return TopologyFinalizationReceiptIssueReport(
                "issued" if same else "blocked",
                "topology_finalization_receipt_already_issued"
                if same
                else "topology_finalization_receipt_conflict",
                existing,
            )

        receipt = self.store.create_receipt(
            pipeline_id=spec.pipeline_id,
            migration_id=migration.migration_id,
            run_id=terminal.run_id,
            lineage=lineage,
            target_topology=target_payload,
            target_fingerprint=target_fingerprint,
            migration_contract=dict(migration_mapping),
            migration_fingerprint=migration_fingerprint,
            live_topology=live_payload,
            live_fingerprint=live_fingerprint,
            readiness_status="ready",
            finalization_code=finalization.code,
        )
        return TopologyFinalizationReceiptIssueReport(
            "issued", "topology_finalization_receipt_issued", receipt
        )

    def _blocked_verification(
        self,
        *,
        receipt: TopologyFinalizationReceipt,
        code: str,
        registry_target_fingerprint: str,
        live_fingerprint: str | None = None,
        readiness_status: str | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> TopologyPostMergeVerificationReport:
        verification = self.store.record_verification(
            receipt_id=receipt.receipt_id,
            status="blocked",
            code=code,
            registry_target_fingerprint=registry_target_fingerprint,
            live_fingerprint=live_fingerprint,
            readiness_status=readiness_status,
            evidence=evidence or {},
        )
        return TopologyPostMergeVerificationReport(
            "blocked", code, receipt, verification
        )

    def verify_post_merge(
        self,
        spec: Any,
        *,
        receipt_id: UUID,
        target_topology: Mapping[str, Any],
        migration_mapping: Mapping[str, Any] | None,
        watchdog_state: Mapping[str, Any] | None,
        observed_at: datetime | None = None,
    ) -> TopologyPostMergeVerificationReport:
        observed_at = observed_at or datetime.now(timezone.utc)
        receipt = self.store.get_receipt(receipt_id)
        if receipt is None:
            return TopologyPostMergeVerificationReport(
                "blocked", "topology_finalization_receipt_missing", None, None
            )
        if receipt.pipeline_id != spec.pipeline_id:
            return TopologyPostMergeVerificationReport(
                "blocked",
                "topology_finalization_receipt_pipeline_mismatch",
                receipt,
                None,
            )
        closed = self.store.closed_verification(receipt_id)
        if closed is not None:
            return TopologyPostMergeVerificationReport(
                "closed", "topology_migration_already_closed", receipt, closed
            )

        target_payload = target_contract_payload(spec, target_topology)
        target_fingerprint = canonical_fingerprint(target_payload)
        if migration_mapping is not None:
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_cleanup_not_merged",
                registry_target_fingerprint=target_fingerprint,
                evidence={"migration_present": True},
            )
        if target_fingerprint != receipt.target_fingerprint:
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_registry_target_changed",
                registry_target_fingerprint=target_fingerprint,
                evidence={
                    "expected_target_fingerprint": receipt.target_fingerprint,
                    "actual_target_fingerprint": target_fingerprint,
                },
            )
        if not self._lineage_matches(receipt):
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_lineage_changed",
                registry_target_fingerprint=target_fingerprint,
            )

        try:
            contract = self._contract_inspector().inspect(
                spec,
                target_topology=target_topology,
                migration_mapping=None,
                observed_at=observed_at,
            )
        except Exception:
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_post_merge_topology_unavailable",
                registry_target_fingerprint=target_fingerprint,
            )
        if contract.matched != "target" or contract.target.status != "ok":
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_post_merge_topology_not_target",
                registry_target_fingerprint=target_fingerprint,
                evidence={
                    "matched": contract.matched,
                    "target_status": contract.target.status,
                },
            )

        live_payload = live_contract_payload(spec, contract)
        live_fingerprint = canonical_fingerprint(live_payload)
        if (
            live_fingerprint != receipt.live_fingerprint
            or live_fingerprint != receipt.target_fingerprint
        ):
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_post_merge_live_fingerprint_changed",
                registry_target_fingerprint=target_fingerprint,
                live_fingerprint=live_fingerprint,
                evidence={
                    "receipt_live_fingerprint": receipt.live_fingerprint,
                    "current_live_fingerprint": live_fingerprint,
                },
            )

        try:
            readiness = self._readiness_collector().collect(
                spec,
                topology=target_topology,
                topology_migration=None,
                database_url=self.database_url,
                nats_url=self.nats_url,
                watchdog_state=watchdog_state,
                observed_at=observed_at,
            )
        except Exception:
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_post_merge_readiness_unavailable",
                registry_target_fingerprint=target_fingerprint,
                live_fingerprint=live_fingerprint,
            )
        if readiness.status != "ready":
            return self._blocked_verification(
                receipt=receipt,
                code="topology_finalization_post_merge_readiness_not_ready",
                registry_target_fingerprint=target_fingerprint,
                live_fingerprint=live_fingerprint,
                readiness_status=readiness.status,
            )

        verification = self.store.record_verification(
            receipt_id=receipt.receipt_id,
            status="closed",
            code="topology_migration_closed",
            registry_target_fingerprint=target_fingerprint,
            live_fingerprint=live_fingerprint,
            readiness_status="ready",
            evidence={
                "migration_present": False,
                "lineage": receipt.lineage,
                "target_fingerprint": target_fingerprint,
                "live_fingerprint": live_fingerprint,
                "readiness_status": "ready",
            },
        )
        return TopologyPostMergeVerificationReport(
            "closed", "topology_migration_closed", receipt, verification
        )


def render_prometheus(report: TopologyPostMergeVerificationReport) -> str:
    pipeline = report.receipt.pipeline_id if report.receipt else "unknown"
    pipeline = (
        pipeline.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )
    state = report.status
    return "\n".join(
        [
            "# HELP spotwo_wms_event_pipeline_topology_migration_closed Whether post-merge verification closed the migration lifecycle.",
            "# TYPE spotwo_wms_event_pipeline_topology_migration_closed gauge",
            f'spotwo_wms_event_pipeline_topology_migration_closed{{pipeline="{pipeline}"}} {1 if report.closed else 0}',
            "# HELP spotwo_wms_event_pipeline_topology_finalization_receipt_state Bounded post-merge receipt verification state.",
            "# TYPE spotwo_wms_event_pipeline_topology_finalization_receipt_state gauge",
            f'spotwo_wms_event_pipeline_topology_finalization_receipt_state{{pipeline="{pipeline}",state="{state}"}} 1',
        ]
    ) + "\n"
