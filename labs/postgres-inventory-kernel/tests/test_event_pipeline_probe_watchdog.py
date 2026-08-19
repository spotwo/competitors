from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from event_pipeline_probe_watchdog import (
    ExternalProbeExecutionReceipt,
    ExternalProbeWatchdogPolicy,
    ExternalProbeWatchdogSnapshot,
    render_prometheus,
)

BASE = datetime(2026, 8, 19, 1, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / "bin" / "inspect-kernel-event-pipeline-probe-watchdog"


def receipt(
    execution_id: str,
    scheduled_at: datetime,
    *,
    outcome: str = "succeeded",
    start_delay: float = 1,
    duration: float = 1,
    probe_status: str | None = "ok",
    failure_code: str | None = None,
) -> ExternalProbeExecutionReceipt:
    started_at = scheduled_at + timedelta(seconds=start_delay)
    if outcome == "running":
        return ExternalProbeExecutionReceipt(
            execution_id=execution_id,
            scheduled_at=scheduled_at,
            started_at=started_at,
            outcome="running",
        )
    if outcome == "failed":
        return ExternalProbeExecutionReceipt(
            execution_id=execution_id,
            scheduled_at=scheduled_at,
            started_at=started_at,
            finished_at=started_at + timedelta(seconds=duration),
            outcome="failed",
            failure_code=failure_code or "probe_process_failed",
            exit_code=2,
        )
    return ExternalProbeExecutionReceipt(
        execution_id=execution_id,
        scheduled_at=scheduled_at,
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=duration),
        outcome="succeeded",
        probe_status=probe_status,
        exit_code=0,
    )


def snapshot(
    *,
    observed_at: datetime,
    expected_execution_at: datetime = BASE,
    receipts: tuple[ExternalProbeExecutionReceipt, ...] = (),
    cadence_seconds: float = 60,
) -> ExternalProbeWatchdogSnapshot:
    return ExternalProbeWatchdogSnapshot(
        observed_at=observed_at,
        stream_name="WMS_EVENTS",
        durable_name="EVENT_PIPELINE_CANARY",
        consumer_name="event_pipeline_canary",
        expected_execution_at=expected_execution_at,
        cadence_seconds=cadence_seconds,
        receipts=receipts,
    )


def test_watchdog_allows_current_slot_to_start_inside_grace():
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(observed_at=BASE + timedelta(seconds=10))
    )
    assert report.status == "ok"
    assert report.alerts == ()


def test_watchdog_treats_never_observed_probe_as_critical_after_grace():
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(observed_at=BASE + timedelta(seconds=31))
    )
    assert report.status == "critical"
    assert [alert.code for alert in report.alerts] == [
        "probe_execution_never_observed"
    ]


def test_watchdog_warns_after_one_missing_execution():
    previous = receipt("prev", BASE - timedelta(seconds=60))
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(
            observed_at=BASE + timedelta(seconds=31),
            receipts=(previous,),
        )
    )
    assert report.status == "warning"
    assert report.alerts[0].code == "probe_execution_missing"
    assert report.alerts[0].observed_value == 1


def test_watchdog_is_critical_after_two_missing_executions():
    previous = receipt("prev", BASE - timedelta(seconds=120))
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(
            observed_at=BASE + timedelta(seconds=31),
            receipts=(previous,),
        )
    )
    assert report.status == "critical"
    assert report.alerts[0].observed_value == 2


def test_watchdog_reports_scheduler_lag_without_waiting_for_probe_completion():
    current = receipt("current", BASE, outcome="running", start_delay=20)
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(
            observed_at=BASE + timedelta(seconds=25),
            receipts=(current,),
        )
    )
    assert report.status == "warning"
    assert report.alerts[0].code == "probe_scheduler_lag"


def test_watchdog_marks_stuck_execution_critical():
    current = receipt("current", BASE, outcome="running", start_delay=1)
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(
            observed_at=BASE + timedelta(seconds=50),
            receipts=(current,),
        )
    )
    assert report.status == "critical"
    assert any(alert.code == "probe_execution_stuck" for alert in report.alerts)


def test_watchdog_marks_external_execution_failure_critical():
    current = ExternalProbeExecutionReceipt(
        execution_id="current",
        scheduled_at=BASE,
        finished_at=BASE + timedelta(seconds=1),
        outcome="failed",
        failure_code="scheduler_launch_failed",
    )
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(
            observed_at=BASE + timedelta(seconds=2),
            receipts=(current,),
        )
    )
    assert report.status == "critical"
    assert report.alerts[0].observed_value == "scheduler_launch_failed"


def test_probe_health_is_not_reclassified_as_watchdog_execution_failure():
    current = receipt("current", BASE, probe_status="critical")
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(
            observed_at=BASE + timedelta(seconds=5),
            receipts=(current,),
        )
    )
    assert report.status == "ok"
    assert report.snapshot.current_receipt.probe_status == "critical"


def test_snapshot_rejects_duplicate_schedule_slots():
    first = receipt("first", BASE)
    second = receipt("second", BASE)
    with pytest.raises(ValueError, match="one receipt"):
        snapshot(
            observed_at=BASE + timedelta(seconds=5),
            receipts=(first, second),
        )


def test_receipt_rejects_unbounded_failure_code():
    with pytest.raises(ValueError, match="bounded failure_code"):
        ExternalProbeExecutionReceipt(
            execution_id="bad",
            scheduled_at=BASE,
            finished_at=BASE + timedelta(seconds=1),
            outcome="failed",
            failure_code="database-host-10-0-0-1-refused",
        )


def test_snapshot_rejects_future_receipt_timestamps():
    current = receipt("current", BASE, outcome="running", start_delay=10)
    with pytest.raises(ValueError, match="started_at cannot be in the future"):
        snapshot(
            observed_at=BASE + timedelta(seconds=5),
            receipts=(current,),
        )


def test_state_loader_uses_external_scheduler_state_without_database():
    payload = {
        "scope": {
            "stream": "WMS_EVENTS",
            "durable": "EVENT_PIPELINE_CANARY",
            "consumer": "event_pipeline_canary",
        },
        "expected_execution_at": BASE.isoformat(),
        "cadence_seconds": 60,
        "receipts": [receipt("current", BASE).to_dict()],
    }
    loaded = ExternalProbeWatchdogSnapshot.from_dict(
        payload,
        observed_at=BASE + timedelta(seconds=5),
    )
    assert loaded.current_receipt is not None
    assert loaded.current_receipt.execution_id == "current"


def test_prometheus_uses_only_bounded_labels_and_never_execution_id():
    current = receipt("secret-execution-id", BASE, probe_status="critical")
    report = ExternalProbeWatchdogPolicy().evaluate(
        snapshot(
            observed_at=BASE + timedelta(seconds=5),
            receipts=(current,),
        )
    )
    output = render_prometheus(report)
    assert 'stream="WMS_EVENTS"' in output
    assert 'status="critical"' in output
    assert "secret-execution-id" not in output
    assert "execution_id" not in output


def test_cli_check_exit_code_tracks_watchdog_health(tmp_path):
    previous = receipt("prev", BASE - timedelta(seconds=60))
    state = {
        "scope": {
            "stream": "WMS_EVENTS",
            "durable": "EVENT_PIPELINE_CANARY",
            "consumer": "event_pipeline_canary",
        },
        "expected_execution_at": BASE.isoformat(),
        "cadence_seconds": 60,
        "receipts": [previous.to_dict()],
    }
    state_file = tmp_path / "watchdog.json"
    state_file.write_text(json.dumps(state), encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(CLI),
            "--state-file",
            str(state_file),
            "--observed-at",
            (BASE + timedelta(seconds=31)).isoformat(),
            "--check",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert json.loads(result.stdout)["status"] == "warning"


def test_cli_invalid_state_fails_closed(tmp_path):
    state_file = tmp_path / "watchdog.json"
    state_file.write_text('{"scope": {}}', encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(CLI), "--state-file", str(state_file)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "watchdog failed" in result.stderr
