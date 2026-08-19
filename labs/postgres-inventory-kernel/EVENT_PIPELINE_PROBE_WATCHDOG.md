# Event Pipeline External Probe Watchdog

This runbook closes the observability gap outside the PostgreSQL-backed event pipeline canary.

The normal canary can only create durable SLI evidence after it reaches PostgreSQL and atomically registers a tracked run. If the scheduler does not launch, the process hangs, or PostgreSQL is unavailable before registration, the WMS database cannot be the only system asked whether the probe ran.

## Boundary

The watchdog is deliberately outside both WMS PostgreSQL and NATS JetStream:

```text
external scheduler / monitor
        |
        | owns schedule + execution receipt
        v
canary process
        |
        +--> WMS PostgreSQL
        +--> NATS JetStream
```

The external system supplies a small JSON state document. In production that document can be generated from a scheduler database, monitoring system, durable job service, or other independent control plane. The JSON file is only the lab interchange format.

## State contract

Example:

```json
{
  "scope": {
    "stream": "WMS_EVENTS",
    "durable": "EVENT_PIPELINE_CANARY",
    "consumer": "event_pipeline_canary"
  },
  "expected_execution_at": "2026-08-19T01:00:00+00:00",
  "cadence_seconds": 60,
  "receipts": [
    {
      "execution_id": "opaque-external-id",
      "scheduled_at": "2026-08-19T00:59:00+00:00",
      "started_at": "2026-08-19T00:59:02+00:00",
      "finished_at": "2026-08-19T00:59:05+00:00",
      "outcome": "succeeded",
      "probe_status": "ok",
      "failure_code": null,
      "exit_code": 0
    }
  ]
}
```

`expected_execution_at` is the latest schedule slot that should already exist. `cadence_seconds` is used only to estimate missing slots from retained receipts.

The external system should retain enough receipts for the operational horizon it wants to inspect. The evaluator does not invent schedule history that was not supplied.

## Outcome semantics

There are three process-level outcomes:

```text
running
succeeded
failed
```

`running` means the process started but has not produced a terminal receipt.

`succeeded` means the process finished and produced a valid canary result. `probe_status` must then contain `ok`, `warning`, or `critical`.

A succeeded execution with `probe_status = critical` is not an execution failure. It proves that the watchdog and canary process worked while the pipeline itself reported critical health. Pipeline health remains owned by the canary/SLI layers.

`failed` means the external execution could not produce a valid canary result. Allowed failure codes are:

```text
scheduler_launch_failed
postgres_probe_store_unavailable
probe_process_failed
probe_timeout
probe_output_invalid
```

Do not put hostnames, exception messages, PIDs, execution IDs, request IDs, or URLs into `failure_code`.

## Evaluate JSON

From repository root:

```bash
bin/inspect-kernel-event-pipeline-probe-watchdog \
  --state-file /path/to/watchdog-state.json \
  --pretty
```

Use an explicit observation time for deterministic tests or replay:

```bash
bin/inspect-kernel-event-pipeline-probe-watchdog \
  --state-file /path/to/watchdog-state.json \
  --observed-at 2026-08-19T01:00:31Z \
  --check
```

With `--check` the exit codes are:

```text
0  ok
1  warning
2  critical or invalid watchdog state
```

Prometheus output:

```bash
bin/inspect-kernel-event-pipeline-probe-watchdog \
  --state-file /path/to/watchdog-state.json \
  --format prometheus
```

## Default operational thresholds

Defaults are intentionally simple deployment inputs:

```text
start grace                 30 s
missing executions warning  1
missing executions critical 2
scheduler lag warning       15 s
scheduler lag critical      60 s
execution timeout           45 s
```

Interpretation:

- no receipt inside the start grace is not yet unhealthy;
- no prior receipt ever observed after grace is critical;
- one missing slot after an older receipt is warning;
- two or more missing slots are critical;
- excessive start delay reports scheduler lag;
- a running execution beyond the timeout is critical;
- a failed current receipt is critical.

These values are not the event pipeline SLO. The existing tracked canary SLI continues to own availability and latency objectives for the pipeline itself.

## Failure examples

Scheduler did not launch the probe:

```text
expected slot exists
current receipt absent
-> probe_execution_missing
```

Scheduler has never produced any evidence:

```text
expected slot overdue
receipt history empty
-> probe_execution_never_observed
```

Process starts but hangs:

```text
outcome = running
running age >= execution timeout
-> probe_execution_stuck
```

PostgreSQL is unavailable before tracked canary registration:

```text
outcome = failed
failure_code = postgres_probe_store_unavailable
-> probe_execution_failed
```

The external receipt is the evidence in this case. No PostgreSQL canary row should be fabricated.

## Cardinality boundary

Prometheus uses only stable deployment identity:

```text
stream
durable
consumer
```

Bounded dimensions are limited to outcome, returned probe status, alert code, alert severity, and the bounded process failure code.

`execution_id` is useful forensic JSON evidence but is never a Prometheus label.

## What the scheduler must do

The production scheduler integration should perform this sequence in its own control plane:

```text
1. create or identify the expected schedule slot
2. persist a running receipt when launch begins
3. run the event pipeline canary
4. persist succeeded + probe_status when valid output returns
5. persist failed + bounded failure_code when no valid result can be produced
6. expose recent receipts independently of WMS PostgreSQL and NATS
7. evaluate or export watchdog health
```

If the scheduler itself cannot persist step 2, a second monitoring mechanism must detect the missing expected slot. The watchdog contract is intentionally designed so missing execution evidence is itself observable.

## Lab tests

The kernel test suite verifies:

- grace-period behavior;
- never-observed critical health;
- one and multiple missed schedule slots;
- scheduler lag;
- stuck executions;
- bounded external execution failures;
- separation of process execution health from returned canary health;
- timestamp and schedule invariants;
- bounded Prometheus labels;
- CLI health exit codes;
- fail-closed invalid input behavior.

Run the full lab with:

```bash
bash bin/check-kernel-lab
```
