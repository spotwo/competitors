# ADR 0044: External Event Pipeline Probe Watchdog

- Status: Accepted
- Date: 2026-08-19

## Context

The event pipeline canary now measures real publish, delivery, projection, ACK confirmation, historical SLI windows, and fault recovery. One failure class remains outside that data plane: the canary process itself can fail before it creates its PostgreSQL tracking row.

Examples include:

- the external scheduler never launches the probe;
- the scheduler is badly delayed;
- the probe process starts and hangs;
- PostgreSQL is unreachable before `start_tracked_event_pipeline_canary()` commits;
- the probe process crashes before it emits a valid result.

A PostgreSQL-backed canary cannot reliably report these failures from inside the same PostgreSQL dependency. Treating an absent canary row as proof that a probe was never scheduled would also conflate scheduler failure with database failure.

## Decision

Add an external probe-execution watchdog contract that is intentionally independent of the WMS PostgreSQL database and NATS JetStream.

The external scheduler or monitoring control plane owns:

- the expected schedule slot;
- the probe execution receipt;
- the start and finish timestamps;
- the process-level outcome;
- the bounded process failure code;
- retention of enough recent receipts to detect missed slots.

The WMS repository provides the evaluator, JSON contract, Prometheus rendering, and executable tests. It does not select a production scheduler or external persistence product.

## Execution receipt semantics

Each scheduled slot has at most one canonical receipt:

```text
scheduled_at
  -> started_at?
  -> finished_at?
  -> outcome = running | succeeded | failed
```

`outcome = succeeded` means that the probe process completed and produced a valid canary health result. The returned canary health may itself be `ok`, `warning`, or `critical`.

This distinction is intentional:

```text
watchdog health  = did the canary execute correctly?
canary health    = did the WMS event pipeline behave correctly?
```

A canary that executes successfully and reports a critical pipeline result is therefore a healthy watchdog execution with a critical canary result. The existing canary and SLI layers own that pipeline-health alert.

`outcome = failed` means the external execution could not produce a valid canary result. Failure codes are bounded to:

- `scheduler_launch_failed`
- `postgres_probe_store_unavailable`
- `probe_process_failed`
- `probe_timeout`
- `probe_output_invalid`

Hostnames, exception text, process IDs, execution IDs, URLs, payloads, and other unbounded values are not failure-code dimensions.

## Watchdog health

The evaluator compares the latest expected schedule slot with external receipts.

Default deployment inputs are:

- 30 seconds start grace;
- one missed execution is warning;
- two missed executions are critical;
- 15 seconds scheduler lag is warning;
- 60 seconds scheduler lag is critical;
- 45 seconds running age is critical.

No prior receipt after the grace period is critical because there is no evidence that the watchdog has ever executed successfully.

A current failed receipt is critical. A current running receipt older than the execution timeout is critical.

These defaults are operational starting points, not SLO or compliance policy.

## External ownership requirement

The source of truth for this watchdog must not be the same WMS PostgreSQL instance that the canary is testing.

Acceptable implementations include an external scheduler database, monitoring platform, durable job service, or another independent control-plane store. A local JSON file is provided only as an executable lab interchange format.

The watchdog must remain able to answer:

```text
Was a probe expected?
Did it start?
How late did it start?
Did it finish?
Did it produce a valid canary result?
```

when WMS PostgreSQL or NATS is unavailable.

## Metrics and cardinality

Prometheus identity is limited to the deployment scope:

```text
stream
durable
consumer
```

Additional labels use bounded values only: outcome, result status, alert code, alert severity, and bounded execution failure code.

Execution IDs are retained only in JSON evidence and are never Prometheus labels.

## Consequences

The observability chain becomes:

```text
external scheduler/watchdog
        -> canary execution
        -> PostgreSQL tracked canary
        -> Outbox
        -> NATS stream
        -> durable consumer
        -> Inbox / projection
        -> ACK confirmation
        -> historical SLI / burn rate
```

The outer watchdog can now distinguish "the pipeline failed" from "the probe never ran" and "the probe could not reach its control-plane dependency."

## Non-goals

This ADR does not:

- choose Kubernetes CronJob, Cloud scheduler, GitHub Actions, systemd timer, or another production scheduler;
- store watchdog state in WMS PostgreSQL;
- make NATS a dependency of the watchdog state store;
- automatically restart services or remediate failures;
- duplicate canary stage-health alerts inside the watchdog;
- define a production paging destination;
- claim exact historical missed-run counts beyond the retained external receipt history.
