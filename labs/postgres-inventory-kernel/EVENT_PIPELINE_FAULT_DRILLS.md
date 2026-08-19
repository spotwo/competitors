# Event Pipeline Fault Drills

This lab verifies that the event-pipeline observability stack reports real failure modes instead of only succeeding on healthy-path tests.

The drills deliberately inject one bounded fault at a time into isolated synthetic canary infrastructure. They never mutate inventory projections or warehouse domain state.

## Run the focused drill suite

```bash
bin/run-kernel-event-pipeline-fault-drills
```

The command creates an isolated PostgreSQL + NATS lab, applies all kernel migrations, runs only the event-pipeline fault drill tests, and tears the lab down afterwards.

The normal kernel gate also runs these tests because they live in the regular test suite.

## Fault matrix

| Fault | Primary canary evidence | Secondary evidence | SLI visibility | Recovery proof |
| --- | --- | --- | --- | --- |
| publisher stopped | `canary_publish_timeout` | none | canary SLI | next canary completes |
| publisher cannot reach NATS | `canary_publish_timeout` | Outbox publish failure + retry state | canary SLI | transport restored, next canary completes |
| consumer stopped | `canary_delivery_timeout` | pending JetStream delivery | canary SLI | consumer restored, next canary completes |
| canary durable misconfigured | `canary_consumer_configuration_invalid` | durable configuration mismatch | canary SLI | durable repaired, next canary completes |
| Inbox handler raises | `canary_delivery_timeout` | `handler_failure` in durable consumer-failure lane | canary SLI | handler restored, next canary completes |
| projection commits but ACK is withheld | `canary_ack_timeout` | ACK floor remains behind canary sequence | canary SLI | ACK path restored, next canary completes |
| PostgreSQL unavailable before canary registration | no durable canary can be created | `postgres_probe_store_unavailable` | control-plane only | database restored, next canary completes |

## Why handler failure appears as delivery timeout in the canary

The Inbox receipt and canary projection are one PostgreSQL transaction. If the handler raises, that transaction rolls back. The consumer-failure lane then durably captures the valid event and ACKs it.

From the synthetic canary's own persisted path there is therefore no committed Inbox receipt or projection, so the canary correctly reports the first missing durable stage as `canary_delivery_timeout`.

The exact handler cause is preserved separately as bounded consumer-failure evidence, for example `handler_failure`, `database_deadlock`, or `handler_constraint_violation`.

The drill verifies both layers instead of inventing a more specific canary code that the canary itself did not observe.

## Why PostgreSQL outage is different

The SLI history is stored in PostgreSQL. If PostgreSQL is unavailable before `start_tracked_event_pipeline_canary(...)` commits, there is no trustworthy canary identity, deadline, Outbox event, or SLI row to finalize.

The drill therefore treats pre-registration PostgreSQL failure as an explicit control-plane dependency rather than fabricating a canary failure record after the fact.

This means production monitoring must also watch the probe process and PostgreSQL reachability outside the database-backed SLI. Once PostgreSQL recovers, the drill requires the next tracked canary to complete normally.

## Recovery semantics

A recovery canary being green does not erase the preceding failure from the SLI window.

For canary-visible faults, the drill proves all of these properties:

1. the injected failure produces the expected bounded terminal code;
2. the failed outcome is durably present in the 5-minute SLI window;
3. the failure consumes availability error budget;
4. the fault is repaired;
5. the next synthetic canary completes publish, delivery, projection, and ACK confirmation;
6. the failure remains in history while the consecutive-failure streak returns to zero.

This is intentional. Recovery means the path works again, not that the incident never happened.

## Isolation and safety

The fault suite creates unique JetStream stream, durable, logical consumer, and subject-prefix identities per test. The stream subject is synthetic and never overlaps the normal `spotwo.wms.events` production route.

The NATS-unavailable drill uses an unreachable local endpoint for the publisher adapter instead of stopping the shared test broker. This exercises the same failed transport boundary without destabilizing unrelated tests.

The ACK drill processes the synthetic event through the real Inbox transaction but intentionally withholds the broker ACK until the failed outcome is recorded. It then sends the ACK before running the recovery canary.

The consumer-misconfiguration drill creates an intentionally wrong exact filter, verifies that the observer fails closed, then replaces the isolated stream and durable with the correct configuration before recovery.

## What these drills prove

The suite is specifically intended to catch observability regressions such as:

- a stopped publisher looking healthy because no database failure row exists;
- a stopped consumer looking idle instead of stalled;
- a broken durable filter silently dropping canary coverage;
- a handler failure being ACKed without durable secondary evidence;
- a committed projection being declared complete before the JetStream ACK floor advances;
- a recovery run accidentally overwriting or hiding historical SLI failure evidence.

These are executable fault contracts, not production chaos automation. Production fault injection should remain separately controlled, explicitly authorized, and scoped to disposable or pre-approved infrastructure.
