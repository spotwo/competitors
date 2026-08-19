# ADR 0043: Event pipeline fault injection and recovery drills

- Status: Accepted
- Date: 2026-08-19
- Scope: PostgreSQL inventory kernel event pipeline observability

## Context

The kernel now has independent operational health views for PostgreSQL Outbox, JetStream streams, JetStream durable consumers, malformed deliveries, consumer failures, projection gaps, an aggregate event-pipeline health view, a synthetic end-to-end canary, and durable SLI/SLO burn-rate history.

Healthy-path tests prove that these components can report success. They do not prove that the observability contract remains truthful when a real pipeline stage fails.

A monitoring system that only passes green-path tests can regress into dangerous false-green states, for example:

- publisher execution stops while PostgreSQL remains healthy;
- the publisher cannot reach NATS;
- the consumer stops and JetStream backlog accumulates;
- the canary durable exists with the wrong filter;
- a valid event handler fails after delivery;
- the Inbox transaction commits but broker ACK confirmation never completes;
- PostgreSQL is unavailable before a synthetic canary can be durably registered.

The next kernel slice therefore needs executable failure contracts, not another read-only metric.

## Decision

Add isolated event-pipeline fault injection and recovery drills to the PostgreSQL kernel lab.

The drill suite MUST inject one bounded failure at a time and verify both failure visibility and recovery.

For faults that can be durably registered in PostgreSQL, each drill MUST prove:

1. the synthetic canary reports the expected bounded terminal stage;
2. the failed canary outcome is durably retained in SLI history;
3. availability error budget is consumed;
4. the fault is repaired;
5. the next canary completes normally;
6. historical failure evidence remains present after recovery;
7. the consecutive-failure streak returns to zero after a successful recovery canary.

The initial fault matrix is:

| Fault | Required canary result | Required secondary evidence |
| --- | --- | --- |
| publisher stopped | `canary_publish_timeout` | none |
| publisher cannot reach NATS | `canary_publish_timeout` | failed Outbox publish attempt and retry state |
| consumer stopped | `canary_delivery_timeout` | broker delivery remains unconsumed |
| canary durable misconfigured | `canary_consumer_configuration_invalid` | exact-filter mismatch |
| Inbox handler failure | `canary_delivery_timeout` | bounded durable consumer-failure code |
| ACK confirmation withheld | `canary_ack_timeout` | durable ACK floor remains behind canary stream sequence |

PostgreSQL unavailability before tracked-canary registration is an explicit exception. The database-backed SLI MUST NOT fabricate a canary identity or terminal outcome that was never durably created. The drill MUST instead prove that registration fails closed and that the first canary after database recovery succeeds.

Production monitoring therefore still requires an out-of-band control-plane check for probe execution and PostgreSQL reachability.

## Synthetic isolation

Fault drills MUST use unique synthetic JetStream stream, durable, logical consumer, and subject-prefix identities.

They MUST NOT reuse the normal production event subject prefix and MUST NOT mutate inventory, allocation, reservation, warehouse work, or inventory projection state.

The focused command is:

```bash
bin/run-kernel-event-pipeline-fault-drills
```

It starts the disposable local lab, applies migrations, runs only the fault drill tests, and tears the lab down.

The regular kernel CI gate also runs the same tests as part of the complete suite.

## Fault-specific semantics

### Publisher stopped

Do not run the publisher after the tracked canary is created. When the deadline expires, the canary must report `canary_publish_timeout`.

Recovery starts another tracked canary, restores normal publishing, consumes all eligible synthetic messages, and requires the recovery canary to be green.

### NATS unavailable to the publisher

Point the NATS transport adapter at an unreachable loopback endpoint. The real Outbox runtime must claim the event, fail publication, and persist normal retry evidence.

The canary remains unpublished and terminates as `canary_publish_timeout`.

This isolates the transport reachability failure without stopping the NATS service used by unrelated tests.

### Consumer stopped

Publish the canary into the real isolated JetStream stream but do not run the consumer. The first missing durable stage is Inbox delivery, so the canary must terminate as `canary_delivery_timeout`.

### Consumer misconfigured

Provision the dedicated canary durable with a filter that does not exactly match `<subject-prefix>.health_check.ping`.

The ACK observer must fail closed immediately with `canary_consumer_configuration_invalid`; the drill does not wait for a timeout to discover a known invalid deployment configuration.

### Inbox handler failure

Run the real Inbox consumer with a handler that raises. The Inbox receipt and projection transaction rolls back. The durable consumer-failure lane captures bounded secondary evidence and only then ACKs the JetStream delivery.

Because no Inbox receipt committed, the canary's first missing durable stage remains delivery and its terminal code is `canary_delivery_timeout`.

The drill must additionally prove the consumer-failure lane contains `handler_failure` for the same event.

### ACK confirmation failure

Fetch the real JetStream delivery and commit the real Inbox receipt plus synthetic projection, but intentionally withhold the broker ACK.

The canary must see the committed projection while the durable ACK floor remains behind its stream sequence, then terminate as `canary_ack_timeout`.

The drill sends the withheld ACK before recovery.

### PostgreSQL unavailable

Attempt tracked-canary registration against an unavailable PostgreSQL endpoint. The operation must fail with no partial synthetic event or SLI record.

This failure cannot be represented truthfully inside the unavailable database itself. It is classified as `postgres_probe_store_unavailable` at the drill control plane only.

After database availability is restored, the next tracked canary must succeed.

## SLI recovery behavior

A green recovery canary MUST NOT erase the preceding failure.

Availability and latency windows are historical measurements. After one failed canary and one successful recovery canary, the SLI must still show the failed outcome and corresponding error-budget consumption while the current consecutive-failure streak is zero.

This distinction prevents a single recovery event from hiding an incident that happened moments earlier.

## Alternatives rejected

### Only unit-test alert policies

Rejected because policy tests can pass while real PostgreSQL transactions, JetStream consumer configuration, ACK floors, or Outbox retry behavior no longer match the model.

### Stop shared PostgreSQL and NATS containers inside every pytest case

Rejected for the initial slice because broad container disruption makes unrelated integration tests flaky and can destroy the evidence needed to evaluate the failure.

The initial suite prefers isolated link-level or component-level fault injection while still using the real PostgreSQL and NATS protocols.

### Invent a database SLI outcome for pre-registration PostgreSQL outage

Rejected because the database cannot truthfully claim that a canary was started when the registration transaction never committed.

An external monitor is the correct place to observe that control-plane dependency.

### Automatically remediate production failures from the drill framework

Rejected. The drill suite is verification, not autonomous chaos or remediation. Production fault injection requires separate authorization, blast-radius controls, and rollback procedures.

## Consequences

Positive:

- observability now has executable negative-path contracts;
- false-green regressions become CI failures;
- failure-stage semantics are documented and machine-tested;
- SLI history is proven to survive recovery;
- the PostgreSQL observability dependency is explicit rather than hidden.

Tradeoffs:

- the kernel CI suite becomes slightly slower because each fault drill uses real PostgreSQL and NATS integration paths;
- handler-failure diagnosis requires combining canary stage evidence with the consumer-failure lane;
- PostgreSQL pre-registration outage remains an out-of-band monitoring responsibility.

## Follow-up

A later production-hardening slice may add externally scheduled black-box probe execution, deployment-specific runbooks, and controlled chaos experiments. Those should consume the same bounded failure semantics proven here rather than introducing a second interpretation of pipeline health.
