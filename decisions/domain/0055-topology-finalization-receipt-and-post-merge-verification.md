# ADR 0055: Bind topology migration cleanup to an immutable receipt and post-merge verification

- Status: accepted
- Date: 2026-08-19

## Context

ADR 0054 introduced a read-only gate that proves when a temporary `topology_migration` compatibility stanza can be removed from the deployment registry. That closes the logical rollout state, but a time-of-check/time-of-use gap remains between the successful finalization check and the Git merge that removes the stanza.

During that gap, the declared target, deployment identity, JetStream configuration, provisioning lineage, or readiness state could change. A cleanup pull request that was safe when prepared must not be treated as proof that the migration is still safely closed after merge.

## Decision

Introduce a two-step finalization receipt protocol.

### 1. Pre-merge immutable receipt

A receipt may be issued only after the ADR 0054 finalization gate is safe. The issuer then takes one additional live JetStream configuration snapshot and requires it to exactly match the reviewed target.

The receipt binds:

- pipeline identity;
- migration ID;
- terminal provisioning run ID;
- successful lineage (`completed` or `resolved_target`);
- the complete target contract, including stream and durable consumer identities;
- a canonical SHA-256 target fingerprint;
- the complete migration contract and its canonical fingerprint;
- the live JetStream configuration snapshot and fingerprint;
- the finalization gate result;
- `READY` as the required pre-merge readiness state.

For an exact target, the target and live fingerprints must be equal before a receipt can be persisted.

A pipeline and migration ID can have only one receipt. Reissuing the same evidence is idempotent. Rebinding the same migration ID to different target, lineage, or live evidence is blocked.

Receipt rows are immutable at the database layer.

### 2. Post-merge verification

The cleanup merge does not itself close the migration lifecycle. The exact receipt must be verified after the merge.

Post-merge verification requires all of the following:

1. the receipt exists and belongs to the requested pipeline;
2. `topology_migration` is absent from the current registry;
3. the current declared target contract fingerprint is identical to the receipt target fingerprint;
4. the original provisioning lineage still matches the receipt;
5. live JetStream exactly matches the steady-state target with no migration compatibility contract;
6. the current live topology fingerprint is identical to the receipt live and target fingerprints;
7. deployment readiness, evaluated with `topology_migration=None`, is `READY`.

Only then is an append-only `closed` verification written.

Blocked verification attempts are also append-only evidence. A later successful verification may close the same receipt after a transient blocker is corrected. Once a successful `closed` verification exists, repeated verification is idempotent.

## Canonical fingerprint scope

The target fingerprint is not only the nested `topology` YAML object. It also includes deployment identity derived from the registry:

- pipeline ID;
- stream name;
- business durable name;
- canary durable name;
- all bounded stream topology fields;
- all bounded business consumer topology fields;
- all bounded canary consumer topology fields.

This prevents a cleanup merge from silently changing resource identity while preserving the same nested topology values.

Fingerprints are canonical SHA-256 hashes over sorted JSON with normalized topology values.

## Safety boundary

The receipt protocol does not:

- edit Git;
- merge a cleanup pull request;
- mutate JetStream;
- create, update, delete, or recreate streams or consumers;
- reopen terminal provisioning runs;
- modify manual-intervention history;
- infer that an unknown live topology is acceptable;
- automatically retry a failed rollout.

It only writes immutable/append-only PostgreSQL evidence around read-only Git-registry, JetStream, provisioning-history, and readiness checks.

## Operational consequence

The lifecycle becomes:

```text
migration rollout succeeds
        |
ADR 0054 finalization gate READY
        |
issue immutable receipt
        |
reviewed Git cleanup removes topology_migration
        |
verify exact receipt after merge
        |
MIGRATION CLOSED
```

A migration is operationally closed only after the successful post-merge verification record exists.

## Consequences

### Positive

- removes the final time-of-check/time-of-use ambiguity between runtime proof and Git cleanup;
- binds cleanup to exact target identity and topology, not just a migration ID;
- preserves immutable evidence for audit and incident analysis;
- keeps Git and JetStream mutations outside the verifier;
- supports deterministic agent workflows without allowing an agent to bless drift by inference.

### Negative

- adds two small append-only PostgreSQL tables;
- cleanup now requires an explicit receipt ID and a post-merge verification step;
- transient readiness or broker failures may leave a receipt open until verification succeeds.

## Follow-up

A later workflow may use the receipt ID as a required input to a Git cleanup PR or deployment orchestrator. That integration must still keep registry mutation reviewable and must not weaken the post-merge verification requirements.
