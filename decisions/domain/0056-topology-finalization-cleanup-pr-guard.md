# ADR 0056: Guard topology migration cleanup pull requests

Status: Accepted

## Context

ADR 0055 introduced immutable pre-merge topology finalization receipts and exact post-merge verification. That closes the runtime race between a finalization decision and the state observed after the Git cleanup merge, but the Git pull request itself still needs a machine-readable review boundary.

Without a dedicated guard, a cleanup PR could accidentally remove `topology_migration` while also changing the target topology, consumer identity, stream identity, runtime policy, or unrelated repository files. A reviewer could also lose the direct association between the cleanup commit and the immutable receipt that authorized it.

The GitHub workflow cannot prove that a PostgreSQL receipt row exists without giving repository CI access to the runtime database. That would widen the failure and credential boundary and is intentionally not required here.

## Decision

Topology migration cleanup is treated as a narrow Git protocol.

When a pull request removes `topology_migration` from an event pipeline deployment, it must:

1. leave the rest of that parsed pipeline mapping unchanged;
2. add one deterministic finalization reference at `operations/event-pipelines/finalizations/<pipeline-id>/<migration-id>.yml`;
3. bind that reference to the immutable receipt identifiers and SHA-256 fingerprints exported from PostgreSQL;
4. prove that the receipt target fingerprint and receipt live fingerprint equal the exact deployment identity and topology that remain in the PR;
5. prove that the receipt migration fingerprint equals the migration contract being removed;
6. include `Topology-Finalization-Receipt: <uuid>` in the PR body for every cleanup reference;
7. contain no unrelated file changes.

A dedicated GitHub Actions workflow evaluates this policy against the PR base commit and head commit, not against an inferred working-tree state.

Persisted finalization references are schema validated and internally fingerprinted. They remain historical audit records after the cleanup merge.

## Security boundary

The cleanup guard does not authenticate the PostgreSQL receipt against the runtime database. It verifies a receipt-shaped reference, its internal fingerprint, the exact Git target fingerprint, the removed migration fingerprint, the finalization-ready status, and the visible PR trailer.

Runtime receipt existence remains enforced by the receipt exporter before the PR is created and by post-merge verification after the PR is merged.

Repository CI therefore does not receive WMS database credentials merely to validate a Git cleanup.

## Consequences

A finalization cleanup PR is intentionally small. Documentation, refactoring, target topology changes, migration edits, or unrelated product work must be submitted separately.

The strict boundary reduces ambiguity during review and makes the cleanup commit reproducibly attributable to one immutable receipt. It also gives merge policy a dedicated status check that can be required independently of the broad knowledge-base and kernel-lab workflows.

This decision does not automate Git merge, mutate JetStream, remove registry state automatically, or replace the post-merge verifier.
