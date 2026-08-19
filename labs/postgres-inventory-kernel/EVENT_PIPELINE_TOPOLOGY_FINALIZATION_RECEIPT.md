# Event pipeline topology finalization receipt runbook

This runbook closes the gap between a successful topology migration finalization check and the Git merge that removes the temporary `topology_migration` compatibility stanza.

The receipt workflow is evidence-only. It writes immutable or append-only PostgreSQL records, but it does not edit Git and does not mutate JetStream.

## Preconditions

Before issuing a receipt:

1. the reviewed `topology_migration` stanza is still present;
2. the rollout has reached the exact target;
3. the topology migration finalization gate reports that removal is safe;
4. the deployment is `READY`;
5. PostgreSQL, NATS, and the external canary watchdog evidence are available.

Set the same runtime environment variables used by the deployment registry:

```bash
export KERNEL_LAB_DATABASE_URL='postgresql://...'
export KERNEL_LAB_NATS_URL='nats://...'
export KERNEL_LAB_CANARY_WATCHDOG_STATE_PATH='/var/lib/spotwo/canary-watchdog.json'
```

## Phase 1 - Issue the pre-merge receipt

Run:

```bash
bin/record-kernel-event-pipeline-topology-finalization \
  --pipeline inventory-position-projection \
  --pretty \
  --check \
  issue-receipt
```

A successful response has:

```text
status = issued
code   = topology_finalization_receipt_issued
```

Record the returned `receipt_id` in the cleanup pull request or deployment record.

The receipt binds the reviewed migration to:

- pipeline ID;
- stream and durable consumer identities;
- terminal provisioning run ID;
- successful lineage;
- complete target topology fingerprint;
- migration contract fingerprint;
- fresh live JetStream topology fingerprint;
- finalization gate result.

The target and live topology fingerprints must be identical at receipt issuance.

Re-running the command with exactly the same evidence returns the same receipt and `topology_finalization_receipt_already_issued`. The same migration ID cannot be rebound to different evidence.

## Phase 2 - Review and merge the Git cleanup

The cleanup change should remove only the temporary `topology_migration` compatibility stanza unless another reviewed change is intentionally part of the rollout.

Do not treat the Git merge itself as proof that the migration lifecycle is closed.

The receipt verifier will reject the cleanup if the current target contract fingerprint differs from the receipt. This includes changes to stream or durable consumer identity, even if the nested `topology` object remains unchanged.

## Phase 3 - Verify after the cleanup merge

After the cleanup commit is on the deployed registry revision, run:

```bash
bin/record-kernel-event-pipeline-topology-finalization \
  --pipeline inventory-position-projection \
  --pretty \
  --check \
  verify-post-merge \
  --receipt '<receipt-uuid>'
```

Successful closure returns:

```text
status = closed
code   = topology_migration_closed
migration_closed = true
```

The verifier requires:

```text
topology_migration absent
        +
current target fingerprint == receipt target fingerprint
        +
original successful lineage still valid
        +
live JetStream == exact steady-state target
        +
current live fingerprint == receipt live fingerprint
        +
current live fingerprint == receipt target fingerprint
        +
readiness with topology_migration=None == READY
```

Only then is an append-only `closed` verification written.

## Blocker codes

Common blockers include:

```text
topology_finalization_receipt_missing
topology_finalization_cleanup_not_merged
topology_finalization_registry_target_changed
topology_finalization_lineage_changed
topology_finalization_post_merge_topology_unavailable
topology_finalization_post_merge_topology_not_target
topology_finalization_post_merge_live_fingerprint_changed
topology_finalization_post_merge_readiness_unavailable
topology_finalization_post_merge_readiness_not_ready
```

A blocked verification does not alter Git or JetStream. It appends a blocked verification record when a receipt exists, preserving evidence for later review.

Correct the blocker and run the same exact receipt again. A successful later verification may close the receipt. Once closed, repeated verification is idempotent.

## Prometheus output

Post-merge verification can emit bounded metrics:

```bash
bin/record-kernel-event-pipeline-topology-finalization \
  --pipeline inventory-position-projection \
  --database-url "$KERNEL_LAB_DATABASE_URL" \
  verify-post-merge \
  --receipt '<receipt-uuid>' \
  --format prometheus
```

Metrics expose only bounded pipeline/state labels. Receipt IDs, run IDs, migration IDs, fingerprints, and operator identities are not labels.

## Failure handling

### Receipt issuance is blocked

Do not remove `topology_migration`. Investigate the finalization gate, provisioning lineage, live topology, or readiness blocker first.

### Cleanup was merged but post-merge verification is blocked

Do not mark the migration closed. Keep the receipt and blocked verification evidence. Determine whether the deployed registry target changed, JetStream drifted, readiness degraded, or runtime evidence is unavailable.

### Live topology is no longer the receipt target

Do not infer a safe state and do not mutate the broker from the verifier. Use the existing topology drift, provisioning, recovery, or manual-intervention workflows as appropriate.

## Database evidence

Receipt rows live in:

```text
kernel_lab.event_pipeline_topology_finalization_receipts
```

Post-merge attempts live in:

```text
kernel_lab.event_pipeline_topology_finalization_verifications
```

Both are protected by database triggers against row update or delete. Test cleanup uses `TRUNCATE ... CASCADE`; operational tooling must treat the records as append-only evidence.

## Definition of done

A topology migration is closed only when all of the following are true:

1. a successful immutable receipt was issued before Git cleanup;
2. the reviewed cleanup removed `topology_migration`;
3. the exact receipt was verified after merge;
4. a `closed` append-only verification exists;
5. live topology still equals the target and deployment readiness is `READY` without migration compatibility.
