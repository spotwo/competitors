# Topology migration finalization references

This directory stores machine-readable references that bind a Git cleanup PR to an immutable topology finalization receipt.

A cleanup PR that removes `topology_migration` from `operations/event-pipelines/registry.yml` must add exactly one reference file per removed migration using this deterministic path:

```text
operations/event-pipelines/finalizations/<pipeline-id>/<migration-id>.yml
```

The reference is exported from the immutable PostgreSQL receipt before the cleanup PR is opened. It contains only bounded identifiers, fingerprints, lineage, readiness status, and the requested registry operation. It contains no database URL, NATS URL, credentials, tokens, or arbitrary exception text.

The GitHub cleanup guard verifies that:

- the PR removes a `topology_migration` that existed in the base revision;
- the pipeline is otherwise byte-for-byte equivalent at the parsed YAML level;
- no unrelated files are mixed into the cleanup PR;
- the deterministic finalization reference is present;
- the reference target and live fingerprints equal the exact deployment identity and topology that remain after cleanup;
- the reference migration fingerprint equals the migration contract being removed;
- the reference says the immutable receipt was finalization-ready;
- the reference payload fingerprint is internally consistent;
- the PR body contains a matching `Topology-Finalization-Receipt: <uuid>` trailer.

This is a Git review boundary, not an authenticity oracle for the runtime database. The guard proves that the cleanup PR is bound to a specific receipt-shaped immutable evidence reference and that the Git target has not been mixed with unrelated topology edits. Runtime receipt existence and post-merge health remain the responsibility of the receipt controller and post-merge verifier.

## Operator flow

While `topology_migration` is still present, issue the immutable receipt and capture its UUID. Then export the cleanup reference:

```bash
bin/export-kernel-event-pipeline-topology-finalization-reference \
  --pipeline inventory-position-projection \
  --receipt '<receipt-uuid>' \
  --show-path \
  > operations/event-pipelines/finalizations/inventory-position-projection/<migration-id>.yml
```

Remove only the `topology_migration` stanza from the registry and add this trailer to the PR body:

```text
Topology-Finalization-Receipt: <receipt-uuid>
```

After merge, run the existing post-merge verification against the same receipt UUID. The migration is closed only when that verifier confirms the cleanup is merged, the target fingerprint is unchanged, live JetStream still exactly matches the receipt, and readiness remains `READY`.
