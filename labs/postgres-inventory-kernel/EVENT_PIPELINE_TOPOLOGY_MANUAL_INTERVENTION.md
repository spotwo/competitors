# Event pipeline topology manual intervention

Use this workflow only after crash-safe provisioning has entered terminal `manual_intervention`.

The workflow is intentionally separate from the provisioning journal. It never reopens the terminal rollout run and never turns unknown drift into automatic repair.

## Safety contract

Every resolution requires:

1. a terminal provisioning run in `manual_intervention`;
2. the same reviewed `topology_migration.id` still present in the deployment registry;
3. a captured live JetStream evidence snapshot and SHA-256 fingerprint;
4. one operator proposal;
5. confirmation by a different operator;
6. an evidence re-check immediately before execution.

Allowed actions are only `restore_source`, `accept_target`, and `abort`.

## Inspect operator attention

```bash
bin/inspect-kernel-event-pipeline-topology-interventions \
  --pipeline inventory-position-projection \
  --pretty \
  --check
```

Exit codes are `0` clear, `1` warning, and `2` critical.

Prometheus output is available with `--format prometheus`. Metrics use only bounded pipeline/state labels. Intervention IDs, run IDs, migration IDs, operator identities, and arbitrary error text are not metric labels.

## Detect

Detection is read-only against JetStream and creates only the PostgreSQL intervention record:

```bash
bin/resolve-kernel-event-pipeline-topology-intervention \
  --pipeline inventory-position-projection \
  detect \
  --pretty
```

Detection is idempotent while an active intervention attempt exists.

If a previous attempt failed, detection can create a new attempt with fresh evidence. A previously resolved or explicitly aborted intervention closes that provisioning incident.

## Propose

Choose exactly one action:

```bash
bin/resolve-kernel-event-pipeline-topology-intervention \
  --pipeline inventory-position-projection \
  propose \
  --intervention <uuid> \
  --action restore_source \
  --operator <operator-id>
```

Possible actions:

- `restore_source` - restore only the journaled resource using the captured reviewed source snapshot;
- `accept_target` - accept only an already exact reviewed target, without topology mutation;
- `abort` - close the intervention attempt without changing JetStream.

The proposal does not mutate JetStream.

## Confirm

A second, different operator must confirm:

```bash
bin/resolve-kernel-event-pipeline-topology-intervention \
  --pipeline inventory-position-projection \
  confirm \
  --intervention <uuid> \
  --operator <second-operator-id>
```

The same operator cannot both propose and confirm.

## Execute

```bash
bin/resolve-kernel-event-pipeline-topology-intervention \
  --pipeline inventory-position-projection \
  execute \
  --intervention <uuid> \
  --tenant-id "$KERNEL_LAB_CANARY_TENANT_ID" \
  --pretty
```

Immediately before execution the controller re-reads the terminal provisioning run, reviewed migration contract, and live JetStream topology. Any fingerprint change fails closed before mutation.

### restore_source

```text
confirmed evidence
      ↓
re-read and fingerprint
      ↓
restore allowlisted fields on journaled resource
      ↓
exact SOURCE contract
      ↓
fresh synthetic canary
      ↓
deployment readiness READY
      ↓
resolved_source
```

The existing provisioner allowlist remains in force. There is no delete/recreate fallback.

### accept_target

```text
confirmed evidence
      ↓
re-read and fingerprint
      ↓
exact TARGET already live
      ↓
no JetStream mutation
      ↓
fresh synthetic canary
      ↓
deployment readiness READY
      ↓
resolved_target
```

Observed topology that is merely plausible or close to the target cannot be accepted.

### abort

`abort` performs no broker mutation, canary, or readiness verification. It records an explicit operator decision to stop the intervention workflow. It does not repair or bless the deployment.

## Failure handling

Execution failures are terminal for that intervention attempt. Do not automatically retry the same confirmation. Detect a new attempt, capture fresh evidence, and repeat proposal plus independent confirmation.

Keep the matching `topology_migration` in the registry until the manual intervention workflow is terminal.
