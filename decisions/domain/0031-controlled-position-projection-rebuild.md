# ADR 0031 - Controlled Position Projection Rebuild

Status: Accepted

Date: 2026-08-18

## Context

ADR 0029 made the Position quantity projection version-aware and durable across delivery gaps. ADR 0030 added non-destructive bootstrap for an absent cursor. Neither contract can repair an existing cursor whose materialized quantities are wrong or close a permanent gap from an authoritative snapshot.

Replacing only the projection row is unsafe. Buffered events may already be ACKed because their complete payload and Inbox receipt committed to PostgreSQL. Resetting the cursor backward also does not make those events replayable because Inbox deduplication will recognize their existing receipts.

A destructive repair therefore needs one transaction boundary across the cursor, pending events, Inbox audit metadata, and consumer delivery behavior.

## Decision

Add a two-phase, single-Position rebuild protocol:

```text
prepare -> execute
        -> cancel
```

The target is exactly one `(consumer_name, position_id)`. There is no global truncate, wildcard target, replace flag on the bootstrap command, or automatic gap repair.

## Prepared plan

`prepare` binds a stable `rebuild_id` to:

- consumer and Position identity;
- expected projection version;
- expected pending-event count;
- exact verified snapshot and artifact SHA-256;
- explicit pending-event disposition;
- operator identity and reason;
- the complete pre-rebuild projection row.

The version and pending count are compare-and-set inputs. If either changed after operator inspection, prepare fails without activating a fence. Reuse of the same rebuild identity with changed input fails closed. An exact retry returns the persisted operation status.

The snapshot version must be greater than or equal to the current cursor. A backward cursor move would require replaying already-deduplicated events and is not supported by this contract.

## Consumer fence

Every Position event transaction and non-destructive bootstrap locks the target's durable control row before changing projection state. A prepared rebuild records its ID on that control row.

While the fence is active, the first handling attempt for a not-yet-received event raises before projection mutation. The surrounding Inbox transaction rolls back. With the ADR 0032 durable failure lane, the complete event is then committed as `deferred` before broker ACK and later retried from PostgreSQL. Without that optional lane, the base ADR 0028 runtime leaves the broker delivery unacknowledged. A delivery whose Inbox receipt already existed remains a normal deduplicated ACK because it cannot run the handler again. Unrelated Positions use different control rows and remain independent at the database layer.

Prepare, event handling, bootstrap, execute, and cancel use the same lock order:

```text
control row -> projection row -> pending rows
```

If an event wins before prepare, prepare observes the advanced cursor and its stale compare-and-set fails. If prepare wins, the event observes the fence and rolls back. They cannot both mutate the target from the same pre-rebuild state.

The fence has no automatic expiry. A process crash after prepare intentionally leaves the target stopped until an audited execute or cancel command resolves the operation.

## Pending-event disposition

Prepare requires one explicit policy.

| Disposition | Behavior |
|---|---|
| `require-empty` | reject prepare unless the target has zero pending events |
| `supersede-covered-retain-future` | snapshot covers pending versions at or below its cursor; higher versions remain durable |

For the second policy:

1. pending versions `<= snapshot_version` are marked `superseded` in their existing Inbox metadata and removed from the pending table;
2. pending versions `> snapshot_version` remain unchanged;
3. the rebuild drains any contiguous retained tail beginning at `snapshot_version + 1`;
4. a later noncontiguous version remains pending until its missing predecessor arrives.

No Inbox receipt is deleted. An ACKed pending event always ends as superseded by explicit snapshot evidence, applied during rebuild drain, or still durably pending.

## Atomic execute

Execute rechecks the fenced projection row and pending count against prepared evidence. In one PostgreSQL transaction it:

1. records covered pending events as superseded;
2. replaces current quantities and cursor with the prepared snapshot;
3. records the rebuild snapshot as the new seed provenance;
4. drains a contiguous retained event tail;
5. writes final projection and pending counts to the audit row;
6. marks the rebuild completed;
7. releases the fence.

Any quantity constraint, state mismatch, or SQL failure rolls back the complete operation and leaves the prepared fence active.

Cancel changes no projection, pending, or Inbox data. It records operator identity and reason, marks the operation cancelled, and releases the fence atomically.

## Audit evidence

`inventory_position_projection_rebuilds` retains:

- exact requested snapshot and checksum;
- complete before and final projection rows;
- expected and actual pending counts;
- superseded, drained, and remaining counts;
- prepare, execute, or cancel operators and timestamps;
- reason, disposition, and terminal status.

The snapshot checksum binds evidence to exact artifact bytes. It does not prove the source data was truthful. Operator authentication, authorization, approval separation, and artifact verification remain deployment responsibilities.

## Retry behavior

- exact prepare retry returns `duplicate` with current operation status;
- exact execute retry by the same executor returns the completed result;
- exact cancel retry returns the cancelled result;
- changed payload or executor under the same operation identity fails closed;
- completed operations cannot be cancelled and cancelled operations cannot execute.

## Executable invariants

The PostgreSQL lab proves:

1. prepare fences the target before Inbox commit and broker ACK;
2. cancel releases the fence without changing projection state;
3. execute replaces a cursor atomically and preserves before/final evidence;
4. a rebuilt snapshot continues with event version `N + 1`;
5. covered pending events become audited `superseded` receipts;
6. contiguous future events drain during rebuild while noncontiguous events remain durable;
7. `require-empty`, stale compare-and-set, and backward snapshots fail before mutation;
8. bootstrap cannot overwrite a fenced target;
9. unrelated Positions continue while one target is fenced;
10. an event/prepare race has one serialized winner;
11. a real JetStream consumer with `max_deliver=1` ACKs only after durable fence handoff, then applies the event through local retry after cancel without broker redelivery.

## Consequences

### Positive

- destructive repair becomes an explicit, reviewable state machine;
- no already-ACKed event payload is silently discarded;
- a rebuild cannot race an event or bootstrap mutation;
- crashes fail stopped rather than releasing an unverified target;
- audit evidence connects the exact snapshot, operators, prior state, and final state;
- the protocol remains broker-neutral because the ACK boundary stays in the Inbox runtime and valid-event handoff uses ADR 0032 PostgreSQL state rather than transport-specific NAK behavior.

### Costs and limits

- a prepared operation blocks one Position until execute or cancel;
- snapshots cannot move the cursor backward;
- snapshot truth and operator authorization remain operational responsibilities;
- this is not a generic projection framework or bulk rebuild coordinator;
- automatic repair, Inbox and failure retention, approval workflow, and prepared-fence alerting remain later slices.
