# Inventory Domain Events

`events/*.yml` defines committed facts emitted by the inventory kernel and their delivery contract.

A Domain Event is not an inventory ledger row and not automatically a GS1 EPCIS event.

## Boundaries

```text
InventoryTransaction
  internal durable inventory journal

DomainEvent
  committed fact for Spotwo modules and integrations

EPCIS Event
  external supply-chain visibility representation when applicable
```

## Contract

- Events describe facts that already committed.
- Outbox rows are written atomically with the authoritative inventory posting.
- Network delivery is at-least-once.
- `event_id` is stable across retries and consumers are idempotent.
- Ordering is explicit per aggregate/stream through versioning; no global order is implied.
- `occurred_at` and `recorded_at` remain separate.
- Event payload contracts are versioned.
- CloudEvents may provide a transport envelope without defining Spotwo business semantics.
- EPCIS/CBV mappings are used only for suitable visibility facts with enough What/When/Where/Why context.

## Authoring

When adding an event:

1. Use a stable semantic past-tense fact type such as `inventory.movement.confirmed`.
2. Describe its ledger relationship explicitly.
3. Add evidence for any external-visibility or standards mapping.
4. Do not publish commands as events.
5. Add payload-specific privacy and retention notes when sensitive data is introduced.
6. Run `bash bin/check`.

## Queries

```bash
python scripts/kernel.py events
python scripts/kernel.py events movement
python scripts/kernel.py envelope
```

See `matrices/events.md` and ADR `0013-inventory-domain-events.md`.
