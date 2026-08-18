# ADR 0006 - Internal Replenishment Scope

Status: Proposed

Date: 2026-08-18

## Context

The term `Replenishment` is overloaded across warehouse and ERP products.

Warehouse-management products such as Dynamics 365 Warehouse Management, Oracle WMS and Infor WMS use replenishment for internal movement of inventory from reserve, bulk or upstream warehouse storage into a pick face or other execution location.

Other systems, including Odoo Inventory, use Replenishment more broadly for supply creation such as purchase, manufacture or inter-warehouse resupply.

Treating these processes as one domain capability would mix different ownership boundaries, lead times, documents, inventory transitions and integrations.

## Proposed decision

Use **Internal Replenishment** as the precise Spotwo concept for warehouse-internal inventory movement that restores stock at an execution location.

```text
Reserve / Bulk / Upstream Location
              |
              v
     Internal Replenishment
              |
              v
 Pick Face / Forward Location
```

Keep external supply processes separate:

```text
Supplier ----------------> Warehouse     = Procurement / Inbound Supply
Production --------------> Warehouse     = Production Supply / Receipt
Warehouse A -------------> Warehouse B   = Inter-Warehouse Resupply / Transfer
Reserve Location --------> Pick Face     = Internal Replenishment
```

## Candidate workflow

```text
Trigger Evaluation
  -> Quantity Calculation
  -> Destination Selection
  -> Source Selection
  -> Work Creation
  -> Release / Assignment
  -> Pick / Move / Put
  -> Completion / Recalculation
```

## Candidate objects

- `ReplenishmentPolicy`
- `ReplenishmentNeed`
- `ReplenishmentQuantity`
- `SourceLocation`
- `TargetLocation`
- `SourceInventory`
- `ReplenishmentWork`
- `ReplenishmentTask`
- `Assignment`
- `MovementConfirmation`

## Trigger strategies observed

- Min/Max
- Wave Demand
- Load Demand
- Immediate / Reactive
- Order Based
- Transaction Driven

These are strategies inside Internal Replenishment, not separate top-level capabilities.

## Rules

1. Do not infer semantic scope from the UI label `Replenishment` alone.
2. Internal replenishment must have both an internal source and internal target warehouse location.
3. Replenishment policy is separate from executable replenishment work.
4. Replenishment need is separate from the task created to satisfy it.
5. Source selection and destination selection are explicit decisions and may use different rule sets.
6. Task execution should reuse generic warehouse movement concepts where possible, while retaining replenishment-specific reason and dependency context.
7. Replenishment priority must be able to depend on blocked or imminent downstream picking demand.
8. Completing a replenishment movement does not guarantee the original need is fully satisfied; the need may need recalculation.

## Consequences

- ERP procurement and warehouse execution APIs do not share one ambiguous `replenishment` command.
- Events can distinguish `replenishment-needed` from purchase/order supply events.
- Pick-face shortages can depend on internal replenishment work without pretending that a purchase order is warehouse work.
- Vendor mappings can preserve their own terminology while Spotwo keeps stable semantics.

## Evidence

See:

- `workflows/replenishment.yml`
- `matrices/workflow-replenishment.md`
- `claims/odoo-replenishment-scope.yml`
- evidence records with `subject: replenishment-workflow`
- `evidence/odoo-replenishment-scope-2026.yml`
