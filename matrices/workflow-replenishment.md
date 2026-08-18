# Internal Replenishment Workflow

> Generated from canonical workflow data. Do not edit by hand.

A cross-vendor decomposition of internal warehouse replenishment that moves inventory from reserve, bulk or upstream storage into a pick face or other execution location in response to policy, demand or an operational trigger.

## Canonical spine

| # | Stage | Purpose |
|---:|---|---|
| 1 | Trigger Evaluation | Detect that a warehouse location, zone or demand set requires internal replenishment. |
| 2 | Quantity Calculation | Determine the quantity required by the selected replenishment policy and unit constraints. |
| 3 | Destination Selection | Identify the pick face, location or zone that should receive stock. |
| 4 | Source Selection | Select eligible reserve inventory and a source location using inventory, lot, route and location rules. |
| 5 | Replenishment Work Creation | Convert the replenishment decision into executable movement work. |
| 6 | Release / Assignment | Make replenishment work executable and optionally assign it to a worker, queue or equipment resource. |
| 7 | Pick / Move / Put | Remove inventory from the source and place it at the target location using a worker or automation resource. |
| 8 | Completion / Recalculation | Confirm inventory and capacity changes, close work and recalculate any remaining replenishment need. |

## Canonical objects

| Object | Role |
|---|---|
| Replenishment Policy | Rules that determine when and how internal replenishment should occur. |
| Replenishment Need | Calculated or event-driven requirement to add inventory to a target location or zone. |
| Replenishment Quantity | Quantity to move after applying demand, thresholds, units, multiples and capacity constraints. |
| Target Location | Pick face, forward location or execution zone that should receive stock. |
| Source Location | Reserve, bulk or upstream location selected to supply the replenishment. |
| Source Inventory | Specific eligible stock selected for the replenishment movement. |
| Replenishment Work | Canonical grouping of one or more executable replenishment movements. |
| Replenishment Task | Smallest independently executable internal replenishment movement instruction. |
| Assignment | Association of replenishment work with a worker, equipment resource, queue or task pool. |
| Movement Confirmation | Actual confirmed source, destination and quantity moved during execution. |

## Task states

| State | Meaning | Terminal |
|---|---|---|
| Detected | A replenishment need exists but executable work has not yet been created. |  |
| Planned | Source, destination and quantity have been determined. |  |
| Released | Replenishment work is available for execution. |  |
| Assigned | Work is associated with an execution resource or directed queue. |  |
| In Progress | Inventory movement has begun. |  |
| Exception | Source, destination, quantity or capacity prevents normal completion. |  |
| Completed | The movement is confirmed and warehouse inventory has been updated. | yes |
| Cancelled | The replenishment work is no longer valid and must not execute. | yes |

## Strategies

| Category | Strategy | Status | Evidence |
|---|---|---|---:|
| trigger | Min/Max Replenishment | adopted | 2 |
| trigger | Wave Demand Replenishment | adopted | 1 |
| trigger | Load Demand Replenishment | observed | 1 |
| trigger | Immediate / Reactive Replenishment | adopted | 2 |
| trigger | Order-Based Replenishment | adopted | 1 |
| trigger | Transaction-Driven Dynamic Replenishment | adopted | 1 |
| source-selection | Location-Directive Source Selection | adopted | 1 |
| source-selection | Route / Location Sequence Source Selection | observed | 1 |
| source-selection | Clean Location Source Selection | observed | 1 |
| assignment | Directed Replenishment Assignment | adopted | 1 |
| assignment | Assisted Replenishment Selection | observed | 1 |
| execution | Pick / Put Work | adopted | 1 |

## Vendor models

| Vendor | Workflow | Strategies | Channels | Evidence |
|---|---|---|---|---:|
| microsoft | trigger-strategy -> replenishment-template -> destination-location-directive -> source-pick-location-directive -> work-template -> replenishment-work -> mobile-execution | wave-demand, min-max, load-demand, immediate | warehouse-management-mobile-app | 1 |
| oracle | trigger-mode -> replenishment-wave -> rule-evaluation -> allocation -> task-creation -> rf-execution | minimum-capacity, percentage-of-max, reactive, order-based | rf, mobile | 1 |
| infor | transaction-change -> replenishment-list-regeneration -> priority-calculation -> source-location-selection -> rf-assisted-or-directed-task -> confirmation | transaction-driven, route-location-sequence, clean-location, descending-location, rf-assisted, rf-directed | rf-assisted, rf-directed | 1 |

## Exceptions

| Exception | Status | Trigger | Outcomes |
|---|---|---|---|
| Insufficient Source Stock | candidate | Eligible reserve inventory cannot cover the required replenishment quantity. | reduce-or-split-quantity, choose-alternate-source, preserve-residual-need |
| Target Capacity Conflict | observed | Requested replenishment would exceed target location capacity or stocking constraints. | reduce-quantity, choose-alternate-target, hold-work |
| No Eligible Source | candidate | Source-selection rules cannot identify suitable inventory or a suitable source location. | exception, alternate-rule, supervisor-review, preserve-residual-need |

## Spotwo candidate model

Status: **candidate**

Canonical spine: `trigger-evaluation -> quantity-calculation -> destination-selection -> source-selection -> work-creation -> release-assignment -> movement -> completion-recalculation`

Task states: `detected -> planned -> released -> assigned -> in-progress -> exception -> completed -> cancelled`

### Commands

- `evaluate-replenishment`
- `plan-replenishment`
- `release-replenishment-work`
- `assign-replenishment-work`
- `start-replenishment-task`
- `confirm-replenishment-move`
- `report-replenishment-exception`
- `cancel-replenishment-work`

### Events

- `replenishment-needed`
- `replenishment-planned`
- `replenishment-work-released`
- `replenishment-work-assigned`
- `replenishment-task-started`
- `replenishment-move-confirmed`
- `replenishment-exception-reported`
- `replenishment-work-completed`
- `replenishment-work-cancelled`

## Research gaps

- Add SAP EWM, Manhattan, Blue Yonder, Mecalux, PSIwms and at least four more WMS models before adopting the Spotwo workflow.
- Compare whether source inventory is reserved at planning time, release time or task-start time across vendors.
- Determine whether residual replenishment need should persist as an object or be recalculated from current inventory after every confirmation.
- Model replenishment priority separately from outbound pick priority and define dependency semantics between replenishment and blocked pick work.
