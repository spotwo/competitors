# Picking Workflow

> Generated from canonical workflow data. Do not edit by hand.

A cross-vendor decomposition of how outbound demand becomes executable pick work, is assigned and navigated, confirmed by a worker or automation resource, resolved through exceptions, and handed off to consolidation, packing, staging or downstream execution.

## Canonical spine

| # | Stage | Purpose |
|---:|---|---|
| 1 | Demand Ready | Establish eligible outbound demand and inventory prerequisites before warehouse execution. |
| 2 | Release / Orchestration | Decide when demand enters execution and whether it is grouped into a wave, streamed continuously, or released directly. |
| 3 | Pick Work Creation | Convert released demand and allocation decisions into executable picking work and source-destination instructions. |
| 4 | Assignment | Match available pick work to a worker, equipment resource, queue, zone or shared task pool. |
| 5 | Sequence / Route | Order stops or tasks to reduce travel and honor priority, permissions, deadlines and dependencies. |
| 6 | Travel and Pick | Guide the picker or automation resource to the source, identify stock and move the requested quantity into a pick handling unit or destination. |
| 7 | Exception Resolution | Capture shortages, damage, validation failures or container constraints and determine downstream corrective work. |
| 8 | Handoff | Transfer completed pick output to consolidation, packing, staging, sortation, loading or another execution zone. |

## Canonical objects

| Object | Role |
|---|---|
| Fulfillment Demand | Business demand that requires inventory to be picked. |
| Allocation | Reservation or source decision linking demand to inventory. |
| Wave | Optional release grouping used to process multiple demand lines together. |
| Pick Work Group | Canonical abstraction for a grouping of executable pick tasks assigned or selected together. |
| Pick Task | Smallest independently executable picking instruction in the canonical model. |
| Assignment | Association between work and an execution resource or eligible task pool. |
| Worker Resource | Human warehouse resource eligible to execute work based on permissions, zone or equipment constraints. |
| Equipment Resource | Forklift, cart, robot, station or other resource that constrains or executes pick work. |
| Pick Route | Ordered sequence of locations or tasks for execution. |
| Pick Handling Unit | Tote, carton, pallet, cart position or other unit into which picked stock is accumulated. |
| Source Location | Addressable place from which stock is removed. |
| Destination Location | Handoff, staging, consolidation or packing destination for completed work. |
| Pick Confirmation | Recorded evidence that requested stock was picked, including actual quantity and identity validations. |
| Pick Exception | Structured deviation between requested work and executable or actual outcome. |

## Task states

| State | Meaning | Terminal |
|---|---|---|
| Planned | Work exists conceptually but is not yet available to execute. |  |
| Released | Work has entered execution and may be assigned or claimed. |  |
| Assigned | Work is associated with a specific worker, resource, queue or execution context. |  |
| In Progress | At least one physical or confirmation action has started. |  |
| Held | Work remains valid but is temporarily unavailable for execution. |  |
| Exception | Work cannot complete normally without a shortage, validation or operational resolution. |  |
| Completed | Required pick outcome is confirmed or explicitly closed with an accepted partial or zero-pick outcome. | yes |
| Cancelled | Work has been invalidated and must not be executed. | yes |

## Strategies

| Category | Strategy | Status | Evidence |
|---|---|---|---:|
| release | Wave Release | adopted | 3 |
| release | Waveless / Order Streaming | candidate | 2 |
| grouping | Single Order Picking | adopted | 1 |
| grouping | Batch Picking | adopted | 2 |
| grouping | Cluster Picking | adopted | 2 |
| grouping | Zone Picking | observed | 1 |
| grouping | Pick and Pass | observed | 1 |
| assignment | System-Directed Assignment | adopted | 4 |
| assignment | Explicit Assignment | adopted | 2 |
| assignment | Shared Task Pool | adopted | 2 |
| routing | Priority and Proximity Routing | observed | 1 |
| routing | Route Sequence | adopted | 1 |
| execution | RF / Barcode Directed Picking | adopted | 4 |
| execution | Voice Picking | adopted | 2 |
| execution | Pick-to-Light | adopted | 1 |
| execution | Put-to-Light | adopted | 1 |
| confirmation | Scan Confirmation | adopted | 2 |
| confirmation | Voice Confirmation | adopted | 1 |

## Vendor models

| Vendor | Workflow | Strategies | Channels | Evidence |
|---|---|---|---|---:|
| sap | warehouse-request -> warehouse-task -> warehouse-order-selection -> rf-execution -> confirmation | system-guided, queue-guided, request-directed, warehouse-order-directed, handling-unit-directed | rf | 1 |
| oracle | allocation -> task-creation -> release-or-assignment -> priority-ordering -> rf-execution -> completion | explicit-assignment, shared-task-pool, priority-ordering | rf | 2 |
| microsoft | release-to-warehouse -> wave -> wave-processing -> work-creation -> mobile-execution -> completion | wave, automated-wave, cluster-picking | warehouse-management-mobile-app | 2 |
| infor | wave-release -> assignment-or-task-pool -> task-selection -> rf-execution -> validation -> next-task | task-directed, cluster-by-order, cluster-by-case, assignment-pick, pick-and-pass | rf, voice, paper | 2 |
| netsuite | order-release -> wave -> pick-task-generation -> assignment -> mobile-picking -> staging -> fulfillment | single-order, bulk-picking, multiple-order, zone-picking | mobile-app | 2 |
| odoo | transfer -> batch-or-wave-grouping -> worker-assignment -> location-grouped-execution -> barcode-confirmation -> output-location | batch, wave, cluster | barcode-app, web | 2 |
| blue-yonder | task-pool -> dynamic-prioritization -> adaptive-assignment -> picking-tour -> execution -> dependency-recalculation | dynamic-priority, proximity-assignment, grouped-tasks, real-time-picking-tours | wms-wes-directed | 1 |
| manhattan-associates | demand-arrival -> dynamic-allocation -> wave-or-stream-release -> adaptive-work-planning -> smart-task-execution | wave, waveless, order-streaming, adaptive-pick-path | mobile-workflow | 1 |
| mecalux | order -> pick-list-generation -> task-allocation -> operator-guidance -> confirmation -> downstream-handoff | rf, voice, pick-to-light, put-to-light, goods-to-person, pick-and-pass | rf, voice, pick-to-light, put-to-light | 1 |
| infios | task-direction -> location-guidance -> quantity-guidance -> verbal-confirmation-or-exception -> next-task | voice, optimized-walk-path | voice | 1 |

## Exceptions

| Exception | Status | Trigger | Outcomes |
|---|---|---|---|
| Short Pick | adopted | Actual available or picked quantity is less than the requested quantity. | record-actual-quantity, capture-reason, create-residual-demand, trigger-inventory-verification |
| Zero Pick | observed | No requested quantity can be picked from the task source. | close-or-replan-task, preserve-unfulfilled-demand, inventory-verification |
| Validation Failure | observed | Scanned or entered location, item, LPN, quantity or other required attribute fails execution validation. | keep-task-in-progress, show-error, retry-or-escalate |
| Damaged Stock | candidate | Worker identifies stock damage during pick execution. | record-exception, exclude-or-hold-stock, continue-or-replan-demand |

## Spotwo candidate model

Status: **candidate**

Canonical spine: `demand-ready -> release -> work-creation -> assignment -> routing -> execution -> exception-resolution -> handoff`

Task states: `planned -> released -> assigned -> in-progress -> held -> exception -> completed -> cancelled`

### Commands

- `release-pick-work`
- `assign-pick-work`
- `start-pick-task`
- `confirm-pick`
- `report-short-pick`
- `report-damage`
- `hold-pick-work`
- `resume-pick-work`
- `cancel-pick-work`
- `complete-pick-work`

### Events

- `pick-work-released`
- `pick-work-assigned`
- `pick-task-started`
- `pick-confirmed`
- `pick-short-reported`
- `pick-damage-reported`
- `pick-work-held`
- `pick-work-resumed`
- `pick-work-cancelled`
- `pick-work-completed`

## Research gaps

- Sample at least ten more WMS products before adopting the Spotwo task state model.
- Separate inventory allocation from pick-source selection where vendors model them independently.
- Determine whether zone handoff should create a new Pick Work Group or preserve one group across zones.
- Compare short-pick residual-demand and automatic reallocation behavior across SAP, Manhattan, Blue Yonder, Oracle, Infor and Mecalux.
- Add UI interaction observations for handheld RF, voice, pick-to-light and goods-to-person stations without treating presentation details as domain semantics.
