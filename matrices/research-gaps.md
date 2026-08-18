# Research Gaps

> Generated from explicit research gaps and non-adopted records. Do not edit by hand.

## Company gaps

| Company | Gap |
|---|---|
| ABM Cloud | Verify legal headquarters country before setting hq_country. |
| Anchanto | Verify canonical deployment model terminology used by Anchanto. |
| Bastian Solutions | Verify current Exacta deployment models and broader geographic support footprint. |
| Daifuku | Map region-specific WMS product naming such as WareNavi and eWareNavi and verify deployment models. |
| Deposco | Verify geographic coverage outside North America. |
| Element Logic | Verify canonical eManager and eController deployment models and customer-size segmentation. |
| ERPNext | Distinguish ERP inventory and warehouse functionality from full WMS competitor scope in future comparisons. |
| ERPNext | Verify canonical hosted versus self-hosted deployment taxonomy from current Frappe documentation. |
| Exotec | Add evidence-backed customer-size segmentation. |
| Extensiv | Verify geographic coverage outside North America. |
| FORTNA | Add evidence-backed customer-size segmentation. |
| FORTNA | Expand geographic coverage from official location/customer evidence. |
| FORTNA | Verify canonical WES deployment model. |
| Geekplus | Add evidence-backed customer-size segmentation. |
| GreyOrange | Verify canonical deployment model for GreyMatter. |
| Hai Robotics | Verify canonical deployment model for HaiQ. |
| Lucas Systems | Verify canonical deployment model and customer-size segmentation. |
| Mantis | Verify current canonical deployment models for LVS. |
| OpenBoxes | Map OpenBoxes Lift commercial service separately from self-hosted open-source deployments. |
| Senior Sistemas | Resolve exact country-by-country commercial coverage beyond Brazil. |
| ShipHero | Verify supported commercial regions outside North America. |
| TGW Logistics | Verify canonical WERX deployment models. |
| TOTVS | Map product-line deployment models and exact market segmentation individually. |
| Vanderlande | Map the exact VISION WES/WCS boundary and canonical deployment model. |

## Terminology requiring review

| Term | Status | Evidence refs |
|---|---|---:|
| Clear Height | candidate | 4 |
| License Plate Number | observed | 2 |
| Material Flow System | observed | 2 |
| Warehouse Order | observed | 1 |
| Warehouse Task | observed | 1 |

## Capability research

| Capability / gap | Status | Vendor observations | Evidence refs |
|---|---|---:|---:|
| 3PL Billing - Compare activity, storage, recurring, minimum, and VAS billing models across 3PL-first WMS vendors. | research | 1 | 1 |
| Allocation - Separate inventory allocation, order allocation, reservation, and WES task assignment terminology. | research | 1 | 1 |
| Consolidation | candidate | 1 | 1 |
| Consolidation - Sample explicit consolidation objects and workstations across enterprise WMS products. | research | 1 | 1 |
| Cross-docking - Compare pre-allocated versus opportunistic cross-dock terminology across additional WMS products. | research | 2 | 2 |
| Cycle Counting - Sample count policy terminology across enterprise and SMB WMS products. | research | 1 | 1 |
| Identification / Labeling | candidate | 0 | 0 |
| Identification / Labeling - Separate identification, labeling, serialization, and handling-unit creation into subcapabilities after broader vendor sampling. | research | 0 | 0 |
| Physical Inventory | adopted | 0 | 0 |
| Physical Inventory - Map freeze, snapshot, recount, and approval workflows across vendors. | research | 0 | 0 |
| Relocation | candidate | 0 | 0 |
| Relocation - Compare vendor terms such as relocate, transfer, move, internal transfer, and bin-to-bin transfer. | research | 0 | 0 |
| Sortation | candidate | 1 | 1 |
| Sortation - Separate software routing logic from physical sorter control in WES/WCS products. | research | 1 | 1 |
| Unloading | candidate | 0 | 0 |
| Unloading - Sample WMS and YMS products to determine whether unloading is modeled as a first-class task, a dock activity, or only a physical operation. | research | 0 | 0 |
| Waveless / Continuous Release | candidate | 1 | 1 |
| Waveless / Continuous Release - Determine whether continuous release, waveless, order streaming, and dynamic release should be aliases or distinct strategies. | research | 1 | 1 |

## Workflow research

| Workflow / gap | Status | Vendor models | Evidence refs |
|---|---|---:|---:|
| Cycle Counting Workflow | candidate | 3 | 4 |
| Cycle Counting Workflow - Compare count freezing versus live-counting behavior across vendors and automation environments. | research | 3 | 4 |
| Cycle Counting Workflow - Define recount policy and tolerance hierarchy across item location owner and facility scopes. | research | 3 | 4 |
| Internal Replenishment Workflow | candidate | 3 | 3 |
| Internal Replenishment Workflow - Add SAP EWM, Manhattan, Blue Yonder, Mecalux, PSIwms and at least four more WMS models before adopting the Spotwo workflow. | research | 3 | 3 |
| Internal Replenishment Workflow - Compare whether source inventory is reserved at planning time, release time or task-start time across vendors. | research | 3 | 3 |
| Internal Replenishment Workflow - Determine whether residual replenishment need should persist as an object or be recalculated from current inventory after every confirmation. | research | 3 | 3 |
| Internal Replenishment Workflow - Model replenishment priority separately from outbound pick priority and define dependency semantics between replenishment and blocked pick work. | research | 3 | 3 |
| Packing Workflow | candidate | 2 | 2 |
| Packing Workflow - Compare cartonization recommendation and carrier-service selection across additional WMS products. | research | 2 | 2 |
| Packing Workflow - Separate packing station UX from packing domain semantics. | research | 2 | 2 |
| Picking Workflow | candidate | 10 | 15 |
| Picking Workflow - Add UI interaction observations for handheld RF, voice, pick-to-light and goods-to-person stations without treating presentation details as domain semantics. | research | 10 | 15 |
| Picking Workflow - Compare short-pick residual-demand and automatic reallocation behavior across SAP, Manhattan, Blue Yonder, Oracle, Infor and Mecalux. | research | 10 | 15 |
| Picking Workflow - Determine whether zone handoff should create a new Pick Work Group or preserve one group across zones. | research | 10 | 15 |
| Picking Workflow - Sample at least ten more WMS products before adopting the Spotwo task state model. | research | 10 | 15 |
| Picking Workflow - Separate inventory allocation from pick-source selection where vendors model them independently. | research | 10 | 15 |
| Putaway Workflow | candidate | 3 | 3 |
| Putaway Workflow - Compare fixed-bin random-storage product-affinity and hazard-class strategies across more vendors. | research | 3 | 3 |
| Putaway Workflow - Decide whether putaway work grouping should reuse a generic Warehouse Work Group abstraction shared with picking. | research | 3 | 3 |
| Receiving Workflow | candidate | 3 | 3 |
| Receiving Workflow - Decide whether Shipment Verification is a canonical object or vendor-specific completion policy. | research | 3 | 3 |
| Receiving Workflow - Sample unexpected receiving and blind receiving models across additional WMS products. | research | 3 | 3 |
| Shipping Workflow | candidate | 2 | 3 |
| Shipping Workflow - Compare carrier manifest close versus warehouse shipment confirmation across parcel and freight WMS products. | research | 2 | 3 |
| Shipping Workflow - Decide whether Outbound Load belongs in WMS core or an adjacent TMS/YMS boundary. | research | 2 | 3 |
