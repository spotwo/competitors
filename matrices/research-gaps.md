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
| Allocation | candidate | 2 |
| Clear Height | candidate | 4 |
| Inventory Availability | candidate | 2 |
| Inventory Movement | candidate | 3 |
| Inventory Transfer | candidate | 1 |
| License Plate Number | observed | 2 |
| Material Flow System | observed | 2 |
| Reservation | candidate | 2 |
| Warehouse Order | observed | 1 |
| Warehouse Task | observed | 1 |

## Capability research

| Capability / gap | Status | Vendor observations | Evidence refs |
|---|---|---:|---:|
| 3PL Billing - Compare activity, storage, recurring, minimum, and VAS billing models across 3PL-first WMS vendors. | research | 1 | 1 |
| Allocation - Separate warehouse allocation from commercial allocation quotas and WES task assignment. | research | 3 | 4 |
| Allocation - Validate Reservation versus Allocation semantics across SAP EWM, Manhattan, Blue Yonder, Infor, Tecsys and 3PL-first systems. | research | 3 | 4 |
| Consolidation | candidate | 1 | 1 |
| Consolidation - Sample explicit consolidation objects and workstations across enterprise WMS products. | research | 1 | 1 |
| Cross-docking - Compare pre-allocated versus opportunistic cross-dock terminology and matching rules across SAP Manhattan Blue Yonder and 3PL-first WMS products. | research | 4 | 4 |
| Cross-docking - Decide whether CrossDockAllocation is a typed use of generic Allocation or a separate aggregate. | research | 4 | 4 |
| Cycle Counting - Sample count policy terminology across enterprise and SMB WMS products. | research | 1 | 1 |
| Identification / Labeling | candidate | 0 | 0 |
| Identification / Labeling - Separate identification, labeling, serialization, and handling-unit creation into subcapabilities after broader vendor sampling. | research | 0 | 0 |
| Physical Inventory - Define recount tolerance approval and segregation-of-duties controls. | research | 2 | 3 |
| Physical Inventory - Map snapshot versus live-count and freeze semantics across Dynamics Oracle Manhattan and Blue Yonder. | research | 2 | 3 |
| Relocation - Compare Oracle Manhattan Blue Yonder and 3PL-first terms such as relocate transfer move internal transfer and bin-to-bin transfer. | research | 3 | 3 |
| Relocation - Define reservation and allocation behavior when committed stock is relocated. | research | 3 | 3 |
| Returns - Add Oracle WMS Blue Yonder Extensiv and ecommerce-focused return workflow evidence. | research | 3 | 3 |
| Returns - Separate customer returns supplier returns and internal reverse movements into explicit subtypes. | research | 3 | 3 |
| Sortation | candidate | 1 | 1 |
| Sortation - Separate software routing logic from physical sorter control in WES/WCS products. | research | 1 | 1 |
| Unloading | candidate | 0 | 0 |
| Unloading - Sample WMS and YMS products to determine whether unloading is modeled as a first-class task, a dock activity, or only a physical operation. | research | 0 | 0 |
| Waveless / Continuous Release | candidate | 1 | 1 |
| Waveless / Continuous Release - Determine whether continuous release, waveless, order streaming, and dynamic release should be aliases or distinct strategies. | research | 1 | 1 |

## Workflow research

| Workflow / gap | Status | Vendor models | Evidence refs |
|---|---|---:|---:|
| Cross-Docking Workflow | candidate | 2 | 2 |
| Cross-Docking Workflow - Compare opportunistic versus pre-planned cross-docking across SAP EWM Manhattan Blue Yonder and 3PL-first systems. | research | 2 | 2 |
| Cross-Docking Workflow - Decide whether CrossDockAllocation should reuse the generic Allocation object with a cross-dock reason or remain a typed domain object. | research | 2 | 2 |
| Cross-Docking Workflow - Model outbound destination ownership when cross-dock flow terminates at packing staging or direct loading. | research | 2 | 2 |
| Customer Returns Workflow | candidate | 1 | 1 |
| Customer Returns Workflow - Add Oracle WMS Manhattan Blue Yonder Infor Extensiv and ecommerce-focused WMS return workflows. | research | 1 | 1 |
| Customer Returns Workflow - Define inspection evidence repair refurbishment and return-to-vendor handoff models. | research | 1 | 1 |
| Customer Returns Workflow - Determine whether blind returns should create Return Demand before receipt or use a receipt-first exception aggregate. | research | 1 | 1 |
| Customer Returns Workflow - Separate customer returns supplier returns and internal reverse movements into explicit subtypes. | research | 1 | 1 |
| Cycle Counting Workflow | candidate | 3 | 4 |
| Cycle Counting Workflow - Compare count freezing versus live-counting behavior across vendors and automation environments. | research | 3 | 4 |
| Cycle Counting Workflow - Define recount policy and tolerance hierarchy across item location owner and facility scopes. | research | 3 | 4 |
| Internal Relocation Workflow | candidate | 3 | 4 |
| Internal Relocation Workflow - Compare Oracle WMS Manhattan Blue Yonder and additional 3PL WMS relocation terminology and work models. | research | 3 | 4 |
| Internal Relocation Workflow - Decide whether movement work is always persisted or may be an atomic command for simple user-directed moves. | research | 3 | 4 |
| Internal Relocation Workflow - Define rules for moving inventory with active reservations allocations or in-progress warehouse work. | research | 3 | 4 |
| Internal Replenishment Workflow | candidate | 3 | 3 |
| Internal Replenishment Workflow - Add SAP EWM, Manhattan, Blue Yonder, Mecalux, PSIwms and at least four more WMS models before adopting the Spotwo workflow. | research | 3 | 3 |
| Internal Replenishment Workflow - Compare whether source inventory is reserved at planning time, release time or task-start time across vendors. | research | 3 | 3 |
| Internal Replenishment Workflow - Determine whether residual replenishment need should persist as an object or be recalculated from current inventory after every confirmation. | research | 3 | 3 |
| Internal Replenishment Workflow - Model replenishment priority separately from outbound pick priority and define dependency semantics between replenishment and blocked pick work. | research | 3 | 3 |
| Inventory Allocation and Commitment Workflow | candidate | 2 | 3 |
| Inventory Allocation and Commitment Workflow - Define concurrency and idempotency invariants for simultaneous reservation and allocation attempts against the same stock. | research | 2 | 3 |
| Inventory Allocation and Commitment Workflow - Define whether allocation can exist without a reservation object in the persisted Spotwo model or whether reservation is an optional conceptual state only. | research | 2 | 3 |
| Inventory Allocation and Commitment Workflow - Sample SAP EWM, Manhattan, Blue Yonder, Infor, Tecsys and 3PL-first systems to validate reservation-versus-allocation boundaries. | research | 2 | 3 |
| Inventory Allocation and Commitment Workflow - Separate soft commercial allocation quotas from warehouse inventory reservation and executable allocation. | research | 2 | 3 |
| Packing Workflow | candidate | 2 | 2 |
| Packing Workflow - Compare cartonization recommendation and carrier-service selection across additional WMS products. | research | 2 | 2 |
| Packing Workflow - Separate packing station UX from packing domain semantics. | research | 2 | 2 |
| Physical Inventory Workflow | candidate | 2 | 3 |
| Physical Inventory Workflow - Decide how Physical Inventory and Cycle Counting share Count Work and Count Result infrastructure while retaining different planning semantics. | research | 2 | 3 |
| Physical Inventory Workflow - Define snapshot versus live-count semantics and whether warehouse operations must freeze scoped inventory during count. | research | 2 | 3 |
| Physical Inventory Workflow - Define tolerance groups approval authority segregation-of-duties and dual-control requirements. | research | 2 | 3 |
| Physical Inventory Workflow - Sample Dynamics 365 Oracle WMS Manhattan Blue Yonder and 3PL WMS physical inventory workflows. | research | 2 | 3 |
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

## Kernel research

| Layer | Record / gap | Status | Evidence refs |
|---|---|---|---:|
| events | Inventory Domain Event Registry | candidate | 3 |
| events | Inventory Domain Event Registry - Decide whether inventory.position.changed is published externally or remains an internal projection event only. | research | 3 |
| events | Inventory Domain Event Registry - Define exact event payload schemas and PII/data-minimization rules for each event type. | research | 3 |
| events | Inventory Domain Event Registry - Define replay and snapshot behavior for consumers rebuilding derived views from domain events versus querying the inventory ledger directly. | research | 3 |
| events | Inventory Domain Event Registry - Map Receiving Shipping Packing Handling Unit aggregation and Cross-docking events to EPCIS 2.0 event classes and CBV vocabulary. | research | 3 |
| ledger | Inventory Ledger Posting Model | candidate | 6 |
| ledger | Inventory Ledger Posting Model - Benchmark row-locking versus optimistic version retry for high-contention pick-face allocation. | research | 6 |
| ledger | Inventory Ledger Posting Model - Decide whether reservation and allocation legs live in the same physical journal as quantity legs or separate logical journals sharing one transaction envelope. | research | 6 |
| ledger | Inventory Ledger Posting Model - Define UOM conversion rounding rules for conservation checks. | research | 6 |
| ledger | Inventory Ledger Posting Model - Define archival and checkpoint policy for very high transaction volumes without breaking traceability. | research | 6 |
| ledger | Inventory Ledger Posting Model - Define negative-inventory exceptions for integration latency and backflush scenarios. | research | 6 |
