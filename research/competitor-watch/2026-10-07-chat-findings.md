# Competitor Watch Findings - 2026-10-07

Status: curated from the September-October 2026 competitor-watch conversation.
Purpose: preserve all material findings before promotion into canonical claims/evidence.
Rule: this file is a research ledger, not normative product truth. Items marked `verify` require source re-verification before canonical promotion.

## Consolidated findings

### Manhattan Associates
- **Active Supply Chain Execution 26.2** - consolidate the repeated chat findings into one release record.
  - WMS agents: Create ASN Agent, Verify ASN Agent, Shipping Supervisor Agent, expanded Wave Coordinator/pre-wave checks.
  - execution: multi-trailer yard locations; alternate PO/ASN identifiers for multi-host/3PL scenarios.
  - TMS/WMS coupling: shipment split optimization can account for warehouse pick sequence.
  - agent platform: working/published versions, validation/promotion lifecycle, scheduled autonomous runs, customer-extensible agents, governance, usage telemetry and observability.
  - interoperability: customer-hosted MCP servers, MCP Connect, external agent/tool invocation, synchronous/asynchronous agent invocation from extension handlers.
  - architecture signal: agent runtime/governance/observability/interoperability are becoming WMS platform capabilities.
  - status: material, consolidate; verify official 26.2 docs before canonical claim promotion.
- **Order Network Exchange (OnX)** - open agent interoperability specification over MCP for fulfillment/order capabilities; Maven described as an MCP server; REST/MCP tools for orders, inventory availability and catalog.
  - architecture signal: domain capabilities should remain distinct from agent implementations.
  - status: material; verify.
- **Active Editions** - Essentials / Enterprise / Enterprise Premier packaging for ActiveWarehouse and other Active applications; same ActivePlatform with progressive capability unlock and no reimplementation/data migration for tier upgrades.
  - pricing: public fixed prices not identified; vendor quote/contact.
  - status: material packaging change; verify.

### Swisslog
- **FastMove** - monorail rename/repositioning; next-generation electrified pallet monorail, with upgrade path and future EU regulatory/cyber-resilience positioning.
  - status: material naming/product taxonomy change; verify.

### VDI
- **VDI 3299 Blatt 2** - process-oriented planning/design of order-picking systems.
  - ontology relevance: picking-system design, GTP, Pick-by-Light, AMR-assisted picking and subsystem decomposition.
  - status: authority update; verify publication metadata.

### ISO
- **ISO/WD 26205 Smart Warehousing - Guidelines** - working-draft smart-warehousing architecture covering infrastructure, equipment, integration, applications, safety, information security and energy efficiency.
  - status: draft/non-normative; do not treat as published standard.
- **ISO 16400-5:2026** - Equipment Behaviour Catalogue interfaces with engineering/manufacturing operations.
  - relevance: equipment behavior models and automation/control integration.
  - status: published authority update; verify exact title/scope.
- **ISO 21423** - Industrial mobile robots communications/interoperability.
  - chat finding: progressed through final publication stage; scope includes interoperability between industrial mobile robots, fleet managers and enterprise resources.
  - architecture signal: vendor-neutral AMR interoperability is a distinct system boundary.
  - status: standards update; verify current publication status before canonical promotion.

### Descartes
- **Tai Software acquisition** - approximately US$100M; expands TMS/freight-broker capabilities: quoting, carrier sourcing, load execution, billing, customer engagement, TL/LTL/drayage/cross-border.
  - architecture signal: Descartes profile should not remain WMS-only.
  - status: material acquisition; verify transaction metadata.
- **Extensiv acquisition** - approximately US$120M; adds 3PL WMS, inventory, B2B/B2C fulfillment, billing, ecommerce/marketplace/carrier connectivity and AI-oriented warehouse insights.
  - architecture signal: Descartes increasingly spans WMS + TMS + logistics network.
  - status: material acquisition; verify transaction metadata.

### Hai Robotics
- **HaiPick Climb scale/capability** - 10,000+ contracted HaiClimber robots across 12 countries; reported single-site scale above 1,500 robots; carton/original-packaging handling demonstrated with palletizing and pallet-AMR integration.
  - taxonomy: native-carton-handling, carton-to-pallet flow, palletizing integration, pallet-AMR integration.
  - status: material capability + scale evidence; verify.

### MotionTech
- **US market entry** - permanent Dallas-Fort Worth presence; end-of-line packaging, storage and conveying; prior US installations and partnerships.
  - status: candidate new competitor / market-expansion signal; verify before adding canonical company entity.

### Geek+
- **RoboShuttle Hyper** - new climbing Tote-to-Person generation; high-throughput claims, dual-robot rack-column operation, double-deep bidirectional retrieval, large fleet coordination, robot-arm and Gino 1 integration.
  - status: material product launch; verify exact performance numbers.
- **subscription/managed-service signal** - reported growth in subscription-based service orders and strategic emphasis on intelligent warehouse subscriptions.
  - status: packaging/business-model signal; verify.
- **European Innovation Lab, Düsseldorf** - EMEA customer validation, pre-deployment testing and partner training hub.
  - status: material market/validation expansion; verify.
- **Gravity** - terminology for a unified embodied-AI framework spanning heterogeneous robotics.
  - status: material terminology/architecture signal; verify exact positioning.

### AutoStore
- **AutoCase first customer installation** - first real-world deployment at Nowaste Logistics, moving AutoStore beyond bin-only flows toward carton/case handling inside the grid.
  - capabilities: automated case handling, carton-in-grid storage, case buffering, full-case fulfillment, case-to-piece picking.
  - status: material production validation; verify current production/GA state.

### Anthropic
- **Model Hardware Standard (MHS)** - research-preview specification for AI-agent interaction with programmable physical equipment via machine descriptions, capabilities, parameters and safety constraints; MCP/CLI/API access discussed.
  - classification: emerging interoperability protocol candidate, not normative ISO/VDI authority.
  - architecture signal: possible standardized agent-to-hardware boundary.
  - status: watch/research; verify and do not create dependency.

### PSI Software / PSIwms
- **AI expansion** - Batch AI described as standard PSIwms capability; PSIwms GO Retail cloud/no-code edition; Business Assistant; AI-powered contextual documentation; Configuration Assistant.
  - production evidence in chat: LPP deployment and reported picking-distance/efficiency improvements.
  - architecture signal: AI configuration/implementation assistance is a separate competitive dimension from runtime optimization.
  - status: material capability/packaging update; verify.
- **Warburg Pincus / delisting** - majority ownership around 83%, delisting process and private-company governance direction.
  - strategic watch: SaaS/cloud-native transformation and international expansion.
  - status: material ownership change; verify latest ownership/delisting status.

### Exotec + Mujin
- **end-to-end automation partnership** - integrated Skypod + Mujin robotic depalletizing/palletizing/transport.
  - orchestration: chat finding says either Exotec Deepsky WES or MujinOS may act as top-level controller depending on configuration.
  - architecture signal: orchestration ownership is itself a competitive layer across heterogeneous automation.
  - status: material integration/architecture update; verify.

### InOrbit / OpenRobOps
- **OpenRobOps (ORO)** - open-source robot-operations/fleet-management foundation, described in chat as a reference implementation aligned with ISO 21423; telemetry, spatial tracking, configuration-as-code, incident handling, edge/cloud and air-gapped deployment; Open-RMF integration.
  - architecture signal: separate robot/fleet operations interoperability from multi-fleet traffic/coordination.
  - status: material open implementation; verify exact ISO relationship wording.

### EU machinery / packaging authorities
- **EN 415-4:2025** - palletisers, depalletisers and associated equipment; chat finding says harmonized under Machinery Directive via Implementing Decision (EU) 2026/2015.
  - status: regulatory authority update; verify legal metadata/effective dates.
- **Machinery Regulation (EU) 2023/1230 transition** - track 20 January 2027 application and new harmonized-standard regime.
  - status: watch.
- **PPWR** - relevant to automated packaging/right-sizing decisions in the Element Logic + Ranpak finding.
  - status: regulatory relation; do not infer compliance from partnership alone.

### SSI SCHAEFER
- **SSI Piece Picking - Pick & Place** - first production cell described in chat at Würth Industrie Service; precise destination placement vs Pick & Drop; WAMAS Vision 3D positioning, gripping/trajectory/destination calculation and ML-based product variation handling.
  - ontology signal: robotic picking should distinguish grasp, drop, precise placement, consolidation and sequencing.
  - status: material capability update; verify.

### AutoScheduler.AI
- **AI App Builder / Warehouse AI Platform positioning** - natural-language operational app creation over warehouse data/semantic model, plus automation engine and optimization.
  - governance: permissions, dry-run, ownership, versioning/rollback, auditability.
  - ontology signal: AI-native warehouse extensibility is a separate competitive dimension.
  - status: material product/positioning update; verify.

### FCC / US market access
- **advanced robotic devices / Covered List** - chat finding describes new US market-access constraints affecting foreign-produced networked advanced robotic devices, potentially including AMR/AGV/autonomous forklifts, with conditional-approval paths and treatment of previously authorized devices.
  - procurement ontology: country of production, authorization status, update path, remote-access/data controls, vendor exit risk.
  - status: material regulatory signal; verify exact FCC order/public notice and legal scope.
- **robotic device conditional approvals** - chat finding cites ANSCER AR250/AR650/AR1250 conditional approvals.
  - status: evidence example; verify before canonical vendor record.
- **UWB modernization NPRM** - proposed modernization for factory automation, autonomous navigation and sensing.
  - relation: industrial RTLS, autonomous navigation, robot safety, omlox.
  - status: proposed rule/NPRM, not current law; verify docket/date.

### KUKA
- **KMF 1500P family / KMF 1500P-CB** - autonomous counterbalance forklift family; 1,500 kg payload, lift capability, open/closed pallet/container handling, conveyor transfer, KUKA.AMR ecosystem and VDA 5050 interface.
  - taxonomy signal: AMR and autonomous forklift should not be mutually exclusive; model mobility, load handling, lift, autonomy and fleet interoperability orthogonally.
  - status: material product launch; verify availability/delivery dates and EU Machinery Regulation claim.

### Element Logic + Ranpak
- **global packaging automation partnership** - end-of-line packaging integrated into warehouse automation: box selection/forming, right-sizing, closing, vision and paper protection.
  - architecture signal: packaging orchestration is a first-class domain between picking and shipping, with resources/decisions around package type, dimensions, void, materials, throughput and transport volume.
  - status: material integration/portfolio update; verify.

### PULPO WMS
- **major platform update** - 3PL Merchant Portal, activity-based billing, purchasing/demand planning, automatic pick-task creation, replenishment rules, pre-wave replenishment/cross-docking, pick-path optimization, put-wall sortation and cartonisation.
  - ontology signal: 3PL billing can be modeled as operational event -> billable activity -> merchant-specific rate -> accrued charge -> invoice.
  - status: material capability/packaging update; verify GA date and exact feature set.

### NEURA Robotics / Bosch Rexroth
- **ACTIVE Shuttle business handover** - ownership/service/software transition from Bosch Rexroth to NEURA, including ACTIVE Fleet Manager and ROKIT navigation technology as described in chat.
  - ontology signal: track robot hardware ownership, fleet-management software and navigation IP separately.
  - status: material ownership/platform update; verify effective handover and transferred assets.

### UNIT AI + Barrett Distribution Centers
- **Networked Physical AI Platform** - planned network-level orchestration across distributed inventory placement, fulfillment, inventory visibility and decentralized returns.
  - commercial model: Warehouse-as-a-Service / pay-per-use.
  - ontology signal: add a network-orchestration layer above facility WMS/WES; distinguish multi-site visibility from cross-node decision-making.
  - status: material architecture/business-model signal; deployment is future/planned, so do not mark production until evidenced.

### Logistics Reply
- **LEA AI Agent Authority Model** - five authority levels: Inform, Recommend, Act, Coordinate, Governed Autonomy.
  - production agents described in chat: Out of Stock, Labor Distribution, ABC Rebalancer, Dock Scheduling, Lost & Found.
  - Agent Builder; per-agent data contract, integration approach, delegated authority and human-oversight policy.
  - ontology signal: `agent capability != agent authority`; authority should be independently modeled with guardrails, approvals and operational evidence.
  - status: material agent-governance update; verify official release and exact agent names.

## Cross-vendor ontology and architecture findings

1. **Agent governance is first-class** - capability, authority, lifecycle, approval policy, observability and interoperability are distinct dimensions.
2. **Agent/tool boundary matters** - expose deterministic domain capabilities as tools independently of agent implementation; MCP is an emerging interoperability mechanism, not domain ownership.
3. **WMS/WES/TMS boundaries are converging** - warehouse execution state increasingly participates directly in transport optimization.
4. **Orchestration ownership is competitive** - WES, robot OS and fleet layers may each compete to become the top-level controller.
5. **Network orchestration is above multi-site visibility** - cross-node inventory/fulfillment/returns decisions deserve their own layer.
6. **Packaging orchestration is a domain** - cartonization/right-sizing/material/packing decisions should not be hidden inside generic shipping.
7. **Robotic picking is multi-dimensional** - grasp, drop, precise placement, consolidation and sequencing are separate capabilities.
8. **Mobile robot taxonomy should be orthogonal** - mobility, load handling, lift, navigation autonomy and fleet interoperability instead of mutually exclusive AMR/AGV/forklift labels.
9. **Robot market access is a procurement attribute** - regulatory authorization/provenance/update/remote-access risks belong beside safety, throughput and TCO.
10. **3PL billing can be operational** - warehouse execution events may directly accrue merchant-specific charges.
11. **AI-native extensibility is emerging** - configuration assistants and natural-language operational app builders are becoming competitive platform surfaces.
12. **Hardware/fleet/navigation ownership are separate** - acquisitions may transfer these layers independently.
13. **Standards maturity must be explicit** - published, draft, proposed-rule, research-preview and announced/future deployment statuses must never collapse into one “supported” state.

## Promotion checklist

- Re-open every official source and capture durable evidence metadata/snapshot.
- Promote only verified statements into `claims/` and company/product records.
- Add/update authority records for ISO, VDI and regulatory bodies with explicit lifecycle status.
- Add taxonomy/ontology changes only after checking existing canonical terms to avoid duplicates.
- Regenerate matrices and run `bin/check`.
