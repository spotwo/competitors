# Competitor-derived architecture signals - 2026-10-07

These are **research inputs**, not canonical architecture decisions. They are extracted from the competitor-watch findings in `research/competitor-watch/2026-10-07-chat-findings.md`.

- Agent capability, delegated authority, lifecycle/version, approval policy, guardrails, observability and interoperability are independent dimensions.
- Deterministic domain capabilities should be exposed independently of the agents that call them.
- MCP is an emerging tool/agent interoperability mechanism; it does not own Spotwo domain semantics.
- Warehouse execution state can participate directly in transportation optimization, so WMS/TMS integration is not only record exchange.
- WES, robot OS and fleet-management systems can compete for top-level orchestration ownership.
- Network orchestration is distinct from multi-site visibility because it makes cross-node placement/fulfillment/returns decisions.
- Packaging orchestration is a first-class execution domain between picking and shipping.
- Robotic picking should distinguish grasp, drop, precise placement, consolidation and sequencing.
- Mobile-robot classification should model mobility, load handling, lift, autonomy and fleet interoperability orthogonally.
- Hardware ownership, fleet-management ownership and navigation-IP ownership must be tracked separately.
- Regulatory market access is a procurement dimension for connected robotics.
- 3PL billing may be driven directly by execution events and should not be reduced to an accounting integration.
- AI-native extensibility includes configuration assistants and generated operational applications, not only runtime optimization.
- Standards/protocol evidence must preserve lifecycle: draft, proposed rule, research preview, published, GA, announced/future.

No item in this file changes a canonical boundary in `architecture/boundaries.yml` by itself. Promotion requires verified evidence and an explicit architecture decision or boundary update.
