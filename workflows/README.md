# Workflow Decompositions

A workflow is the layer below a canonical capability.

Use a workflow when the question is no longer merely **does the system support this capability?** and becomes **how does executable work actually move through the system?**

## Decomposition contract

Each workflow should describe:

1. **Stages** - stable operational phases, independent of one vendor UI.
2. **Objects** - the domain concepts that move between stages.
3. **States** - lifecycle states for executable work, not order or shipment states unless explicitly mapped.
4. **Strategies** - alternative release, grouping, routing, assignment, execution or confirmation models.
5. **Exceptions** - first-class deviations and their possible downstream outcomes.
6. **Vendor models** - evidence-backed mappings of vendor terminology and workflow shape.
7. **Spotwo candidate model** - explicit proposal separated from observations.
8. **Research gaps** - questions that block adoption.

## Do not flatten vendor vocabulary

For example, the following concepts occupy related but not necessarily identical positions:

```text
SAP              Warehouse Order -> Warehouse Task
Dynamics 365     Work -> Work Line
Oracle WMS       Task -> Allocation
NetSuite WMS     Wave -> Pick Task
Infor WMS        Assignment -> Pick Task
```

Do not create an alias list and pretend these objects are identical. Preserve the vendor mapping and derive a separate Spotwo abstraction only when the operational role is sufficiently stable across systems.

## Keep execution channels orthogonal

RF, barcode, voice, pick-to-light, put-to-light, goods-to-person and robot execution are interaction or execution strategies unless evidence shows that they materially change business semantics.

Avoid domain types such as:

```text
VoicePickTask
BarcodePickTask
PickToLightTask
```

when a shared `PickTask` plus execution adapter/channel is sufficient.

## Evidence rule

Every vendor model, strategy or exception observation must reference at least one evidence record. Prefer official product documentation and current release documentation.

## Query

```bash
python scripts/kb.py workflow
python scripts/kb.py workflow picking
python scripts/kb.py workflow picking --vendor sap
python scripts/kb.py workflow picking --strategy voice
python scripts/kb.py workflow picking --json
```

Generated views live under `matrices/workflows.md` and `matrices/workflow-<id>.md`.
