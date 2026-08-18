# Capability Knowledge Graph

Each YAML file in this directory describes one canonical warehouse capability from `taxonomy/processes.yml`.

A capability answers **what operational outcome exists**. Strategies and implementation patterns answer **how it is executed**.

Examples:

- `Picking` is a capability; discrete, batch, cluster, zone, goods-to-person, voice and pick-to-light are strategies or execution methods.
- `Replenishment` is a capability; min/max, percentage-of-max, reactive and order-based are trigger strategies.
- `Wave Planning / Release` and `Waveless / Continuous Release` are separate execution models because they materially change how work enters the warehouse execution layer.
- `Automation Orchestration` is distinct from WCS or machine control. Orchestration decides and coordinates work; lower control layers execute equipment commands.

## Vendor observation levels

- `core` - explicitly part of the vendor's normal WMS/WES workflow or base capability set.
- `advanced` - explicitly positioned as higher-order optimization or execution behavior.
- `module` - sold or described as a distinct optional/module capability.
- `observed` - clearly present in documented workflow, but current evidence is not strong enough to classify packaging/edition level.

Never infer support from the absence or presence of a generic marketing phrase. Add an observation only with an `evidence_ref`.

## Query examples

```bash
python scripts/kb.py capability replenishment
python scripts/kb.py capability picking --vendor blue-yonder
python scripts/kb.py capability --group fulfillment
python scripts/kb.py capability automation --json
```

`matrices/capabilities.md` is generated from these records and must not be edited by hand.
