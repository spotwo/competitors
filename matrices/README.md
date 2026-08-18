# Generated Matrices

Files in this directory are human-readable projections of canonical YAML records.

Do not edit generated matrices by hand.

```bash
python scripts/generate_matrices.py
python scripts/generate_audit.py
python scripts/generate_domain_views.py
python scripts/generate_workflow_views.py
python scripts/generate_kernel_views.py
```

CI runs the same generators with `--check` and fails when committed views are stale.

Current generated views:

- `vendors.md` - vendor/segment/layer/market/deployment/product overview.
- `solution-layers.md` - vendors grouped by WMS/WES/WCS/MFS/etc. layer.
- `terminology.md` - term status, Spotwo preferred term, aliases and evidence count.
- `stats.md` - live canonical record counts including inventory kernel records.
- `research-gaps.md` - explicit company, terminology, capability, workflow and kernel research backlog.
- `ontology.md` - domain ontology coverage including inventory movement and inventory transaction/position.
- `capabilities.md` - canonical capability coverage, status, vendor observations and evidence counts.
- `workflows.md` - compact deep-workflow coverage matrix.
- `ledger.md` - inventory transaction types, posting semantics and candidate invariants.
- `events.md` - inventory domain-event registry, envelope and external visibility mappings.
- `picking-supplemental-observations.md` - broader picking vendor sample from current claims.
- `kpis.md` - warehouse KPI registry projection.
- `integrations.md` - integration-pattern registry projection including EPCIS visibility events.

Canonical detail remains in YAML records. Generated Markdown is a navigation and review surface, not a second source of truth.
