# Generated Matrices

Files in this directory are human-readable projections of canonical YAML records.

Do not edit generated matrices by hand.

```bash
python scripts/generate_matrices.py
python scripts/generate_audit.py
python scripts/generate_domain_views.py
```

CI runs the same generators with `--check` and fails when committed views are stale.

Current generated views:

- `vendors.md` - vendor/segment/layer/market/deployment/product overview.
- `solution-layers.md` - vendors grouped by WMS/WES/WCS/MFS/etc. layer.
- `terminology.md` - term status, Spotwo preferred term, aliases and evidence count.
- `stats.md` - live canonical record counts.
- `research-gaps.md` - explicit company gaps and unresolved terminology.
- `ontology.md` - domain ontology coverage.
- `kpis.md` - warehouse KPI registry projection.
- `integrations.md` - integration-pattern registry projection.
