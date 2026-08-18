# Generated Matrices

Files in this directory are human-readable projections of the canonical YAML records.

Do not edit generated matrices by hand. After changing a company or terminology record, run:

```bash
python scripts/generate_matrices.py
python scripts/validate.py
```

CI runs `python scripts/generate_matrices.py --check` and fails when committed matrices are stale.

Current generated views:

- `vendors.md` - vendor/segment/layer/market/deployment/product overview.
- `solution-layers.md` - vendors grouped by WMS/WES/WCS/MFS/etc. layer.
- `terminology.md` - term status, Spotwo preferred term, aliases and evidence count.
