# Spotwo Technology Decisions

This directory converts research into explicit, scoped engineering decisions.

The repository already contains three different evidence layers:

- `technology-intelligence/` records what competitor products are publicly known to use;
- `research/open-source/catalog.yml` records open-source projects, standards, and architecture references worth adopting, prototyping, studying, or watching;
- accepted ADRs and executable labs record decisions that Spotwo has already tested or committed to.

Those layers are intentionally not equivalent. Competitor usage is a market signal, not a reason to copy a stack. Open-source relevance is a research signal, not permission to add a dependency. An accepted Spotwo ADR or measured lab result has stronger authority for a Spotwo-specific decision.

## Canonical registry

`registry.yml` is the machine-readable source of truth.

Every record answers:

- what technology is being considered;
- the exact scope of the decision;
- one of `adopt`, `trial`, `watch`, or `reject`;
- confidence;
- rationale;
- repository, open-source, and competitor evidence references;
- constraints and alternatives;
- the next concrete action;
- explicit triggers that should reopen the decision.

The human-readable matrix is generated at:

`matrices/spotwo-technology-decisions.md`

## Disposition semantics

`adopt` means the technology is the current default only inside the stated scope. It does not mean "use everywhere".

`trial` means build or continue a bounded prototype before adding a production dependency or standardizing the contract.

`watch` means the technology remains researched and relevant, but there is no commitment to use it now.

`reject` is also scoped. For example, Cloudflare Queues is rejected as the canonical operational WMS domain-event transport, while remaining acceptable for a separately reviewed Workers-native background-job workload.

## Evidence rules

The validator enforces the following boundaries:

1. every decision must cite at least one evidence source;
2. repository file references must exist;
3. open-source references must exist in `research/open-source/catalog.yml`;
4. competitor references must exist in `technology-intelligence/matrix.csv` and cannot point to `research-gap` rows with no source evidence;
5. an `adopt` decision must cite repository decision or lab evidence;
6. a `reject` decision must cite repository decision evidence;
7. `adopt` and `reject` cannot be low-confidence decisions.

Run:

```bash
python scripts/validate_technology_decisions.py
python scripts/generate_technology_decision_matrix.py --check
```

Both commands are included in `bin/check`.

## Decision lifecycle

A normal path is:

```text
research
  -> watch
  -> trial
  -> adopt
```

But this is not a maturity ladder that every technology must follow. A technology may remain `watch` indefinitely, or a measured architecture decision may explicitly `reject` it for one scope while using it elsewhere.

When a revisit trigger becomes true, the correct action is to update the evidence and decision through a normal pull request. Do not silently reinterpret an old decision outside its original scope.
