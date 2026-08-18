# Terminology Registry

This directory is Spotwo's canonical registry of warehouse and intralogistics language.

Each term records preferred market term, definition, aliases and competing terms, evidence, Spotwo adoption status, and notes where vendor semantics differ.

Statuses:

- `adopted` - safe default for Spotwo product language.
- `candidate` - likely choice, but still under review.
- `observed` - real industry usage; semantics may be vendor-specific.
- `needs-research` - explicitly unresolved.
- `deprecated` - avoid in new Spotwo product language.

When a term materially affects the domain model or UX, create a decision record under `decisions/terminology/`.
