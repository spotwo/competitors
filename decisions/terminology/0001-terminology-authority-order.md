# 0001 - Terminology authority order

Status: Accepted
Date: 2026-08-18

## Context

Warehouse terminology varies between regulation, formal standards, associations, regions, vendors, integrators, and operator slang. Spotwo needs stable language without blindly copying one competitor.

## Decision

Use the following order as a decision aid:

1. Applicable regulation or law
2. Formal standard
3. Industry association or recognized industry guide
4. Dominant industry usage
5. Major competitor usage
6. Internal terminology

This is not a mechanical precedence rule. User recognition and operational clarity can justify a market term in the UI while preserving the formal term as a canonical alias.

## Consequences

- Naming decisions must be evidence-backed.
- Market term and standard term can coexist.
- Regional terminology differences should be recorded instead of silently normalized.
- Unresolved terms remain `needs-research`.
