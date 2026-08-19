# ADR 0058 - Evidence-Grounded Technology Decisions

Status: Accepted

Date: 2026-08-19

## Context

The repository now contains several forms of technology evidence:

- competitor product technology and deployment matrices;
- primary-source evidence for those matrices;
- an open-source logistics architecture catalog;
- weighted Spotwo-specific technology selections such as the Domain Event transport decision;
- executable PostgreSQL and NATS labs.

Without a separate decision layer, it is easy to confuse "a competitor uses this", "an open-source project is interesting", and "Spotwo has chosen this".

That becomes especially dangerous in an agent-driven development workflow because an agent can turn a descriptive research signal into a new runtime dependency faster than a human review cycle can notice the semantic jump.

## Decision

Create a machine-readable technology decision registry at `decisions/technology/registry.yml` and a generated matrix at `matrices/spotwo-technology-decisions.md`.

Every technology decision MUST be scoped and MUST use one of four dispositions:

- `adopt` - current default inside the stated scope;
- `trial` - bounded prototype before adoption;
- `watch` - researched option or architecture reference without runtime commitment;
- `reject` - do not use inside the stated scope.

A disposition is not a universal judgment about a technology.

## Evidence authority

Evidence is deliberately separated into three channels:

1. repository decisions and executable labs;
2. open-source project or standard research;
3. competitor product evidence.

Competitor adoption and open-source popularity are signals, not authorization. They may justify research or a prototype, but they do not by themselves justify `adopt`.

An `adopt` decision therefore requires repository decision or lab evidence. A `reject` decision also requires repository decision evidence because rejection can remove an option from an engineering scope.

## Initial decisions

The first registry records the current state rather than pretending every topic is settled.

Examples:

- PostgreSQL - `adopt` for WMS OLTP and durable control-plane journals;
- NATS JetStream - `adopt` for the operational WMS Domain Event transport profile;
- REST + OpenAPI - `trial` as the external HTTP API contract baseline;
- VDA 5050, PLC4X, Sparkplug, OR-Tools, Ocava, and openTCS - `trial`;
- Kubernetes as a universal default - `watch`, not automatic adoption;
- Cloudflare Queues - `reject` only as the canonical operational WMS Domain Event transport.

## Validation

`scripts/validate_technology_decisions.py` rejects:

- duplicate decision IDs;
- missing repository evidence files;
- unknown open-source project references;
- unknown competitor product references;
- competitor evidence that is still a source-free `research-gap`;
- evidence-free decisions;
- `adopt` without repository decision or lab evidence;
- `reject` without repository decision evidence;
- low-confidence `adopt` or `reject` decisions.

The generated matrix is checked for drift by `scripts/generate_technology_decision_matrix.py --check`.

Both checks are part of `bin/check` and therefore the protected `main` merge gate.

## Consequences

Technology research can now feed engineering choices without silently becoming them.

Agents have a machine-readable answer to questions such as:

- what is already canonical;
- what should be prototyped next;
- what is only being watched;
- what has been rejected for a specific scope;
- what evidence supports the decision;
- what condition should reopen it.

The registry does not replace detailed ADRs. It is the cross-cutting index that points from research to the current Spotwo decision and next action.
