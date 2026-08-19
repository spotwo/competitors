# ADR 0062: Bound architecture research with reference coverage

Status: **Accepted**

Date: 2026-08-19

## Context

ADR 0061 introduced a canonical Architecture Boundary Map. That map makes ownership, reliability, security, versioning, offline behavior, simulation, standards, and candidate mechanisms explicit, but it does not by itself answer whether a boundary has enough reference evidence to support the next decision.

Without a second layer, research can drift back into an unbounded catalog of interesting technologies and vendors. A long list of references is not the same as knowing which architecture question is under-supported.

The repository already contains several different evidence classes:

- Spotwo-owned ADRs and executable labs;
- open-source projects and reference implementations;
- commercial product evidence;
- standards/specification references;
- explicit research gaps and future targets.

These evidence classes serve different purposes and must not be silently collapsed into one score.

## Decision

Create `architecture/coverage-gaps.yml` as the canonical machine-readable **reference coverage backlog** for the boundaries in `architecture/boundaries.yml`.

Every architecture boundary must have exactly one coverage record. The record classifies four research lenses:

1. `standards`;
2. `open_source`;
3. `commercial`;
4. `executable`.

Each lens declares whether it is `required`, merely `useful`, or `not-required`, and records a coverage status of `covered`, `partial`, `missing`, or `not-required`.

Evidence references and future research targets are separate fields. A target is a question or source we intend to investigate. It is never evidence and must not be cited as proof that a product, protocol, or standard behaves in a particular way.

The overall boundary coverage state is deliberately simple:

- `covered` when every required lens is covered;
- `partial` when the boundary has relevant captured evidence but at least one required lens is incomplete;
- `missing` when no relevant evidence is captured for the required research question.

The validator recomputes this state instead of trusting a manually entered summary.

## Evidence integrity

Coverage references are validated by kind:

- `repository` must resolve to an existing repository file;
- `open-source-project` must resolve to the open-source catalog;
- `competitor-product` must resolve to a verified product row with at least one source;
- `standard` must already be declared on the corresponding architecture boundary.

This does not make boundary-level standard names a complete standards corpus. It only prevents a coverage record from inventing a standard relationship that the boundary map does not declare.

Commercial products are evidence only when a verified product reference is explicitly attached. A company or product named under `research_targets` remains a target, not evidence.

## Runtime promotion rule

A candidate runtime contract is not ready to become canonical merely because standards, open-source, or commercial references look persuasive. The repository policy requires executable evidence before promotion.

This does not mean every external interoperability mapping needs a production deployment before it can be adopted. It means a runtime mechanism that will carry commands, state, telemetry, optimization, robot control, PLC control, or simulation semantics must be proven by a bounded executable experiment appropriate to that boundary.

## Initial result

The first coverage pass contains 10 boundaries:

```text
2 covered
7 partial
1 missing
```

The P0 research queue is intentionally small:

```text
external asynchronous integration
Spotwo Edge control and synchronization
PLC and machine control
AMR and AGV fleet control
```

This is a research-priority classification, not a product roadmap priority.

## Consequences

Positive:

- research becomes bounded by architecture questions;
- missing evidence is visible without pretending unknowns are decisions;
- agents can query the backlog machine-readably;
- evidence and future targets cannot be confused structurally;
- standards, commercial products, open-source projects, and labs keep distinct roles;
- P0/P1/P2 research can be reprioritized without changing runtime architecture.

Tradeoffs:

- coverage records add another maintained registry;
- some lenses will intentionally remain `not-required` or `useful` rather than forcing artificial completeness;
- the first coverage map reflects current repository evidence and must evolve as research is added.

## Follow-up

Use the P0 coverage queue to drive the next research slices. Prefer adding evidence to a known boundary over adding unrelated catalog entries. When new evidence changes a technology decision or boundary contract, update those canonical registries in a separate explicit decision step.
