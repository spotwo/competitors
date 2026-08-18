# ADR 0005 - Picking Work Model

Status: Proposed

Date: 2026-08-18

## Context

WMS products expose similar picking behavior through materially different object models and terminology.

Observed examples include:

- SAP EWM: `Warehouse Order -> Warehouse Task`.
- Microsoft Dynamics 365: `Wave -> Work -> Work Line`.
- Oracle WMS Cloud: `Task -> Allocation`.
- NetSuite WMS: `Wave -> Pick Task`.
- Infor WMS: `Assignment -> Pick Task`.
- Odoo: `Wave/Batch Transfer -> Detailed Operation`.

Picking execution methods also vary independently of these objects: RF/barcode, voice, pick-to-light, put-to-light, goods-to-person, cluster picking and other methods can all execute the same business capability.

The evidence is captured in `workflows/picking.yml` and its evidence records.

## Proposed decision

Use the following Spotwo abstractions until broader vendor sampling confirms or changes them:

```text
Fulfillment Demand
       |
       v
Allocation / Source Decision
       |
       v
Release / Orchestration -----> optional Wave
       |
       v
Pick Work Group
       |
       +---- Pick Task
       +---- Pick Task
       +---- Pick Task
                |
                v
            Assignment
                |
                v
        Execution / Confirmation
                |
        +-------+--------+
        |                |
        v                v
    Completed        Pick Exception
                         |
                         v
                  Resolution / Replan
```

### Rules

1. `Wave` is optional. The domain model must support wave, waveless and mixed release models.
2. `Pick Work Group` is the canonical grouping abstraction. It is not automatically equivalent to a vendor Warehouse Order, Work, Assignment or Pick List at integration boundaries.
3. `Pick Task` is the smallest independently executable picking instruction in the canonical model.
4. `Assignment` is a relationship between work and an execution resource or task pool. It is not the task itself.
5. Execution channel is orthogonal to task semantics. RF, barcode, voice, light-directed and automation execution must not require different business task types solely because of the UI/device.
6. `Pick Confirmation` records the actual execution outcome, not merely a UI acknowledgement.
7. `Pick Exception` is a first-class domain concept. Short, zero, damaged or validation-failed outcomes must not be hidden as arbitrary quantity mutations.
8. A short pick should preserve the distinction between completed physical work and residual unfulfilled demand. Automatic reallocation behavior remains configurable and requires more cross-vendor evidence.

## Candidate task states

```text
planned
released
assigned
in-progress
held
exception
completed
cancelled
```

These states are not yet adopted. At least ten additional WMS workflow models should be sampled before finalizing the state machine.

## Candidate commands

- `release-pick-work`
- `assign-pick-work`
- `start-pick-task`
- `confirm-pick`
- `report-short-pick`
- `report-damage`
- `hold-pick-work`
- `resume-pick-work`
- `cancel-pick-work`
- `complete-pick-work`

## Candidate events

- `pick-work-released`
- `pick-work-assigned`
- `pick-task-started`
- `pick-confirmed`
- `pick-short-reported`
- `pick-damage-reported`
- `pick-work-held`
- `pick-work-resumed`
- `pick-work-cancelled`
- `pick-work-completed`

## Consequences

- Integrations require explicit mappings from vendor concepts into Spotwo work-group/task/assignment concepts.
- Waveless execution does not force a parallel domain model.
- Voice, RF, PTL and automation can share the same picking API and events while providing different interaction adapters.
- Exception handling becomes observable and auditable.
- Picking workflow state can evolve independently from outbound order state.

## Evidence

See:

- `workflows/picking.yml`
- `matrices/workflow-picking.md`
- evidence records with `subject: picking-workflow`
