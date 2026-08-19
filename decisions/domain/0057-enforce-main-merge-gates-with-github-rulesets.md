# ADR 0057: Enforce main merge gates with a GitHub repository ruleset

- Status: Accepted
- Date: 2026-08-19

## Context

The repository has machine validation, the PostgreSQL inventory kernel lab, and a topology finalization cleanup guard. Until now these workflows were advisory because `main` was not protected. An administrator or automation with write access could merge or push without all gates being present and successful.

Path-filtered required workflows are also unsafe as a repository rule contract: when a workflow is not created for a pull request, GitHub has no successful check with that context to satisfy a required status check.

## Decision

Protect `refs/heads/main` with the checked-in repository ruleset `.github/rulesets/main.json`.

The ruleset requires:

1. all changes enter through a pull request;
2. squash is the only allowed merge method;
3. history remains linear;
4. deletion and force push are rejected;
5. required status checks run in strict mode against the latest `main`;
6. the required contexts are exactly:
   - `validate`;
   - `postgres-kernel-lab`;
   - `cleanup-guard`.

The required workflow jobs must be created for every pull request. Domain-specific workflows may decide internally that expensive work is not applicable, but pull-request path filters are forbidden on a required workflow.

The ruleset intentionally requires zero approving reviews. The current repository governance boundary is machine-enforced pull request review and deterministic gates, not a mandatory second human approval for every change. Workflows that need two-person approval, such as topology manual intervention, keep that requirement in their own operational protocol.

No bypass actor is declared in the ruleset. Administrators are therefore expected to follow the same merge gates during normal operation.

## Application boundary

Repository files cannot themselves activate GitHub repository settings. `bin/apply-github-main-ruleset` is the reviewed administrative adapter. It requires an authenticated GitHub identity with repository Administration write permission and creates or updates the named ruleset through GitHub's repository ruleset API.

Normal CI never receives an administration token and cannot silently weaken or replace the repository ruleset.

## Consequences

A required check context is now part of the repository contract. Renaming a required job requires an atomic reviewed change to the ruleset manifest and workflow, followed by an administrative ruleset update.

The kernel workflow remains cost-aware: it starts for every pull request so the required context always exists, but skips setup, containers, tests, and performance work when the diff is outside kernel scope.

The topology cleanup guard follows the same model and skips its Python setup when the deployment registry did not change.

Strict required checks can require a pull request to be updated after `main` moves. This is intentional because the merge decision must cover the latest target state.

## Rejected alternatives

### Keep `main` unprotected

Rejected because CI would remain advisory.

### Require path-filtered workflows directly

Rejected because a required status context must exist on every pull request.

### Give normal CI an administration token

Rejected because validation code must not be able to rewrite its own enforcement boundary.

### Require one human approval for every pull request

Deferred. It adds a social gate that is not currently necessary for the repository's single-owner agent workflow and is independent of the machine safety gates defined here.
