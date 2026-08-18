# Agentic CI Policy

This file defines the default CI and merge policy for agent-driven software development.

The model is **ASDLC - Agentic Software Development Life Cycle**: agents perform most implementation iterations locally, while GitHub Actions acts primarily as the verification, policy, and merge-control plane.

Use this policy as the default for new projects unless a repository explicitly documents a stricter project-specific rule.

## 1. Change isolation and ownership

### One task, one branch, one PR, one agent

**1 task = 1 branch = 1 PR = 1 agent.** An agent must never work directly on `main` or another protected default branch.

### Keep the agent inside the intended diff

The agent owns the requested diff, not the repository. Define or infer the allowed files, paths, or components for the task, and treat unexpected scope expansion as a failure that must be reviewed.

### Prefer small PRs

Prefer several small, independently understandable PRs over one large autonomous change. Five PRs of roughly 100 lines are generally easier to verify and recover than one 3,000-line agent-generated PR.

## 2. Agent inner development loop

### Run the inner loop locally first

Formatting, linting, type checking, targeted unit tests, and other cheap deterministic checks should run in the agent's local environment before code is pushed. GitHub CI should verify the result rather than act as the agent's primary development loop.

### Push checkpoints, not thoughts

Do not push after every tiny agent iteration. Push only after a logical unit of work is coherent enough to be reviewed or verified remotely.

### Keep remote history logical

Agent work may contain many local technical commits or iterations, but merge history should remain readable. Squash noisy agent history so `main` receives logical commits rather than dozens of implementation checkpoints.

## 3. Fast PR CI

### Keep PR CI small

The default PR gate should contain only fast, high-signal checks such as formatting/linting, type checking, affected tests, and basic security or schema validation. Aim for feedback in minutes, not a full-system rebuild on every push.

### Run affected CI instead of global CI

Use path or change detection so a change runs only the jobs and test suites relevant to the affected modules, directories, packages, or services.

### Use lightweight runners for lightweight jobs

Use `ubuntu-slim` for jobs such as linting, formatting, schema validation, and simple unit tests when those jobs do not need privileged Docker, kernel features, or a large preinstalled toolset. Keep `ubuntu-latest` or an appropriate full runner for Docker-dependent, kernel-dependent, integration-heavy, or tooling-heavy jobs.

### Cancel stale runs

A new push to the same PR should cancel obsolete CI for the previous SHA with workflow concurrency and `cancel-in-progress` where safe. Do not spend CI minutes validating code that has already been superseded.

### Cache deterministic work

Cache dependencies, package-manager downloads, compiled artifacts, and reusable test caches when deterministic and safe. Do not repeatedly rebuild identical inputs from scratch.

## 4. Risk-based verification

### Make CI proportional to risk

A README change and an authentication, payment, database-migration, infrastructure, or security-sensitive change should not consume the same CI budget. Expand validation as change risk increases.

### Make review proportional to risk

Low-risk agent PRs may be eligible for automatic merge after required checks pass. High-risk changes involving areas such as authentication, payments, infrastructure, migrations, permissions, or security should require human approval.

### Treat tests as the executable contract

The more autonomous the agent, the more important executable acceptance tests, invariants, schemas, contract tests, and other machine-verifiable constraints become. Agent freedom should increase only when the system can verify behavior mechanically.

## 5. Full merge gate

### Run full CI before merge, not after merge

Run the complete merge-blocking validation against the exact code that is about to enter `main`, preferably through GitHub Merge Queue / `merge_group` when supported. A full regression suite that runs only after merge is too late to act as a quality gate.

### Prefer Merge Queue over repeated manual branch updates

When many agents create PRs in parallel, prefer Merge Queue instead of repeatedly updating every branch from `main` and rerunning the entire CI suite. Validate the merge candidate that will actually land.

### Protect `main` structurally

Use branch protection or repository rulesets with required checks and required reviews where appropriate. Agents must not be able to bypass merge policy merely because a prompt told them not to.

## 6. Post-merge validation

### Keep post-merge work focused on production confidence

After merge, run smoke tests, deployment verification, health checks, or other post-deploy validation. The primary full regression gate should already have passed before the merge.

### Keep `main` boring

Agents may perform many experimental iterations in isolated branches, but `main` should receive only small, coherent, reviewed, and verified logical changes.

## 7. Security and permissions

### Use least-privilege agent credentials

Give an agent only the repository, environment, workflow, package, deployment, or infrastructure permissions required for the current task.

### Do not expose production secrets to ordinary PR CI

Untrusted or agent-generated PR code should not automatically receive privileged secrets. Keep privileged deployment, release, and sensitive security workflows separate from ordinary PR code execution.

### Pin third-party GitHub Actions immutably

Prefer pinning third-party GitHub Actions to an immutable commit SHA rather than relying only on mutable tags such as `@v4`.

## 8. CI cost and reliability

### Treat CI budget as an engineering metric

Track metrics such as `CI minutes / merged PR`, `CI minutes / agent task`, rerun rate, failure rate, queue time, and time-to-green. CI cost is part of the architecture of an agent-driven development system.

### Eliminate flaky tests from the agent loop

Fix flaky tests quickly or quarantine them from merge-blocking paths until repaired. Flakiness is especially expensive for autonomous agents because it can create useless retries, false failures, and repeated CI consumption.

### Batch heavy suites when appropriate

Heavy E2E suites, broad compatibility matrices, expensive integration environments, deep security scans such as CodeQL, and other costly checks may run at the merge gate, on `main`, or on a scheduled/nightly basis depending on risk. Do not automatically pay their full cost on every agent push when they are not required for immediate feedback.

## 9. Human and machine responsibilities

### Humans review intent; machines review mechanics

Use humans primarily for architecture, product intent, security judgment, risk, tradeoffs, and unusual behavior. Use machines for formatting, linting, types, schemas, deterministic tests, policies, and repeatable correctness checks.

## 10. Reference ASDLC pipeline

Use this four-level pipeline as the default mental model:

```text
Agent local loop
    -> Fast PR CI
    -> Full Merge-Queue CI
    -> Main smoke/deploy verification
```

### Level 1 - Agent local loop

The agent iterates locally and runs formatter, linter, type checker, targeted tests, and other cheap task-specific checks before pushing.

### Level 2 - Fast PR CI

GitHub verifies the pushed checkpoint with a small affected-change gate and immediately rejects obvious correctness, quality, or security problems.

### Level 3 - Full Merge-Queue CI

Before merge, the exact merge candidate receives the expensive or comprehensive validation required to protect `main`.

### Level 4 - Main smoke/deploy verification

After merge or deployment, verify that the deployed system is healthy and that critical production paths still work.

## Core principle

**GitHub Actions is not the agent's inner development loop.**

In an agent-driven SDLC, the high-frequency implementation loop belongs on the agent's devbox, container, sandbox, or other local execution environment. GitHub Actions should primarily serve as the independent verification layer and the merge-control plane.
