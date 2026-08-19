# GitHub repository rulesets

This directory stores the reviewed repository-side branch protection contract.

## Main branch

`main.json` targets only `refs/heads/main` and is intended to be installed as an active repository ruleset named `Protect main merge gates`.

The contract requires:

- pull requests for all changes to `main`;
- squash merge only;
- linear history;
- no branch deletion;
- no force push;
- strict required status checks against the latest `main`;
- required check contexts `validate`, `postgres-kernel-lab`, and `cleanup-guard`.

The three required workflow jobs must exist for every pull request. They may perform an internal no-op when their expensive domain-specific work is not relevant, but the workflow itself must not use pull-request path filters because a missing required check would make the merge contract ambiguous.

## Apply

An administrator with GitHub CLI authentication that includes repository Administration write permission can install or update the reviewed ruleset:

```bash
bin/apply-github-main-ruleset spotwo/competitors
```

The helper validates the local policy first, then creates or updates the repository ruleset through the GitHub REST API and prints the active rules that apply to `main`.

The helper is intentionally not part of normal CI. A pull request must be merged before the repository-level policy can safely be updated to require the new workflow behavior.

## Validation

`bin/check` executes:

```bash
python scripts/validate_github_main_ruleset.py
```

This prevents drift between the checked-in ruleset and the job contexts exposed by the workflows.
