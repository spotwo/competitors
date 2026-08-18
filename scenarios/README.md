# Golden Warehouse Scenarios

`scenarios/` contains machine-readable end-to-end architecture contracts for the Spotwo WMS kernel.

A Golden Scenario is deliberately broader than a unit test and narrower than a UI/E2E product test. It composes multiple domain layers and asserts stable business invariants at meaningful checkpoints.

Canonical flow:

```text
Preconditions
  -> Domain Action
  -> Domain Action
  -> Checkpoint
  -> ...
  -> Final invariants
```

## Authoring rules

1. Give every scenario and every step a stable kebab-case ID.
2. Describe domain actions, not button clicks or implementation-specific SQL.
3. Every checkpoint must reference a real step ID from the same scenario.
4. Checkpoints should assert domain quantities, states or relationships that would matter after an internal refactor.
5. Every scenario must map to exactly one pytest entry point in `labs/postgres-inventory-kernel/tests/test_golden_scenarios.py`.
6. The executable test may add lower-level guard assertions, but it must prove every material invariant stated by the canonical scenario.
7. Do not model a scenario as successful by directly mutating state when a candidate kernel API exists.
8. Cross-layer actions that must be atomic should use a capability-owned confirmation bridge rather than relying on eventual best-effort updates between engines.
9. Add unresolved architecture questions to `research_gaps` instead of hiding them in test fixtures.

Validate with:

```bash
python scripts/validate_scenarios.py
python scripts/generate_scenario_views.py --check
```

The full PostgreSQL implementation runs through:

```bash
bash bin/check-kernel-lab
```

See `decisions/domain/0018-golden-warehouse-scenarios.md` for the architectural decision.
