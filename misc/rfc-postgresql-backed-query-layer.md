# RFC: PostgreSQL-backed query layer for a future release

Status: **Deferred / proposed for later discussion**

This RFC captures a possible future Spotwo application-to-database query structure. It is deliberately not an implementation commitment. The current labs and experiments should continue to optimize for learning rather than prematurely moving database behavior into permanent application architecture.

## Motivation

While testing architecture options, it is easy to promote every useful experiment into the product too early. This RFC preserves the idea so it can be evaluated later against real query shapes, performance evidence, operational requirements, and the final application architecture.

The central proposal is to keep the Rails Query Object as the application-facing boundary while allowing PostgreSQL to provide a stable database interface underneath it when that is justified.

## Candidate structure

```text
Controller / application use case
              |
              v
        Rails Query Object
              |
              v
      stable DB interface
              |
      +-------+-------------------+------------------+
      |                           |                  |
      v                           v                  v
 plain SQL / CTE                VIEW        MATERIALIZED VIEW
                                                      |
                                   +------------------+------------------+
                                   |                                     |
                                   v                                     v
                             SQL function                        PL/pgSQL function
                                   |                                     |
                                   +------------------+------------------+
                                                      |
                                                      v
                                                    tables
```

A simpler conceptual form is:

```text
Rails Query Object
        |
        v
stable database interface
        |
        +--> plain SQL / CTE
        +--> VIEW
        +--> MATERIALIZED VIEW
        +--> SQL function
        +--> PL/pgSQL function
        |
        v
      tables
```

The Query Object and PL/pgSQL are therefore not competing abstractions. A Query Object may be a client of a PostgreSQL function implemented in PL/pgSQL, just as it may query a table, view, materialized view, CTE, or aggregate directly.

## Proposed responsibility split

### Rails Query Object

Owns the application-facing read/query API:

- parameters and application-level intent;
- composition with Rails use cases;
- result mapping;
- authorization and tenant context where appropriate;
- a stable interface for callers even if the underlying database implementation changes.

### Plain SQL

Remain the default for set-based relational work when one query is sufficient:

- joins;
- filtering;
- CTEs;
- window functions;
- grouping;
- built-in aggregate functions;
- projections.

### VIEW

Candidate for reusable relational read models that should always reflect current source data and do not need persisted precomputation.

### MATERIALIZED VIEW

Candidate for expensive read models where precomputation and explicit refresh semantics are justified by measured workload.

### SQL function

Candidate for a reusable parameterized database query that remains naturally expressible as SQL without procedural control flow.

### PL/pgSQL function

Candidate only when the database-local operation genuinely benefits from procedural behavior such as branching, variables, multiple coordinated statements, exception handling, or strongly data-local atomic behavior.

### Trigger

Not a default extension of this proposal. Triggers should require separate justification because they introduce implicit behavior and can make application flow harder to reason about.

## Selection bias to preserve

Prefer the least powerful mechanism that cleanly solves the problem:

```text
constraint / index
        |
        v
plain SQL
        |
        v
VIEW
        |
        v
MATERIALIZED VIEW
        |
        v
SQL function
        |
        v
PL/pgSQL function
        |
        v
trigger
```

This is a decision heuristic, not a mandatory ladder. A later design review may change the ordering for specific workloads.

## Relationship to aggregates

PostgreSQL aggregate functions such as `SUM`, `COUNT`, `AVG`, `jsonb_agg`, and `array_agg` are SQL capabilities and do not imply PL/pgSQL. They may be used directly by Query Objects, views, materialized views, SQL functions, or PL/pgSQL functions.

Custom aggregates may use helper functions, and those helper functions may be implemented in PL/pgSQL, but that is an implementation choice rather than an intrinsic relationship.

## Why defer this now

The current repository work contains labs whose purpose is to discover architecture, not freeze it. Adopting the full structure now could make experimental findings look canonical before we have enough evidence.

For the current phase:

- keep experiments isolated;
- use the simplest implementation that answers the lab question;
- record evidence rather than promote mechanisms automatically;
- do not introduce PL/pgSQL merely because PostgreSQL supports it;
- avoid building a database abstraction layer before repeated query patterns justify one.

## Revisit criteria

Reopen this RFC when at least one of the following becomes true:

1. repeated complex Query Objects expose the same database-level read model;
2. measured query cost suggests persisted or precomputed read models;
3. round trips or concurrency make database-local atomic execution materially useful;
4. multiple application entry points need one stable database-side contract;
5. a production workload provides evidence that ActiveRecord/plain SQL alone is becoming an architectural liability;
6. schema versioning, testing, observability, and deployment conventions for database functions/views are ready to be made explicit.

## Questions for the later design review

- Which use cases should remain ActiveRecord or plain raw SQL?
- Which read models justify `VIEW` versus `MATERIALIZED VIEW`?
- When should a parameterized query become a SQL function?
- What exact bar must be met before PL/pgSQL is accepted?
- Should database functions be versioned through Rails migrations, dedicated SQL files, or another schema-management mechanism?
- Should the project use `structure.sql` once PostgreSQL-specific schema objects become first-class?
- How should database functions and materialized views be tested, observed, benchmarked, and rolled back?
- Which database-side interfaces are internal implementation details and which, if any, become stable contracts?

## Non-goals

This RFC does not:

- move current Rails business logic into PostgreSQL;
- require PL/pgSQL;
- require materialized views;
- make triggers a preferred pattern;
- replace Query Objects with database functions;
- establish a production architecture during the current test phase;
- claim that database-side execution is automatically faster than well-written set-based SQL.

## Future decision target

A later release may adopt the structure if evidence supports it:

```text
application use case
        |
        v
Query Object
        |
        v
stable DB interface
        |
        v
best-fit PostgreSQL primitive
        |
        v
canonical tables
```

The key principle is to preserve a clean application boundary while choosing the PostgreSQL primitive by evidence, not by novelty or availability.
