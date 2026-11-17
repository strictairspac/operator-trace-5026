# Schema v1
SQLAlchemy metadata creates users, audits, datasets, plans, traces. Natural names are unique; foreign keys reference parent entities. Each trace stores immutable JSON input snapshots. PostgreSQL persists metadata and teaching datasets; the bounded Python operators execute the plan.

## Additive change: traces.mode
`traces.mode varchar(10) not null default 'memory'` records the execution engine. `memory` is the original fully in-memory engine; `spill` runs the external engine under `SPILL_ROOT` (default `/artifacts/spill`). The column is additive: existing volumes receive it through `ensure_schema()` at backend/worker startup, and rows created before the change behave exactly as `memory`.
