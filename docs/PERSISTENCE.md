# Persistence

S2 and S3 implement the synchronous SQLite persistence boundary in
`crates/resolve-store`. Resolve owns the operational snapshot and event
records; this crate does not own Lantern history or Tethers execution state.

## SQLite configuration

File-backed stores enable `journal_mode = WAL`, `foreign_keys = ON`,
`synchronous = FULL`, and a bounded busy timeout. WAL is required for a
file-backed store and is checked when the store opens. SQLite in-memory stores
are available only through `open_in_memory_for_tests`; SQLite does not support
WAL for that connection mode.

`synchronous = FULL` is a conservative durability setting, but this section
makes no claim that it protects against every filesystem or hardware failure.
The implementation is synchronous and uses one connection per store; no pool
or async database framework is present.

The direct `rusqlite` dependency is the only SQLite boundary and uses its
bundled SQLite build for a consistent local and CI engine. `serde` and
`serde_json` are private implementation dependencies for typed state/event
projections; the core domain is not made globally serializable. The
`tempfile` dependency is dev-only and is used solely to create disposable
file-backed databases for WAL tests.

## Schema ownership

Schema version `2` is recorded in `metadata`. S3 migrates schema version 1 to
2 transactionally without recreating or discarding S2 data. The store owns
these tables:

```text
metadata
goals
goal_revisions
commitments
commitment_prerequisites
commitment_acceptance_refs
claims
attention_items
work_events
execution_guards
execution_guard_scopes
scope_locks
```

`metadata.boot_generation` is explicit store context. Claims persist the boot
generation under which they were recorded; guard issuance and admission reject
claims or guards from another generation. Restart invalidation itself remains
S4 work.

Goals and commitments carry store-owned `state_version` values. Goal revision
rows are append-only. Prerequisites and acceptance references are normalized
rows, while commitment state is stored as a private tagged JSON projection so
waiting-reason variants retain their type.

`execution_guards` stores the typed lifecycle projection and reservation
deadline. `execution_guard_scopes` stores the canonical exact scope set.
`scope_locks` has one row per exact key and distinguishes short-lived
`reserved` locks from indefinite `held` locks with SQL checks. A migration
failure rolls back its DDL and metadata version update. Reopening schema v2 is
idempotent; newer schema versions fail closed.

## Atomic mutation boundary

Each accepted goal, commitment, or attention insertion/change opens an
explicit SQLite `BEGIN IMMEDIATE` transaction. The mutation validates its
expected current `state_version`, appends one stable event type plus typed
JSON payload to `work_events`, updates the materialized rows, and commits.
Any SQLite or encoding error rolls the complete transaction back. The public
API does not expose the raw connection or a generic event-bus/repository
abstraction.

Existing aggregate changes require the caller's expected `state_version`; a
stale value returns `StoreError::VersionConflict` and never appends an event.
The store never silently performs last-writer-wins updates and does not use a
claim epoch as a substitute for the independent snapshot version.

Guard issuance, reactive reservation expiry, and admission use one
`BEGIN IMMEDIATE` transaction. Admission promotes reservations and records the
commitment's outstanding action in the same transaction as `GuardAdmitted`.
Held locks are not released by worker lease expiry. S3 contains no outcome,
recovery, restart, or background-sweeper behavior.

## Read projections and restoration boundary

Read methods return immutable store-side records. They are projections, not
rehydrated mutable `GoalSpec`, `Commitment`, or `ClaimLease` values. In
particular, persisted claim epochs and monotonic heartbeat ticks are exposed
as validated raw numeric projection values. Those monotonic ticks are not
valid lease time after process restart; S4 owns restart invalidation and
recovery semantics.

No unchecked constructors, generic setters, or unsafe reconstruction were
added to the core domain for S2 convenience.
