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

Schema version `4` is recorded in `metadata`. S4 migrates schema version 2 to
3, and S5 migrates schema version 3 to 4, transactionally without recreating
or discarding earlier data. The store owns
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

Schema v4 adds nullable `commitments.replacement_terminal_id`, a validated
store-owned link to a direct same-goal child used only for composite-barrier
completion. Commitment prerequisites remain normalized typed rows; only
`Prerequisite::Commitment` contributes an edge to the deterministic cycle
check. No graph database or planning dependency is introduced.

`metadata.boot_generation` is explicit store context. Claims persist the boot
generation under which they were recorded; guard issuance and admission reject
claims or guards from another generation. Commitments also persist the
monotonic claim-epoch high-water mark. The v2-to-v3 migration backfills that
mark from every well-formed historical `CommitmentClaimed` event and current
claim, and fails closed on malformed history. Non-null action references are
unique across guards.

Goals and commitments carry store-owned `state_version` values. Goal revision
rows are append-only. Prerequisites and acceptance references are normalized
rows, while commitment state is stored as a private tagged JSON projection so
waiting-reason variants retain their type.

`execution_guards` stores the typed lifecycle projection and reservation
deadline. `execution_guard_scopes` stores the canonical, non-empty exact scope
set. Guard issuance requires Tethers to provide at least one opaque
authoritative scope key; Resolve does not invent a dummy or global key.
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

Guard issuance, reactive reservation expiry, admission, heartbeat, claim
expiry, restart recovery, and outcome handling use one `BEGIN IMMEDIATE`
transaction. Issuance treats the persisted claim
`last_heartbeat` as authoritative, adds only the trusted
`claim_lease_duration`, and derives the claim deadline inside that transaction;
an absolute worker-supplied deadline is not part of the API. Admission promotes
reservations and records the commitment's outstanding action in the same
transaction as `GuardAdmitted`, and only a `WORKING` commitment can be
admitted. Heartbeats advance persisted monotonic time only forwards. Expiry
moves every claim-bearing commitment state (`CLAIMED`, `WORKING`, ordinary
`WAITING`, or `COMPLETION_PROPOSED`) to `RECOVERY_PENDING` and invalidates
issued reservations. The persisted heartbeat is authoritative for each
heartbeat lease calculation; a heartbeat at or after its derived deadline is
rejected and cannot resurrect the claim. Held locks are not released by worker
lease expiry. Safe outcomes release held locks atomically; `UNCERTAIN`
deliberately retains the action and locks.

Structural proposals use their own immediate transaction. The store rechecks
the exact current worker claim, epoch, boot generation, state version, and
guard safety before applying one of the closed proposal forms. Decomposition
inserts proposed direct children, appends typed child/parent/audit events, and
records the replacement-terminal link atomically. Add-prerequisite appends one
normalized row without an automatic state change. Abandonment records the
normal terminal mutation and rationale evidence. Completion of a replacement
terminal is followed by a bounded event-reactive barrier cascade; no scheduler
or background loop is involved.

The domain restoration and startup paths use one additional invariant: when an
active claim exists, its epoch must equal the commitment's durable
`last_claim_epoch` exactly. On restart, all claim-bearing states must have a
claim before recovery processing, and no active claim may remain after old
claims have been moved to `RECOVERY_PENDING`; inconsistent rows fail closed.

## Read projections and restoration boundary

Read methods return immutable store-side records. `load_commitment` is the
narrow validated restoration boundary for domain operations; it does not expose
unchecked or mutable persisted fields. Persisted claim epochs and monotonic
heartbeat ticks remain validated projection values, and old monotonic ticks are
never treated as a live lease after restart. File-backed `open` performs schema
migration and startup recovery before returning a usable store. The in-memory
test constructor intentionally does not auto-recover.

No unchecked constructors, generic setters, or unsafe reconstruction were
added to the core domain for S2 convenience.
