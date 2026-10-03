# Resolve Service and Tethers Transport Boundaries

R1-A1 introduced the typed in-process service seam in
`crates/resolve-store/src/service.rs`. R1-A2 adds a separate authenticated HTTPS
adapter in `crates/resolve-transport` for the two operations in the canonical
`docs/TETHERS_GUARD_PROTOCOL.md`.

## Purpose

The service remains in-process and reuses the existing store authority. The
transport translates only protocol requests into typed service calls; it does
not own policy, provider execution, retries, scheduling, or outcome
reconciliation. The transport router exposes only guard admission and outcome
delivery and is served through the explicit Rustls HTTPS entry point.

## Ownership

- Owner: Resolve (live coordination)
- Inputs: Typed domain values
- Outputs: Typed domain records
- Authority: Reuses existing store authority
- Failure semantics: Service errors are typed; the transport maps closed guard
  admission reasons into the canonical protocol vocabulary

## What this does NOT own

- Policy decisions (Tethers authority)
- Execution (Tethers authority)
- Historical memory (Lantern authority)

## Selected operational surface

The service exposes the following store operations through typed methods:

### Goal operations
- `create_goal` - Create a new goal with initial revision
- `revise_goal` - Revise an existing goal with append-only semantics
- `load_goal` - Load a goal record by identifier

### Commitment operations
- `create_commitment` - Create a new commitment in Proposed state
- `change_commitment` - Persist a commitment state change with optimistic concurrency
- `load_commitment` - Load a commitment through the validated restoration boundary
- `load_commitment_record` - Load a commitment record for inspection

### Guard operations
- `issue_guard` - Issue a guard and reserve scope keys atomically
- `admit_guard` - Admit a guard using the exact scope set from the Tethers boundary
- `admit_guard_with_preparation` - Atomically admit a guard and persist its typed
  Tethers preparation digest for subsequent outcome correlation
- `invalidate_guard` - Explicitly invalidate an unadmitted guard
- `load_guard` - Load a guard record by identifier

### Heartbeat operations
- `record_heartbeat` - Record a monotonic heartbeat for the current claim

### Outcome operations
- `record_outcome` - Record a Tethers outcome for the uniquely correlated action
- `record_outcome_with_preparation` - Record an outcome only when its preparation
  digest matches the value pinned at admission

### Recovery operations
- `process_expired_claims` - Process expired claims and move them to recovery pending
- `recover_without_action` - Release a recovery-pending commitment with no outstanding action

### Structural proposal operations
- `apply_structural_proposal` - Apply one closed, claim-fenced structural proposal atomically

### Attention operations
- `raise_attention` - Insert a new attention item
- `clear_attention` - Clear an open attention item
- `load_attention` - Load an attention record by identifier

### Event operations
- `list_events` - List all events in order

### Store metadata
- `schema_version` - Get the current schema version
- `boot_generation` - Get the current boot generation
- `journal_mode` - Check if WAL journal mode is enabled
- `foreign_keys_enabled` - Check if foreign keys are enabled

## HTTPS bridge

The adapter accepts only `POST /internal/tethers/v1/guard/admit` and
`POST /internal/tethers/v1/outcome`, with an explicit `X-Resolve-Tethers-Key`,
`application/json`, strict duplicate-key rejection, closed object shapes, and a
16 KiB request cap. It uses the existing canonical JSON/SHA-256 convention to
bind responses to complete requests. A configured host supplies both Resolve
clock readings and TLS certificate/key files. No plaintext production serve
helper is provided.

Schema v5 stores the preparation digest on each newly admitted guard. A v4 to
v5 migration leaves existing rows unbound; outcome delivery through the new
transport fails closed for those rows because no preparation identity can be
reconstructed safely.

The canonical Tethers wire vocabulary remains defined only in
`docs/TETHERS_GUARD_PROTOCOL.md`.
