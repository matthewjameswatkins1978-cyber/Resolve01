# Local Service Boundary

R1-A1 introduces a typed in-process service seam in
`crates/resolve-store/src/service.rs` that wraps store operations into a
narrow API boundary.

## Purpose

This is an in-process typed service seam intended to support future external
transport. It is not an external-process boundary today. It reuses existing
store authority and provides typed, testable access to a selected operational
surface.

## Ownership

- Owner: Resolve (live coordination)
- Inputs: Typed domain values
- Outputs: Typed domain records
- Authority: Reuses existing store authority
- Failure semantics: All errors are typed and deterministic

## What this does NOT own

- Policy decisions (Tethers authority)
- Execution (Tethers authority)
- Historical memory (Lantern authority)
- Transport (future concern)

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
- `invalidate_guard` - Explicitly invalidate an unadmitted guard
- `load_guard` - Load a guard record by identifier

### Heartbeat operations
- `record_heartbeat` - Record a monotonic heartbeat for the current claim

### Outcome operations
- `record_outcome` - Record a Tethers outcome for the uniquely correlated action

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

## Future use

This service seam is designed to be wrapped by transport layers (HTTP, IPC)
in future R1 packets. The Tethers Guard Protocol endpoints can be implemented
by translating wire requests into service method calls.

The canonical Tethers wire vocabulary (`resolve.tethers-guard/1`) is not
duplicated here. It will be mapped explicitly when the actual transport slice
is implemented.
