# Execution guard

An `ExecutionGuard` is a Resolve-issued, opaque coordination token. It proves
only that Resolve currently recognises a worker as the fenced owner of a
commitment and its exact opaque scope keys. It is not a permission grant and
contains no Tethers policy.

The R0 lifecycle is:

```text
ISSUED -> ADMITTED(action_ref) -> terminal outcome
       \-> INVALIDATED
```

Issuance reserves exact scope keys. Admission promotes those reservations to
held locks atomically with the outstanding action reference. Held locks are
not released by worker lease expiry or guard-token expiry.

S3 implements the guard boundary in `resolve-core` and the synchronous SQLite
operations in `resolve-store`. A guard's validated `ScopeSet` must contain at
least one `ScopeKey`; an empty scope set cannot reach guard issuance. Tethers
must therefore provide at least one real opaque authoritative `ScopeKey` for
every guarded consequential action, including any future unrestricted action.
`ScopeKey` values are validated only for being non-empty, then sorted and
de-duplicated by exact equality. Resolve does not interpret their resource
meaning.

Issuance requires the commitment to be `Claimed` or `Working`, the requesting
worker and typed epoch to match the current claim, the claim to belong to the
store's current explicit `BootGeneration`, and the commitment to have no
outstanding action. Within the issuance transaction, Resolve reads the
persisted claim `last_heartbeat` and adds the trusted `MonotonicDuration`
claim-lease bound supplied by the caller. It does not accept an absolute
worker-supplied lease deadline. The reservation deadline is the earlier of
`now + guard_ttl` and that derived claim deadline; an already-expired claim and
arithmetic overflow fail closed. Scope availability and reservation rows are
established in one `BEGIN IMMEDIATE` transaction.

Admission requires the guard to be issued, unexpired, current-boot, and still
backed by every exact reserved scope row. It promotes all reservations to
`Held`, records the typed action reference on the commitment, and appends
`GuardAdmitted` atomically. Repeating the same admission is idempotent; a
different action or scope set is rejected. Explicit invalidation is available
for an unadmitted current-claim guard. Expired reservations are invalidated
reactively during relevant scope operations and append
`GuardReservationExpired`; no background sweeper exists.

`GuardState::Resolved` and `GuardState::Uncertain` are present as typed domain
states for the frozen R0 vocabulary, but outcome handling and recovery remain
S4 work. S3 does not execute actions, apply Tethers policy, release held locks
on outcomes, or implement restart recovery.
