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

S3 and S4 implement the guard boundary in `resolve-core` and the synchronous SQLite
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
backed by every exact reserved scope row. Resolve may reserve scope while the
commitment is `CLAIMED`, but consequential Tethers admission requires the
commitment to have explicitly entered `WORKING`; admission does not start it.
If the worker claim later expires or the process restarts, an admitted guard is
reconstructed only with its commitment in `RECOVERY_PENDING`; its held locks
remain fenced for the outcome path.
It promotes all reservations to `Held`, records the typed action reference on
the commitment, and appends `GuardAdmitted` atomically. Repeating the same
admission is idempotent; a different action or scope set is rejected. Explicit
invalidation is available for an unadmitted current-claim guard. Expired
reservations are invalidated reactively during relevant scope operations and
append `GuardReservationExpired`; no background sweeper exists.

`GuardState::Resolved` and `GuardState::Uncertain` are typed domain states.
S4 records Tethers outcomes transactionally: safe outcomes resolve the guard,
clear the outstanding action, and release its held locks; `UNCERTAIN` retains
the action and held locks and moves the commitment to
`WAITING(UncertainAction(action_ref))`. A normal resume cannot bypass that
recovery requirement.

On file-backed startup, Resolve advances the boot generation in one immediate
transaction, invalidates active claims and issued reservations, and rebuilds
held locks from consistent admitted/uncertain guards. A restart never releases
an admitted held lock merely because its worker claim ended. Expiry processing
has the same distinction: issued reservations are invalidated, while admitted
actions remain fenced for outcome handling. The in-memory constructor remains a
test-only deterministic constructor and does not perform automatic startup
recovery.
