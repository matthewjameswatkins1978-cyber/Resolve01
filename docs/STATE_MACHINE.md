# Commitment state machine

S1 implements the frozen R0 commitment-state boundary in
`crates/resolve-core/src/state_machine.rs` and the claimant operations on
`Commitment`.

The states are:

```text
PROPOSED -> READY -> CLAIMED -> WORKING
                         |         |
                         |         +-> WAITING
                         |         +-> RECOVERY_PENDING
                         +-> COMPLETION_PROPOSED -> COMPLETED
```

`CANCELLED` and `ABANDONED` are explicit terminal exits. `COMPLETED`,
`CANCELLED`, and `ABANDONED` are terminal and immutable: every attempted
transition out of one returns `DomainError::TerminalCommitmentImmutable`.

`validate_transition` checks the legal R0 topology and returns
`DomainError::InvalidTransition` for other non-terminal edges. The
`Commitment` API exposes `activate`, `claim_for`, `start`, `wait`, `resume`,
`propose_completion`, `complete`, `cancel`, and `abandon`. Worker-owned
operations validate the current worker and claim epoch before changing state.

`resume` explicitly chooses `Ready` (releasing the current claim) or `Working`
(retaining it). `claim_for` creates the next typed epoch rather than accepting
an epoch supplied by a worker.

S4 adds the recovery boundary without adding new planning or execution states.
An expired claim or a restart moves active `CLAIMED`/`WORKING` work to
`RECOVERY_PENDING` and clears its active claim. A commitment with no outstanding
action may be explicitly released from recovery to `READY`, recording
`RecoveryReleased`; the next claim uses the persisted epoch high-water mark.
An admitted action may remain outstanding in `RECOVERY_PENDING` until its
authoritative Tethers outcome is recorded.

`WAITING` for any ordinary reason and `COMPLETION_PROPOSED` also retain their
active claim. Heartbeats are valid in every claim-bearing state, and lease
expiry or restart moves them to `RECOVERY_PENDING` while preserving the epoch
high-water mark. `WAITING(UncertainAction(...))` is the exception: it has no
active claim and cannot transition to `RECOVERY_PENDING` through this path.

Guard issuance may reserve scope while a commitment is `CLAIMED`, preserving
the pre-admission fence. Consequential Tethers admission requires the
commitment to have explicitly entered `WORKING`; admission does not perform the
`CLAIMED -> WORKING` transition.

`UNCERTAIN` is represented as
`WAITING(UncertainAction(action_ref))`, retains the outstanding action and held
scope locks, and cannot be resumed through the ordinary `resume` API. A safe
Tethers outcome clears the action and held locks atomically; when recovery was
pending it also returns the commitment to `READY`, otherwise the commitment
remains `WORKING`. Contradictory or repeated outcomes are handled by the store's
typed outcome boundary.
