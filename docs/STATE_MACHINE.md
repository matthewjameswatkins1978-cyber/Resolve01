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

The full transition contract, including lease expiry, restart invalidation,
waiting reasons, and recovery, is frozen by Issue #1. S1 establishes the
commitment transitions; S2 adds persistence; S3 adds fencing and records an
outstanding action only through the atomic guard-admission boundary. Lease
expiry, restart recovery, outcome handling, structural planning, and service
layers remain unimplemented.
