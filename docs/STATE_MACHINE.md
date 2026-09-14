# Commitment state machine

The frozen R0 commitment states are:

```text
PROPOSED -> READY -> CLAIMED -> WORKING
                         |         |
                         |         +-> WAITING
                         |         +-> RECOVERY_PENDING
                         +-> COMPLETION_PROPOSED -> COMPLETED
```

`CANCELLED` and `ABANDONED` are explicit terminal exits. `COMPLETED`,
`CANCELLED`, and `ABANDONED` are terminal and immutable.

The full transition contract, including lease expiry, restart invalidation,
waiting reasons, and recovery, is frozen by Issue #1. S0 records the contract
only; no state-machine implementation exists until S1.
