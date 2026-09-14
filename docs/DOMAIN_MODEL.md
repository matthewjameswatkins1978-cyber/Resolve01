# Domain model

The R0 domain will contain only the concepts frozen by Issue #1:

- `GoalSpec` with append-only revisions;
- `Commitment` and its explicit state;
- `Prerequisite` values referring to commitments, Tethers verification, or a
  human decision;
- `ClaimLease` with a monotonically increasing epoch;
- `ExecutionGuard` as an opaque Resolve-issued admission proof;
- `AttentionItem` for asynchronous operational escalation; and
- closed, typed `WorkEvent` values.

S0 creates the `resolve-core` crate boundary but deliberately defines none of
these types yet. S1 is responsible for the domain types, typed errors, and
state-machine rules. Persistence is a separate `resolve-store` concern.
