# Domain model

S1 implements the core domain model in `crates/resolve-core`. It contains
only the concepts frozen by Issue #1; it does not persist or execute them.

## Identifiers and value types

The domain uses distinct newtypes for `GoalId`, `CommitmentId`, `WorkerId`,
`GuardId`, `TethersActionRef`, `TethersContractRef`, `HumanDecisionRef`,
`AcceptanceRef`, and `AttentionId`. Their `try_new` constructors reject empty
or whitespace-only values with `DomainError::InvalidIdentifier`.

`GoalRevision`, `ClaimEpoch`, `Timestamp`, `MonotonicInstant`, and
`BootGeneration` are also distinct numeric value types. Claim epochs are
created by `Commitment::claim_for`, begin at one, and advance monotonically
for that commitment. The current claim is never replaced by reusing an old
epoch.

## Goals and commitments

`GoalSpec` contains `goal_id`, `revision`, `description`, `state`, and
`created_at`. `GoalSpec::revised` returns a new value only for the exact next
revision, preserving the prior value for append-only callers.

`Commitment` contains its goal and optional parent references, description,
typed prerequisites, typed acceptance references, optional `ClaimLease`, and
an optional `TethersActionRef`. Its state is the closed `CommitmentState`
enum. Claimant mutations require both the current `WorkerId` and
`ClaimEpoch`; stale or foreign claims return typed errors.

`Prerequisite` has exactly the three R0 forms: commitment, Tethers
verification, and human decision. `WaitingReason` keeps prerequisite,
authority, human-decision, verification, uncertain-action, and external
waiting semantically distinct. `TethersOutcome` likewise has distinct
`Succeeded`, `Failed`, and `Uncertain` variants; S1 provides no outcome
recording or reconciliation behavior.

`AttentionItem` models asynchronous operational escalation, and `WorkEvent`
is a closed enum covering the R0 event vocabulary. S1 defines these values but
does not yet append events to storage.
