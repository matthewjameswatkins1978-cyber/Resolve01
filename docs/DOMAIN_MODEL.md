# Domain model

S1 implements the core domain model in `crates/resolve-core`. It contains
only the concepts frozen by Issue #1; it does not persist or execute them.

## Identifiers and value types

The domain uses distinct newtypes for `GoalId`, `CommitmentId`, `WorkerId`,
`GuardId`, `TethersActionRef`, `TethersContractRef`, `HumanDecisionRef`,
`AcceptanceRef`, and `AttentionId`. Their `try_new` constructors reject empty
or whitespace-only values with `DomainError::InvalidIdentifier`.

`GoalRevision`, `ClaimEpoch`, `Timestamp`, `MonotonicInstant`,
`MonotonicDuration`, and `BootGeneration` are also distinct numeric value
types. Claim epochs are
created by `Commitment::claim_for`, begin at one, and advance monotonically
for that commitment. The current claim is never replaced by reusing an old
epoch.

## Goals and commitments

`GoalSpec` contains `goal_id`, `revision`, `description`, `state`, and
`created_at`. `GoalSpec::revised` returns a new value only for the exact next
revision, preserving the prior value for append-only callers. These fields
are private and exposed through read-only accessors; there are no generic
setters.

`Commitment` contains its goal and optional parent references, description,
typed prerequisites, typed acceptance references, optional `ClaimLease`, and
an optional `TethersActionRef`. Its state is the closed `CommitmentState`
enum. Claimant mutations require both the current `WorkerId` and
`ClaimEpoch`; stale or foreign claims return typed errors.
All commitment fields are private. Callers receive IDs and parent references,
descriptions, prerequisite and acceptance slices, state, claim, and
outstanding-action references through read-only accessors. The outstanding
action is intentionally read-only; S4 changes it only through the controlled
guard/outcome lifecycle at the store boundary.
`CommitmentState::requires_active_claim()` is the shared domain rule for claim
ownership: `CLAIMED`, `WORKING`, ordinary `WAITING`, and
`COMPLETION_PROPOSED` require a live claim, while
`WAITING(UncertainAction(...))` is deliberately claimless.

`Prerequisite` has exactly the three R0 forms: commitment, Tethers
verification, and human decision. `WaitingReason` keeps prerequisite,
authority, human-decision, verification, uncertain-action, and external
waiting semantically distinct. `TethersOutcome` likewise has distinct
`Succeeded`, `Failed`, and `Uncertain` variants. The S4 store boundary records
outcomes by their typed action reference; `Uncertain` remains fenced and is
never collapsed into `Failed`.

`AttentionItem` models asynchronous operational escalation, and `WorkEvent`
is a closed enum covering the R0 event vocabulary, including recovery and
heartbeat/outcome events. `AttentionItem::new` creates an open item after
validating its description; its state and description are read-only, with no
attention workflow implemented.

## Execution guards

`ScopeKey` is an opaque, validated newtype. Resolve canonicalizes a requested
scope set by deterministic ordering and exact de-duplication; it does not
interpret resource names or path relationships. `ExecutionGuard` has private
identity, claim, scope, boot-generation, lifecycle, and reservation fields and
is constructed only as an issued guard. Its typed `GuardState` includes
issued, admitted, resolved, uncertain, and invalidated forms. `MonotonicDuration` and
`MonotonicInstant` make reservation TTL and claim deadlines explicit.

## Structural proposals

S5 adds the closed `StructuralProposal` enum in `resolve-core`. It contains
only `Decompose`, `AddPrerequisite`, and `Abandon`, each carrying a typed
commitment target and claim epoch. `NewCommitment` is a validated child value;
it exposes only its ID, description, typed prerequisites, and acceptance
references. The pure planning boundary checks proposal shape and deterministic
typed commitment-graph acyclicity. It does not choose a plan, schedule work,
or call SQLite.

The store applies a proposal only inside one immediate transaction after
rechecking the current worker, claim epoch, boot generation, state, and guard
fence. Decomposition creates direct same-goal proposed children, keeps the
parent identity, records a replacement-terminal child, and makes the parent
wait on that typed prerequisite. Adding a prerequisite does not automatically
change state. Abandonment requires a non-empty rationale and clears the active
claim through the normal terminal operation.
