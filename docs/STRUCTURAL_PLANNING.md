# Structural planning

R0 implements the frozen structural mutation boundary. It is a small,
claim-fenced mutation surface, not a planner or workflow engine.

## Pure proposal boundary

`crates/resolve-core/src/planning.rs` defines:

- `StructuralProposal::Decompose`, with validated `NewCommitment` children and
  one replacement-terminal child;
- `StructuralProposal::AddPrerequisite`, with one typed prerequisite;
- `StructuralProposal::Abandon`, with a non-empty rationale.

The module performs only proposal-local validation and deterministic cycle
checking over `Prerequisite::Commitment` edges. Tethers verification and human
decision prerequisites are typed but are not graph edges. The module has no
SQLite, clock, scheduler, executor, or provider dependency.

## Transactional application

`SqliteStore::apply_structural_proposal` opens one `BEGIN IMMEDIATE`
transaction. Before mutation it validates the target exists, is not terminal,
has a currently live persisted Claim, has the exact current worker claim and
epoch, belongs to the current boot generation, has no outstanding action, and
has no active guard fence. The live check derives
`last_heartbeat + claim_lease_duration` inside the transaction; a worker
deadline is never trusted and a stale Claim is never refreshed or transitioned
automatically. The transaction then rechecks the persisted graph and state
version while applying the proposal.

Decomposition is exactly `WORKING ->
WAITING(Prerequisite(replacement_terminal_id))`. It creates proposed direct
children with the target's goal and
parent, preserves the parent commitment ID, adds the replacement-terminal
prerequisite, and records `replacement_terminal_id`. The parent becomes
`WAITING(Prerequisite(child))`; the existing parent claim remains explicit.
An active composite parent cannot be decomposed again or receive another
prerequisite: ordinary mutation must not create topology that the barrier
cascade ignores. Add-prerequisite adds one normalized prerequisite and leaves
ordinary state unchanged only for non-composite targets.
Abandonment requires a rationale, enters the existing `ABANDONED` terminal
state, clears the claim, and leaves downstream prerequisites untouched.

Every accepted mutation appends typed structural evidence. Decomposition also
records child creation and parent waiting events. Any validation, encoding, or
SQLite failure rolls back children, normalized rows, state, claims, and events
together.

The persisted replacement-terminal field makes the parent a special composite
barrier. Ordinary `resume`, completion-proposal, or other generic persistence
cannot move a non-terminal composite parent to another operational state.
The only non-cascade exit is an explicit terminal decision: `Abandon` remains
available with a live Claim and rationale, and explicit cancellation remains
valid where the domain state machine permits it.

## Composite barrier

When a replacement terminal is persisted as `COMPLETED`, the store performs a
bounded deterministic cascade over parents whose validated
`replacement_terminal_id` names that child. A parent must be a direct child
relationship, have the same goal, have no outstanding action or held lock,
and be waiting on that exact child (or be recovery-pending after lease
expiry). Every queued child is re-read and must be persisted as `COMPLETED`
before it can satisfy a parent. The private cascade clears the parent claim
and records completion, then examines composite ancestors. Existing terminal
parents (`COMPLETED`, `CANCELLED`, or `ABANDONED`) are no-ops: explicit
terminalisation wins over later child completion and is never rewritten.
The cascade cannot create a public generic state transition.
`recover_without_action` refuses to release an unfinished composite barrier to
`READY`; a completed replacement terminal completes the parent directly.

No structural planning code schedules, executes, decomposes recursively by
policy, invokes Tethers, or adds service/background-loop behavior.
