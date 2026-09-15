# Structural planning

S5 implements the frozen R0 structural mutation boundary. It is a small,
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
is not in recovery/uncertainty, has the exact current worker claim and epoch,
belongs to the current boot generation, has no outstanding action, and has no
active guard fence. The transaction then rechecks the persisted graph and
state version while applying the proposal.

Decomposition creates proposed direct children with the target's goal and
parent, preserves the parent commitment ID, adds the replacement-terminal
prerequisite, and records `replacement_terminal_id`. The parent becomes
`WAITING(Prerequisite(child))`; the existing parent claim remains explicit.
Add-prerequisite adds one normalized prerequisite and leaves state unchanged.
Abandonment requires a rationale, enters the existing `ABANDONED` terminal
state, clears the claim, and leaves downstream prerequisites untouched.

Every accepted mutation appends typed structural evidence. Decomposition also
records child creation and parent waiting events. Any validation, encoding, or
SQLite failure rolls back children, normalized rows, state, claims, and events
together.

## Composite barrier

When a replacement terminal is persisted as `COMPLETED`, the store performs a
bounded deterministic cascade over parents whose validated
`replacement_terminal_id` names that child. A parent must be a direct child
relationship, have the same goal, have no outstanding action or held lock,
and be waiting on that exact child (or be recovery-pending after lease
expiry). The private cascade clears the parent claim and records completion,
then examines composite ancestors. It cannot create a public generic state
transition. `recover_without_action` refuses to release an unfinished
composite barrier to `READY`.

No structural planning code schedules, executes, decomposes recursively by
policy, invokes Tethers, or adds service/background-loop behavior.
