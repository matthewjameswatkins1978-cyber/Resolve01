# Resolve01 Agent Map

This file is deliberately small. It routes an agent to the semantic authority needed for the current task without preloading the whole repository.

## AI-first operating rules

1. Make consequential truth explicit.
2. Keep one semantic authority.
3. Keep reasoning bounded and dependency-complete.
4. Prefer simple explicit structure over clever indirection.
5. Make knowledge discoverable, reusable, and load it on demand.
6. Put critical constraints below the model.
7. Verify the actual requirement independently.
8. Let evidence rewrite the method.

Start with the smallest dependency-complete working set. Expand only when a necessary semantic, interface, implementation, or verification dependency is missing.

## Project identity

Resolve is the live commitment plane between Lantern Keeper and Tethers.

> Lantern knows. Resolve coordinates. Tethers controls.

Resolve is a durable, fenced blackboard for live commitments. It is **not** a workflow engine, scheduler, executor, policy engine, agent runtime, or long-term memory system.

## Permanent ownership boundary

- Lantern Keeper owns historical truth and provenance.
- Resolve owns goals, commitments, claims, leases, and live waiting state.
- Tethers owns authority, capability execution, verification, outcomes, and execution trail.

Resolve coordinates proposed work. Tethers governs and executes consequential actions. Lantern remembers meaningful completed history.

## Authority map

Load only what the current task needs:

- `README.md` — current project status and workspace shape.
- `docs/DOMAIN_MODEL.md` — domain entities and relationships.
- `docs/STATE_MACHINE.md` — lifecycle semantics and allowed transitions.
- `docs/R0_INVARIANTS.md` — frozen R0 ownership/invariant boundary.
- `docs/PERSISTENCE.md` — SQLite/WAL persistence semantics.
- `docs/EXECUTION_GUARD.md` — execution-guard semantics.
- `docs/TETHERS_EXECUTION_GUARD_CONTRACT.md` — Resolve/Tethers guard contract.
- `docs/LANTERN_BOUNDARY.md` — Resolve/Lantern boundary.
- current issue/task packet — current scope and acceptance.
- code/tests/Git — implementation evidence.

A filename is a route, not an import. Do not read all architecture documents by default.

## Consequential invariants

These are important enough to remain in always-loaded context:

- Resolve must not absorb Lantern or Tethers responsibilities.
- Exact scope-key equality remains opaque; do not infer broader equivalence.
- Reserved and Held lock semantics remain distinct.
- Stale epochs are fenced.
- Restart invalidates leases according to the canonical persistence/lifecycle contract.
- `UNCERTAIN` must propagate explicitly rather than being silently converted into success or failure.
- A new section must preserve already accepted earlier-section semantics unless an explicit architecture change authorises otherwise.

For exact definitions, retrieve the relevant authority above rather than expanding this file.

## Work protocol

Before mutation:

1. Inspect branch, exact `HEAD`, and `git status`.
2. Identify the current bounded task and acceptance conditions.
3. Load the semantic authorities and code/tests directly relevant to that task.
4. Preserve unrelated and user-authored work.

For substantial work, reason from:

```text
GOAL
SCOPE
RELEVANT SEMANTICS
MUST REMAIN TRUE
ACCEPTANCE CONDITIONS
```

Do not widen a bounded task into unrelated refactors, speculative cleanup, dependency replacement, or architecture redesign. If required product semantics are contradictory or missing, surface the decision rather than inventing it.

Do not pay twice for discovery. Consult existing durable truth before broad exploration. If exploration establishes a reusable architectural fact, place the distilled fact in the appropriate authority/evidence surface instead of preserving exploratory noise.

## Verification

Normal workspace gates are:

```text
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Add focused behavioural/invariant evidence when the task requires more than those gates prove. A green inherited suite is not automatically proof of the requested behaviour.

Return concise evidence:

```text
WHAT CHANGED
WHAT WAS VERIFIED
WHAT REMAINS UNVERIFIED
SEMANTIC ASSUMPTIONS
BLOCKERS
DURABLE DISCOVERIES (only if any)
```

"Done" is not evidence.
