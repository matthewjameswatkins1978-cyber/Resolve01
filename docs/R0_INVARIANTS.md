# R0 invariants

This document records the implementation boundary for Resolve R0. The frozen
architecture and normative invariant text are defined by GitHub Issue #1,
“Implement Resolve R0 — Fenced Commitment Blackboard”.

R0 is implemented in the small `resolve-core` and `resolve-store` crates and
is closed by deterministic, adversarial, restart, migration, rollback, and
concurrency tests. This document records the boundary; the concrete evidence
matrix is in `docs/R0_VERIFICATION_MATRIX.md`.

The constitutional ownership is:

| Concern | Authoritative owner |
| --- | --- |
| Historical truth and provenance | Lantern Keeper |
| Goals, commitments, claims, leases, and live waiting state | Resolve |
| Authority, capability execution, verification, outcomes, and execution trail | Tethers |

The central boundary is directional: Resolve coordinates proposed work;
Tethers governs and executes consequential actions; Lantern remembers
meaningful completed history. Resolve must not become a workflow engine,
scheduler, executor, policy engine, agent runtime, or memory system.

The implementation preserves opaque exact scope-key equality, Reserved/Held
lock separation, stale-epoch fencing, restart lease invalidation, explicit
propagation of `UNCERTAIN`, and topology-only structural mutation. R0 remains
a single local SQLite authority and does not provide transport, scheduling,
distributed consensus, or production Tethers integration.
