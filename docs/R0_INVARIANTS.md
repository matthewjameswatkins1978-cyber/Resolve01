# R0 invariants

This document records the implementation boundary for Resolve R0. The frozen
architecture and normative invariant text are defined by GitHub Issue #1,
“Implement Resolve R0 — Fenced Commitment Blackboard”.

S0 does not implement or claim to prove these invariants. It establishes the
workspace and keeps the boundary visible while later sections add one
coherent capability at a time.

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

Later sections must preserve, among others, opaque exact scope-key equality,
Reserved/Held lock separation, stale-epoch fencing, restart lease
invalidation, and explicit propagation of `UNCERTAIN`.
