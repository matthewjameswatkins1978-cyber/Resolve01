# Resolve R0 verification matrix

This matrix maps each frozen R0 invariant to the implementation boundary and
an executable test. S6 closes the cross-boundary cases; it does not add a
runtime feature or a new authority.

| Invariant | Implementation evidence | Executable evidence | Status | Limitation |
| --- | --- | --- | --- | --- |
| I1 consequential flow ownership | `resolve-store::SqliteStore::issue_guard`, `admit_guard`, and `record_tethers_outcome` | `canonical_split_brain_survives_restart_and_duplicate_delivery`, `final_r0_scenario_composes_recovery_structure_action_and_downstream_identity` | PASS | The Tethers side is a simulated semantic boundary. |
| I2 one authority per concept | Private domain fields and store-owned snapshots/events; no raw connection export | `generic_persistence_cannot_rewrite_authority_or_terminal_state`, `sqlite_integrity_and_event_encoding_remain_explicit` | PASS | Lantern and Tethers are integration boundaries, not dependencies here. |
| I3 no orphan consequential action | Atomic guard admission binds action, commitment, guard, and Held locks | `newer_transactions_rollback_without_half_state`, `structural_and_outcome_transactions_rollback_without_half_state`, `uncertain_outcome_remains_fenced_through_restart_and_hostile_retries` | PASS | No production transport is included in R0. |
| I4 Tethers does not understand Resolve concepts | Typed action/scope outcome API contains no Goal, Commitment, or Claim protocol | `final_r0_scenario_composes_recovery_structure_action_and_downstream_identity`, `multi_scope_conflicts_are_atomic_and_scope_keys_are_opaque` | PASS | The adapter contract is semantic only; wire/authentication design is later. |
| I5 opaque exact scope locking | `ScopeKey`, `ScopeSet`, and exact canonical lock-row matching | `multi_scope_conflicts_are_atomic_and_scope_keys_are_opaque`, `canonical_split_brain_survives_restart_and_duplicate_delivery` | PASS | Resolve does not interpret path or hierarchy semantics. |
| I6 Reserved/Held two-tier lifecycle | `issue_guard`, `admit_guard`, reactive reservation invalidation, and outcome release | `claim_epoch_and_lease_boundaries_have_no_refresh_first_side_door`, `restart_reconstructs_each_meaningful_live_state_without_resurrecting_leases` | PASS | There is no background expiry loop. |
| I7 admitted action freezes ownership | Admitted/Uncertain guards retain Held locks independently of claim expiry | `uncertain_outcome_remains_fenced_through_restart_and_hostile_retries`, `canonical_split_brain_survives_restart_and_duplicate_delivery` | PASS | Outcome handling remains the only release path for Held locks. |
| I8 no blind reassignment | Worker, epoch, boot, heartbeat, and state-version checks fence mutators | `claim_epoch_and_lease_boundaries_have_no_refresh_first_side_door`, `generic_persistence_cannot_rewrite_authority_or_terminal_state` | PASS | Recovery is explicit and local; no worker-ranking policy exists. |
| I9 UNCERTAIN stays fenced | Typed `TethersOutcome::Uncertain`, `GuardState::Uncertain`, and `WaitingReason::UncertainAction` preserve action and locks | `uncertain_outcome_remains_fenced_through_restart_and_hostile_retries`, `restart_reconstructs_each_meaningful_live_state_without_resurrecting_leases` | PASS | Reconciliation is intentionally outside R0. |
| I10 safe outcomes release Held | Transactional safe-outcome path resolves the guard, clears action, and releases locks | `canonical_split_brain_survives_restart_and_duplicate_delivery`, `final_r0_scenario_composes_recovery_structure_action_and_downstream_identity` | PASS | Only authoritative Tethers outcome calls this path. |
| I11 restart invalidates Claims | File-backed startup advances boot generation, clears active claims, and preserves epoch high-water | `restart_reconstructs_each_meaningful_live_state_without_resurrecting_leases`, `malformed_persistence_corpus_fails_closed_without_silent_repair` | PASS | Monotonic timestamps are not treated as live across restart. |
| I12 structural planning is topology only | Closed `StructuralProposal` forms, typed acyclic graph validation, and bounded barrier cascade | `bounded_composite_cascade_completes_each_ancestor_once`, `final_r0_scenario_composes_recovery_structure_action_and_downstream_identity` | PASS | No plan selection, scheduler, or recursive policy engine exists. |
| I13 SQLite/WAL sole persistent operational truth | `SqliteStore`, schema migrations, immediate transactions, WAL, foreign keys, and validated restoration | `supported_schema_versions_migrate_to_v4_without_losing_data`, `sqlite_integrity_and_event_encoding_remain_explicit`, `two_connections_use_version_conflicts_and_single_scope_winner` | PASS | This theorem is for one local SQLite authority, not distributed storage. |

## Layer ownership

SQLite constraints provide relational integrity and uniqueness. The Resolve
store provides typed decoding, state-transition, claim/epoch, guard/lock,
topology, event, and cross-table invariants. The core domain provides the
closed value and state types. Tests that bypass SQLite with direct SQL are
test-only malformed-corpus probes; production code never treats that bypass as
an authority surface.
