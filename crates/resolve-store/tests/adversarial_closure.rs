use resolve_core::{
    AcceptanceRef, BootGeneration, ClaimEpoch, ClaimLease, Commitment, CommitmentId,
    CommitmentState, GoalId, GoalRevision, GoalSpec, GoalState, GuardId, HumanDecisionRef,
    MonotonicDuration, MonotonicInstant, NewCommitment, Prerequisite, ScopeKey, ScopeSet,
    StructuralProposal, TethersActionRef, TethersContractRef, TethersOutcome, Timestamp,
    WaitingReason, WorkEvent, WorkerId,
};
use resolve_store::{
    GuardAdmission, GuardIssueRequest, HeartbeatResult, OutcomeRecording, SqliteStore, StoreError,
};
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::path::Path;
use tempfile::tempdir;

struct ManualClock {
    now: MonotonicInstant,
}

impl ManualClock {
    fn new(ticks: u64) -> Self {
        Self {
            now: MonotonicInstant::from_ticks(ticks),
        }
    }

    fn advance_to(&mut self, ticks: u64) -> MonotonicInstant {
        self.now = MonotonicInstant::from_ticks(ticks);
        self.now
    }
}

fn goal_id(value: &str) -> GoalId {
    GoalId::try_new(value).expect("valid test goal")
}

fn commitment_id(value: &str) -> CommitmentId {
    CommitmentId::try_new(value).expect("valid test commitment")
}

fn worker_id(value: &str) -> WorkerId {
    WorkerId::try_new(value).expect("valid test worker")
}

fn guard_id(value: &str) -> GuardId {
    GuardId::try_new(value).expect("valid test guard")
}

fn action(value: &str) -> TethersActionRef {
    TethersActionRef::try_new(value).expect("valid test action")
}

fn scope(value: &str) -> ScopeKey {
    ScopeKey::try_new(value).expect("valid test scope")
}

fn scopes(values: &[&str]) -> ScopeSet {
    ScopeSet::try_new(values.iter().map(|value| scope(value)).collect())
        .expect("non-empty test scopes")
}

fn duration(ticks: u64) -> MonotonicDuration {
    MonotonicDuration::try_from_ticks(ticks).expect("positive test duration")
}

fn create_goal(store: &mut SqliteStore, id: &str) -> GoalSpec {
    let goal = GoalSpec::new(
        goal_id(id),
        GoalRevision::initial(),
        "coordinate bounded work",
        GoalState::Active,
        Timestamp::from_unix_seconds(0),
    )
    .expect("valid test goal");
    store
        .insert_goal(
            &goal,
            &WorkEvent::GoalCreated {
                goal_id: goal.goal_id().clone(),
                revision: goal.revision(),
            },
        )
        .expect("goal persists");
    goal
}

fn create_commitment(
    store: &mut SqliteStore,
    goal: &GoalSpec,
    id: &str,
    parent_id: Option<CommitmentId>,
    prerequisites: Vec<Prerequisite>,
) -> Commitment {
    let commitment = Commitment::new(
        commitment_id(id),
        goal.goal_id().clone(),
        parent_id,
        "perform bounded commitment",
        prerequisites,
        vec![AcceptanceRef::try_new("acceptance-1").expect("valid acceptance")],
    )
    .expect("valid test commitment");
    store
        .insert_commitment(
            &commitment,
            &WorkEvent::CommitmentCreated {
                commitment_id: commitment.commitment_id().clone(),
                goal_id: goal.goal_id().clone(),
            },
            1,
        )
        .expect("commitment persists");
    commitment
}

fn persist(store: &mut SqliteStore, commitment: &Commitment, event: WorkEvent, version: i64) {
    store
        .persist_commitment_change(commitment, version, &event, 2)
        .expect("commitment change persists");
}

fn claimed(
    store: &mut SqliteStore,
    goal: &GoalSpec,
    id: &str,
    worker: &str,
    heartbeat: u64,
) -> (Commitment, ClaimLease) {
    let mut commitment = create_commitment(store, goal, id, None, Vec::new());
    commitment.activate().expect("activation succeeds");
    persist(
        store,
        &commitment,
        WorkEvent::CommitmentActivated {
            commitment_id: commitment.commitment_id().clone(),
        },
        1,
    );
    let claim = commitment
        .claim_for(worker_id(worker), MonotonicInstant::from_ticks(heartbeat))
        .expect("claim succeeds");
    persist(
        store,
        &commitment,
        WorkEvent::CommitmentClaimed {
            commitment_id: commitment.commitment_id().clone(),
            worker_id: claim.worker_id().clone(),
            epoch: claim.epoch(),
        },
        2,
    );
    (commitment, claim)
}

fn working(
    store: &mut SqliteStore,
    goal: &GoalSpec,
    id: &str,
    worker: &str,
    heartbeat: u64,
) -> (Commitment, ClaimLease) {
    let (mut commitment, claim) = claimed(store, goal, id, worker, heartbeat);
    commitment
        .start(claim.worker_id(), claim.epoch())
        .expect("start succeeds");
    persist(
        store,
        &commitment,
        WorkEvent::CommitmentStarted {
            commitment_id: commitment.commitment_id().clone(),
        },
        3,
    );
    (commitment, claim)
}

fn working_from_proposed(
    store: &mut SqliteStore,
    mut commitment: Commitment,
    worker: &str,
    heartbeat: u64,
) -> (Commitment, ClaimLease) {
    commitment.activate().expect("proposed child activates");
    store
        .persist_commitment_change(
            &commitment,
            1,
            &WorkEvent::CommitmentActivated {
                commitment_id: commitment.commitment_id().clone(),
            },
            heartbeat as i64,
        )
        .expect("child activation persists");
    let claim = commitment
        .claim_for(worker_id(worker), MonotonicInstant::from_ticks(heartbeat))
        .expect("child claims");
    store
        .persist_commitment_change(
            &commitment,
            2,
            &WorkEvent::CommitmentClaimed {
                commitment_id: commitment.commitment_id().clone(),
                worker_id: claim.worker_id().clone(),
                epoch: claim.epoch(),
            },
            heartbeat as i64 + 1,
        )
        .expect("child claim persists");
    commitment
        .start(claim.worker_id(), claim.epoch())
        .expect("child starts");
    store
        .persist_commitment_change(
            &commitment,
            3,
            &WorkEvent::CommitmentStarted {
                commitment_id: commitment.commitment_id().clone(),
            },
            heartbeat as i64 + 2,
        )
        .expect("child start persists");
    (commitment, claim)
}

fn complete_working_child(store: &mut SqliteStore, mut child: Commitment, claim: &ClaimLease) {
    child
        .propose_completion(claim.worker_id(), claim.epoch())
        .expect("child proposes completion");
    store
        .persist_commitment_change(
            &child,
            4,
            &WorkEvent::CommitmentCompletionProposed {
                commitment_id: child.commitment_id().clone(),
            },
            45,
        )
        .expect("completion proposal persists");
    child
        .complete(claim.worker_id(), claim.epoch())
        .expect("child completes");
    store
        .persist_commitment_change(
            &child,
            5,
            &WorkEvent::CommitmentCompleted {
                commitment_id: child.commitment_id().clone(),
            },
            46,
        )
        .expect("child completion persists");
}

fn issue(
    store: &mut SqliteStore,
    guard: &str,
    commitment: &Commitment,
    claim: &ClaimLease,
    scope_keys: ScopeSet,
    now: u64,
) -> Result<resolve_core::ExecutionGuard, StoreError> {
    issue_with_durations(store, guard, commitment, claim, scope_keys, now, 100, 100)
}

#[allow(clippy::too_many_arguments)]
fn issue_with_durations(
    store: &mut SqliteStore,
    guard: &str,
    commitment: &Commitment,
    claim: &ClaimLease,
    scope_keys: ScopeSet,
    now: u64,
    guard_ttl: u64,
    claim_lease_duration: u64,
) -> Result<resolve_core::ExecutionGuard, StoreError> {
    store.issue_guard(GuardIssueRequest {
        guard_id: guard_id(guard),
        commitment_id: commitment.commitment_id().clone(),
        worker_id: claim.worker_id().clone(),
        claim_epoch: claim.epoch(),
        scope_keys,
        boot_generation: store.boot_generation()?,
        now: MonotonicInstant::from_ticks(now),
        guard_ttl: duration(guard_ttl),
        claim_lease_duration: duration(claim_lease_duration),
    })
}

fn admit(
    store: &mut SqliteStore,
    guard: &str,
    scope_keys: ScopeSet,
    action_ref: &str,
    now: u64,
) -> Result<GuardAdmission, StoreError> {
    store.admit_guard(
        &guard_id(guard),
        scope_keys,
        action(action_ref),
        MonotonicInstant::from_ticks(now),
    )
}

fn count_rows(path: &Path, table: &str) -> i64 {
    let connection = Connection::open(path).expect("inspection connection opens");
    connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .expect("row count reads")
}

fn count_query(path: &Path, query: &str) -> i64 {
    let connection = Connection::open(path).expect("inspection connection opens");
    connection
        .query_row(query, [], |row| row.get(0))
        .expect("query count reads")
}

#[test]
fn canonical_split_brain_survives_restart_and_duplicate_delivery() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("canonical.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    let goal = create_goal(&mut store, "goal-canonical");
    let (commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    let (conflict, conflict_claim) = working(&mut store, &goal, "commitment-2", "worker-2", 1);
    let old_boot = store.boot_generation().expect("boot reads");
    let guard = issue(
        &mut store,
        "guard-1",
        &commitment,
        &claim,
        scopes(&["repo-A", "repo-B"]),
        10,
    )
    .expect("guard issues");
    assert_eq!(guard.scope_keys().len(), 2);
    assert!(matches!(
        issue(
            &mut store,
            "guard-conflict",
            &conflict,
            &conflict_claim,
            scopes(&["repo-A"]),
            10,
        ),
        Err(StoreError::ScopeLocked { .. })
    ));
    assert!(matches!(
        admit(
            &mut store,
            "guard-1",
            scopes(&["repo-A", "repo-B"]),
            "A100",
            11
        ),
        Ok(GuardAdmission::Admitted)
    ));
    assert!(matches!(
        admit(
            &mut store,
            "guard-1",
            scopes(&["repo-B", "repo-A"]),
            "A100",
            12
        ),
        Ok(GuardAdmission::AlreadyAdmitted)
    ));
    assert!(matches!(
        admit(
            &mut store,
            "guard-1",
            scopes(&["repo-A", "repo-B"]),
            "other-action",
            12
        ),
        Err(StoreError::GuardInvalid { .. })
    ));

    let mut clock = ManualClock::new(11);
    let expired_at = clock.advance_to(101);
    assert_eq!(
        store
            .process_expired_claims(expired_at, duration(100))
            .expect("expiry processes"),
        2
    );
    assert_eq!(
        store
            .load_commitment_record(commitment.commitment_id())
            .expect("commitment loads")
            .state(),
        &CommitmentState::RecoveryPending
    );
    assert!(matches!(
        store.record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            claim.epoch(),
            MonotonicInstant::from_ticks(101),
            duration(100)
        ),
        Err(StoreError::ClaimMismatch { .. })
    ));
    assert!(matches!(
        store.apply_structural_proposal(
            StructuralProposal::AddPrerequisite {
                target: commitment.commitment_id().clone(),
                prerequisite: Prerequisite::HumanDecision(
                    HumanDecisionRef::try_new("decision-old").expect("decision")
                ),
                epoch: claim.epoch(),
            },
            claim.worker_id(),
            old_boot,
            MonotonicInstant::from_ticks(101),
            duration(100),
            101,
        ),
        Err(StoreError::Domain(_)) | Err(StoreError::ClaimMismatch { .. })
    ));

    drop(store);
    let mut reopened = SqliteStore::open(&path).expect("restart recovers");
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("guard-1"))
            .expect("guard reloads")
            .state(),
        &resolve_core::GuardState::Admitted {
            action_ref: action("A100")
        }
    );
    assert!(matches!(
        reopened.load_commitment(&commitment_id("commitment-1")),
        Ok(value) if value.state() == &CommitmentState::RecoveryPending
    ));
    assert!(matches!(
        reopened.record_tethers_outcome(
            TethersOutcome::Succeeded {
                action_ref: action("A100")
            },
            200
        ),
        Ok(OutcomeRecording::Recorded)
    ));
    assert_eq!(
        reopened
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("commitment reloads")
            .state(),
        &CommitmentState::Ready
    );
    assert_eq!(
        reopened
            .record_tethers_outcome(
                TethersOutcome::Succeeded {
                    action_ref: action("A100")
                },
                201
            )
            .expect("duplicate outcome is idempotent"),
        OutcomeRecording::AlreadyRecorded
    );
    assert!(matches!(
        reopened.record_tethers_outcome(
            TethersOutcome::Failed {
                action_ref: action("A100")
            },
            202
        ),
        Err(StoreError::OutcomeConflict { .. })
    ));

    let mut restored = reopened
        .load_commitment(&commitment_id("commitment-1"))
        .expect("ready commitment restores");
    let epoch_two = restored
        .claim_for(worker_id("worker-2"), MonotonicInstant::from_ticks(210))
        .expect("new worker claims new epoch");
    assert_eq!(epoch_two.epoch().value(), 2);
    let restored_version = reopened
        .load_commitment_record(restored.commitment_id())
        .expect("ready commitment loads")
        .state_version();
    persist(
        &mut reopened,
        &restored,
        WorkEvent::CommitmentClaimed {
            commitment_id: restored.commitment_id().clone(),
            worker_id: epoch_two.worker_id().clone(),
            epoch: epoch_two.epoch(),
        },
        restored_version,
    );
    assert_ne!(guard.guard_id(), &guard_id("guard-2"));
    assert!(
        issue(
            &mut reopened,
            "guard-2",
            &restored,
            &epoch_two,
            scopes(&["repo-A", "repo-B"]),
            211
        )
        .is_ok()
    );
    assert!(matches!(
        reopened.record_heartbeat(
            &commitment_id("commitment-1"),
            claim.worker_id(),
            claim.epoch(),
            MonotonicInstant::from_ticks(212),
            duration(100)
        ),
        Err(StoreError::ClaimMismatch { .. })
    ));
}

#[test]
fn uncertain_outcome_remains_fenced_through_restart_and_hostile_retries() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("uncertain.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    let goal = create_goal(&mut store, "goal-uncertain");
    let (commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    let (conflict, conflict_claim) = working(&mut store, &goal, "commitment-2", "worker-2", 1);
    issue(
        &mut store,
        "guard-uncertain",
        &commitment,
        &claim,
        scopes(&["uncertain-scope"]),
        2,
    )
    .expect("guard issues");
    admit(
        &mut store,
        "guard-uncertain",
        scopes(&["uncertain-scope"]),
        "A200",
        3,
    )
    .expect("guard admits");
    assert_eq!(
        store
            .record_tethers_outcome(
                TethersOutcome::Uncertain {
                    action_ref: action("A200")
                },
                4
            )
            .expect("uncertain outcome records"),
        OutcomeRecording::Recorded
    );
    assert_eq!(
        store
            .record_tethers_outcome(
                TethersOutcome::Uncertain {
                    action_ref: action("A200")
                },
                5
            )
            .expect("duplicate uncertain is idempotent"),
        OutcomeRecording::AlreadyRecorded
    );
    let restored = store
        .load_commitment(&commitment_id("commitment-1"))
        .expect("uncertain commitment restores");
    assert_eq!(restored.outstanding_action(), Some(&action("A200")));
    assert!(matches!(
        restored.clone().resume(
            claim.worker_id(),
            claim.epoch(),
            resolve_core::ResumeTarget::Working
        ),
        Err(resolve_core::DomainError::RecoveryRequired { .. })
    ));
    assert!(matches!(
        restored
            .clone()
            .propose_completion(claim.worker_id(), claim.epoch()),
        Err(resolve_core::DomainError::NotClaimed)
    ));
    assert!(matches!(
        store.apply_structural_proposal(
            StructuralProposal::AddPrerequisite {
                target: commitment_id("commitment-1"),
                prerequisite: Prerequisite::TethersVerification(
                    TethersContractRef::try_new("contract-uncertain").expect("contract")
                ),
                epoch: claim.epoch(),
            },
            claim.worker_id(),
            store.boot_generation().expect("boot reads"),
            MonotonicInstant::from_ticks(6),
            duration(100),
            6,
        ),
        Err(StoreError::Domain(_)) | Err(StoreError::ClaimMismatch { .. })
    ));
    assert!(matches!(
        issue(
            &mut store,
            "guard-conflict",
            &conflict,
            &conflict_claim,
            scopes(&["uncertain-scope"]),
            6
        ),
        Err(StoreError::ScopeLocked { .. })
    ));
    assert!(matches!(
        store.record_tethers_outcome(
            TethersOutcome::Succeeded {
                action_ref: action("A200")
            },
            7
        ),
        Err(StoreError::OutcomeConflict { .. })
    ));
    drop(store);
    let mut reopened = SqliteStore::open(&path).expect("uncertainty survives restart");
    let restored = reopened
        .load_commitment(&commitment_id("commitment-1"))
        .expect("uncertain commitment reloads");
    assert!(
        matches!(restored.state(), CommitmentState::Waiting(WaitingReason::UncertainAction(reference)) if reference == &action("A200"))
    );
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("guard-uncertain"))
            .expect("uncertain guard reloads")
            .state(),
        &resolve_core::GuardState::Uncertain {
            action_ref: action("A200")
        }
    );
    assert!(matches!(
        restored.clone().resume(
            &worker_id("worker-2"),
            ClaimEpoch::initial(),
            resolve_core::ResumeTarget::Ready
        ),
        Err(resolve_core::DomainError::RecoveryRequired { .. })
    ));
    reopened
        .recover_without_action(&commitment_id("commitment-2"), 8)
        .expect("conflicting recovery releases");
    let mut conflict_restored = reopened
        .load_commitment(&commitment_id("commitment-2"))
        .expect("conflicting commitment restores");
    let conflict_epoch_two = conflict_restored
        .claim_for(worker_id("worker-2"), MonotonicInstant::from_ticks(8))
        .expect("conflicting worker reclaims");
    let conflict_version = reopened
        .load_commitment_record(conflict_restored.commitment_id())
        .expect("conflicting commitment loads")
        .state_version();
    persist(
        &mut reopened,
        &conflict_restored,
        WorkEvent::CommitmentClaimed {
            commitment_id: conflict_restored.commitment_id().clone(),
            worker_id: conflict_epoch_two.worker_id().clone(),
            epoch: conflict_epoch_two.epoch(),
        },
        conflict_version,
    );
    assert!(matches!(
        reopened.issue_guard(GuardIssueRequest {
            guard_id: guard_id("guard-after-restart"),
            commitment_id: commitment_id("commitment-2"),
            worker_id: conflict_epoch_two.worker_id().clone(),
            claim_epoch: conflict_epoch_two.epoch(),
            scope_keys: scopes(&["uncertain-scope"]),
            boot_generation: reopened.boot_generation().expect("boot reads"),
            now: MonotonicInstant::from_ticks(8),
            guard_ttl: duration(100),
            claim_lease_duration: duration(100),
        }),
        Err(StoreError::ScopeLocked { .. })
    ));
}

#[test]
fn claim_epoch_and_lease_boundaries_have_no_refresh_first_side_door() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    let goal = create_goal(&mut store, "goal-boundaries");
    let (commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    let mut clock = ManualClock::new(1);
    assert_eq!(
        store
            .record_heartbeat(
                commitment.commitment_id(),
                claim.worker_id(),
                claim.epoch(),
                clock.advance_to(2),
                duration(10)
            )
            .expect("current heartbeat succeeds"),
        HeartbeatResult::Recorded
    );
    assert!(matches!(
        store.record_heartbeat(
            commitment.commitment_id(),
            &worker_id("worker-2"),
            claim.epoch(),
            clock.advance_to(3),
            duration(10)
        ),
        Err(StoreError::ClaimMismatch { .. })
    ));
    assert!(matches!(
        store.record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            ClaimEpoch::try_from_raw(2).expect("future epoch"),
            clock.advance_to(3),
            duration(10)
        ),
        Err(StoreError::ClaimMismatch { .. })
    ));
    assert!(matches!(
        store.record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            claim.epoch(),
            clock.advance_to(12),
            duration(10)
        ),
        Err(StoreError::LeaseExpired { .. })
    ));

    let (issued_commitment, issued_claim) =
        working(&mut store, &goal, "commitment-2", "worker-2", 1);
    issue_with_durations(
        &mut store,
        "guard-boundary",
        &issued_commitment,
        &issued_claim,
        scopes(&["boundary-scope"]),
        10,
        100,
        10,
    )
    .expect("deadline minus one is live");
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-boundary"))
            .expect("boundary guard loads")
            .reservation_expires_at(),
        Some(MonotonicInstant::from_ticks(11))
    );
    assert!(matches!(
        admit(
            &mut store,
            "guard-boundary",
            scopes(&["boundary-scope"]),
            "A-boundary",
            11
        ),
        Err(StoreError::LeaseExpired { .. })
    ));
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-boundary"))
            .expect("guard remains inspectable")
            .state(),
        &resolve_core::GuardState::Issued
    );
    assert!(matches!(
        store.apply_structural_proposal(
            StructuralProposal::AddPrerequisite {
                target: issued_commitment.commitment_id().clone(),
                prerequisite: Prerequisite::HumanDecision(
                    HumanDecisionRef::try_new("boundary-decision").expect("decision")
                ),
                epoch: issued_claim.epoch(),
            },
            issued_claim.worker_id(),
            store.boot_generation().expect("boot reads"),
            MonotonicInstant::from_ticks(11),
            duration(10),
            11,
        ),
        Err(StoreError::LeaseExpired { .. })
    ));
}

#[test]
fn multi_scope_conflicts_are_atomic_and_scope_keys_are_opaque() {
    for conflict_scope in ["A", "B", "C"] {
        let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
        let goal = create_goal(&mut store, "goal-multi");
        let (holder, holder_claim) = working(&mut store, &goal, "holder", "holder-worker", 1);
        let (requester, requester_claim) =
            working(&mut store, &goal, "requester", "requester-worker", 1);
        issue(
            &mut store,
            "holder-guard",
            &holder,
            &holder_claim,
            scopes(&[conflict_scope]),
            2,
        )
        .expect("holder guard issues");
        let before_events = store.list_events().expect("events load").len();
        assert!(matches!(
            issue(
                &mut store,
                "requester-guard",
                &requester,
                &requester_claim,
                scopes(&["A", "B", "C"]),
                2
            ),
            Err(StoreError::ScopeLocked { .. })
        ));
        assert_eq!(
            store.list_events().expect("events load").len(),
            before_events
        );
        let free_scope = ["A", "B", "C"]
            .into_iter()
            .find(|value| *value != conflict_scope)
            .expect("free scope");
        assert!(
            issue(
                &mut store,
                "free-guard",
                &requester,
                &requester_claim,
                scopes(&[free_scope]),
                2
            )
            .is_ok(),
            "no partial multi-scope lock survives"
        );
    }

    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    let goal = create_goal(&mut store, "goal-opaque");
    let (first, first_claim) = working(&mut store, &goal, "first", "worker-1", 1);
    let (second, second_claim) = working(&mut store, &goal, "second", "worker-2", 1);
    let (third, third_claim) = working(&mut store, &goal, "third", "worker-3", 1);
    issue(
        &mut store,
        "repo",
        &first,
        &first_claim,
        scopes(&["repo"]),
        2,
    )
    .expect("repo scope issues");
    issue(
        &mut store,
        "repo-src",
        &second,
        &second_claim,
        scopes(&["repo/src"]),
        2,
    )
    .expect("repo/src is unrelated");
    issue(
        &mut store,
        "repo-file",
        &third,
        &third_claim,
        scopes(&["repo/src/main.rs"]),
        2,
    )
    .expect("repo/src/main.rs is unrelated");
}

#[test]
fn generic_persistence_cannot_rewrite_authority_or_terminal_state() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    let goal = create_goal(&mut store, "goal-bypass");
    let (commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    let version = store
        .load_commitment_record(commitment.commitment_id())
        .expect("commitment loads")
        .state_version();
    let altered_goal = Commitment::restore(
        commitment.commitment_id().clone(),
        goal_id("different-goal"),
        None,
        commitment.description(),
        commitment.state().clone(),
        commitment.prerequisites().to_vec(),
        commitment.acceptance_refs().to_vec(),
        Some((
            claim.worker_id().clone(),
            claim.epoch(),
            claim.last_heartbeat(),
        )),
        None,
        commitment.last_claim_epoch(),
    )
    .expect("altered aggregate is representable");
    let before_events = store.list_events().expect("events load").len();
    assert!(matches!(
        store.persist_commitment_change(
            &altered_goal,
            version,
            &WorkEvent::CommitmentStarted {
                commitment_id: commitment.commitment_id().clone(),
            },
            10,
        ),
        Err(StoreError::InvalidMutation(_))
    ));
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );

    let fabricated_action = Commitment::restore(
        commitment.commitment_id().clone(),
        commitment.goal_id().clone(),
        None,
        commitment.description(),
        commitment.state().clone(),
        commitment.prerequisites().to_vec(),
        commitment.acceptance_refs().to_vec(),
        Some((
            claim.worker_id().clone(),
            claim.epoch(),
            claim.last_heartbeat(),
        )),
        Some(action("fabricated-action")),
        commitment.last_claim_epoch(),
    )
    .expect("fabricated action aggregate is representable");
    let fabricated_error = store
        .persist_commitment_change(
            &fabricated_action,
            version,
            &WorkEvent::CommitmentStarted {
                commitment_id: commitment.commitment_id().clone(),
            },
            10,
        )
        .expect_err("fabricated action must be rejected");
    assert!(
        matches!(fabricated_error, StoreError::InvalidMutation(_)),
        "unexpected error: {fabricated_error:?}"
    );

    let mut terminal = commitment.clone();
    terminal
        .propose_completion(claim.worker_id(), claim.epoch())
        .expect("completion proposal succeeds");
    persist(
        &mut store,
        &terminal,
        WorkEvent::CommitmentCompletionProposed {
            commitment_id: terminal.commitment_id().clone(),
        },
        version,
    );
    terminal
        .complete(claim.worker_id(), claim.epoch())
        .expect("completion succeeds");
    let completion_version = store
        .load_commitment_record(terminal.commitment_id())
        .expect("terminal commitment loads")
        .state_version();
    persist(
        &mut store,
        &terminal,
        WorkEvent::CommitmentCompleted {
            commitment_id: terminal.commitment_id().clone(),
        },
        completion_version,
    );
    let forged_reopen = Commitment::restore(
        terminal.commitment_id().clone(),
        terminal.goal_id().clone(),
        None,
        terminal.description(),
        CommitmentState::Ready,
        terminal.prerequisites().to_vec(),
        terminal.acceptance_refs().to_vec(),
        None,
        None,
        terminal.last_claim_epoch(),
    )
    .expect("terminal forgery is representable");
    let terminal_version = store
        .load_commitment_record(terminal.commitment_id())
        .expect("terminal commitment reloads")
        .state_version();
    assert!(matches!(
        store.persist_commitment_change(
            &forged_reopen,
            terminal_version,
            &WorkEvent::CommitmentActivated {
                commitment_id: terminal.commitment_id().clone(),
            },
            11,
        ),
        Err(StoreError::Domain(
            resolve_core::DomainError::TerminalCommitmentImmutable { .. }
        ))
    ));
    assert_eq!(
        store
            .load_commitment_record(terminal.commitment_id())
            .expect("terminal remains")
            .state(),
        &CommitmentState::Completed
    );
}

#[test]
fn attention_is_explicit_and_cannot_change_commitment_authority() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    let goal = create_goal(&mut store, "goal-attention");
    let (commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    let attention_id = resolve_core::AttentionId::try_new("attention-1").expect("attention id");
    let attention = resolve_core::AttentionItem::new(
        attention_id.clone(),
        Some(commitment.commitment_id().clone()),
        "human judgement required",
    )
    .expect("attention is valid");
    store
        .insert_attention(
            goal.goal_id(),
            &attention,
            &WorkEvent::AttentionRaised {
                attention_id: attention_id.clone(),
                commitment_id: attention.commitment_id().cloned(),
            },
            10,
        )
        .expect("attention persists");
    store
        .clear_attention(goal.goal_id(), &attention_id, 11)
        .expect("attention clears");
    assert_eq!(
        store
            .load_attention_record(&attention_id)
            .expect("attention loads")
            .state(),
        resolve_core::AttentionState::Cleared
    );
    assert!(matches!(
        store.clear_attention(goal.goal_id(), &attention_id, 12),
        Err(StoreError::InvalidMutation(_))
    ));
    let commitment_record = store
        .load_commitment_record(commitment.commitment_id())
        .expect("commitment remains unchanged");
    assert_eq!(commitment_record.state(), &CommitmentState::Working);
    assert_eq!(
        commitment_record.claim().expect("claim remains").epoch(),
        claim.epoch().value()
    );
    assert!(
        store
            .list_events()
            .expect("events load")
            .iter()
            .any(|event| event.event_type() == "attention_cleared")
    );
}

#[test]
fn newer_transactions_rollback_without_half_state() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("rollback.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    let goal = create_goal(&mut store, "goal-rollback");
    let (commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    issue(
        &mut store,
        "guard-1",
        &commitment,
        &claim,
        scopes(&["rollback-scope"]),
        4,
    )
    .expect("guard issues");
    let before_events = store.list_events().expect("events load").len();
    Connection::open(&path)
        .expect("fault connection opens")
        .execute_batch(
            "CREATE TRIGGER fail_lock_promotion
             BEFORE UPDATE OF lock_state ON scope_locks
             WHEN NEW.lock_state = 'held'
             BEGIN SELECT RAISE(ABORT, 'test lock promotion failure'); END;",
        )
        .expect("lock fault trigger installs");
    assert!(matches!(
        admit(
            &mut store,
            "guard-1",
            scopes(&["rollback-scope"]),
            "A-rollback",
            5
        ),
        Err(StoreError::Sqlite(_))
    ));
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-1"))
            .expect("guard remains")
            .state(),
        &resolve_core::GuardState::Issued
    );
    assert_eq!(count_rows(&path, "scope_locks"), 1);
}

#[test]
fn structural_and_outcome_transactions_rollback_without_half_state() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("structural-rollback.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    let goal = create_goal(&mut store, "goal-structural-rollback");
    let (parent, parent_claim) = working(&mut store, &goal, "parent", "worker-1", 1);
    let before_events = store.list_events().expect("events load").len();
    Connection::open(&path)
        .expect("fault connection opens")
        .execute_batch(
            "CREATE TRIGGER fail_second_child
             BEFORE INSERT ON commitments
             WHEN NEW.commitment_id = 'child-2'
             BEGIN SELECT RAISE(ABORT, 'test child failure'); END;",
        )
        .expect("child fault trigger installs");
    let child = |id: &str| {
        NewCommitment::new(commitment_id(id), "child work", Vec::new(), Vec::new())
            .expect("child proposal is valid")
    };
    assert!(matches!(
        store.apply_structural_proposal(
            StructuralProposal::Decompose {
                target: parent.commitment_id().clone(),
                epoch: parent_claim.epoch(),
                children: vec![child("child-1"), child("child-2")],
                replacement_terminal_id: commitment_id("child-2"),
            },
            parent_claim.worker_id(),
            store.boot_generation().expect("boot reads"),
            MonotonicInstant::from_ticks(5),
            duration(100),
            5,
        ),
        Err(StoreError::Sqlite(_))
    ));
    assert_eq!(count_rows(&path, "commitments"), 1);
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );
    let claim = parent_claim;
    issue(
        &mut store,
        "guard-outcome",
        &parent,
        &claim,
        scopes(&["outcome-scope"]),
        8,
    )
    .expect("guard issues");
    admit(
        &mut store,
        "guard-outcome",
        scopes(&["outcome-scope"]),
        "A-outcome",
        9,
    )
    .expect("guard admits");
    let before_events = store.list_events().expect("events load").len();
    Connection::open(&path)
        .expect("outcome fault connection opens")
        .execute_batch(
            "CREATE TRIGGER fail_uncertain_state
             BEFORE UPDATE OF state_json ON commitments
             WHEN NEW.state_json LIKE '%UncertainAction%'
             BEGIN SELECT RAISE(ABORT, 'test outcome failure'); END;",
        )
        .expect("outcome fault trigger installs");
    assert!(matches!(
        store.record_tethers_outcome(
            TethersOutcome::Uncertain {
                action_ref: action("A-outcome")
            },
            10
        ),
        Err(StoreError::Sqlite(_))
    ));
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );
    assert_eq!(
        store
            .load_commitment_record(parent.commitment_id())
            .expect("parent remains")
            .state_version(),
        5
    );
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-outcome"))
            .expect("guard remains")
            .state(),
        &resolve_core::GuardState::Admitted {
            action_ref: action("A-outcome")
        }
    );
}

fn assert_corrupt_open_rejected(sql: &str) {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("corrupt.sqlite");
    let mut store = SqliteStore::open(&path).expect("base store opens");
    let goal = create_goal(&mut store, "goal-corrupt");
    let _ = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    drop(store);
    Connection::open(&path)
        .expect("corruption connection opens")
        .execute_batch(&format!("PRAGMA foreign_keys = OFF; {sql}"))
        .expect("test corruption applies");
    let result = SqliteStore::open(&path);
    assert!(
        matches!(
            result,
            Err(StoreError::InvalidPersistedData(_))
                | Err(StoreError::UnsupportedSchemaVersion { .. })
                | Err(StoreError::Domain(_))
                | Err(StoreError::RecoveryBlocked { .. })
                | Err(StoreError::ClaimMismatch { .. })
        ),
        "corrupt database was not rejected: {result:?}"
    );
}

#[test]
fn malformed_persistence_corpus_fails_closed_without_silent_repair() {
    assert_corrupt_open_rejected(
        "UPDATE commitments SET state_json = '{\"state\":\"not-a-state\"}'
         WHERE commitment_id = 'commitment-1';",
    );
    assert_corrupt_open_rejected(
        "UPDATE commitments SET last_claim_epoch = 2
         WHERE commitment_id = 'commitment-1';",
    );
    assert_corrupt_open_rejected(
        "UPDATE claims SET claim_epoch = 2
         WHERE commitment_id = 'commitment-1';",
    );
    assert_corrupt_open_rejected(
        "UPDATE claims SET boot_generation = 99
         WHERE commitment_id = 'commitment-1';",
    );
    assert_corrupt_open_rejected(
        "UPDATE commitments SET state_json = '{\"state\":\"Ready\"}'
         WHERE commitment_id = 'commitment-1';",
    );
    assert_corrupt_open_rejected("DELETE FROM claims WHERE commitment_id = 'commitment-1';");
    assert_corrupt_open_rejected(
        "UPDATE work_events SET event_type = 'invented_event'
         WHERE event_id = 1;",
    );
    assert_corrupt_open_rejected(
        "INSERT INTO execution_guards
             (guard_id, commitment_id, claim_epoch, boot_generation, state,
              action_ref, outcome_json, reservation_expires_at, state_version)
         VALUES ('orphan-guard', 'missing-commitment', 1, 1, 'issued', NULL, NULL, 10, 1);",
    );

    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("issued-lock-corrupt.sqlite");
    let mut store = SqliteStore::open(&path).expect("base store opens");
    let goal = create_goal(&mut store, "goal-lock-corrupt");
    let (commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    issue(
        &mut store,
        "guard-1",
        &commitment,
        &claim,
        scopes(&["scope-1"]),
        3,
    )
    .expect("guard issues");
    drop(store);
    Connection::open(&path)
        .expect("corruption connection opens")
        .execute_batch(
            "PRAGMA foreign_keys = OFF;
             UPDATE scope_locks SET lock_state = 'held', action_ref = 'wrong-action',
                 reservation_expires_at = NULL WHERE scope_key = 'scope-1';",
        )
        .expect("lifecycle corruption applies");
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StoreError::InvalidPersistedData(_))
    ));

    assert_corrupt_open_rejected(
        "UPDATE metadata SET value = 'future' WHERE key = 'schema_version';",
    );
}

#[test]
fn restart_reconstructs_each_meaningful_live_state_without_resurrecting_leases() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("restart-matrix.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    let goal = create_goal(&mut store, "goal-restart-matrix");

    let mut ready = create_commitment(&mut store, &goal, "ready", None, Vec::new());
    ready.activate().expect("ready activates");
    persist(
        &mut store,
        &ready,
        WorkEvent::CommitmentActivated {
            commitment_id: ready.commitment_id().clone(),
        },
        1,
    );
    let (_, _) = claimed(&mut store, &goal, "claimed", "worker-claimed", 1);
    let (_, _) = working(&mut store, &goal, "working", "worker-working", 1);
    let (mut waiting, waiting_claim) = working(&mut store, &goal, "waiting", "worker-waiting", 1);
    waiting
        .wait(
            waiting_claim.worker_id(),
            waiting_claim.epoch(),
            WaitingReason::HumanDecision(
                HumanDecisionRef::try_new("decision-1").expect("decision"),
            ),
        )
        .expect("ordinary waiting is valid");
    persist(
        &mut store,
        &waiting,
        WorkEvent::CommitmentWaiting {
            commitment_id: waiting.commitment_id().clone(),
            reason: WaitingReason::HumanDecision(
                HumanDecisionRef::try_new("decision-1").expect("decision"),
            ),
        },
        4,
    );
    let (mut completion, completion_claim) =
        working(&mut store, &goal, "completion", "worker-completion", 1);
    completion
        .propose_completion(completion_claim.worker_id(), completion_claim.epoch())
        .expect("completion proposal is valid");
    persist(
        &mut store,
        &completion,
        WorkEvent::CommitmentCompletionProposed {
            commitment_id: completion.commitment_id().clone(),
        },
        4,
    );
    let (issued, issued_claim) = working(&mut store, &goal, "issued", "worker-issued", 1);
    issue(
        &mut store,
        "issued-guard",
        &issued,
        &issued_claim,
        scopes(&["issued-scope"]),
        2,
    )
    .expect("issued guard persists");
    let (admitted, admitted_claim) = working(&mut store, &goal, "admitted", "worker-admitted", 1);
    issue(
        &mut store,
        "admitted-guard",
        &admitted,
        &admitted_claim,
        scopes(&["admitted-scope"]),
        2,
    )
    .expect("admitted guard issues");
    admit(
        &mut store,
        "admitted-guard",
        scopes(&["admitted-scope"]),
        "A-admitted",
        3,
    )
    .expect("admitted guard admits");
    let (uncertain, uncertain_claim) =
        working(&mut store, &goal, "uncertain", "worker-uncertain", 1);
    issue(
        &mut store,
        "uncertain-guard",
        &uncertain,
        &uncertain_claim,
        scopes(&["uncertain-scope"]),
        2,
    )
    .expect("uncertain guard issues");
    admit(
        &mut store,
        "uncertain-guard",
        scopes(&["uncertain-scope"]),
        "A-uncertain",
        3,
    )
    .expect("uncertain guard admits");
    store
        .record_tethers_outcome(
            TethersOutcome::Uncertain {
                action_ref: action("A-uncertain"),
            },
            4,
        )
        .expect("uncertain outcome records");
    let (mut terminal, terminal_claim) =
        working(&mut store, &goal, "terminal", "worker-terminal", 1);
    terminal
        .propose_completion(terminal_claim.worker_id(), terminal_claim.epoch())
        .expect("terminal proposal is valid");
    persist(
        &mut store,
        &terminal,
        WorkEvent::CommitmentCompletionProposed {
            commitment_id: terminal.commitment_id().clone(),
        },
        4,
    );
    terminal
        .complete(terminal_claim.worker_id(), terminal_claim.epoch())
        .expect("terminal completion is valid");
    persist(
        &mut store,
        &terminal,
        WorkEvent::CommitmentCompleted {
            commitment_id: terminal.commitment_id().clone(),
        },
        5,
    );
    let (composite, composite_claim) =
        working(&mut store, &goal, "composite", "worker-composite", 1);
    let child = NewCommitment::new(
        commitment_id("composite-child"),
        "child",
        Vec::new(),
        Vec::new(),
    )
    .expect("child proposal is valid");
    store
        .apply_structural_proposal(
            StructuralProposal::Decompose {
                target: composite.commitment_id().clone(),
                epoch: composite_claim.epoch(),
                children: vec![child],
                replacement_terminal_id: commitment_id("composite-child"),
            },
            composite_claim.worker_id(),
            store.boot_generation().expect("boot reads"),
            MonotonicInstant::from_ticks(5),
            duration(100),
            5,
        )
        .expect("composite barrier persists");
    drop(store);

    let reopened = SqliteStore::open(&path).expect("restart matrix opens");
    assert_eq!(
        reopened
            .load_commitment_record(&commitment_id("ready"))
            .expect("ready loads")
            .state(),
        &CommitmentState::Ready
    );
    for id in [
        "claimed",
        "working",
        "waiting",
        "completion",
        "issued",
        "composite",
    ] {
        assert_eq!(
            reopened
                .load_commitment_record(&commitment_id(id))
                .expect("recovered state loads")
                .state(),
            &CommitmentState::RecoveryPending
        );
    }
    assert_eq!(
        reopened
            .load_commitment_record(&commitment_id("terminal"))
            .expect("terminal loads")
            .state(),
        &CommitmentState::Completed
    );
    assert!(
        matches!(reopened.load_commitment_record(&commitment_id("uncertain")).expect("uncertain loads").state(), CommitmentState::Waiting(WaitingReason::UncertainAction(reference)) if reference == &action("A-uncertain"))
    );
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("issued-guard"))
            .expect("issued guard loads")
            .state(),
        &resolve_core::GuardState::Invalidated
    );
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("admitted-guard"))
            .expect("admitted guard loads")
            .state(),
        &resolve_core::GuardState::Admitted {
            action_ref: action("A-admitted")
        }
    );
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("uncertain-guard"))
            .expect("uncertain guard loads")
            .state(),
        &resolve_core::GuardState::Uncertain {
            action_ref: action("A-uncertain")
        }
    );
    assert_eq!(
        count_query(
            &path,
            "SELECT COUNT(*) FROM scope_locks WHERE lock_state = 'reserved'"
        ),
        0
    );
    assert_eq!(
        count_query(
            &path,
            "SELECT COUNT(*) FROM scope_locks WHERE lock_state = 'held'"
        ),
        2
    );
}

#[test]
fn sqlite_integrity_and_event_encoding_remain_explicit() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    let goal = create_goal(&mut store, "goal-integrity");
    let (mut commitment, claim) = working(&mut store, &goal, "commitment-1", "worker-1", 1);
    let attention_id = resolve_core::AttentionId::try_new("attention-1").expect("attention id");
    let attention = resolve_core::AttentionItem::new(attention_id.clone(), None, "attention")
        .expect("attention is valid");
    store
        .insert_attention(
            goal.goal_id(),
            &attention,
            &WorkEvent::AttentionRaised {
                attention_id: attention_id.clone(),
                commitment_id: None,
            },
            3,
        )
        .expect("attention persists");
    store
        .clear_attention(goal.goal_id(), &attention_id, 4)
        .expect("attention clears");
    commitment
        .wait(
            claim.worker_id(),
            claim.epoch(),
            WaitingReason::HumanDecision(HumanDecisionRef::try_new("decision").expect("decision")),
        )
        .expect("waiting state is valid");
    let version = store
        .load_commitment_record(commitment.commitment_id())
        .expect("commitment loads")
        .state_version();
    persist(
        &mut store,
        &commitment,
        WorkEvent::CommitmentWaiting {
            commitment_id: commitment.commitment_id().clone(),
            reason: WaitingReason::HumanDecision(
                HumanDecisionRef::try_new("decision").expect("decision"),
            ),
        },
        version,
    );
    let events = store.list_events().expect("events load");
    let stable_types: BTreeSet<_> = events.iter().map(|event| event.event_type()).collect();
    for event in &events {
        let payload: serde_json::Value =
            serde_json::from_str(event.payload_json()).expect("event payload is JSON");
        assert!(payload.is_object(), "event payload is tagged object");
        assert!(!event.event_type().is_empty());
    }
    assert!(stable_types.contains("goal_created"));
    assert!(stable_types.contains("commitment_waiting"));
    assert!(stable_types.contains("attention_raised"));
    assert!(stable_types.contains("attention_cleared"));
    assert!(stable_types.iter().all(|event_type| {
        [
            "goal_created",
            "goal_revised",
            "commitment_created",
            "commitment_activated",
            "commitment_claimed",
            "commitment_started",
            "commitment_waiting",
            "commitment_recovery_pending",
            "commitment_completion_proposed",
            "commitment_completed",
            "commitment_cancelled",
            "commitment_abandoned",
            "heartbeat_accepted",
            "lease_expired",
            "guard_issued",
            "guard_admitted",
            "guard_invalidated",
            "guard_reservation_expired",
            "tethers_outcome_recorded",
            "recovery_completed",
            "recovery_released",
            "structural_proposal_applied",
            "attention_raised",
            "attention_cleared",
        ]
        .contains(event_type)
    }));
}

fn create_legacy_fixture(path: &Path, version: i64) {
    let commitment_extra = if version >= 3 {
        ", last_claim_epoch INTEGER"
    } else {
        ""
    };
    let claims_extra = if version >= 2 {
        ", boot_generation INTEGER NOT NULL DEFAULT 0"
    } else {
        ""
    };
    let fencing = if version >= 2 {
        "; CREATE TABLE execution_guards (
             guard_id TEXT PRIMARY KEY NOT NULL, commitment_id TEXT NOT NULL,
             claim_epoch INTEGER NOT NULL, boot_generation INTEGER NOT NULL,
             state TEXT NOT NULL, action_ref TEXT, outcome_json TEXT,
             reservation_expires_at INTEGER, state_version INTEGER NOT NULL
         );
         CREATE TABLE execution_guard_scopes (
             guard_id TEXT NOT NULL, ordinal INTEGER NOT NULL, scope_key TEXT NOT NULL,
             PRIMARY KEY (guard_id, ordinal)
         );
         CREATE TABLE scope_locks (
             scope_key TEXT PRIMARY KEY NOT NULL, guard_id TEXT NOT NULL,
             commitment_id TEXT NOT NULL, lock_state TEXT NOT NULL,
             action_ref TEXT, reservation_expires_at INTEGER
         )"
    } else {
        ""
    };
    let ddl = format!(
        "CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
         CREATE TABLE goals (
             goal_id TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL,
             description TEXT NOT NULL, state TEXT NOT NULL, created_at INTEGER NOT NULL,
             state_version INTEGER NOT NULL
         );
         CREATE TABLE goal_revisions (
             goal_id TEXT NOT NULL, revision INTEGER NOT NULL, description TEXT NOT NULL,
             state TEXT NOT NULL, created_at INTEGER NOT NULL,
             PRIMARY KEY (goal_id, revision)
         );
         CREATE TABLE commitments (
             commitment_id TEXT PRIMARY KEY NOT NULL, goal_id TEXT NOT NULL,
             parent_id TEXT, description TEXT NOT NULL, state_json TEXT NOT NULL,
             outstanding_action TEXT, state_version INTEGER NOT NULL{commitment_extra}
         );
         CREATE TABLE commitment_prerequisites (
             commitment_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
             prerequisite_type TEXT NOT NULL, reference_id TEXT NOT NULL,
             prerequisite_commitment_id TEXT
         );
         CREATE TABLE commitment_acceptance_refs (
             commitment_id TEXT NOT NULL, ordinal INTEGER NOT NULL, reference_id TEXT NOT NULL
         );
         CREATE TABLE claims (
             commitment_id TEXT PRIMARY KEY NOT NULL, worker_id TEXT NOT NULL,
             claim_epoch INTEGER NOT NULL, last_heartbeat INTEGER NOT NULL{claims_extra}
         );
         CREATE TABLE attention_items (
             attention_id TEXT PRIMARY KEY NOT NULL, commitment_id TEXT,
             description TEXT NOT NULL, state TEXT NOT NULL
         );
         CREATE TABLE work_events (
             event_id INTEGER PRIMARY KEY AUTOINCREMENT, goal_id TEXT NOT NULL,
             commitment_id TEXT, event_type TEXT NOT NULL,
             payload_json TEXT NOT NULL, created_at INTEGER NOT NULL
         ){fencing};",
    );
    let connection = Connection::open(path).expect("legacy fixture opens");
    connection
        .execute_batch(&ddl)
        .expect("legacy schema creates");
    connection
        .execute("INSERT INTO metadata(key, value) VALUES ('schema_version', ?1), ('boot_generation', '0')", [version.to_string()])
        .expect("legacy metadata persists");
    connection
        .execute(
            "INSERT INTO goals VALUES ('goal-legacy', 1, 'legacy goal', 'active', 1, 1)",
            [],
        )
        .expect("legacy goal persists");
    connection
        .execute(
            "INSERT INTO goal_revisions VALUES ('goal-legacy', 1, 'legacy goal', 'active', 1)",
            [],
        )
        .expect("legacy revision persists");
    let state = "{\"state\":\"Proposed\"}";
    if version >= 3 {
        connection.execute("INSERT INTO commitments VALUES ('commitment-legacy', 'goal-legacy', NULL, 'legacy commitment', ?1, NULL, 1, NULL)", [state])
            .expect("legacy commitment persists");
    } else {
        connection.execute("INSERT INTO commitments VALUES ('commitment-legacy', 'goal-legacy', NULL, 'legacy commitment', ?1, NULL, 1)", [state])
            .expect("legacy commitment persists");
    }
    connection.execute(
        "INSERT INTO work_events (goal_id, commitment_id, event_type, payload_json, created_at)
         VALUES ('goal-legacy', NULL, 'goal_created',
             '{\"kind\":\"GoalCreated\",\"data\":{\"goal_id\":\"goal-legacy\",\"revision\":1}}', 1)",
        [],
    ).expect("legacy event persists");
}

#[test]
fn supported_schema_versions_migrate_to_v4_without_losing_data() {
    for version in [1_i64, 2, 3] {
        let directory = tempdir().expect("temporary directory creates");
        let path = directory.path().join(format!("legacy-v{version}.sqlite"));
        create_legacy_fixture(&path, version);
        let store = SqliteStore::open(&path).expect("legacy fixture migrates");
        assert_eq!(store.schema_version().expect("schema version reads"), 4);
        assert_eq!(store.journal_mode().expect("journal mode reads"), "wal");
        assert!(store.foreign_keys_enabled().expect("foreign keys read"));
        assert_eq!(
            store
                .load_goal_record(&goal_id("goal-legacy"))
                .expect("goal survives")
                .description(),
            "legacy goal"
        );
        assert_eq!(
            store
                .load_commitment_record(&commitment_id("commitment-legacy"))
                .expect("commitment survives")
                .description(),
            "legacy commitment"
        );
        drop(store);
        let reopened = SqliteStore::open(&path).expect("v4 reopen succeeds");
        assert_eq!(reopened.schema_version().expect("schema version reads"), 4);
    }
}

#[test]
fn bounded_composite_cascade_completes_each_ancestor_once() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    let goal = create_goal(&mut store, "goal-cascade");
    let (mut current, mut claim) = working(&mut store, &goal, "cascade-root", "worker", 1);
    let mut ids = vec![current.commitment_id().clone()];

    for depth in 1..=5 {
        let child_id = format!("cascade-{depth}");
        let child = NewCommitment::new(commitment_id(&child_id), &child_id, Vec::new(), Vec::new())
            .expect("cascade child is valid");
        store
            .apply_structural_proposal(
                StructuralProposal::Decompose {
                    target: current.commitment_id().clone(),
                    epoch: claim.epoch(),
                    children: vec![child],
                    replacement_terminal_id: commitment_id(&child_id),
                },
                claim.worker_id(),
                BootGeneration::from_raw(0),
                MonotonicInstant::from_ticks(10 + depth as u64),
                duration(100),
                10 + depth as i64,
            )
            .expect("cascade decomposition applies");
        let proposed = store
            .load_commitment(&commitment_id(&child_id))
            .expect("cascade child restores");
        let (working_child, child_claim) = working_from_proposed(
            &mut store,
            proposed,
            &format!("worker-{depth}"),
            20 + depth as u64,
        );
        ids.push(working_child.commitment_id().clone());
        current = working_child;
        claim = child_claim;
    }

    complete_working_child(&mut store, current, &claim);
    let events = store.list_events().expect("events load");
    for id in &ids {
        assert_eq!(
            store
                .load_commitment_record(id)
                .expect("ancestor reloads")
                .state(),
            &CommitmentState::Completed
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type() == "commitment_completed")
                .filter(|event| event.commitment_id() == Some(id))
                .count(),
            1,
            "each ancestor completes exactly once"
        );
    }
}

#[test]
fn final_r0_scenario_composes_recovery_structure_action_and_downstream_identity() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    let goal = create_goal(&mut store, "goal-final");
    let (mut parent, first_claim) = working(&mut store, &goal, "parent", "worker-failed", 1);
    assert_eq!(
        store
            .process_expired_claims(MonotonicInstant::from_ticks(101), duration(100))
            .expect("worker failure recovers"),
        1
    );
    store
        .recover_without_action(parent.commitment_id(), 102)
        .expect("recovery completes");
    let recovered_parent = store
        .load_commitment(parent.commitment_id())
        .expect("parent restores");
    parent = recovered_parent;
    let recovered_claim = parent
        .claim_for(
            worker_id("worker-recovered"),
            MonotonicInstant::from_ticks(103),
        )
        .expect("replacement worker claims");
    store
        .persist_commitment_change(
            &parent,
            6,
            &WorkEvent::CommitmentClaimed {
                commitment_id: parent.commitment_id().clone(),
                worker_id: recovered_claim.worker_id().clone(),
                epoch: recovered_claim.epoch(),
            },
            104,
        )
        .expect("replacement claim persists");
    parent
        .start(recovered_claim.worker_id(), recovered_claim.epoch())
        .expect("replacement worker starts");
    store
        .persist_commitment_change(
            &parent,
            7,
            &WorkEvent::CommitmentStarted {
                commitment_id: parent.commitment_id().clone(),
            },
            105,
        )
        .expect("replacement start persists");
    let downstream = create_commitment(
        &mut store,
        &goal,
        "downstream",
        None,
        vec![Prerequisite::Commitment(parent.commitment_id().clone())],
    );
    let child = NewCommitment::new(
        commitment_id("action-child"),
        "action child",
        Vec::new(),
        vec![AcceptanceRef::try_new("child-acceptance").expect("acceptance")],
    )
    .expect("child is valid");
    store
        .apply_structural_proposal(
            StructuralProposal::Decompose {
                target: parent.commitment_id().clone(),
                epoch: recovered_claim.epoch(),
                children: vec![child],
                replacement_terminal_id: commitment_id("action-child"),
            },
            recovered_claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(106),
            duration(100),
            106,
        )
        .expect("structural decomposition persists");

    let proposed_child = store
        .load_commitment(&commitment_id("action-child"))
        .expect("child restores");
    let (child, child_claim) =
        working_from_proposed(&mut store, proposed_child, "worker-child", 108);
    issue(
        &mut store,
        "final-guard",
        &child,
        &child_claim,
        scopes(&["final-authoritative-scope"]),
        109,
    )
    .expect("guard issues");
    assert_eq!(
        admit(
            &mut store,
            "final-guard",
            scopes(&["final-authoritative-scope"]),
            "A-final",
            110,
        )
        .expect("guard admits"),
        GuardAdmission::Admitted
    );
    assert_eq!(
        store
            .record_tethers_outcome(
                TethersOutcome::Succeeded {
                    action_ref: action("A-final"),
                },
                111,
            )
            .expect("authoritative outcome records"),
        OutcomeRecording::Recorded
    );
    let mut completed_child = store
        .load_commitment(&commitment_id("action-child"))
        .expect("child reloads");
    completed_child
        .propose_completion(child_claim.worker_id(), child_claim.epoch())
        .expect("child proposes completion");
    store
        .persist_commitment_change(
            &completed_child,
            6,
            &WorkEvent::CommitmentCompletionProposed {
                commitment_id: completed_child.commitment_id().clone(),
            },
            112,
        )
        .expect("completion proposal persists");
    completed_child
        .complete(child_claim.worker_id(), child_claim.epoch())
        .expect("child completes");
    store
        .persist_commitment_change(
            &completed_child,
            7,
            &WorkEvent::CommitmentCompleted {
                commitment_id: completed_child.commitment_id().clone(),
            },
            113,
        )
        .expect("child completion persists");

    assert_eq!(
        store
            .load_commitment_record(parent.commitment_id())
            .expect("parent reloads")
            .state(),
        &CommitmentState::Completed
    );
    assert_eq!(
        store
            .load_commitment_record(downstream.commitment_id())
            .expect("downstream reloads")
            .prerequisites(),
        &[Prerequisite::Commitment(parent.commitment_id().clone())]
    );
    assert_eq!(first_claim.epoch(), ClaimEpoch::initial());
    assert!(
        store
            .list_events()
            .expect("events load")
            .iter()
            .any(|event| event.event_type() == "lease_expired")
    );
}

#[test]
fn two_connections_use_version_conflicts_and_single_scope_winner() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("concurrency.sqlite");
    let mut writer_a = SqliteStore::open(&path).expect("first connection opens");
    let mut writer_b = SqliteStore::open(&path).expect("second connection opens");
    let goal = create_goal(&mut writer_a, "goal-concurrency");
    let next = goal.revision().next().expect("revision advances");
    let revised = goal
        .revised(next, "concurrent revision")
        .expect("revision is valid");
    writer_a
        .persist_goal_change(
            &revised,
            1,
            &WorkEvent::GoalRevised {
                goal_id: goal.goal_id().clone(),
                revision: next,
            },
        )
        .expect("writer A commits");
    assert!(matches!(
        writer_b.persist_goal_change(
            &revised,
            1,
            &WorkEvent::GoalRevised {
                goal_id: goal.goal_id().clone(),
                revision: next
            }
        ),
        Err(StoreError::VersionConflict { .. })
    ));
    let (first, first_claim) = working(&mut writer_a, &goal, "first", "worker-1", 1);
    let (second, second_claim) = working(&mut writer_a, &goal, "second", "worker-2", 1);
    issue(
        &mut writer_a,
        "winner",
        &first,
        &first_claim,
        scopes(&["same-scope"]),
        3,
    )
    .expect("first guard wins");
    assert!(matches!(
        issue(
            &mut writer_b,
            "loser",
            &second,
            &second_claim,
            scopes(&["same-scope"]),
            3
        ),
        Err(StoreError::ScopeLocked { .. })
    ));
}
