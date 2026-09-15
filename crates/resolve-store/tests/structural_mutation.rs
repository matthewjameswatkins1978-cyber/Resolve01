use resolve_core::{
    BootGeneration, ClaimLease, Commitment, CommitmentId, CommitmentState, GoalId, GoalRevision,
    GoalSpec, GoalState, MonotonicInstant, NewCommitment, Prerequisite, StructuralProposal,
    TethersContractRef, Timestamp, WaitingReason, WorkEvent, WorkerId,
};
use resolve_store::{SqliteStore, StoreError};

fn goal_id() -> GoalId {
    GoalId::try_new("goal-1").expect("valid goal")
}

fn commitment_id(value: &str) -> CommitmentId {
    CommitmentId::try_new(value).expect("valid commitment")
}

fn worker_id() -> WorkerId {
    WorkerId::try_new("worker-1").expect("valid worker")
}

fn prepare_goal(store: &mut SqliteStore) {
    let goal = GoalSpec::new(
        goal_id(),
        GoalRevision::initial(),
        "bounded structural work",
        GoalState::Active,
        Timestamp::from_unix_seconds(0),
    )
    .expect("valid goal");
    store
        .insert_goal(
            &goal,
            &WorkEvent::GoalCreated {
                goal_id: goal_id(),
                revision: GoalRevision::initial(),
            },
        )
        .expect("goal inserts");
}

fn add_commitment(
    store: &mut SqliteStore,
    id: &str,
    parent_id: Option<CommitmentId>,
    prerequisites: Vec<Prerequisite>,
) -> Commitment {
    let commitment = Commitment::new(
        commitment_id(id),
        goal_id(),
        parent_id,
        id,
        prerequisites,
        Vec::new(),
    )
    .expect("valid commitment");
    store
        .insert_commitment(
            &commitment,
            &WorkEvent::CommitmentCreated {
                commitment_id: commitment.commitment_id().clone(),
                goal_id: goal_id(),
            },
            1,
        )
        .expect("commitment inserts");
    commitment
}

fn make_working(store: &mut SqliteStore, mut commitment: Commitment) -> (Commitment, ClaimLease) {
    commitment.activate().expect("activation");
    store
        .persist_commitment_change(
            &commitment,
            1,
            &WorkEvent::CommitmentActivated {
                commitment_id: commitment.commitment_id().clone(),
            },
            2,
        )
        .expect("activation persists");
    let claim = commitment
        .claim_for(worker_id(), MonotonicInstant::from_ticks(1))
        .expect("claim");
    store
        .persist_commitment_change(
            &commitment,
            2,
            &WorkEvent::CommitmentClaimed {
                commitment_id: commitment.commitment_id().clone(),
                worker_id: worker_id(),
                epoch: claim.epoch(),
            },
            3,
        )
        .expect("claim persists");
    commitment
        .start(claim.worker_id(), claim.epoch())
        .expect("start");
    store
        .persist_commitment_change(
            &commitment,
            3,
            &WorkEvent::CommitmentStarted {
                commitment_id: commitment.commitment_id().clone(),
            },
            4,
        )
        .expect("start persists");
    (commitment, claim)
}

fn store_with_working_target() -> (SqliteStore, Commitment, ClaimLease) {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    prepare_goal(&mut store);
    let target = add_commitment(&mut store, "target", None, Vec::new());
    let (target, claim) = make_working(&mut store, target);
    (store, target, claim)
}

#[test]
fn decomposition_is_typed_direct_same_goal_and_barrier_completes_parent() {
    let (mut store, target, claim) = store_with_working_target();
    let _downstream = add_commitment(
        &mut store,
        "downstream",
        None,
        vec![Prerequisite::Commitment(target.commitment_id().clone())],
    );
    let child_a = NewCommitment::new(commitment_id("child-a"), "child A", Vec::new(), Vec::new())
        .expect("child A");
    let child_b = NewCommitment::new(
        commitment_id("child-b"),
        "child B",
        vec![Prerequisite::Commitment(commitment_id("child-a"))],
        Vec::new(),
    )
    .expect("child B");
    store
        .apply_structural_proposal(
            StructuralProposal::Decompose {
                target: target.commitment_id().clone(),
                epoch: claim.epoch(),
                children: vec![child_a, child_b],
                replacement_terminal_id: commitment_id("child-b"),
            },
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(10),
            10,
        )
        .expect("decomposition applies");

    let parent = store
        .load_commitment_record(&commitment_id("target"))
        .expect("parent loads");
    assert_eq!(
        parent.state(),
        &CommitmentState::Waiting(WaitingReason::Prerequisite(commitment_id("child-b")))
    );
    assert_eq!(
        parent.replacement_terminal_id(),
        Some(&commitment_id("child-b"))
    );
    assert!(
        parent.claim().is_some(),
        "parent remains claim-bearing while waiting"
    );
    assert_eq!(
        store
            .load_commitment_record(&commitment_id("child-a"))
            .expect("child A loads")
            .parent_id(),
        Some(&commitment_id("target"))
    );
    assert_eq!(
        store
            .load_commitment_record(&commitment_id("downstream"))
            .expect("downstream loads")
            .prerequisites(),
        &[Prerequisite::Commitment(commitment_id("target"))]
    );

    let mut child = store
        .load_commitment(&commitment_id("child-b"))
        .expect("child B restores");
    child.activate().expect("child activates");
    store
        .persist_commitment_change(
            &child,
            1,
            &WorkEvent::CommitmentActivated {
                commitment_id: commitment_id("child-b"),
            },
            11,
        )
        .expect("child activation persists");
    let child_claim = child
        .claim_for(worker_id(), MonotonicInstant::from_ticks(11))
        .expect("child claims");
    store
        .persist_commitment_change(
            &child,
            2,
            &WorkEvent::CommitmentClaimed {
                commitment_id: commitment_id("child-b"),
                worker_id: worker_id(),
                epoch: child_claim.epoch(),
            },
            12,
        )
        .expect("child claim persists");
    child
        .start(child_claim.worker_id(), child_claim.epoch())
        .expect("child starts");
    store
        .persist_commitment_change(
            &child,
            3,
            &WorkEvent::CommitmentStarted {
                commitment_id: commitment_id("child-b"),
            },
            13,
        )
        .expect("child start persists");
    child
        .propose_completion(child_claim.worker_id(), child_claim.epoch())
        .expect("child proposes completion");
    store
        .persist_commitment_change(
            &child,
            4,
            &WorkEvent::CommitmentCompletionProposed {
                commitment_id: commitment_id("child-b"),
            },
            14,
        )
        .expect("completion proposal persists");
    child
        .complete(child_claim.worker_id(), child_claim.epoch())
        .expect("child completes");
    store
        .persist_commitment_change(
            &child,
            5,
            &WorkEvent::CommitmentCompleted {
                commitment_id: commitment_id("child-b"),
            },
            15,
        )
        .expect("child completion persists");
    assert_eq!(
        store
            .load_commitment_record(&commitment_id("target"))
            .expect("parent reloads")
            .state(),
        &CommitmentState::Completed
    );
    assert!(
        store
            .list_events()
            .expect("events load")
            .iter()
            .any(|event| event.event_type() == "structural_proposal_applied")
    );
}

#[test]
fn add_prerequisite_is_atomic_typed_and_cycle_checked() {
    let (mut store, target, claim) = store_with_working_target();
    let dependency = add_commitment(&mut store, "dependency", None, Vec::new());
    let (_dependency, dependency_claim) = make_working(&mut store, dependency);
    store
        .apply_structural_proposal(
            StructuralProposal::AddPrerequisite {
                target: target.commitment_id().clone(),
                prerequisite: Prerequisite::Commitment(commitment_id("dependency")),
                epoch: claim.epoch(),
            },
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(10),
            10,
        )
        .expect("prerequisite applies");
    let record = store
        .load_commitment_record(target.commitment_id())
        .expect("target reloads");
    assert_eq!(record.state(), &CommitmentState::Working);
    assert_eq!(
        record.prerequisites(),
        &[Prerequisite::Commitment(commitment_id("dependency"))]
    );

    let event_count = store.list_events().expect("events load").len();
    let error = store
        .apply_structural_proposal(
            StructuralProposal::AddPrerequisite {
                target: commitment_id("dependency"),
                prerequisite: Prerequisite::Commitment(target.commitment_id().clone()),
                epoch: dependency_claim.epoch(),
            },
            dependency_claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(11),
            11,
        )
        .expect_err("cycle must fail");
    assert!(matches!(
        error,
        StoreError::Domain(resolve_core::DomainError::DependencyCycle)
    ));
    assert_eq!(store.list_events().expect("events load").len(), event_count);
}

#[test]
fn abandon_requires_rationale_and_clears_claim_without_touching_downstream() {
    let (mut store, target, claim) = store_with_working_target();
    add_commitment(
        &mut store,
        "downstream",
        None,
        vec![Prerequisite::Commitment(target.commitment_id().clone())],
    );
    store
        .apply_structural_proposal(
            StructuralProposal::Abandon {
                target: target.commitment_id().clone(),
                epoch: claim.epoch(),
                rationale: "external authority withdrew the work".to_owned(),
            },
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(10),
            10,
        )
        .expect("abandon applies");
    let abandoned = store
        .load_commitment_record(target.commitment_id())
        .expect("abandoned loads");
    assert_eq!(abandoned.state(), &CommitmentState::Abandoned);
    assert!(abandoned.claim().is_none());
    assert_eq!(
        store
            .load_commitment_record(&commitment_id("downstream"))
            .expect("downstream loads")
            .prerequisites(),
        &[Prerequisite::Commitment(target.commitment_id().clone())]
    );
}

#[test]
fn composite_recovery_blocks_until_replacement_terminal_completes() {
    let (mut store, target, claim) = store_with_working_target();
    let child =
        NewCommitment::new(commitment_id("child"), "child", Vec::new(), Vec::new()).expect("child");
    store
        .apply_structural_proposal(
            StructuralProposal::Decompose {
                target: target.commitment_id().clone(),
                epoch: claim.epoch(),
                children: vec![child],
                replacement_terminal_id: commitment_id("child"),
            },
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(10),
            10,
        )
        .expect("decomposition applies");
    assert_eq!(
        store
            .process_expired_claims(
                MonotonicInstant::from_ticks(100),
                resolve_core::MonotonicDuration::try_from_ticks(5).expect("duration")
            )
            .expect("expiry"),
        1
    );
    let error = store
        .recover_without_action(target.commitment_id(), 101)
        .expect_err("unfinished composite barrier blocks recovery");
    assert!(matches!(error, StoreError::RecoveryBlocked { .. }));
}

#[allow(dead_code)]
fn _typed_non_commitment_reference() -> Prerequisite {
    Prerequisite::TethersVerification(TethersContractRef::try_new("contract-1").expect("contract"))
}
