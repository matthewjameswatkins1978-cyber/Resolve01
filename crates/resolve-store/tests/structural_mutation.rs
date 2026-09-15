use resolve_core::{
    BootGeneration, ClaimLease, Commitment, CommitmentId, CommitmentState, GoalId, GoalRevision,
    GoalSpec, GoalState, MonotonicDuration, MonotonicInstant, NewCommitment, Prerequisite,
    StructuralProposal, TethersContractRef, Timestamp, WaitingReason, WorkEvent, WorkerId,
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

fn make_working(
    store: &mut SqliteStore,
    mut commitment: Commitment,
    heartbeat: u64,
) -> (Commitment, ClaimLease) {
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
        .claim_for(worker_id(), MonotonicInstant::from_ticks(heartbeat))
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

fn store_with_working_target_at(heartbeat: u64) -> (SqliteStore, Commitment, ClaimLease) {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    prepare_goal(&mut store);
    let target = add_commitment(&mut store, "target", None, Vec::new());
    let (target, claim) = make_working(&mut store, target, heartbeat);
    (store, target, claim)
}

fn store_with_working_target() -> (SqliteStore, Commitment, ClaimLease) {
    store_with_working_target_at(1)
}

fn store_with_claimed_target() -> (SqliteStore, Commitment, ClaimLease) {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    prepare_goal(&mut store);
    let mut target = add_commitment(&mut store, "target", None, Vec::new());
    target.activate().expect("activation");
    store
        .persist_commitment_change(
            &target,
            1,
            &WorkEvent::CommitmentActivated {
                commitment_id: target.commitment_id().clone(),
            },
            2,
        )
        .expect("activation persists");
    let claim = target
        .claim_for(worker_id(), MonotonicInstant::from_ticks(10))
        .expect("claim");
    store
        .persist_commitment_change(
            &target,
            2,
            &WorkEvent::CommitmentClaimed {
                commitment_id: target.commitment_id().clone(),
                worker_id: worker_id(),
                epoch: claim.epoch(),
            },
            3,
        )
        .expect("claim persists");
    (store, target, claim)
}

fn decompose_proposal(target: &Commitment, claim: &ClaimLease) -> StructuralProposal {
    StructuralProposal::Decompose {
        target: target.commitment_id().clone(),
        epoch: claim.epoch(),
        children: vec![
            NewCommitment::new(commitment_id("child"), "child", Vec::new(), Vec::new())
                .expect("child"),
        ],
        replacement_terminal_id: commitment_id("child"),
    }
}

fn decompose_one_child(store: &mut SqliteStore, target: &Commitment, claim: &ClaimLease) {
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
            MonotonicDuration::try_from_ticks(99).expect("duration"),
            10,
        )
        .expect("decomposition applies");
}

fn complete_child(store: &mut SqliteStore, id: &str) {
    let mut child = store
        .load_commitment(&commitment_id(id))
        .expect("child restores");
    child.activate().expect("child activates");
    store
        .persist_commitment_change(
            &child,
            1,
            &WorkEvent::CommitmentActivated {
                commitment_id: commitment_id(id),
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
                commitment_id: commitment_id(id),
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
                commitment_id: commitment_id(id),
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
                commitment_id: commitment_id(id),
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
                commitment_id: commitment_id(id),
            },
            15,
        )
        .expect("child completion persists");
}

#[test]
fn expired_claim_cannot_add_prerequisite_and_live_claim_can() {
    let (mut store, target, claim) = store_with_working_target_at(10);
    add_commitment(&mut store, "dependency", None, Vec::new());
    let before_events = store.list_events().expect("events load").len();
    let proposal = StructuralProposal::AddPrerequisite {
        target: target.commitment_id().clone(),
        prerequisite: Prerequisite::Commitment(commitment_id("dependency")),
        epoch: claim.epoch(),
    };
    let error = store
        .apply_structural_proposal(
            proposal.clone(),
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(20),
            MonotonicDuration::try_from_ticks(10).expect("duration"),
            20,
        )
        .expect_err("expired claim cannot structurally mutate");
    assert!(matches!(error, StoreError::LeaseExpired { .. }));
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );
    assert!(
        store
            .load_commitment_record(target.commitment_id())
            .expect("target loads")
            .prerequisites()
            .is_empty()
    );

    store
        .apply_structural_proposal(
            proposal,
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(19),
            MonotonicDuration::try_from_ticks(10).expect("duration"),
            21,
        )
        .expect("live claim can structurally mutate");
    assert_eq!(
        store
            .load_commitment_record(target.commitment_id())
            .expect("target reloads")
            .prerequisites(),
        &[Prerequisite::Commitment(commitment_id("dependency"))]
    );
}

#[test]
fn expired_claim_cannot_decompose_and_live_claim_can() {
    let (mut store, target, claim) = store_with_working_target_at(10);
    let child =
        NewCommitment::new(commitment_id("child"), "child", Vec::new(), Vec::new()).expect("child");
    let proposal = StructuralProposal::Decompose {
        target: target.commitment_id().clone(),
        epoch: claim.epoch(),
        children: vec![child],
        replacement_terminal_id: commitment_id("child"),
    };
    let before_events = store.list_events().expect("events load").len();
    let error = store
        .apply_structural_proposal(
            proposal.clone(),
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(20),
            MonotonicDuration::try_from_ticks(10).expect("duration"),
            20,
        )
        .expect_err("expired claim cannot decompose");
    assert!(matches!(error, StoreError::LeaseExpired { .. }));
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );
    assert!(matches!(
        store.load_commitment_record(&commitment_id("child")),
        Err(StoreError::NotFound { .. })
    ));
    store
        .apply_structural_proposal(
            proposal,
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(19),
            MonotonicDuration::try_from_ticks(10).expect("duration"),
            21,
        )
        .expect("live claim can decompose");
}

#[test]
fn decomposition_is_working_only() {
    let cases = ["claimed", "waiting", "completion-proposed"];
    for case in cases {
        let (mut store, mut target, claim) = if case == "claimed" {
            store_with_claimed_target()
        } else {
            store_with_working_target()
        };
        if case == "waiting" {
            target
                .wait(
                    claim.worker_id(),
                    claim.epoch(),
                    WaitingReason::External("pause".to_owned()),
                )
                .expect("wait");
            store
                .persist_commitment_change(
                    &target,
                    4,
                    &WorkEvent::CommitmentWaiting {
                        commitment_id: target.commitment_id().clone(),
                        reason: WaitingReason::External("pause".to_owned()),
                    },
                    5,
                )
                .expect("waiting persists");
        } else if case == "completion-proposed" {
            target
                .propose_completion(claim.worker_id(), claim.epoch())
                .expect("completion proposal");
            store
                .persist_commitment_change(
                    &target,
                    4,
                    &WorkEvent::CommitmentCompletionProposed {
                        commitment_id: target.commitment_id().clone(),
                    },
                    5,
                )
                .expect("completion proposal persists");
        }
        let before_events = store.list_events().expect("events load").len();
        let error = store
            .apply_structural_proposal(
                decompose_proposal(&target, &claim),
                claim.worker_id(),
                BootGeneration::from_raw(0),
                MonotonicInstant::from_ticks(10),
                MonotonicDuration::try_from_ticks(99).expect("duration"),
                10,
            )
            .expect_err("decomposition must require WORKING");
        assert!(matches!(
            error,
            StoreError::Domain(resolve_core::DomainError::InvalidStructuralProposal { .. })
        ));
        assert_eq!(
            store.list_events().expect("events load").len(),
            before_events
        );
        assert!(matches!(
            store.load_commitment_record(&commitment_id("child")),
            Err(StoreError::NotFound { .. })
        ));
    }
}

#[test]
fn active_composite_barrier_rejects_resume_completion_and_new_prerequisite() {
    let (mut store, target, claim) = store_with_working_target();
    decompose_one_child(&mut store, &target, &claim);
    let before_events = store.list_events().expect("events load").len();

    let mut resumed = store
        .load_commitment(target.commitment_id())
        .expect("parent restores");
    resumed
        .resume(
            claim.worker_id(),
            claim.epoch(),
            resolve_core::ResumeTarget::Working,
        )
        .expect("domain resume remains representable");
    let error = store
        .persist_commitment_change(
            &resumed,
            5,
            &WorkEvent::CommitmentStarted {
                commitment_id: target.commitment_id().clone(),
            },
            11,
        )
        .expect_err("ordinary resume cannot bypass composite barrier");
    assert!(matches!(
        error,
        StoreError::Domain(resolve_core::DomainError::InvalidStructuralProposal { .. })
    ));
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );

    let mut proposed = store
        .load_commitment(target.commitment_id())
        .expect("parent restores");
    proposed
        .resume(
            claim.worker_id(),
            claim.epoch(),
            resolve_core::ResumeTarget::Working,
        )
        .expect("resume");
    proposed
        .propose_completion(claim.worker_id(), claim.epoch())
        .expect("completion proposal remains representable");
    let error = store
        .persist_commitment_change(
            &proposed,
            5,
            &WorkEvent::CommitmentCompletionProposed {
                commitment_id: target.commitment_id().clone(),
            },
            12,
        )
        .expect_err("completion proposal cannot bypass composite barrier");
    assert!(matches!(
        error,
        StoreError::Domain(resolve_core::DomainError::InvalidStructuralProposal { .. })
    ));

    let error = store
        .apply_structural_proposal(
            StructuralProposal::AddPrerequisite {
                target: target.commitment_id().clone(),
                prerequisite: Prerequisite::TethersVerification(
                    TethersContractRef::try_new("contract-1").expect("contract"),
                ),
                epoch: claim.epoch(),
            },
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(10),
            MonotonicDuration::try_from_ticks(99).expect("duration"),
            13,
        )
        .expect_err("active composite barrier rejects added prerequisites");
    assert!(matches!(
        error,
        StoreError::Domain(resolve_core::DomainError::InvalidStructuralProposal { .. })
    ));
    assert_eq!(
        store.list_events().expect("events load").len(),
        before_events
    );
    assert_eq!(
        store
            .load_commitment_record(target.commitment_id())
            .expect("parent reloads")
            .state(),
        &CommitmentState::Waiting(WaitingReason::Prerequisite(commitment_id("child")))
    );
}

#[test]
fn abandoned_and_cancelled_composite_parents_survive_child_completion() {
    let (mut store, target, claim) = store_with_working_target();
    decompose_one_child(&mut store, &target, &claim);
    store
        .apply_structural_proposal(
            StructuralProposal::Abandon {
                target: target.commitment_id().clone(),
                epoch: claim.epoch(),
                rationale: "authority withdrew the composite work".to_owned(),
            },
            claim.worker_id(),
            BootGeneration::from_raw(0),
            MonotonicInstant::from_ticks(10),
            MonotonicDuration::try_from_ticks(99).expect("duration"),
            11,
        )
        .expect("abandon applies");
    assert_eq!(
        store
            .load_commitment_record(target.commitment_id())
            .expect("parent loads")
            .state(),
        &CommitmentState::Abandoned
    );
    assert!(
        store
            .load_commitment_record(target.commitment_id())
            .expect("parent loads")
            .replacement_terminal_id()
            .is_some()
    );
    complete_child(&mut store, "child");
    assert_eq!(
        store
            .load_commitment_record(target.commitment_id())
            .expect("abandoned parent remains")
            .state(),
        &CommitmentState::Abandoned
    );

    let (mut store, target, claim) = store_with_working_target();
    decompose_one_child(&mut store, &target, &claim);
    let mut parent = store
        .load_commitment(target.commitment_id())
        .expect("parent restores");
    parent.cancel().expect("explicit cancellation");
    store
        .persist_commitment_change(
            &parent,
            5,
            &WorkEvent::CommitmentCancelled {
                commitment_id: target.commitment_id().clone(),
            },
            11,
        )
        .expect("cancellation persists");
    complete_child(&mut store, "child");
    assert_eq!(
        store
            .load_commitment_record(target.commitment_id())
            .expect("cancelled parent remains")
            .state(),
        &CommitmentState::Cancelled
    );
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
            MonotonicDuration::try_from_ticks(99).expect("duration"),
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
    let (_dependency, dependency_claim) = make_working(&mut store, dependency, 1);
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
            MonotonicDuration::try_from_ticks(99).expect("duration"),
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
            MonotonicDuration::try_from_ticks(99).expect("duration"),
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
            MonotonicDuration::try_from_ticks(99).expect("duration"),
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
            MonotonicDuration::try_from_ticks(99).expect("duration"),
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
