use resolve_core::{
    AcceptanceRef, AttentionId, AttentionItem, Commitment, CommitmentId, CommitmentState, GoalId,
    GoalRevision, GoalSpec, GoalState, HumanDecisionRef, MonotonicInstant, Prerequisite,
    TethersActionRef, TethersContractRef, Timestamp, WaitingReason, WorkEvent, WorkerId,
};
use resolve_store::{SqliteStore, StoreError};
use tempfile::tempdir;

fn goal_id() -> GoalId {
    GoalId::try_new("goal-1").expect("test identifier is valid")
}

fn goal() -> GoalSpec {
    GoalSpec::new(
        goal_id(),
        GoalRevision::initial(),
        "coordinate the bounded test goal",
        GoalState::Active,
        Timestamp::from_unix_seconds(1_700_000_000),
    )
    .expect("test goal is valid")
}

fn commitment_id(value: &str) -> CommitmentId {
    CommitmentId::try_new(value).expect("test identifier is valid")
}

fn worker_id(value: &str) -> WorkerId {
    WorkerId::try_new(value).expect("test identifier is valid")
}

fn commitment(id: &str, prerequisites: Vec<Prerequisite>) -> Commitment {
    Commitment::new(
        commitment_id(id),
        goal_id(),
        None,
        "persist the bounded commitment",
        prerequisites,
        vec![AcceptanceRef::try_new("acceptance-1").expect("test identifier is valid")],
    )
    .expect("test commitment is valid")
}

fn created_event(id: &CommitmentId) -> WorkEvent {
    WorkEvent::CommitmentCreated {
        commitment_id: id.clone(),
        goal_id: goal_id(),
    }
}

fn activated_event(id: &CommitmentId) -> WorkEvent {
    WorkEvent::CommitmentActivated {
        commitment_id: id.clone(),
    }
}

fn store_with_goal() -> (SqliteStore, GoalSpec) {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("in-memory store opens");
    let goal = goal();
    store
        .insert_goal(
            &goal,
            &WorkEvent::GoalCreated {
                goal_id: goal.goal_id().clone(),
                revision: goal.revision(),
            },
        )
        .expect("goal persists");
    (store, goal)
}

#[test]
fn successful_mutation_appends_event_and_increments_snapshot_version() {
    let (mut store, _) = store_with_goal();
    let commitment = commitment("commitment-1", Vec::new());
    let event_id = store
        .insert_commitment(&commitment, &created_event(commitment.commitment_id()), 2)
        .expect("commitment persists");

    let record = store
        .load_commitment_record(commitment.commitment_id())
        .expect("commitment projection loads");
    assert_eq!(record.state(), &CommitmentState::Proposed);
    assert_eq!(record.state_version(), 1);
    assert_eq!(record.prerequisites(), &[]);
    assert_eq!(record.acceptance_refs(), commitment.acceptance_refs());

    let events = store.list_events().expect("events load");
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_id(), event_id);
    assert_eq!(events[1].event_type(), "commitment_created");
}

#[test]
fn failure_before_event_append_preserves_old_state() {
    let (mut store, _) = store_with_goal();
    let mut commitment = commitment("commitment-1", Vec::new());
    store
        .insert_commitment(&commitment, &created_event(commitment.commitment_id()), 2)
        .expect("commitment persists");
    commitment.activate().expect("commitment activates");
    let event = activated_event(commitment.commitment_id());

    let error = store
        .persist_commitment_change(&commitment, 99, &event, 3)
        .expect_err("stale version is rejected before append");
    assert!(matches!(error, StoreError::VersionConflict { .. }));
    assert_eq!(store.list_events().expect("events load").len(), 2);
    assert_eq!(
        store
            .load_commitment_record(commitment.commitment_id())
            .expect("old projection loads")
            .state_version(),
        1
    );
}

#[test]
fn failure_after_event_append_before_snapshot_rolls_back_event() {
    let (mut store, _) = store_with_goal();
    let original = commitment("commitment-1", Vec::new());
    store
        .insert_commitment(&original, &created_event(original.commitment_id()), 2)
        .expect("commitment persists");
    let duplicate = commitment("commitment-1", Vec::new());

    let error = store
        .insert_commitment(&duplicate, &created_event(duplicate.commitment_id()), 3)
        .expect_err("duplicate snapshot insert fails");
    assert!(matches!(error, StoreError::Sqlite(_)));
    assert_eq!(store.list_events().expect("events load").len(), 2);
    assert_eq!(
        store
            .load_commitment_record(original.commitment_id())
            .expect("original projection loads")
            .state_version(),
        1
    );
}

#[test]
fn failure_after_snapshot_update_before_commit_rolls_back_both() {
    let (mut store, _) = store_with_goal();
    let original = commitment("commitment-1", Vec::new());
    store
        .insert_commitment(&original, &created_event(original.commitment_id()), 2)
        .expect("commitment persists");

    let missing = Prerequisite::Commitment(commitment_id("missing-commitment"));
    let mut changed = commitment("commitment-1", vec![missing]);
    changed.activate().expect("changed commitment activates");
    let error = store
        .persist_commitment_change(&changed, 1, &activated_event(changed.commitment_id()), 3)
        .expect_err("foreign-key failure rolls back transaction");
    assert!(matches!(error, StoreError::Sqlite(_)));
    assert_eq!(store.list_events().expect("events load").len(), 2);
    let record = store
        .load_commitment_record(original.commitment_id())
        .expect("original projection loads");
    assert_eq!(record.state(), &CommitmentState::Proposed);
    assert_eq!(record.state_version(), 1);
    assert!(record.prerequisites().is_empty());
}

#[test]
fn stale_state_version_allows_only_one_of_two_callers() {
    let (mut store, _) = store_with_goal();
    let original = commitment("commitment-1", Vec::new());
    store
        .insert_commitment(&original, &created_event(original.commitment_id()), 2)
        .expect("commitment persists");
    let mut changed = commitment("commitment-1", Vec::new());
    changed.activate().expect("commitment activates");
    let event = activated_event(changed.commitment_id());

    store
        .persist_commitment_change(&changed, 1, &event, 3)
        .expect("first caller succeeds");
    let error = store
        .persist_commitment_change(&changed, 1, &event, 3)
        .expect_err("second caller has stale version");
    assert!(matches!(error, StoreError::VersionConflict { .. }));
    assert_eq!(store.list_events().expect("events load").len(), 3);
    assert_eq!(
        store
            .load_commitment_record(original.commitment_id())
            .expect("projection loads")
            .state_version(),
        2
    );
}

#[test]
fn file_store_uses_wal_and_foreign_keys() {
    let directory = tempdir().expect("temporary directory opens");
    let path = directory.path().join("resolve.sqlite3");
    let store = SqliteStore::open(&path).expect("file store opens");

    assert_eq!(
        store
            .journal_mode()
            .expect("journal mode reads")
            .to_lowercase(),
        "wal"
    );
    assert!(
        store
            .foreign_keys_enabled()
            .expect("foreign-key setting reads")
    );
    assert_eq!(store.schema_version().expect("schema version reads"), 1);
}

#[test]
fn goal_revisions_are_append_only_in_storage() {
    let (mut store, original) = store_with_goal();
    let next_revision = original.revision().next().expect("revision advances");
    let revised = original
        .revised(next_revision, "the revised persisted goal")
        .expect("next revision is valid");
    store
        .persist_goal_change(
            &revised,
            1,
            &WorkEvent::GoalRevised {
                goal_id: revised.goal_id().clone(),
                revision: revised.revision(),
            },
        )
        .expect("revision persists");

    let record = store
        .load_goal_record(revised.goal_id())
        .expect("goal loads");
    assert_eq!(record.revision(), 2);
    assert_eq!(record.description(), revised.description());
    assert_eq!(record.state_version(), 2);

    let error = store
        .persist_goal_change(
            &revised,
            2,
            &WorkEvent::GoalRevised {
                goal_id: revised.goal_id().clone(),
                revision: revised.revision(),
            },
        )
        .expect_err("historical revision cannot be overwritten");
    assert!(matches!(
        error,
        StoreError::DuplicateImmutableRevision { .. }
    ));
    assert_eq!(store.list_events().expect("events load").len(), 2);
}

#[test]
fn normalized_references_claim_projection_and_attention_round_trip() {
    let (mut store, _) = store_with_goal();
    let dependency = commitment("dependency-1", Vec::new());
    store
        .insert_commitment(&dependency, &created_event(dependency.commitment_id()), 2)
        .expect("dependency persists");
    let mut commitment = commitment(
        "commitment-1",
        vec![
            Prerequisite::Commitment(dependency.commitment_id().clone()),
            Prerequisite::TethersVerification(
                TethersContractRef::try_new("contract-1").expect("test identifier is valid"),
            ),
            Prerequisite::HumanDecision(
                HumanDecisionRef::try_new("decision-1").expect("test identifier is valid"),
            ),
        ],
    );
    store
        .insert_commitment(&commitment, &created_event(commitment.commitment_id()), 2)
        .expect("commitment persists");
    commitment.activate().expect("commitment activates");
    let claim = commitment
        .claim_for(worker_id("worker-1"), MonotonicInstant::from_ticks(44))
        .expect("claim succeeds");
    store
        .persist_commitment_change(
            &commitment,
            1,
            &WorkEvent::CommitmentClaimed {
                commitment_id: commitment.commitment_id().clone(),
                worker_id: claim.worker_id().clone(),
                epoch: claim.epoch(),
            },
            3,
        )
        .expect("claimed projection persists");

    let record = store
        .load_commitment_record(commitment.commitment_id())
        .expect("commitment projection loads");
    assert_eq!(record.prerequisites(), commitment.prerequisites());
    assert_eq!(record.acceptance_refs(), commitment.acceptance_refs());
    let persisted_claim = record.claim().expect("claim projection exists");
    assert_eq!(persisted_claim.worker_id(), claim.worker_id());
    assert_eq!(persisted_claim.epoch(), claim.epoch().value());
    assert_eq!(persisted_claim.last_heartbeat(), 44);

    let attention = AttentionItem::new(
        AttentionId::try_new("attention-1").expect("test identifier is valid"),
        Some(commitment.commitment_id().clone()),
        "human decision required",
    )
    .expect("attention item is valid");
    store
        .insert_attention(
            &goal_id(),
            &attention,
            &WorkEvent::AttentionRaised {
                attention_id: attention.attention_id().clone(),
                commitment_id: attention.commitment_id().cloned(),
            },
            4,
        )
        .expect("attention persists");
    let attention_record = store
        .load_attention_record(attention.attention_id())
        .expect("attention projection loads");
    assert_eq!(attention_record.description(), attention.description());
    assert_eq!(attention_record.state(), attention.state());
}

#[test]
fn uncertain_waiting_reason_is_typed_in_snapshot_and_event_payload() {
    let (mut store, _) = store_with_goal();
    let mut commitment = commitment("commitment-1", Vec::new());
    store
        .insert_commitment(&commitment, &created_event(commitment.commitment_id()), 2)
        .expect("commitment persists");
    commitment.activate().expect("commitment activates");
    store
        .persist_commitment_change(
            &commitment,
            1,
            &activated_event(commitment.commitment_id()),
            3,
        )
        .expect("activation persists");
    let claim = commitment
        .claim_for(worker_id("worker-1"), MonotonicInstant::from_ticks(10))
        .expect("claim succeeds");
    store
        .persist_commitment_change(
            &commitment,
            2,
            &WorkEvent::CommitmentClaimed {
                commitment_id: commitment.commitment_id().clone(),
                worker_id: claim.worker_id().clone(),
                epoch: claim.epoch(),
            },
            4,
        )
        .expect("claim persists");
    commitment
        .start(claim.worker_id(), claim.epoch())
        .expect("claim starts work");
    store
        .persist_commitment_change(
            &commitment,
            3,
            &WorkEvent::CommitmentStarted {
                commitment_id: commitment.commitment_id().clone(),
            },
            5,
        )
        .expect("start persists");
    let action_ref = TethersActionRef::try_new("action-1").expect("test identifier is valid");
    let reason = WaitingReason::UncertainAction(action_ref);
    commitment
        .wait(claim.worker_id(), claim.epoch(), reason.clone())
        .expect("uncertain waiting state is valid");
    store
        .persist_commitment_change(
            &commitment,
            4,
            &WorkEvent::CommitmentWaiting {
                commitment_id: commitment.commitment_id().clone(),
                reason,
            },
            6,
        )
        .expect("uncertain waiting state persists");

    let record = store
        .load_commitment_record(commitment.commitment_id())
        .expect("commitment projection loads");
    assert_eq!(record.state(), commitment.state());
    assert!(matches!(
        record.state(),
        CommitmentState::Waiting(WaitingReason::UncertainAction(_))
    ));
    let event = store
        .list_events()
        .expect("events load")
        .pop()
        .expect("event exists");
    assert_eq!(event.event_type(), "commitment_waiting");
    assert!(event.payload_json().contains("UncertainAction"));
    assert!(!event.payload_json().contains("Failed"));
}
