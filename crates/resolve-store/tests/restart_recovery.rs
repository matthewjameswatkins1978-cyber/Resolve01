use resolve_core::{
    ClaimEpoch, ClaimLease, Commitment, CommitmentId, CommitmentState, ExecutionGuard, GoalId,
    GoalRevision, GoalSpec, GoalState, GuardId, GuardState, MonotonicDuration, MonotonicInstant,
    ScopeKey, ScopeSet, TethersActionRef, TethersOutcome, Timestamp, WaitingReason, WorkEvent,
    WorkerId,
};
use resolve_store::{
    GuardIssueRequest, HeartbeatResult, OutcomeRecording, SqliteStore, StoreError,
};
use rusqlite::Connection;
use tempfile::tempdir;

fn goal_id() -> GoalId {
    GoalId::try_new("goal-1").expect("valid test id")
}

fn commitment_id(value: &str) -> CommitmentId {
    CommitmentId::try_new(value).expect("valid test id")
}

fn worker_id(value: &str) -> WorkerId {
    WorkerId::try_new(value).expect("valid test id")
}

fn guard_id(value: &str) -> GuardId {
    GuardId::try_new(value).expect("valid test id")
}

fn scope(value: &str) -> ScopeKey {
    ScopeKey::try_new(value).expect("valid test scope")
}

fn action(value: &str) -> TethersActionRef {
    TethersActionRef::try_new(value).expect("valid test action")
}

fn setup_goal(store: &mut SqliteStore) {
    let goal = GoalSpec::new(
        goal_id(),
        GoalRevision::initial(),
        "recover the bounded commitment",
        GoalState::Active,
        Timestamp::from_unix_seconds(0),
    )
    .expect("valid test goal");
    store
        .insert_goal(
            &goal,
            &WorkEvent::GoalCreated {
                goal_id: goal_id(),
                revision: GoalRevision::initial(),
            },
        )
        .expect("goal persists");
}

fn setup_claimed(store: &mut SqliteStore, id: &str) -> (Commitment, ClaimLease) {
    let id = commitment_id(id);
    let mut commitment = Commitment::new(
        id.clone(),
        goal_id(),
        None,
        "perform bounded work",
        Vec::new(),
        Vec::new(),
    )
    .expect("valid test commitment");
    store
        .insert_commitment(
            &commitment,
            &WorkEvent::CommitmentCreated {
                commitment_id: id.clone(),
                goal_id: goal_id(),
            },
            1,
        )
        .expect("commitment persists");
    commitment.activate().expect("commitment activates");
    store
        .persist_commitment_change(
            &commitment,
            1,
            &WorkEvent::CommitmentActivated {
                commitment_id: id.clone(),
            },
            2,
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
                commitment_id: id,
                worker_id: claim.worker_id().clone(),
                epoch: claim.epoch(),
            },
            3,
        )
        .expect("claim persists");
    (commitment, claim)
}

fn setup_working(store: &mut SqliteStore, id: &str) -> (Commitment, ClaimLease) {
    let (mut commitment, claim) = setup_claimed(store, id);
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
            4,
        )
        .expect("working state persists");
    (commitment, claim)
}

fn issue(
    store: &mut SqliteStore,
    guard: &str,
    commitment: &Commitment,
    claim: &ClaimLease,
    scope_key: &str,
) -> ExecutionGuard {
    store
        .issue_guard(GuardIssueRequest {
            guard_id: guard_id(guard),
            commitment_id: commitment.commitment_id().clone(),
            worker_id: claim.worker_id().clone(),
            claim_epoch: claim.epoch(),
            scope_keys: ScopeSet::try_new(vec![scope(scope_key)]).expect("scope set is valid"),
            boot_generation: store.boot_generation().expect("boot generation reads"),
            now: MonotonicInstant::from_ticks(20),
            guard_ttl: MonotonicDuration::try_from_ticks(100).expect("duration is valid"),
            claim_lease_duration: MonotonicDuration::try_from_ticks(100).expect("duration valid"),
        })
        .expect("guard issues")
}

fn create_v2_fixture(path: &std::path::Path, claim_event_payload: &str) {
    let connection = Connection::open(path).expect("fixture database opens");
    let escaped_payload = claim_event_payload.replace('\'', "''");
    connection
        .execute_batch(&format!(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
             INSERT INTO metadata VALUES ('schema_version', '2');
             INSERT INTO metadata VALUES ('boot_generation', '0');
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
                 outstanding_action TEXT, state_version INTEGER NOT NULL
             );
             CREATE TABLE commitment_prerequisites (
                 commitment_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                 prerequisite_type TEXT NOT NULL, reference_id TEXT NOT NULL,
                 prerequisite_commitment_id TEXT
             );
             CREATE TABLE commitment_acceptance_refs (
                 commitment_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                 reference_id TEXT NOT NULL
             );
             CREATE TABLE claims (
                 commitment_id TEXT PRIMARY KEY NOT NULL, worker_id TEXT NOT NULL,
                 claim_epoch INTEGER NOT NULL, last_heartbeat INTEGER NOT NULL,
                 boot_generation INTEGER NOT NULL
             );
             CREATE TABLE attention_items (
                 attention_id TEXT PRIMARY KEY NOT NULL, commitment_id TEXT,
                 description TEXT NOT NULL, state TEXT NOT NULL
             );
             CREATE TABLE work_events (
                 event_id INTEGER PRIMARY KEY AUTOINCREMENT, goal_id TEXT NOT NULL,
                 commitment_id TEXT, event_type TEXT NOT NULL,
                 payload_json TEXT NOT NULL, created_at INTEGER NOT NULL
             );
             CREATE TABLE execution_guards (
                 guard_id TEXT PRIMARY KEY NOT NULL, commitment_id TEXT NOT NULL,
                 claim_epoch INTEGER NOT NULL, boot_generation INTEGER NOT NULL,
                 state TEXT NOT NULL, action_ref TEXT, outcome_json TEXT,
                 reservation_expires_at INTEGER, state_version INTEGER NOT NULL
             );
             CREATE TABLE execution_guard_scopes (
                 guard_id TEXT NOT NULL, ordinal INTEGER NOT NULL, scope_key TEXT NOT NULL
             );
             CREATE TABLE scope_locks (
                 scope_key TEXT PRIMARY KEY NOT NULL, guard_id TEXT NOT NULL,
                 commitment_id TEXT NOT NULL, lock_state TEXT NOT NULL,
                 action_ref TEXT, reservation_expires_at INTEGER
             );
             INSERT INTO goals VALUES ('goal-legacy', 1, 'legacy', 'active', 1, 1);
             INSERT INTO commitments VALUES
                 ('commitment-legacy', 'goal-legacy', NULL, 'legacy commitment',
                  '{{\"state\":\"Proposed\"}}', NULL, 1);
             INSERT INTO work_events
                 (goal_id, commitment_id, event_type, payload_json, created_at)
             VALUES ('goal-legacy', 'commitment-legacy', 'commitment_claimed', '{}', 2);",
            escaped_payload
        ))
        .expect("v2 fixture creates");
}

#[test]
fn restart_invalidates_claims_and_preserves_epoch_high_water() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("resolve.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    setup_goal(&mut store);
    setup_working(&mut store, "commitment-1");
    drop(store);

    let mut reopened = SqliteStore::open(&path).expect("startup recovery runs");
    assert_eq!(
        reopened
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("commitment loads")
            .state(),
        &CommitmentState::RecoveryPending
    );
    assert!(
        reopened
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("commitment loads")
            .claim()
            .is_none()
    );
    reopened
        .recover_without_action(&commitment_id("commitment-1"), 30)
        .expect("no-action recovery releases commitment");
    let mut restored = reopened
        .load_commitment(&commitment_id("commitment-1"))
        .expect("validated commitment restoration succeeds");
    let claim = restored
        .claim_for(worker_id("worker-2"), MonotonicInstant::from_ticks(40))
        .expect("next claim succeeds");
    assert_eq!(
        claim.epoch().value(),
        2,
        "claim epoch high-water is retained"
    );
}

#[test]
fn admitted_held_locks_survive_restart_and_safe_outcome_releases_them() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("resolve.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    setup_goal(&mut store);
    let (commitment, claim) = setup_working(&mut store, "commitment-1");
    issue(&mut store, "guard-1", &commitment, &claim, "exclusive");
    store
        .admit_guard(
            &guard_id("guard-1"),
            ScopeSet::try_new(vec![scope("exclusive")]).expect("scope set is valid"),
            action("action-1"),
            MonotonicInstant::from_ticks(25),
        )
        .expect("guard admits");
    drop(store);

    let mut reopened = SqliteStore::open(&path).expect("startup recovery runs");
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("guard-1"))
            .expect("guard loads")
            .state(),
        &GuardState::Admitted {
            action_ref: action("action-1")
        }
    );
    assert_eq!(
        reopened
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("commitment loads")
            .state(),
        &CommitmentState::RecoveryPending
    );
    assert_eq!(
        reopened
            .record_tethers_outcome(
                TethersOutcome::Succeeded {
                    action_ref: action("action-1"),
                },
                40,
            )
            .expect("safe outcome records"),
        OutcomeRecording::Recorded
    );
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("guard-1"))
            .expect("resolved guard loads")
            .state(),
        &GuardState::Resolved {
            action_ref: action("action-1"),
            outcome: TethersOutcome::Succeeded {
                action_ref: action("action-1"),
            }
        }
    );
    assert_eq!(
        reopened
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("ready commitment loads")
            .state(),
        &CommitmentState::Ready
    );
    assert_eq!(
        reopened
            .record_tethers_outcome(
                TethersOutcome::Succeeded {
                    action_ref: action("action-1"),
                },
                41,
            )
            .expect("safe outcome redelivery is idempotent"),
        OutcomeRecording::AlreadyRecorded
    );
}

#[test]
fn issued_reservations_die_on_restart() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("resolve.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    setup_goal(&mut store);
    let (commitment, claim) = setup_working(&mut store, "commitment-1");
    issue(&mut store, "guard-1", &commitment, &claim, "reusable");
    drop(store);

    let mut reopened = SqliteStore::open(&path).expect("startup recovery runs");
    assert_eq!(
        reopened
            .load_guard_record(&guard_id("guard-1"))
            .expect("guard loads")
            .state(),
        &GuardState::Invalidated
    );
    let (second, second_claim) = setup_working(&mut reopened, "commitment-2");
    issue(&mut reopened, "guard-2", &second, &second_claim, "reusable");
}

#[test]
fn uncertain_outcome_remains_fenced_across_restart() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("resolve.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    setup_goal(&mut store);
    let (commitment, claim) = setup_working(&mut store, "commitment-1");
    issue(
        &mut store,
        "guard-1",
        &commitment,
        &claim,
        "uncertain-scope",
    );
    store
        .admit_guard(
            &guard_id("guard-1"),
            ScopeSet::try_new(vec![scope("uncertain-scope")]).expect("scope set is valid"),
            action("action-1"),
            MonotonicInstant::from_ticks(25),
        )
        .expect("guard admits");
    assert_eq!(
        store
            .record_tethers_outcome(
                TethersOutcome::Uncertain {
                    action_ref: action("action-1"),
                },
                30,
            )
            .expect("uncertain outcome records"),
        OutcomeRecording::Recorded
    );
    drop(store);

    let mut reopened = SqliteStore::open(&path).expect("uncertain state recovers");
    let restored = reopened
        .load_commitment(&commitment_id("commitment-1"))
        .expect("uncertain commitment restores");
    assert!(matches!(restored.state(), CommitmentState::Waiting(_)));
    let error = restored
        .clone()
        .resume(
            &worker_id("worker-1"),
            ClaimEpoch::initial(),
            resolve_core::ResumeTarget::Working,
        )
        .expect_err("ordinary resume cannot bypass uncertain recovery");
    assert!(matches!(
        error,
        resolve_core::DomainError::RecoveryRequired { .. }
    ));
    assert_eq!(
        reopened
            .record_tethers_outcome(
                TethersOutcome::Uncertain {
                    action_ref: action("action-1"),
                },
                31,
            )
            .expect("uncertain redelivery is idempotent"),
        OutcomeRecording::AlreadyRecorded
    );
    let conflict = reopened
        .record_tethers_outcome(
            TethersOutcome::Succeeded {
                action_ref: action("action-1"),
            },
            32,
        )
        .expect_err("contradictory outcome is fenced");
    assert!(matches!(conflict, StoreError::OutcomeConflict { .. }));
}

#[test]
fn heartbeat_is_monotonic_and_expiry_is_transactional() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    setup_goal(&mut store);
    let (commitment, claim) = setup_working(&mut store, "commitment-1");
    assert_eq!(
        store
            .record_heartbeat(
                commitment.commitment_id(),
                claim.worker_id(),
                claim.epoch(),
                MonotonicInstant::from_ticks(15),
                MonotonicDuration::try_from_ticks(10).expect("duration valid"),
            )
            .expect("heartbeat records"),
        HeartbeatResult::Recorded
    );
    assert_eq!(
        store
            .record_heartbeat(
                commitment.commitment_id(),
                claim.worker_id(),
                claim.epoch(),
                MonotonicInstant::from_ticks(15),
                MonotonicDuration::try_from_ticks(10).expect("duration valid"),
            )
            .expect("duplicate heartbeat is idempotent"),
        HeartbeatResult::AlreadyCurrent
    );
    let error = store
        .record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            claim.epoch(),
            MonotonicInstant::from_ticks(14),
            MonotonicDuration::try_from_ticks(10).expect("duration valid"),
        )
        .expect_err("backwards heartbeat is rejected");
    assert!(matches!(error, StoreError::InvalidMutation(_)));
    assert_eq!(
        store
            .process_expired_claims(
                MonotonicInstant::from_ticks(26),
                MonotonicDuration::try_from_ticks(10).expect("duration valid"),
            )
            .expect("expiry processes"),
        1
    );
    assert_eq!(
        store
            .load_commitment_record(commitment.commitment_id())
            .expect("commitment loads")
            .state(),
        &CommitmentState::RecoveryPending
    );
}

#[test]
fn late_heartbeat_cannot_resurrect_an_expired_claim() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    setup_goal(&mut store);
    let (commitment, claim) = setup_working(&mut store, "commitment-1");
    store
        .record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            claim.epoch(),
            MonotonicInstant::from_ticks(15),
            MonotonicDuration::try_from_ticks(10).expect("duration valid"),
        )
        .expect("live heartbeat records");
    let event_count = store.list_events().expect("events load").len();
    let error = store
        .record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            claim.epoch(),
            MonotonicInstant::from_ticks(25),
            MonotonicDuration::try_from_ticks(10).expect("duration valid"),
        )
        .expect_err("heartbeat at the exact lease deadline is expired");
    assert!(matches!(error, StoreError::LeaseExpired { .. }));
    assert_eq!(store.list_events().expect("events load").len(), event_count);
    assert_eq!(
        store
            .load_commitment_record(commitment.commitment_id())
            .expect("commitment loads")
            .claim()
            .expect("claim remains current")
            .last_heartbeat(),
        15
    );
}

#[test]
fn waiting_claim_expires_to_recovery_and_next_epoch_is_allowed() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    setup_goal(&mut store);
    let (mut commitment, claim) = setup_working(&mut store, "commitment-waiting");
    commitment
        .wait(
            claim.worker_id(),
            claim.epoch(),
            WaitingReason::External("awaiting input".to_owned()),
        )
        .expect("working commitment waits");
    store
        .persist_commitment_change(
            &commitment,
            4,
            &WorkEvent::CommitmentWaiting {
                commitment_id: commitment.commitment_id().clone(),
                reason: WaitingReason::External("awaiting input".to_owned()),
            },
            5,
        )
        .expect("waiting state persists");
    store
        .record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            claim.epoch(),
            MonotonicInstant::from_ticks(15),
            MonotonicDuration::try_from_ticks(10).expect("duration valid"),
        )
        .expect("waiting claim heartbeat records");
    assert_eq!(
        store
            .process_expired_claims(
                MonotonicInstant::from_ticks(25),
                MonotonicDuration::try_from_ticks(10).expect("duration valid"),
            )
            .expect("waiting claim expires"),
        1
    );
    let restored = store
        .load_commitment(&commitment_id("commitment-waiting"))
        .expect("recovery-pending commitment restores");
    assert_eq!(restored.state(), &CommitmentState::RecoveryPending);
    assert!(restored.claim().is_none());
    assert_eq!(restored.last_claim_epoch(), Some(ClaimEpoch::initial()));
    store
        .recover_without_action(&commitment_id("commitment-waiting"), 30)
        .expect("recovery releases commitment");
    let mut next = store
        .load_commitment(&commitment_id("commitment-waiting"))
        .expect("ready commitment restores");
    let next_claim = next
        .claim_for(worker_id("worker-2"), MonotonicInstant::from_ticks(31))
        .expect("next claim succeeds");
    assert_eq!(next_claim.epoch().value(), 2);
}

#[test]
fn completion_proposed_claim_expires_to_recovery() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    setup_goal(&mut store);
    let (mut commitment, claim) = setup_working(&mut store, "commitment-completion");
    commitment
        .propose_completion(claim.worker_id(), claim.epoch())
        .expect("completion proposal succeeds");
    store
        .persist_commitment_change(
            &commitment,
            4,
            &WorkEvent::CommitmentCompletionProposed {
                commitment_id: commitment.commitment_id().clone(),
            },
            5,
        )
        .expect("completion proposal persists");
    store
        .record_heartbeat(
            commitment.commitment_id(),
            claim.worker_id(),
            claim.epoch(),
            MonotonicInstant::from_ticks(15),
            MonotonicDuration::try_from_ticks(10).expect("duration valid"),
        )
        .expect("completion proposal heartbeat records");
    assert_eq!(
        store
            .process_expired_claims(
                MonotonicInstant::from_ticks(25),
                MonotonicDuration::try_from_ticks(10).expect("duration valid"),
            )
            .expect("completion proposal claim expires"),
        1
    );
    let restored = store
        .load_commitment(&commitment_id("commitment-completion"))
        .expect("recovery-pending commitment restores");
    assert_eq!(restored.state(), &CommitmentState::RecoveryPending);
    assert!(restored.claim().is_none());
    assert_eq!(restored.last_claim_epoch(), Some(ClaimEpoch::initial()));
}

#[test]
fn waiting_and_completion_proposed_claims_recover_on_restart() {
    for (id, completion_proposed) in [
        ("commitment-waiting-restart", false),
        ("commitment-completion-restart", true),
    ] {
        let directory = tempdir().expect("temporary directory creates");
        let path = directory.path().join("resolve.sqlite");
        let mut store = SqliteStore::open(&path).expect("store opens");
        setup_goal(&mut store);
        let (mut commitment, claim) = setup_working(&mut store, id);
        if completion_proposed {
            commitment
                .propose_completion(claim.worker_id(), claim.epoch())
                .expect("completion proposal succeeds");
            store
                .persist_commitment_change(
                    &commitment,
                    4,
                    &WorkEvent::CommitmentCompletionProposed {
                        commitment_id: commitment.commitment_id().clone(),
                    },
                    5,
                )
                .expect("completion proposal persists");
        } else {
            commitment
                .wait(
                    claim.worker_id(),
                    claim.epoch(),
                    WaitingReason::External("awaiting input".to_owned()),
                )
                .expect("working commitment waits");
            store
                .persist_commitment_change(
                    &commitment,
                    4,
                    &WorkEvent::CommitmentWaiting {
                        commitment_id: commitment.commitment_id().clone(),
                        reason: WaitingReason::External("awaiting input".to_owned()),
                    },
                    5,
                )
                .expect("waiting state persists");
        }
        drop(store);

        let mut reopened = SqliteStore::open(&path).expect("startup recovery runs");
        let restored = reopened
            .load_commitment(&commitment_id(id))
            .expect("recovered commitment restores");
        assert_eq!(restored.state(), &CommitmentState::RecoveryPending);
        assert!(restored.claim().is_none());
        assert_eq!(restored.last_claim_epoch(), Some(ClaimEpoch::initial()));
        reopened
            .recover_without_action(&commitment_id(id), 30)
            .expect("recovery releases commitment");
        let mut next = reopened
            .load_commitment(&commitment_id(id))
            .expect("ready commitment restores");
        assert_eq!(
            next.claim_for(worker_id("worker-2"), MonotonicInstant::from_ticks(31))
                .expect("next claim succeeds")
                .epoch()
                .value(),
            2
        );
    }
}

#[test]
fn startup_rejects_claim_epoch_that_does_not_match_high_water() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("resolve.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    setup_goal(&mut store);
    setup_working(&mut store, "commitment-malformed-epoch");
    drop(store);

    let connection = Connection::open(&path).expect("database reopens");
    connection
        .execute(
            "UPDATE commitments SET last_claim_epoch = 2
             WHERE commitment_id = 'commitment-malformed-epoch'",
            [],
        )
        .expect("malformed high-water mark is written");
    drop(connection);

    assert!(matches!(
        SqliteStore::open(&path),
        Err(StoreError::InvalidPersistedData(_))
    ));
}

#[test]
fn startup_rejects_claim_bearing_state_without_a_claim() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("resolve.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    setup_goal(&mut store);
    setup_working(&mut store, "commitment-missing-claim");
    drop(store);

    let connection = Connection::open(&path).expect("database reopens");
    connection
        .execute(
            "DELETE FROM claims WHERE commitment_id = 'commitment-missing-claim'",
            [],
        )
        .expect("malformed claim row is removed");
    drop(connection);

    assert!(matches!(
        SqliteStore::open(&path),
        Err(StoreError::InvalidPersistedData(_))
    ));
}

#[test]
fn claim_expiry_preserves_admitted_action_and_held_scope() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    setup_goal(&mut store);
    let (commitment, claim) = setup_working(&mut store, "commitment-1");
    issue(&mut store, "guard-1", &commitment, &claim, "held-scope");
    store
        .admit_guard(
            &guard_id("guard-1"),
            ScopeSet::try_new(vec![scope("held-scope")]).expect("scope set valid"),
            action("action-1"),
            MonotonicInstant::from_ticks(25),
        )
        .expect("guard admits");
    assert_eq!(
        store
            .process_expired_claims(
                MonotonicInstant::from_ticks(21),
                MonotonicDuration::try_from_ticks(10).expect("duration valid"),
            )
            .expect("claim expires"),
        1
    );
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-1"))
            .expect("guard loads")
            .state(),
        &GuardState::Admitted {
            action_ref: action("action-1")
        }
    );
    assert_eq!(
        store
            .record_tethers_outcome(
                TethersOutcome::Failed {
                    action_ref: action("action-1"),
                },
                22,
            )
            .expect("outcome resolves expired claim action"),
        OutcomeRecording::Recorded
    );
}

#[test]
fn generic_persistence_cannot_clear_an_outstanding_action() {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    setup_goal(&mut store);
    let (commitment, claim) = setup_working(&mut store, "commitment-1");
    issue(&mut store, "guard-1", &commitment, &claim, "action-scope");
    store
        .admit_guard(
            &guard_id("guard-1"),
            ScopeSet::try_new(vec![scope("action-scope")]).expect("scope set valid"),
            action("action-1"),
            MonotonicInstant::from_ticks(25),
        )
        .expect("guard admits");
    let event_count = store.list_events().expect("events load").len();
    let mut stale = commitment;
    stale
        .propose_completion(claim.worker_id(), claim.epoch())
        .expect("stale object can be changed in memory");
    let error = store
        .persist_commitment_change(
            &stale,
            4,
            &WorkEvent::CommitmentCompletionProposed {
                commitment_id: stale.commitment_id().clone(),
            },
            26,
        )
        .expect_err("generic persistence cannot bypass outstanding action");
    assert!(matches!(error, StoreError::OutstandingAction { .. }));
    assert_eq!(store.list_events().expect("events load").len(), event_count);
}

#[test]
fn schema_v2_to_v3_backfills_claim_epoch_high_water() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("v2.sqlite");
    create_v2_fixture(
        &path,
        r#"{"kind":"CommitmentClaimed","data":{"commitment_id":"commitment-legacy","worker_id":"worker-legacy","epoch":4}}"#,
    );
    let store = SqliteStore::open(&path).expect("v2 migrates and starts");
    assert_eq!(store.schema_version().expect("schema version reads"), 4);
    assert_eq!(
        store
            .load_commitment_record(&commitment_id("commitment-legacy"))
            .expect("legacy commitment loads")
            .last_claim_epoch(),
        Some(4)
    );
}

#[test]
fn malformed_schema_v2_claim_history_fails_closed() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("malformed-v2.sqlite");
    create_v2_fixture(&path, "not-json");
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StoreError::InvalidPersistedData(_))
    ));
    let connection = Connection::open(&path).expect("database reopens");
    let version: String = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("schema version remains readable");
    assert_eq!(version, "2");
}

#[test]
fn schema_v2_duplicate_action_refs_fail_closed() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("duplicate-action-v2.sqlite");
    create_v2_fixture(
        &path,
        r#"{"kind":"CommitmentClaimed","data":{"commitment_id":"commitment-legacy","worker_id":"worker-legacy","epoch":1}}"#,
    );
    let connection = Connection::open(&path).expect("database reopens");
    connection
        .execute_batch(
            "INSERT INTO execution_guards
                 VALUES ('guard-1', 'commitment-legacy', 1, 0, 'admitted', 'action-1', NULL, NULL, 1);
             INSERT INTO execution_guards
                 VALUES ('guard-2', 'commitment-legacy', 1, 0, 'admitted', 'action-1', NULL, NULL, 1);",
        )
        .expect("duplicate fixture creates");
    drop(connection);
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StoreError::InvalidPersistedData(_))
    ));
}
