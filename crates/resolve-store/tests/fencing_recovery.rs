use resolve_core::{
    BootGeneration, ClaimEpoch, ClaimLease, Commitment, CommitmentId, ExecutionGuard, GoalId,
    GoalRevision, GoalSpec, GoalState, GuardId, GuardState, MonotonicDuration, MonotonicInstant,
    ScopeKey, ScopeSet, TethersActionRef, Timestamp, WorkEvent, WorkerId,
};
use resolve_store::{GuardAdmission, GuardIssueRequest, SqliteStore, StoreError};
use rusqlite::Connection;
use tempfile::tempdir;

fn goal_id() -> GoalId {
    GoalId::try_new("goal-1").expect("test identifier is valid")
}

fn worker_id(value: &str) -> WorkerId {
    WorkerId::try_new(value).expect("test identifier is valid")
}

fn commitment_id(value: &str) -> CommitmentId {
    CommitmentId::try_new(value).expect("test identifier is valid")
}

fn guard_id(value: &str) -> GuardId {
    GuardId::try_new(value).expect("test identifier is valid")
}

fn scope(value: &str) -> ScopeKey {
    ScopeKey::try_new(value).expect("test scope is valid")
}

fn action(value: &str) -> TethersActionRef {
    TethersActionRef::try_new(value).expect("test action is valid")
}

fn scope_set(values: Vec<ScopeKey>) -> ScopeSet {
    ScopeSet::try_new(values).expect("test scope set is valid")
}

fn add_claimed_commitment_with_claim(
    store: &mut SqliteStore,
    id: &str,
    worker: &str,
    heartbeat: u64,
) -> (Commitment, ClaimLease) {
    let id = commitment_id(id);
    let mut commitment = Commitment::new(
        id.clone(),
        goal_id(),
        None,
        "perform bounded work",
        Vec::new(),
        Vec::new(),
    )
    .expect("test commitment is valid");
    store
        .insert_commitment(
            &commitment,
            &WorkEvent::CommitmentCreated {
                commitment_id: id.clone(),
                goal_id: goal_id(),
            },
            1,
        )
        .expect("commitment inserts");
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
        .claim_for(worker_id(worker), MonotonicInstant::from_ticks(heartbeat))
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

fn add_claimed_commitment(
    store: &mut SqliteStore,
    id: &str,
    worker: &str,
    heartbeat: u64,
) -> ClaimEpoch {
    add_claimed_commitment_with_claim(store, id, worker, heartbeat)
        .1
        .epoch()
}

fn prepare_goal_store(store: &mut SqliteStore) {
    let goal = GoalSpec::new(
        goal_id(),
        GoalRevision::initial(),
        "fence bounded work",
        GoalState::Active,
        Timestamp::from_unix_seconds(0),
    )
    .expect("test goal is valid");
    store
        .insert_goal(
            &goal,
            &WorkEvent::GoalCreated {
                goal_id: goal_id(),
                revision: GoalRevision::initial(),
            },
        )
        .expect("goal inserts");
    store
        .set_boot_generation(BootGeneration::from_raw(7))
        .expect("boot generation sets");
}

fn prepare_claimed_store(store: &mut SqliteStore) {
    prepare_goal_store(store);
    add_claimed_commitment(store, "commitment-1", "worker-1", 1);
}

fn prepare_working_store(store: &mut SqliteStore) {
    prepare_goal_store(store);
    let (mut commitment, claim) =
        add_claimed_commitment_with_claim(store, "commitment-1", "worker-1", 1);
    commitment
        .start(claim.worker_id(), claim.epoch())
        .expect("current claimant starts work");
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
}

fn claimed_store() -> SqliteStore {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    prepare_claimed_store(&mut store);
    store
}

fn claimed_store_with_local_commitment() -> (SqliteStore, Commitment, ClaimLease) {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    prepare_goal_store(&mut store);
    let (commitment, claim) =
        add_claimed_commitment_with_claim(&mut store, "commitment-1", "worker-1", 1);
    (store, commitment, claim)
}

fn working_store() -> SqliteStore {
    let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
    prepare_working_store(&mut store);
    store
}

struct Timing {
    now: u64,
    ttl: u64,
    claim_lease_duration: u64,
}

fn issue(
    store: &mut SqliteStore,
    guard: &str,
    commitment: &str,
    epoch: ClaimEpoch,
    worker: &str,
    scopes: Vec<ScopeKey>,
    timing: Timing,
) -> Result<ExecutionGuard, StoreError> {
    store.issue_guard(GuardIssueRequest {
        guard_id: guard_id(guard),
        commitment_id: commitment_id(commitment),
        worker_id: worker_id(worker),
        claim_epoch: epoch,
        scope_keys: scope_set(scopes),
        boot_generation: BootGeneration::from_raw(7),
        now: MonotonicInstant::from_ticks(timing.now),
        guard_ttl: MonotonicDuration::try_from_ticks(timing.ttl).expect("test duration is valid"),
        claim_lease_duration: MonotonicDuration::try_from_ticks(timing.claim_lease_duration)
            .expect("test claim duration is valid"),
    })
}

#[test]
fn empty_scope_set_is_rejected_without_store_mutation() {
    let store = claimed_store();
    let event_count = store.list_events().expect("events load").len();

    let error = ScopeSet::try_new(Vec::new()).expect_err("empty scope set must be rejected");
    assert_eq!(error, resolve_core::DomainError::EmptyScopeSet);
    assert_eq!(store.list_events().expect("events load").len(), event_count);
    assert!(matches!(
        store.load_guard_record(&guard_id("guard-empty")),
        Err(StoreError::NotFound {
            entity: "guard",
            ..
        })
    ));
}

#[test]
fn claimed_commitment_must_start_before_admission() {
    let (mut store, mut commitment, claim) = claimed_store_with_local_commitment();
    issue(
        &mut store,
        "guard-claimed",
        "commitment-1",
        claim.epoch(),
        "worker-1",
        vec![scope("claimed-admission")],
        Timing {
            now: 10,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect("guard issuance remains valid while claimed");
    let event_count = store.list_events().expect("events load").len();

    let error = store
        .admit_guard(
            &guard_id("guard-claimed"),
            scope_set(vec![scope("claimed-admission")]),
            action("action-claimed"),
            MonotonicInstant::from_ticks(15),
        )
        .expect_err("claimed commitment cannot admit consequential work");
    assert!(matches!(error, StoreError::GuardInvalid { .. }));
    assert_eq!(store.list_events().expect("events load").len(), event_count);
    assert!(
        !store
            .list_events()
            .expect("events load")
            .iter()
            .any(|event| event.event_type() == "guard_admitted")
    );
    let guard = store
        .load_guard_record(&guard_id("guard-claimed"))
        .expect("issued guard remains present");
    assert_eq!(guard.state(), &GuardState::Issued);
    assert_eq!(
        guard.reservation_expires_at(),
        Some(MonotonicInstant::from_ticks(30))
    );
    assert!(
        store
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("commitment loads")
            .outstanding_action()
            .is_none()
    );

    let second_epoch = add_claimed_commitment(&mut store, "commitment-2", "worker-2", 1);
    let lock_error = issue(
        &mut store,
        "guard-conflict",
        "commitment-2",
        second_epoch,
        "worker-2",
        vec![scope("claimed-admission")],
        Timing {
            now: 15,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect_err("reserved scope remains locked after failed admission");
    assert!(matches!(lock_error, StoreError::ScopeLocked { .. }));

    commitment
        .start(claim.worker_id(), claim.epoch())
        .expect("current claimant starts work explicitly");
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
    assert_eq!(commitment.state(), &resolve_core::CommitmentState::Working);

    assert_eq!(
        store
            .admit_guard(
                &guard_id("guard-claimed"),
                scope_set(vec![scope("claimed-admission")]),
                action("action-claimed"),
                MonotonicInstant::from_ticks(20),
            )
            .expect("working commitment admits before reservation expiry"),
        GuardAdmission::Admitted
    );
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-claimed"))
            .expect("admitted guard loads")
            .state(),
        &GuardState::Admitted {
            action_ref: action("action-claimed")
        }
    );
    assert_eq!(
        store
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("working commitment loads")
            .outstanding_action(),
        Some(&action("action-claimed"))
    );
}

#[test]
fn persisted_heartbeat_and_trusted_duration_bound_reservation() {
    let mut store = claimed_store();
    let guard = issue(
        &mut store,
        "guard-lease-bound",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("lease-bound")],
        Timing {
            now: 10,
            ttl: 100,
            claim_lease_duration: 20,
        },
    )
    .expect("guard issues");

    assert_eq!(
        guard.reservation_expires_at(),
        MonotonicInstant::from_ticks(21),
        "stored heartbeat 1 plus trusted duration 20 is the claim bound"
    );
}

#[test]
fn expired_persisted_claim_cannot_issue_or_append() {
    let mut store = claimed_store();
    let event_count = store.list_events().expect("events load").len();

    let error = issue(
        &mut store,
        "guard-expired-claim",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("expired-claim")],
        Timing {
            now: 10,
            ttl: 100,
            claim_lease_duration: 5,
        },
    )
    .expect_err("expired persisted claim must be rejected");
    assert!(matches!(error, StoreError::LeaseExpired { .. }));
    assert_eq!(store.list_events().expect("events load").len(), event_count);
    assert!(matches!(
        store.load_guard_record(&guard_id("guard-expired-claim")),
        Err(StoreError::NotFound {
            entity: "guard",
            ..
        })
    ));
}

#[test]
fn persisted_lease_arithmetic_overflow_fails_closed() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("overflow.sqlite");
    let mut store = SqliteStore::open(&path).expect("store opens");
    prepare_claimed_store(&mut store);
    drop(store);

    let connection = Connection::open(&path).expect("database reopens");
    connection
        .execute(
            "UPDATE claims SET last_heartbeat = ?1 WHERE commitment_id = ?2",
            rusqlite::params![i64::MAX, "commitment-1"],
        )
        .expect("test heartbeat update succeeds");
    drop(connection);

    let mut store = SqliteStore::open(&path).expect("store reopens");
    let event_count = store.list_events().expect("events load").len();
    let error = issue(
        &mut store,
        "guard-overflow",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("overflow")],
        Timing {
            now: 0,
            ttl: 1,
            claim_lease_duration: u64::MAX,
        },
    )
    .expect_err("overflow must reject guard issuance");

    assert!(matches!(
        error,
        StoreError::Domain(resolve_core::DomainError::TimeOverflow)
    ));
    assert_eq!(store.list_events().expect("events load").len(), event_count);
    assert!(matches!(
        store.load_guard_record(&guard_id("guard-overflow")),
        Err(StoreError::NotFound {
            entity: "guard",
            ..
        })
    ));
}

#[test]
fn shorter_guard_ttl_wins_over_remaining_claim_lease() {
    let mut store = claimed_store();
    let guard = issue(
        &mut store,
        "guard-short-ttl",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("short-ttl")],
        Timing {
            now: 10,
            ttl: 5,
            claim_lease_duration: 100,
        },
    )
    .expect("guard issues");

    assert_eq!(
        guard.reservation_expires_at(),
        MonotonicInstant::from_ticks(15)
    );
}

#[test]
fn shorter_remaining_claim_lease_wins_over_guard_ttl() {
    let mut store = claimed_store();
    let guard = issue(
        &mut store,
        "guard-short-claim",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("short-claim")],
        Timing {
            now: 3,
            ttl: 100,
            claim_lease_duration: 5,
        },
    )
    .expect("guard issues while claim remains live");

    assert_eq!(
        guard.reservation_expires_at(),
        MonotonicInstant::from_ticks(6)
    );
}

#[test]
fn issuance_canonicalizes_scopes_and_binds_guard_metadata() {
    let mut store = claimed_store();
    let guard = issue(
        &mut store,
        "guard-1",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("z"), scope("a"), scope("z")],
        Timing {
            now: 10,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect("guard issues");

    assert_eq!(guard.scope_keys(), &[scope("a"), scope("z")]);
    assert_eq!(guard.claim_epoch(), ClaimEpoch::initial());
    assert_eq!(guard.boot_generation(), BootGeneration::from_raw(7));
    assert_eq!(
        guard.reservation_expires_at(),
        MonotonicInstant::from_ticks(30)
    );
    let stored = store
        .load_guard_record(&guard_id("guard-1"))
        .expect("guard projection loads");
    assert_eq!(stored.state(), &GuardState::Issued);
    assert_eq!(stored.scope_keys(), guard.scope_keys());
}

#[test]
fn admission_promotes_reservations_and_freezes_outstanding_action_atomically() {
    let mut store = working_store();
    issue(
        &mut store,
        "guard-1",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("b"), scope("a")],
        Timing {
            now: 10,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect("guard issues");

    let error = store
        .admit_guard(
            &guard_id("guard-1"),
            scope_set(vec![scope("a")]),
            action("action-1"),
            MonotonicInstant::from_ticks(15),
        )
        .expect_err("scope mismatch must be rejected");
    assert!(matches!(error, StoreError::GuardInvalid { .. }));

    assert_eq!(
        store
            .admit_guard(
                &guard_id("guard-1"),
                scope_set(vec![scope("a"), scope("b"), scope("a")]),
                action("action-1"),
                MonotonicInstant::from_ticks(15),
            )
            .expect("matching admission succeeds"),
        GuardAdmission::Admitted
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
            .load_commitment_record(&commitment_id("commitment-1"))
            .expect("commitment loads")
            .outstanding_action(),
        Some(&action("action-1"))
    );
    assert_eq!(
        store
            .admit_guard(
                &guard_id("guard-1"),
                scope_set(vec![scope("b"), scope("a")]),
                action("action-1"),
                MonotonicInstant::from_ticks(16),
            )
            .expect("same admission is idempotent"),
        GuardAdmission::AlreadyAdmitted
    );
}

#[test]
fn held_scope_survives_worker_lease_expiry_and_blocks_conflicting_guard() {
    let mut store = working_store();
    let epoch = add_claimed_commitment(&mut store, "commitment-2", "worker-2", 1);
    issue(
        &mut store,
        "guard-1",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("exclusive")],
        Timing {
            now: 10,
            ttl: 10,
            claim_lease_duration: 19,
        },
    )
    .expect("guard issues");
    store
        .admit_guard(
            &guard_id("guard-1"),
            scope_set(vec![scope("exclusive")]),
            action("action-1"),
            MonotonicInstant::from_ticks(15),
        )
        .expect("guard admits");

    let error = issue(
        &mut store,
        "guard-2",
        "commitment-2",
        epoch,
        "worker-2",
        vec![scope("exclusive")],
        Timing {
            now: 100,
            ttl: 10,
            claim_lease_duration: 199,
        },
    )
    .expect_err("held scope remains fenced after lease expiry");
    assert!(matches!(error, StoreError::ScopeLocked { .. }));
}

#[test]
fn expired_reservations_are_released_reactively_with_typed_event() {
    let mut store = claimed_store();
    let epoch = add_claimed_commitment(&mut store, "commitment-2", "worker-2", 1);
    issue(
        &mut store,
        "guard-1",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("reusable")],
        Timing {
            now: 10,
            ttl: 10,
            claim_lease_duration: 99,
        },
    )
    .expect("first reservation issues");
    issue(
        &mut store,
        "guard-2",
        "commitment-2",
        epoch,
        "worker-2",
        vec![scope("reusable")],
        Timing {
            now: 20,
            ttl: 10,
            claim_lease_duration: 99,
        },
    )
    .expect("expired reservation is reclaimed on relevant operation");

    let events = store.list_events().expect("events load");
    assert!(
        events
            .iter()
            .any(|event| event.event_type() == "guard_reservation_expired")
    );
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-1"))
            .expect("expired guard loads")
            .state(),
        &GuardState::Invalidated
    );
}

#[test]
fn stale_epoch_and_invalid_claim_requests_cannot_issue_or_mutate_guards() {
    let mut store = claimed_store();
    let stale = ClaimEpoch::try_from_raw(2).expect("test epoch is valid");
    let error = issue(
        &mut store,
        "guard-stale",
        "commitment-1",
        stale,
        "worker-1",
        vec![scope("scope")],
        Timing {
            now: 10,
            ttl: 10,
            claim_lease_duration: 99,
        },
    )
    .expect_err("stale epoch cannot issue a guard");
    assert!(matches!(error, StoreError::ClaimMismatch { .. }));
    assert!(matches!(
        store.load_guard_record(&guard_id("guard-stale")),
        Err(StoreError::NotFound {
            entity: "guard",
            ..
        })
    ));
}

#[test]
fn claims_from_an_older_boot_generation_are_fenced() {
    let mut store = claimed_store();
    store
        .set_boot_generation(BootGeneration::from_raw(8))
        .expect("boot generation advances");
    let error = issue(
        &mut store,
        "guard-old-boot",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("scope")],
        Timing {
            now: 10,
            ttl: 10,
            claim_lease_duration: 99,
        },
    )
    .expect_err("old boot claim cannot issue a guard");
    assert!(matches!(error, StoreError::ClaimMismatch { .. }));
}

#[test]
fn multi_scope_conflict_is_all_or_nothing() {
    let mut store = claimed_store();
    let epoch = add_claimed_commitment(&mut store, "commitment-2", "worker-2", 1);
    let epoch3 = add_claimed_commitment(&mut store, "commitment-3", "worker-3", 1);
    issue(
        &mut store,
        "guard-1",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("busy")],
        Timing {
            now: 10,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect("first guard issues");
    let error = issue(
        &mut store,
        "guard-2",
        "commitment-2",
        epoch,
        "worker-2",
        vec![scope("free"), scope("busy")],
        Timing {
            now: 10,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect_err("one conflicting scope rejects the whole request");
    assert!(matches!(error, StoreError::ScopeLocked { .. }));
    issue(
        &mut store,
        "guard-3",
        "commitment-3",
        epoch3,
        "worker-3",
        vec![scope("free")],
        Timing {
            now: 10,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect("non-conflicting scope was not partially reserved");
}

#[test]
fn explicit_invalidation_releases_only_unadmitted_reservations() {
    let mut store = claimed_store();
    let epoch = add_claimed_commitment(&mut store, "commitment-2", "worker-2", 1);
    issue(
        &mut store,
        "guard-1",
        "commitment-1",
        ClaimEpoch::initial(),
        "worker-1",
        vec![scope("releasable")],
        Timing {
            now: 10,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect("reservation issues");
    store
        .invalidate_guard(
            &guard_id("guard-1"),
            &worker_id("worker-1"),
            ClaimEpoch::initial(),
            MonotonicInstant::from_ticks(15),
            MonotonicInstant::from_ticks(100),
        )
        .expect("current claimant can invalidate issued guard");
    assert_eq!(
        store
            .load_guard_record(&guard_id("guard-1"))
            .expect("guard loads")
            .state(),
        &GuardState::Invalidated
    );
    issue(
        &mut store,
        "guard-2",
        "commitment-2",
        epoch,
        "worker-2",
        vec![scope("releasable")],
        Timing {
            now: 15,
            ttl: 20,
            claim_lease_duration: 99,
        },
    )
    .expect("invalidated reservation is released");
}

#[test]
fn migration_preserves_v1_data_and_reopen_is_idempotent() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("resolve.sqlite");
    let connection = Connection::open(&path).expect("v1 database opens");
    connection
        .execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
             INSERT INTO metadata VALUES ('schema_version', '1');
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
             CREATE TABLE claims (
                 commitment_id TEXT PRIMARY KEY NOT NULL, worker_id TEXT NOT NULL,
                 claim_epoch INTEGER NOT NULL, last_heartbeat INTEGER NOT NULL
             );
             CREATE TABLE work_events (
                 event_id INTEGER PRIMARY KEY AUTOINCREMENT, goal_id TEXT NOT NULL,
                 commitment_id TEXT, event_type TEXT NOT NULL,
                 payload_json TEXT NOT NULL, created_at INTEGER NOT NULL
             );
             INSERT INTO goals VALUES ('goal-legacy', 1, 'legacy', 'active', 4, 1);
             INSERT INTO goal_revisions VALUES ('goal-legacy', 1, 'legacy', 'active', 4);
             INSERT INTO work_events
                 (goal_id, event_type, payload_json, created_at)
             VALUES ('goal-legacy', 'goal_created',
                 '{\"kind\":\"GoalCreated\",\"data\":{\"goal_id\":\"goal-legacy\",\"revision\":1}}', 4);",
        )
        .expect("v1 fixture creates");
    drop(connection);

    let store = SqliteStore::open(&path).expect("v1 migrates");
    assert_eq!(store.schema_version().expect("version reads"), 2);
    assert_eq!(
        store
            .load_goal_record(&GoalId::try_new("goal-legacy").expect("id is valid"))
            .expect("legacy goal survives")
            .description(),
        "legacy"
    );
    assert_eq!(store.list_events().expect("legacy events survive").len(), 1);
    drop(store);
    assert_eq!(
        SqliteStore::open(&path)
            .expect("reopen succeeds")
            .schema_version()
            .expect("version reads"),
        2
    );
}

#[test]
fn failed_migration_rolls_back_schema_version() {
    let directory = tempdir().expect("temporary directory creates");
    let path = directory.path().join("broken.sqlite");
    let connection = Connection::open(&path).expect("database opens");
    connection
        .execute_batch(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
             INSERT INTO metadata VALUES ('schema_version', '1');
             CREATE TABLE commitments (commitment_id TEXT PRIMARY KEY NOT NULL);
             CREATE TABLE claims (commitment_id TEXT PRIMARY KEY NOT NULL);
             CREATE TRIGGER block_schema_migration
             BEFORE UPDATE OF value ON metadata
             WHEN OLD.key = 'schema_version'
             BEGIN SELECT RAISE(ABORT, 'migration intentionally blocked'); END;",
        )
        .expect("broken migration fixture creates");
    drop(connection);

    assert!(SqliteStore::open(&path).is_err());
    let connection = Connection::open(&path).expect("database reopens");
    let version: String = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("schema version remains readable");
    assert_eq!(version, "1");
    let guards_table: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'execution_guards'",
            [],
            |row| row.get(0),
        )
        .expect("migration table check succeeds");
    assert_eq!(guards_table, 0);
}
