use crate::error::StoreError;
use crate::schema;
use resolve_core::{
    AcceptanceRef, AttentionId, AttentionItem, AttentionState, BootGeneration, ClaimEpoch,
    ClaimLease, Commitment, CommitmentId, CommitmentState, ExecutionGuard, GoalId, GoalSpec,
    GoalState, GuardId, GuardState, HumanDecisionRef, MonotonicDuration, MonotonicInstant,
    Prerequisite, ScopeKey, ScopeSet, TethersActionRef, TethersContractRef, TethersOutcome,
    WaitingReason, WorkEvent, WorkerId,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

const INITIAL_STATE_VERSION: i64 = 1;

#[derive(Debug)]
pub struct SqliteStore {
    connection: Connection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalRecord {
    goal_id: GoalId,
    revision: u64,
    description: String,
    state: GoalState,
    created_at: i64,
    state_version: i64,
}

impl GoalRecord {
    pub fn goal_id(&self) -> &GoalId {
        &self.goal_id
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn state(&self) -> &GoalState {
        &self.state
    }

    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    pub fn state_version(&self) -> i64 {
        self.state_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimRecord {
    worker_id: WorkerId,
    epoch: u64,
    last_heartbeat: u64,
}

impl ClaimRecord {
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    /// Raw persisted epoch value; S2 does not rehydrate a live ClaimLease.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Raw persisted monotonic ticks; they are not valid lease time after restart.
    pub fn last_heartbeat(&self) -> u64 {
        self.last_heartbeat
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitmentRecord {
    commitment_id: CommitmentId,
    goal_id: GoalId,
    parent_id: Option<CommitmentId>,
    description: String,
    state: CommitmentState,
    prerequisites: Vec<Prerequisite>,
    acceptance_refs: Vec<AcceptanceRef>,
    claim: Option<ClaimRecord>,
    outstanding_action: Option<TethersActionRef>,
    state_version: i64,
}

impl CommitmentRecord {
    pub fn commitment_id(&self) -> &CommitmentId {
        &self.commitment_id
    }

    pub fn goal_id(&self) -> &GoalId {
        &self.goal_id
    }

    pub fn parent_id(&self) -> Option<&CommitmentId> {
        self.parent_id.as_ref()
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn state(&self) -> &CommitmentState {
        &self.state
    }

    pub fn prerequisites(&self) -> &[Prerequisite] {
        &self.prerequisites
    }

    pub fn acceptance_refs(&self) -> &[AcceptanceRef] {
        &self.acceptance_refs
    }

    pub fn claim(&self) -> Option<&ClaimRecord> {
        self.claim.as_ref()
    }

    pub fn outstanding_action(&self) -> Option<&TethersActionRef> {
        self.outstanding_action.as_ref()
    }

    pub fn state_version(&self) -> i64 {
        self.state_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttentionRecord {
    attention_id: AttentionId,
    commitment_id: Option<CommitmentId>,
    description: String,
    state: AttentionState,
}

impl AttentionRecord {
    pub fn attention_id(&self) -> &AttentionId {
        &self.attention_id
    }

    pub fn commitment_id(&self) -> Option<&CommitmentId> {
        self.commitment_id.as_ref()
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn state(&self) -> AttentionState {
        self.state
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRecord {
    event_id: i64,
    goal_id: GoalId,
    commitment_id: Option<CommitmentId>,
    event_type: String,
    payload_json: String,
    created_at: i64,
}

#[derive(Debug)]
struct GuardRow {
    guard_id: GuardId,
    commitment_id: CommitmentId,
    claim_epoch: ClaimEpoch,
    boot_generation: BootGeneration,
    state: GuardState,
    reservation_expires_at: Option<MonotonicInstant>,
    state_version: i64,
}

struct CommitmentContext {
    goal_id: String,
    state_json: String,
    outstanding_action: Option<String>,
    state_version: i64,
    claim_epoch: Option<i64>,
    worker: Option<String>,
    boot_generation: Option<i64>,
}

struct RawGuardColumns {
    guard_id: String,
    commitment_id: String,
    claim_epoch: i64,
    boot_generation: i64,
    state: String,
    action_ref: Option<String>,
    outcome_json: Option<String>,
    reservation_expires_at: Option<i64>,
    state_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuardRecord {
    guard_id: GuardId,
    commitment_id: CommitmentId,
    claim_epoch: ClaimEpoch,
    scope_keys: ScopeSet,
    boot_generation: BootGeneration,
    state: GuardState,
    reservation_expires_at: Option<MonotonicInstant>,
}

impl GuardRecord {
    pub fn guard_id(&self) -> &GuardId {
        &self.guard_id
    }

    pub fn commitment_id(&self) -> &CommitmentId {
        &self.commitment_id
    }

    pub fn claim_epoch(&self) -> ClaimEpoch {
        self.claim_epoch
    }

    pub fn scope_keys(&self) -> &[ScopeKey] {
        self.scope_keys.as_slice()
    }

    pub fn boot_generation(&self) -> BootGeneration {
        self.boot_generation
    }

    pub fn state(&self) -> &GuardState {
        &self.state
    }

    pub fn reservation_expires_at(&self) -> Option<MonotonicInstant> {
        self.reservation_expires_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardAdmission {
    Admitted,
    AlreadyAdmitted,
}

#[derive(Debug)]
pub struct GuardIssueRequest {
    pub guard_id: GuardId,
    pub commitment_id: CommitmentId,
    pub worker_id: WorkerId,
    pub claim_epoch: ClaimEpoch,
    pub scope_keys: ScopeSet,
    pub boot_generation: BootGeneration,
    pub now: MonotonicInstant,
    pub guard_ttl: MonotonicDuration,
    pub claim_lease_duration: MonotonicDuration,
}

impl EventRecord {
    pub fn event_id(&self) -> i64 {
        self.event_id
    }

    pub fn goal_id(&self) -> &GoalId {
        &self.goal_id
    }

    pub fn commitment_id(&self) -> Option<&CommitmentId> {
        self.commitment_id.as_ref()
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub fn payload_json(&self) -> &str {
        &self.payload_json
    }

    pub fn created_at(&self) -> i64 {
        self.created_at
    }
}

impl SqliteStore {
    /// Open a file-backed store with WAL, foreign keys, and FULL synchronous mode.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection, true)
    }

    /// Open an isolated in-memory store for deterministic tests.
    pub fn open_in_memory_for_tests() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        Self::from_connection(connection, false)
    }

    pub fn journal_mode(&self) -> Result<String, StoreError> {
        self.connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(StoreError::from)
    }

    pub fn foreign_keys_enabled(&self) -> Result<bool, StoreError> {
        let enabled: i64 = self
            .connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
        Ok(enabled == 1)
    }

    pub fn schema_version(&self) -> Result<i64, StoreError> {
        self.connection
            .query_row(
                "SELECT value FROM metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(StoreError::from)
            .and_then(|value| {
                value.parse::<i64>().map_err(|error| {
                    StoreError::InvalidPersistedData(format!(
                        "schema_version is not an integer: {error}"
                    ))
                })
            })
    }

    /// Establish the explicit boot generation used to fence newly persisted
    /// claims and guards. Restart recovery policy is intentionally S4 work.
    pub fn set_boot_generation(
        &mut self,
        boot_generation: BootGeneration,
    ) -> Result<(), StoreError> {
        let boot_generation = checked_i64(boot_generation.value(), "boot generation")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = current_boot_generation(&transaction)?;
        if boot_generation <= checked_i64(current.value(), "boot generation")? {
            return Err(StoreError::InvalidMutation(
                "boot generation must advance beyond the current generation".to_owned(),
            ));
        }
        transaction.execute(
            "UPDATE metadata SET value = ?1 WHERE key = 'boot_generation'",
            params![boot_generation.to_string()],
        )?;
        if transaction.changes() != 1 {
            return Err(StoreError::InvalidPersistedData(
                "boot_generation metadata is missing".to_owned(),
            ));
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn boot_generation(&self) -> Result<BootGeneration, StoreError> {
        self.connection
            .query_row(
                "SELECT value FROM metadata WHERE key = 'boot_generation'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(StoreError::from)
            .and_then(|value| {
                let raw = value.parse::<i64>().map_err(|error| {
                    StoreError::InvalidPersistedData(format!(
                        "boot_generation is not an integer: {error}"
                    ))
                })?;
                Ok(BootGeneration::try_from_raw(raw)?)
            })
    }

    pub fn insert_goal(&mut self, goal: &GoalSpec, event: &WorkEvent) -> Result<i64, StoreError> {
        validate_goal_event(event, goal.goal_id(), "goal creation")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let event_id = append_event(
            &transaction,
            goal.goal_id(),
            event,
            goal.created_at().as_unix_seconds(),
        )?;
        transaction.execute(
            "INSERT INTO goals
                (goal_id, revision, description, state, created_at, state_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                goal.goal_id().as_ref(),
                goal.revision().value(),
                goal.description(),
                encode_goal_state(goal.state()),
                goal.created_at().as_unix_seconds(),
                INITIAL_STATE_VERSION,
            ],
        )?;
        transaction.execute(
            "INSERT INTO goal_revisions
                (goal_id, revision, description, state, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                goal.goal_id().as_ref(),
                goal.revision().value(),
                goal.description(),
                encode_goal_state(goal.state()),
                goal.created_at().as_unix_seconds(),
            ],
        )?;
        transaction.commit()?;
        Ok(event_id)
    }

    pub fn persist_goal_change(
        &mut self,
        goal: &GoalSpec,
        expected_state_version: i64,
        event: &WorkEvent,
    ) -> Result<i64, StoreError> {
        validate_goal_event(event, goal.goal_id(), "goal revision")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (current_version, current_revision): (i64, i64) = transaction
            .query_row(
                "SELECT state_version, revision FROM goals WHERE goal_id = ?1",
                params![goal.goal_id().as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| not_found("goal", goal.goal_id().to_string()))?;
        ensure_version(
            "goal",
            goal.goal_id().to_string(),
            expected_state_version,
            current_version,
        )?;
        let expected_revision = current_revision.checked_add(1).ok_or_else(|| {
            StoreError::InvalidMutation("goal revision space is exhausted".to_owned())
        })?;
        if goal.revision().value() != expected_revision as u64 {
            if goal.revision().value() <= current_revision as u64 {
                return Err(StoreError::DuplicateImmutableRevision {
                    goal_id: goal.goal_id().to_string(),
                    revision: goal.revision().value(),
                });
            }
            return Err(StoreError::InvalidMutation(format!(
                "goal revision must be {expected_revision}, received {}",
                goal.revision().value()
            )));
        }
        let next_state_version = next_state_version(expected_state_version)?;
        let event_id = append_event(
            &transaction,
            goal.goal_id(),
            event,
            goal.created_at().as_unix_seconds(),
        )?;
        transaction.execute(
            "UPDATE goals
                SET revision = ?1, description = ?2, state = ?3,
                    created_at = ?4, state_version = ?5
             WHERE goal_id = ?6 AND state_version = ?7",
            params![
                goal.revision().value(),
                goal.description(),
                encode_goal_state(goal.state()),
                goal.created_at().as_unix_seconds(),
                next_state_version,
                goal.goal_id().as_ref(),
                expected_state_version,
            ],
        )?;
        transaction.execute(
            "INSERT INTO goal_revisions
                (goal_id, revision, description, state, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                goal.goal_id().as_ref(),
                goal.revision().value(),
                goal.description(),
                encode_goal_state(goal.state()),
                goal.created_at().as_unix_seconds(),
            ],
        )?;
        transaction.commit()?;
        Ok(event_id)
    }

    pub fn insert_commitment(
        &mut self,
        commitment: &Commitment,
        event: &WorkEvent,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        validate_commitment_event(event, commitment.commitment_id(), "commitment creation")?;
        let state_json = encode_commitment_state(commitment.state())?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let boot_generation = current_boot_generation(&transaction)?;
        let event_id = append_event(&transaction, commitment.goal_id(), event, created_at)?;
        transaction.execute(
            "INSERT INTO commitments
                (commitment_id, goal_id, parent_id, description, state_json,
                 outstanding_action, state_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                commitment.commitment_id().as_ref(),
                commitment.goal_id().as_ref(),
                commitment.parent_id().map(AsRef::as_ref),
                commitment.description(),
                state_json,
                commitment.outstanding_action().map(AsRef::as_ref),
                INITIAL_STATE_VERSION,
            ],
        )?;
        sync_commitment_children(&transaction, commitment, boot_generation)?;
        transaction.commit()?;
        Ok(event_id)
    }

    pub fn persist_commitment_change(
        &mut self,
        commitment: &Commitment,
        expected_state_version: i64,
        event: &WorkEvent,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        validate_commitment_event(event, commitment.commitment_id(), "commitment change")?;
        let state_json = encode_commitment_state(commitment.state())?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let boot_generation = current_boot_generation(&transaction)?;
        let current_version: i64 = transaction
            .query_row(
                "SELECT state_version FROM commitments WHERE commitment_id = ?1",
                params![commitment.commitment_id().as_ref()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| not_found("commitment", commitment.commitment_id().to_string()))?;
        ensure_version(
            "commitment",
            commitment.commitment_id().to_string(),
            expected_state_version,
            current_version,
        )?;
        let next_state_version = next_state_version(expected_state_version)?;
        let event_id = append_event(&transaction, commitment.goal_id(), event, created_at)?;
        transaction.execute(
            "UPDATE commitments
                SET goal_id = ?1, parent_id = ?2, description = ?3, state_json = ?4,
                    outstanding_action = ?5, state_version = ?6
             WHERE commitment_id = ?7 AND state_version = ?8",
            params![
                commitment.goal_id().as_ref(),
                commitment.parent_id().map(AsRef::as_ref),
                commitment.description(),
                state_json,
                commitment.outstanding_action().map(AsRef::as_ref),
                next_state_version,
                commitment.commitment_id().as_ref(),
                expected_state_version,
            ],
        )?;
        sync_commitment_children(&transaction, commitment, boot_generation)?;
        transaction.commit()?;
        Ok(event_id)
    }

    pub fn insert_attention(
        &mut self,
        goal_id: &GoalId,
        attention: &AttentionItem,
        event: &WorkEvent,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        validate_attention_event(event, attention.attention_id())?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let event_id = append_event(&transaction, goal_id, event, created_at)?;
        transaction.execute(
            "INSERT INTO attention_items
                (attention_id, commitment_id, description, state)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                attention.attention_id().as_ref(),
                attention.commitment_id().map(AsRef::as_ref),
                attention.description(),
                encode_attention_state(attention.state()),
            ],
        )?;
        transaction.commit()?;
        Ok(event_id)
    }

    /// Issue a guard and reserve every requested exact opaque scope atomically.
    pub fn issue_guard(
        &mut self,
        request: GuardIssueRequest,
    ) -> Result<ExecutionGuard, StoreError> {
        let GuardIssueRequest {
            guard_id,
            commitment_id,
            worker_id,
            claim_epoch,
            scope_keys,
            boot_generation,
            now,
            guard_ttl,
            claim_lease_duration,
        } = request;
        let now_ticks = checked_i64(now.ticks(), "current monotonic instant")?;
        let claim_epoch_ticks = checked_i64(claim_epoch.value(), "claim epoch")?;
        let boot_generation_ticks = checked_i64(boot_generation.value(), "boot generation")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (goal_id, state_json, outstanding_action): (String, String, Option<String>) =
            transaction
                .query_row(
                    "SELECT goal_id, state_json, outstanding_action
                     FROM commitments WHERE commitment_id = ?1",
                    params![commitment_id.as_ref()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?
                .ok_or_else(|| not_found("commitment", commitment_id.to_string()))?;
        let state = decode_commitment_state(&state_json)?;
        if !matches!(state, CommitmentState::Claimed | CommitmentState::Working) {
            return Err(StoreError::GuardInvalid {
                guard_id: guard_id.to_string(),
                reason: format!("commitment is in {state:?}, not a work-owning state"),
            });
        }
        if outstanding_action.is_some() {
            return Err(StoreError::GuardInvalid {
                guard_id: guard_id.to_string(),
                reason: "commitment already has an outstanding action".to_owned(),
            });
        }
        let (stored_worker, stored_epoch, stored_last_heartbeat, stored_boot_generation): (
            String,
            i64,
            i64,
            i64,
        ) = transaction
            .query_row(
                "SELECT worker_id, claim_epoch, last_heartbeat, boot_generation
                 FROM claims WHERE commitment_id = ?1",
                params![commitment_id.as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: commitment_id.to_string(),
                reason: "commitment has no current claim".to_owned(),
            })?;
        if stored_worker != worker_id.as_ref() {
            return Err(StoreError::ClaimMismatch {
                commitment_id: commitment_id.to_string(),
                reason: format!("claim belongs to worker {stored_worker}, not {worker_id}"),
            });
        }
        if stored_epoch != claim_epoch_ticks {
            return Err(StoreError::ClaimMismatch {
                commitment_id: commitment_id.to_string(),
                reason: format!("claim epoch is {stored_epoch}, not {claim_epoch}"),
            });
        }
        let current_boot_generation = current_boot_generation(&transaction)?;
        if stored_boot_generation
            != checked_i64(current_boot_generation.value(), "boot generation")?
            || boot_generation != current_boot_generation
        {
            return Err(StoreError::ClaimMismatch {
                commitment_id: commitment_id.to_string(),
                reason: "claim and guard request are not from the current boot generation"
                    .to_owned(),
            });
        }
        let stored_last_heartbeat = MonotonicInstant::try_from_raw(stored_last_heartbeat)?;
        let claim_lease_deadline = claim_lease_duration.deadline_from(stored_last_heartbeat)?;
        if claim_lease_deadline <= now {
            return Err(StoreError::LeaseExpired {
                commitment_id: commitment_id.to_string(),
            });
        }
        let reservation_expires_at =
            ExecutionGuard::reservation_deadline(now, guard_ttl, claim_lease_deadline)?;
        let reservation_ticks = checked_i64(
            reservation_expires_at.ticks(),
            "guard reservation expiration",
        )?;

        expire_relevant_reservations(&transaction, scope_keys.as_slice(), now_ticks)?;
        for scope_key in scope_keys.as_slice() {
            let lock_owner: Option<String> = transaction
                .query_row(
                    "SELECT guard_id FROM scope_locks WHERE scope_key = ?1",
                    params![scope_key.as_ref()],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(lock_owner) = lock_owner {
                return Err(StoreError::ScopeLocked {
                    scope_key: scope_key.as_ref().to_owned(),
                    guard_id: lock_owner,
                });
            }
        }
        let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
        transaction.execute(
            "INSERT INTO execution_guards
                (guard_id, commitment_id, claim_epoch, boot_generation, state,
                 action_ref, outcome_json, reservation_expires_at, state_version)
             VALUES (?1, ?2, ?3, ?4, 'issued', NULL, NULL, ?5, ?6)",
            params![
                guard_id.as_ref(),
                commitment_id.as_ref(),
                claim_epoch_ticks,
                boot_generation_ticks,
                reservation_ticks,
                INITIAL_STATE_VERSION,
            ],
        )?;
        for (ordinal, scope_key) in scope_keys.as_slice().iter().enumerate() {
            transaction.execute(
                "INSERT INTO execution_guard_scopes (guard_id, ordinal, scope_key)
                 VALUES (?1, ?2, ?3)",
                params![guard_id.as_ref(), ordinal as i64, scope_key.as_ref()],
            )?;
            transaction.execute(
                "INSERT INTO scope_locks
                    (scope_key, guard_id, commitment_id, lock_state,
                     action_ref, reservation_expires_at)
                 VALUES (?1, ?2, ?3, 'reserved', NULL, ?4)",
                params![
                    scope_key.as_ref(),
                    guard_id.as_ref(),
                    commitment_id.as_ref(),
                    reservation_ticks,
                ],
            )?;
        }
        append_event(
            &transaction,
            &goal_id,
            &WorkEvent::GuardIssued {
                guard_id: guard_id.clone(),
                commitment_id: commitment_id.clone(),
                claim_epoch,
            },
            now_ticks,
        )?;
        transaction.commit()?;
        Ok(ExecutionGuard::issue(
            guard_id,
            commitment_id.clone(),
            claim_epoch,
            scope_keys,
            boot_generation,
            reservation_expires_at,
        ))
    }

    /// Admit a guard using the exact scope set supplied by the Tethers boundary.
    /// Reservation promotion and the commitment's outstanding action are one
    /// transaction; a held lock is never expired by this operation.
    pub fn admit_guard(
        &mut self,
        guard_id: &GuardId,
        scope_keys: ScopeSet,
        action_ref: TethersActionRef,
        now: MonotonicInstant,
    ) -> Result<GuardAdmission, StoreError> {
        let now_ticks = checked_i64(now.ticks(), "current monotonic instant")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut guard = query_guard(&transaction, guard_id)?;
        let stored_scope_keys = query_guard_scopes(&transaction, guard_id)?;
        match &guard.state {
            GuardState::Admitted {
                action_ref: existing,
            } if existing == &action_ref => {
                if stored_scope_keys == scope_keys {
                    return Ok(GuardAdmission::AlreadyAdmitted);
                }
                return Err(StoreError::GuardInvalid {
                    guard_id: guard_id.to_string(),
                    reason: "admitted guard scope set cannot be changed".to_owned(),
                });
            }
            GuardState::Admitted { .. } => {
                return Err(StoreError::GuardInvalid {
                    guard_id: guard_id.to_string(),
                    reason: "guard is already admitted for another action".to_owned(),
                });
            }
            GuardState::Issued => {}
            _ => {
                return Err(StoreError::GuardInvalid {
                    guard_id: guard_id.to_string(),
                    reason: format!("guard is in terminal state {:?}", guard.state),
                });
            }
        }

        expire_relevant_reservations(&transaction, stored_scope_keys.as_slice(), now_ticks)?;
        guard = query_guard(&transaction, guard_id)?;
        if !matches!(guard.state, GuardState::Issued) {
            return Err(StoreError::LeaseExpired {
                commitment_id: guard.commitment_id.to_string(),
            });
        }
        if stored_scope_keys != scope_keys {
            return Err(StoreError::GuardInvalid {
                guard_id: guard_id.to_string(),
                reason: "admission scope set does not exactly match issuance".to_owned(),
            });
        }
        for scope_key in stored_scope_keys.as_slice() {
            let lock_matches: bool = transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM scope_locks
                    WHERE scope_key = ?1 AND guard_id = ?2 AND lock_state = 'reserved'
                )",
                params![scope_key.as_ref(), guard_id.as_ref()],
                |row| row.get(0),
            )?;
            if !lock_matches {
                return Err(StoreError::GuardInvalid {
                    guard_id: guard_id.to_string(),
                    reason: "reserved scope lock is missing or owned by another guard".to_owned(),
                });
            }
        }
        if guard
            .reservation_expires_at
            .is_none_or(|deadline| deadline.ticks() <= now.ticks())
        {
            return Err(StoreError::LeaseExpired {
                commitment_id: guard.commitment_id.to_string(),
            });
        }
        let commitment = transaction
            .query_row(
                "SELECT c.goal_id, c.state_json, c.outstanding_action, c.state_version,
                        cl.claim_epoch, cl.worker_id, cl.boot_generation
                 FROM commitments c
                 LEFT JOIN claims cl ON cl.commitment_id = c.commitment_id
                 WHERE c.commitment_id = ?1",
                params![guard.commitment_id.as_ref()],
                |row| {
                    Ok(CommitmentContext {
                        goal_id: row.get(0)?,
                        state_json: row.get(1)?,
                        outstanding_action: row.get(2)?,
                        state_version: row.get(3)?,
                        claim_epoch: row.get(4)?,
                        worker: row.get(5)?,
                        boot_generation: row.get(6)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| not_found("commitment", guard.commitment_id.to_string()))?;
        let state = decode_commitment_state(&commitment.state_json)?;
        if !matches!(state, CommitmentState::Working) {
            return Err(StoreError::GuardInvalid {
                guard_id: guard_id.to_string(),
                reason: format!("commitment is in {state:?}, not WORKING"),
            });
        }
        if commitment.outstanding_action.is_some() {
            return Err(StoreError::GuardInvalid {
                guard_id: guard_id.to_string(),
                reason: "commitment already has an outstanding action".to_owned(),
            });
        }
        let stored_epoch = commitment
            .claim_epoch
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: "commitment has no current claim".to_owned(),
            })?;
        if stored_epoch != checked_i64(guard.claim_epoch.value(), "claim epoch")? {
            return Err(StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: format!(
                    "claim epoch is {stored_epoch}, guard epoch is {}",
                    guard.claim_epoch
                ),
            });
        }
        let _worker = commitment
            .worker
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: "commitment has no current claim".to_owned(),
            })
            .and_then(|value| parse_id(value, "worker", WorkerId::try_new))?;
        let current_boot_generation = current_boot_generation(&transaction)?;
        if commitment.boot_generation
            != Some(checked_i64(
                current_boot_generation.value(),
                "boot generation",
            )?)
            || guard.boot_generation != current_boot_generation
        {
            return Err(StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: "claim and guard are not from the current boot generation".to_owned(),
            });
        }
        let next_commitment_version = next_state_version(commitment.state_version)?;
        let next_guard_version = next_state_version(guard.state_version)?;
        let goal_id = parse_id(commitment.goal_id, "goal", GoalId::try_new)?;
        let changed = transaction.execute(
            "UPDATE commitments SET outstanding_action = ?1, state_version = ?2
             WHERE commitment_id = ?3 AND state_version = ?4 AND outstanding_action IS NULL",
            params![
                action_ref.as_ref(),
                next_commitment_version,
                guard.commitment_id.as_ref(),
                commitment.state_version,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidMutation(
                "commitment changed while admitting guard".to_owned(),
            ));
        }
        let changed = transaction.execute(
            "UPDATE execution_guards
             SET state = 'admitted', action_ref = ?1, reservation_expires_at = NULL,
                 state_version = ?2
             WHERE guard_id = ?3 AND state = 'issued' AND state_version = ?4",
            params![
                action_ref.as_ref(),
                next_guard_version,
                guard_id.as_ref(),
                guard.state_version
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidMutation(
                "guard changed while admitting".to_owned(),
            ));
        }
        let changed = transaction.execute(
            "UPDATE scope_locks
             SET lock_state = 'held', action_ref = ?, reservation_expires_at = NULL
             WHERE guard_id = ? AND lock_state = 'reserved'",
            params![action_ref.as_ref(), guard_id.as_ref()],
        )?;
        if changed != stored_scope_keys.len() {
            return Err(StoreError::InvalidMutation(
                "guard scope locks changed while admitting".to_owned(),
            ));
        }
        append_event(
            &transaction,
            &goal_id,
            &WorkEvent::GuardAdmitted {
                guard_id: guard_id.clone(),
                commitment_id: guard.commitment_id.clone(),
                action_ref,
            },
            now_ticks,
        )?;
        transaction.commit()?;
        Ok(GuardAdmission::Admitted)
    }

    /// Explicitly invalidate an unadmitted guard after rechecking its current claim.
    pub fn invalidate_guard(
        &mut self,
        guard_id: &GuardId,
        worker_id: &WorkerId,
        claim_epoch: ClaimEpoch,
        now: MonotonicInstant,
        claim_lease_deadline: MonotonicInstant,
    ) -> Result<(), StoreError> {
        let now_ticks = checked_i64(now.ticks(), "current monotonic instant")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut guard = query_guard(&transaction, guard_id)?;
        if !matches!(guard.state, GuardState::Issued) {
            return Err(StoreError::GuardInvalid {
                guard_id: guard_id.to_string(),
                reason: "only an issued guard can be invalidated".to_owned(),
            });
        }
        let scope_keys = query_guard_scopes(&transaction, guard_id)?;
        expire_relevant_reservations(&transaction, scope_keys.as_slice(), now_ticks)?;
        guard = query_guard(&transaction, guard_id)?;
        if !matches!(guard.state, GuardState::Issued) {
            return Err(StoreError::LeaseExpired {
                commitment_id: guard.commitment_id.to_string(),
            });
        }
        if guard
            .reservation_expires_at
            .is_none_or(|deadline| deadline.ticks() <= now.ticks())
        {
            return Err(StoreError::LeaseExpired {
                commitment_id: guard.commitment_id.to_string(),
            });
        }
        let (goal_id, stored_worker, stored_epoch, stored_boot_generation): (
            String,
            String,
            i64,
            i64,
        ) = transaction
            .query_row(
                "SELECT c.goal_id, cl.worker_id, cl.claim_epoch, cl.boot_generation
                 FROM commitments c JOIN claims cl ON cl.commitment_id = c.commitment_id
                 WHERE c.commitment_id = ?1",
                params![guard.commitment_id.as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: "commitment has no current claim".to_owned(),
            })?;
        if claim_lease_deadline <= now {
            return Err(StoreError::LeaseExpired {
                commitment_id: guard.commitment_id.to_string(),
            });
        }
        if stored_worker != worker_id.as_ref()
            || stored_epoch != checked_i64(claim_epoch.value(), "claim epoch")?
        {
            return Err(StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: "invalidation request does not match the current claim".to_owned(),
            });
        }
        let current_boot_generation = current_boot_generation(&transaction)?;
        if stored_boot_generation
            != checked_i64(current_boot_generation.value(), "boot generation")?
            || guard.boot_generation != current_boot_generation
        {
            return Err(StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: "claim and guard are not from the current boot generation".to_owned(),
            });
        }
        transaction.execute(
            "DELETE FROM scope_locks WHERE guard_id = ?1 AND lock_state = 'reserved'",
            params![guard_id.as_ref()],
        )?;
        let changed = transaction.execute(
            "UPDATE execution_guards
             SET state = 'invalidated', reservation_expires_at = NULL,
                 state_version = ?1
             WHERE guard_id = ?2 AND state = 'issued' AND state_version = ?3",
            params![
                next_state_version(guard.state_version)?,
                guard_id.as_ref(),
                guard.state_version
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidMutation(
                "guard changed while invalidating".to_owned(),
            ));
        }
        let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
        append_event(
            &transaction,
            &goal_id,
            &WorkEvent::GuardInvalidated {
                guard_id: guard_id.clone(),
            },
            now_ticks,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn load_guard_record(&self, guard_id: &GuardId) -> Result<GuardRecord, StoreError> {
        let guard = query_guard_connection(&self.connection, guard_id)?;
        let scope_keys = query_guard_scopes_connection(&self.connection, guard_id)?;
        Ok(GuardRecord {
            guard_id: guard.guard_id,
            commitment_id: guard.commitment_id,
            claim_epoch: guard.claim_epoch,
            scope_keys,
            boot_generation: guard.boot_generation,
            state: guard.state,
            reservation_expires_at: guard.reservation_expires_at,
        })
    }

    pub fn load_goal_record(&self, goal_id: &GoalId) -> Result<GoalRecord, StoreError> {
        self.connection
            .query_row(
                "SELECT goal_id, revision, description, state, created_at, state_version
                 FROM goals WHERE goal_id = ?1",
                params![goal_id.as_ref()],
                |row| {
                    let stored_id: String = row.get(0)?;
                    let revision: i64 = row.get(1)?;
                    let state: String = row.get(3)?;
                    Ok((
                        stored_id,
                        revision,
                        row.get::<_, String>(2)?,
                        state,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| not_found("goal", goal_id.to_string()))
            .and_then(
                |(stored_id, revision, description, state, created_at, state_version)| {
                    let goal_id = parse_id(stored_id, "goal", GoalId::try_new)?;
                    let revision = positive_u64(revision, "goal revision")?;
                    Ok(GoalRecord {
                        goal_id,
                        revision,
                        description,
                        state: decode_goal_state(&state)?,
                        created_at,
                        state_version,
                    })
                },
            )
    }

    pub fn load_commitment_record(
        &self,
        commitment_id: &CommitmentId,
    ) -> Result<CommitmentRecord, StoreError> {
        let row = self
            .connection
            .query_row(
                "SELECT commitment_id, goal_id, parent_id, description, state_json,
                        outstanding_action, state_version
                 FROM commitments WHERE commitment_id = ?1",
                params![commitment_id.as_ref()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| not_found("commitment", commitment_id.to_string()))?;
        let (stored_id, goal_id, parent_id, description, state_json, outstanding_action, version) =
            row;
        let claim = self
            .connection
            .query_row(
                "SELECT worker_id, claim_epoch, last_heartbeat
                 FROM claims WHERE commitment_id = ?1",
                params![commitment_id.as_ref()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?
            .map(
                |(worker_id, epoch, heartbeat)| -> Result<ClaimRecord, StoreError> {
                    Ok(ClaimRecord {
                        worker_id: parse_id(worker_id, "worker", WorkerId::try_new)?,
                        epoch: positive_u64(epoch, "claim epoch")?,
                        last_heartbeat: nonnegative_u64(heartbeat, "claim heartbeat")?,
                    })
                },
            )
            .transpose()?;
        Ok(CommitmentRecord {
            commitment_id: parse_id(stored_id, "commitment", CommitmentId::try_new)?,
            goal_id: parse_id(goal_id, "goal", GoalId::try_new)?,
            parent_id: parent_id
                .map(|value| parse_id(value, "parent commitment", CommitmentId::try_new))
                .transpose()?,
            description,
            state: decode_commitment_state(&state_json)?,
            prerequisites: self.load_prerequisites(commitment_id)?,
            acceptance_refs: self.load_acceptance_refs(commitment_id)?,
            claim,
            outstanding_action: outstanding_action
                .map(|value| parse_id(value, "Tethers action", TethersActionRef::try_new))
                .transpose()?,
            state_version: version,
        })
    }

    pub fn load_attention_record(
        &self,
        attention_id: &AttentionId,
    ) -> Result<AttentionRecord, StoreError> {
        self.connection
            .query_row(
                "SELECT attention_id, commitment_id, description, state
                 FROM attention_items WHERE attention_id = ?1",
                params![attention_id.as_ref()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| not_found("attention", attention_id.to_string()))
            .and_then(|(stored_id, commitment_id, description, state)| {
                Ok(AttentionRecord {
                    attention_id: parse_id(stored_id, "attention", AttentionId::try_new)?,
                    commitment_id: commitment_id
                        .map(|value| parse_id(value, "commitment", CommitmentId::try_new))
                        .transpose()?,
                    description,
                    state: decode_attention_state(&state)?,
                })
            })
    }

    pub fn list_events(&self) -> Result<Vec<EventRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT event_id, goal_id, commitment_id, event_type, payload_json, created_at
             FROM work_events ORDER BY event_id",
        )?;
        let mut rows = statement.query([])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            events.push(EventRecord {
                event_id: row.get(0)?,
                goal_id: parse_id(row.get(1)?, "goal", GoalId::try_new)?,
                commitment_id: row
                    .get::<_, Option<String>>(2)?
                    .map(|value| parse_id(value, "commitment", CommitmentId::try_new))
                    .transpose()?,
                event_type: row.get(3)?,
                payload_json: row.get(4)?,
                created_at: row.get(5)?,
            });
        }
        drop(rows);
        for event in &events {
            serde_json::from_str::<StoredEventPayload>(event.payload_json()).map_err(|error| {
                StoreError::InvalidPersistedData(format!(
                    "event {} has invalid payload JSON: {error}",
                    event.event_id()
                ))
            })?;
        }
        Ok(events)
    }

    fn from_connection(mut connection: Connection, file_backed: bool) -> Result<Self, StoreError> {
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA synchronous = FULL;",
        )?;
        if file_backed {
            let journal_mode: String =
                connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
            if !journal_mode.eq_ignore_ascii_case("wal") {
                return Err(StoreError::InvalidMutation(format!(
                    "SQLite did not enable WAL; reported journal mode {journal_mode}"
                )));
            }
        }
        schema::initialize(&mut connection)?;
        let store = Self { connection };
        let version = store.schema_version()?;
        if version != schema::SCHEMA_VERSION {
            return Err(StoreError::InvalidPersistedData(format!(
                "unsupported schema version {version}; expected {}",
                schema::SCHEMA_VERSION
            )));
        }
        Ok(store)
    }

    fn load_prerequisites(
        &self,
        commitment_id: &CommitmentId,
    ) -> Result<Vec<Prerequisite>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT prerequisite_type, reference_id, prerequisite_commitment_id
             FROM commitment_prerequisites
             WHERE commitment_id = ?1 ORDER BY ordinal",
        )?;
        let mut rows = statement.query(params![commitment_id.as_ref()])?;
        let mut prerequisites = Vec::new();
        while let Some(row) = rows.next()? {
            let kind: String = row.get(0)?;
            let reference_id: String = row.get(1)?;
            let prerequisite_commitment_id: Option<String> = row.get(2)?;
            prerequisites.push(decode_prerequisite(
                kind,
                reference_id,
                prerequisite_commitment_id,
            )?);
        }
        Ok(prerequisites)
    }

    fn load_acceptance_refs(
        &self,
        commitment_id: &CommitmentId,
    ) -> Result<Vec<AcceptanceRef>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT reference_id FROM commitment_acceptance_refs
             WHERE commitment_id = ?1 ORDER BY ordinal",
        )?;
        let mut rows = statement.query(params![commitment_id.as_ref()])?;
        let mut references = Vec::new();
        while let Some(row) = rows.next()? {
            let value: String = row.get(0)?;
            references.push(parse_id(value, "acceptance", AcceptanceRef::try_new)?);
        }
        Ok(references)
    }
}

fn checked_i64(value: u64, field: &str) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::InvalidMutation(format!("{field} does not fit in SQLite INTEGER")))
}

fn current_boot_generation(transaction: &Transaction<'_>) -> Result<BootGeneration, StoreError> {
    let value: String = transaction.query_row(
        "SELECT value FROM metadata WHERE key = 'boot_generation'",
        [],
        |row| row.get(0),
    )?;
    let value = value.parse::<i64>().map_err(|error| {
        StoreError::InvalidPersistedData(format!("boot_generation is not an integer: {error}"))
    })?;
    Ok(BootGeneration::try_from_raw(value)?)
}

fn query_guard(transaction: &Transaction<'_>, guard_id: &GuardId) -> Result<GuardRow, StoreError> {
    transaction
        .query_row(
            "SELECT guard_id, commitment_id, claim_epoch, boot_generation, state,
                    action_ref, outcome_json, reservation_expires_at, state_version
             FROM execution_guards WHERE guard_id = ?1",
            params![guard_id.as_ref()],
            |row| {
                Ok(RawGuardColumns {
                    guard_id: row.get(0)?,
                    commitment_id: row.get(1)?,
                    claim_epoch: row.get(2)?,
                    boot_generation: row.get(3)?,
                    state: row.get(4)?,
                    action_ref: row.get(5)?,
                    outcome_json: row.get(6)?,
                    reservation_expires_at: row.get(7)?,
                    state_version: row.get(8)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| not_found("guard", guard_id.to_string()))
        .and_then(decode_guard_row)
}

fn query_guard_connection(
    connection: &Connection,
    guard_id: &GuardId,
) -> Result<GuardRow, StoreError> {
    connection
        .query_row(
            "SELECT guard_id, commitment_id, claim_epoch, boot_generation, state,
                    action_ref, outcome_json, reservation_expires_at, state_version
             FROM execution_guards WHERE guard_id = ?1",
            params![guard_id.as_ref()],
            |row| {
                Ok(RawGuardColumns {
                    guard_id: row.get(0)?,
                    commitment_id: row.get(1)?,
                    claim_epoch: row.get(2)?,
                    boot_generation: row.get(3)?,
                    state: row.get(4)?,
                    action_ref: row.get(5)?,
                    outcome_json: row.get(6)?,
                    reservation_expires_at: row.get(7)?,
                    state_version: row.get(8)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| not_found("guard", guard_id.to_string()))
        .and_then(decode_guard_row)
}

fn decode_guard_row(columns: RawGuardColumns) -> Result<GuardRow, StoreError> {
    let state = decode_guard_state(
        &columns.state,
        columns.action_ref,
        columns.outcome_json,
        columns.reservation_expires_at,
    )?;
    Ok(GuardRow {
        guard_id: parse_id(columns.guard_id, "guard", GuardId::try_new)?,
        commitment_id: parse_id(columns.commitment_id, "commitment", CommitmentId::try_new)?,
        claim_epoch: ClaimEpoch::try_from_raw(columns.claim_epoch)?,
        boot_generation: BootGeneration::try_from_raw(columns.boot_generation)?,
        state,
        reservation_expires_at: columns
            .reservation_expires_at
            .map(MonotonicInstant::try_from_raw)
            .transpose()?,
        state_version: columns.state_version,
    })
}

fn query_guard_scopes(
    transaction: &Transaction<'_>,
    guard_id: &GuardId,
) -> Result<ScopeSet, StoreError> {
    let mut statement = transaction.prepare(
        "SELECT scope_key FROM execution_guard_scopes
         WHERE guard_id = ?1 ORDER BY ordinal",
    )?;
    let mut rows = statement.query(params![guard_id.as_ref()])?;
    let mut scope_keys = Vec::new();
    while let Some(row) = rows.next()? {
        scope_keys.push(parse_id(row.get(0)?, "scope", ScopeKey::try_new)?);
    }
    Ok(ScopeSet::try_new(scope_keys)?)
}

fn query_guard_scopes_connection(
    connection: &Connection,
    guard_id: &GuardId,
) -> Result<ScopeSet, StoreError> {
    let mut statement = connection.prepare(
        "SELECT scope_key FROM execution_guard_scopes
         WHERE guard_id = ?1 ORDER BY ordinal",
    )?;
    let mut rows = statement.query(params![guard_id.as_ref()])?;
    let mut scope_keys = Vec::new();
    while let Some(row) = rows.next()? {
        scope_keys.push(parse_id(row.get(0)?, "scope", ScopeKey::try_new)?);
    }
    Ok(ScopeSet::try_new(scope_keys)?)
}

fn expire_relevant_reservations(
    transaction: &Transaction<'_>,
    scope_keys: &[ScopeKey],
    now_ticks: i64,
) -> Result<(), StoreError> {
    let mut expired_guards = HashSet::new();
    for scope_key in scope_keys {
        let guard_id: Option<String> = transaction
            .query_row(
                "SELECT guard_id FROM scope_locks
                 WHERE scope_key = ?1 AND lock_state = 'reserved'
                   AND reservation_expires_at <= ?2",
                params![scope_key.as_ref(), now_ticks],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(guard_id) = guard_id {
            expired_guards.insert(guard_id);
        }
    }
    for guard_id in expired_guards {
        let goal_id: String = transaction.query_row(
            "SELECT c.goal_id
             FROM execution_guards g JOIN commitments c
               ON c.commitment_id = g.commitment_id
             WHERE g.guard_id = ?1",
            params![guard_id],
            |row| row.get(0),
        )?;
        let changed = transaction.execute(
            "UPDATE execution_guards
             SET state = 'invalidated', reservation_expires_at = NULL,
                 state_version = state_version + 1
             WHERE guard_id = ?1 AND state = 'issued'",
            params![guard_id],
        )?;
        if changed == 1 {
            transaction.execute(
                "DELETE FROM scope_locks WHERE guard_id = ?1 AND lock_state = 'reserved'",
                params![guard_id],
            )?;
            let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
            let guard_id = parse_id(guard_id, "guard", GuardId::try_new)?;
            append_event(
                transaction,
                &goal_id,
                &WorkEvent::GuardReservationExpired { guard_id },
                now_ticks,
            )?;
        }
    }
    Ok(())
}

fn sync_commitment_children(
    transaction: &Transaction<'_>,
    commitment: &Commitment,
    boot_generation: BootGeneration,
) -> Result<(), StoreError> {
    transaction.execute(
        "DELETE FROM commitment_prerequisites WHERE commitment_id = ?1",
        params![commitment.commitment_id().as_ref()],
    )?;
    for (ordinal, prerequisite) in commitment.prerequisites().iter().enumerate() {
        let (kind, reference_id, prerequisite_commitment_id) = match prerequisite {
            Prerequisite::Commitment(id) => ("commitment", id.as_ref(), Some(id.as_ref())),
            Prerequisite::TethersVerification(reference) => {
                ("tethers_verification", reference.as_ref(), None)
            }
            Prerequisite::HumanDecision(reference) => ("human_decision", reference.as_ref(), None),
        };
        transaction.execute(
            "INSERT INTO commitment_prerequisites
                (commitment_id, ordinal, prerequisite_type, reference_id,
                 prerequisite_commitment_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                commitment.commitment_id().as_ref(),
                ordinal as i64,
                kind,
                reference_id,
                prerequisite_commitment_id,
            ],
        )?;
    }
    transaction.execute(
        "DELETE FROM commitment_acceptance_refs WHERE commitment_id = ?1",
        params![commitment.commitment_id().as_ref()],
    )?;
    for (ordinal, reference) in commitment.acceptance_refs().iter().enumerate() {
        transaction.execute(
            "INSERT INTO commitment_acceptance_refs
                (commitment_id, ordinal, reference_id)
             VALUES (?1, ?2, ?3)",
            params![
                commitment.commitment_id().as_ref(),
                ordinal as i64,
                reference.as_ref()
            ],
        )?;
    }
    transaction.execute(
        "DELETE FROM claims WHERE commitment_id = ?1",
        params![commitment.commitment_id().as_ref()],
    )?;
    if let Some(claim) = commitment.claim() {
        insert_claim(
            transaction,
            commitment.commitment_id(),
            claim,
            boot_generation,
        )?;
    }
    Ok(())
}

fn insert_claim(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
    claim: &ClaimLease,
    boot_generation: BootGeneration,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO claims
            (commitment_id, worker_id, claim_epoch, last_heartbeat, boot_generation)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            commitment_id.as_ref(),
            claim.worker_id().as_ref(),
            claim.epoch().value(),
            claim.last_heartbeat().ticks(),
            boot_generation.value(),
        ],
    )?;
    Ok(())
}

fn append_event(
    transaction: &Transaction<'_>,
    goal_id: &GoalId,
    event: &WorkEvent,
    created_at: i64,
) -> Result<i64, StoreError> {
    let payload = serde_json::to_string(&encode_event_payload(event))?;
    transaction.execute(
        "INSERT INTO work_events
            (goal_id, commitment_id, event_type, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            goal_id.as_ref(),
            event_commitment_id(event).map(AsRef::as_ref),
            event_type(event),
            payload,
            created_at,
        ],
    )?;
    Ok(transaction.last_insert_rowid())
}

fn validate_goal_event(
    event: &WorkEvent,
    goal_id: &GoalId,
    operation: &str,
) -> Result<(), StoreError> {
    match event {
        WorkEvent::GoalCreated {
            goal_id: event_goal,
            ..
        }
        | WorkEvent::GoalRevised {
            goal_id: event_goal,
            ..
        } if event_goal == goal_id => Ok(()),
        _ => Err(StoreError::InvalidMutation(format!(
            "{operation} requires a matching GoalCreated or GoalRevised event"
        ))),
    }
}

fn validate_commitment_event(
    event: &WorkEvent,
    commitment_id: &CommitmentId,
    operation: &str,
) -> Result<(), StoreError> {
    if event_commitment_id(event) == Some(commitment_id) {
        Ok(())
    } else {
        Err(StoreError::InvalidMutation(format!(
            "{operation} requires an event for commitment {commitment_id}"
        )))
    }
}

fn validate_attention_event(
    event: &WorkEvent,
    attention_id: &AttentionId,
) -> Result<(), StoreError> {
    match event {
        WorkEvent::AttentionRaised {
            attention_id: event_id,
            ..
        } if event_id == attention_id => Ok(()),
        _ => Err(StoreError::InvalidMutation(
            "attention mutation requires a matching attention event".to_owned(),
        )),
    }
}

fn ensure_version(
    entity: &'static str,
    id: String,
    expected: i64,
    actual: i64,
) -> Result<(), StoreError> {
    if expected == actual {
        Ok(())
    } else {
        Err(StoreError::VersionConflict {
            entity,
            id,
            expected,
            actual,
        })
    }
}

fn next_state_version(current: i64) -> Result<i64, StoreError> {
    current
        .checked_add(1)
        .ok_or_else(|| StoreError::InvalidMutation("state version space is exhausted".to_owned()))
}

fn not_found(entity: &'static str, id: String) -> StoreError {
    StoreError::NotFound { entity, id }
}

fn parse_id<T>(
    value: String,
    kind: &'static str,
    constructor: impl FnOnce(String) -> Result<T, resolve_core::DomainError>,
) -> Result<T, StoreError> {
    constructor(value.clone()).map_err(|_| {
        StoreError::InvalidPersistedData(format!("invalid {kind} identifier {value:?}"))
    })
}

fn positive_u64(value: i64, field: &str) -> Result<u64, StoreError> {
    if value > 0 {
        Ok(value as u64)
    } else {
        Err(StoreError::InvalidPersistedData(format!(
            "{field} must be positive, received {value}"
        )))
    }
}

fn nonnegative_u64(value: i64, field: &str) -> Result<u64, StoreError> {
    if value >= 0 {
        Ok(value as u64)
    } else {
        Err(StoreError::InvalidPersistedData(format!(
            "{field} must not be negative, received {value}"
        )))
    }
}

fn encode_goal_state(state: &GoalState) -> &'static str {
    match state {
        GoalState::Active => "active",
        GoalState::Completed => "completed",
        GoalState::Cancelled => "cancelled",
        GoalState::Abandoned => "abandoned",
    }
}

fn decode_goal_state(value: &str) -> Result<GoalState, StoreError> {
    match value {
        "active" => Ok(GoalState::Active),
        "completed" => Ok(GoalState::Completed),
        "cancelled" => Ok(GoalState::Cancelled),
        "abandoned" => Ok(GoalState::Abandoned),
        _ => Err(StoreError::InvalidPersistedData(format!(
            "unknown goal state {value:?}"
        ))),
    }
}

fn encode_attention_state(state: AttentionState) -> &'static str {
    match state {
        AttentionState::Open => "open",
        AttentionState::Cleared => "cleared",
    }
}

fn decode_attention_state(value: &str) -> Result<AttentionState, StoreError> {
    match value {
        "open" => Ok(AttentionState::Open),
        "cleared" => Ok(AttentionState::Cleared),
        _ => Err(StoreError::InvalidPersistedData(format!(
            "unknown attention state {value:?}"
        ))),
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "state", content = "reason")]
enum StoredCommitmentState {
    Proposed,
    Ready,
    Claimed,
    Working,
    Waiting(StoredWaitingReason),
    RecoveryPending,
    CompletionProposed,
    Completed,
    Cancelled,
    Abandoned,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", content = "value")]
enum StoredWaitingReason {
    Prerequisite(String),
    Authority(String),
    HumanDecision(String),
    Verification(String),
    UncertainAction(String),
    External(String),
}

fn encode_commitment_state(state: &CommitmentState) -> Result<String, StoreError> {
    Ok(serde_json::to_string(&StoredCommitmentState::from(state))?)
}

fn decode_commitment_state(value: &str) -> Result<CommitmentState, StoreError> {
    let stored: StoredCommitmentState = serde_json::from_str(value).map_err(|error| {
        StoreError::InvalidPersistedData(format!("invalid commitment state JSON: {error}"))
    })?;
    stored.into_domain()
}

impl From<&CommitmentState> for StoredCommitmentState {
    fn from(state: &CommitmentState) -> Self {
        match state {
            CommitmentState::Proposed => Self::Proposed,
            CommitmentState::Ready => Self::Ready,
            CommitmentState::Claimed => Self::Claimed,
            CommitmentState::Working => Self::Working,
            CommitmentState::Waiting(reason) => Self::Waiting(reason.into()),
            CommitmentState::RecoveryPending => Self::RecoveryPending,
            CommitmentState::CompletionProposed => Self::CompletionProposed,
            CommitmentState::Completed => Self::Completed,
            CommitmentState::Cancelled => Self::Cancelled,
            CommitmentState::Abandoned => Self::Abandoned,
        }
    }
}

impl From<&WaitingReason> for StoredWaitingReason {
    fn from(reason: &WaitingReason) -> Self {
        match reason {
            WaitingReason::Prerequisite(id) => Self::Prerequisite(id.as_ref().to_owned()),
            WaitingReason::Authority(id) => Self::Authority(id.as_ref().to_owned()),
            WaitingReason::HumanDecision(id) => Self::HumanDecision(id.as_ref().to_owned()),
            WaitingReason::Verification(id) => Self::Verification(id.as_ref().to_owned()),
            WaitingReason::UncertainAction(id) => Self::UncertainAction(id.as_ref().to_owned()),
            WaitingReason::External(value) => Self::External(value.clone()),
        }
    }
}

impl StoredCommitmentState {
    fn into_domain(self) -> Result<CommitmentState, StoreError> {
        Ok(match self {
            Self::Proposed => CommitmentState::Proposed,
            Self::Ready => CommitmentState::Ready,
            Self::Claimed => CommitmentState::Claimed,
            Self::Working => CommitmentState::Working,
            Self::Waiting(reason) => CommitmentState::Waiting(reason.into_domain()?),
            Self::RecoveryPending => CommitmentState::RecoveryPending,
            Self::CompletionProposed => CommitmentState::CompletionProposed,
            Self::Completed => CommitmentState::Completed,
            Self::Cancelled => CommitmentState::Cancelled,
            Self::Abandoned => CommitmentState::Abandoned,
        })
    }
}

impl StoredWaitingReason {
    fn into_domain(self) -> Result<WaitingReason, StoreError> {
        Ok(match self {
            Self::Prerequisite(value) => {
                WaitingReason::Prerequisite(parse_id(value, "commitment", CommitmentId::try_new)?)
            }
            Self::Authority(value) => WaitingReason::Authority(parse_id(
                value,
                "Tethers action",
                TethersActionRef::try_new,
            )?),
            Self::HumanDecision(value) => WaitingReason::HumanDecision(parse_id(
                value,
                "human decision",
                HumanDecisionRef::try_new,
            )?),
            Self::Verification(value) => WaitingReason::Verification(parse_id(
                value,
                "Tethers contract",
                TethersContractRef::try_new,
            )?),
            Self::UncertainAction(value) => WaitingReason::UncertainAction(parse_id(
                value,
                "Tethers action",
                TethersActionRef::try_new,
            )?),
            Self::External(value) => WaitingReason::External(value),
        })
    }
}

fn decode_prerequisite(
    kind: String,
    reference_id: String,
    prerequisite_commitment_id: Option<String>,
) -> Result<Prerequisite, StoreError> {
    match kind.as_str() {
        "commitment" => {
            let stored_id = prerequisite_commitment_id.ok_or_else(|| {
                StoreError::InvalidPersistedData(
                    "commitment prerequisite is missing its typed reference".to_owned(),
                )
            })?;
            if stored_id != reference_id {
                return Err(StoreError::InvalidPersistedData(
                    "commitment prerequisite references disagree".to_owned(),
                ));
            }
            Ok(Prerequisite::Commitment(parse_id(
                reference_id,
                "commitment",
                CommitmentId::try_new,
            )?))
        }
        "tethers_verification" => Ok(Prerequisite::TethersVerification(parse_id(
            reference_id,
            "Tethers contract",
            TethersContractRef::try_new,
        )?)),
        "human_decision" => Ok(Prerequisite::HumanDecision(parse_id(
            reference_id,
            "human decision",
            HumanDecisionRef::try_new,
        )?)),
        _ => Err(StoreError::InvalidPersistedData(format!(
            "unknown prerequisite type {kind:?}"
        ))),
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", content = "data")]
enum StoredEventPayload {
    GoalCreated {
        goal_id: String,
        revision: u64,
    },
    GoalRevised {
        goal_id: String,
        revision: u64,
    },
    CommitmentCreated {
        commitment_id: String,
        goal_id: String,
    },
    CommitmentActivated {
        commitment_id: String,
    },
    CommitmentClaimed {
        commitment_id: String,
        worker_id: String,
        epoch: u64,
    },
    CommitmentStarted {
        commitment_id: String,
    },
    CommitmentWaiting {
        commitment_id: String,
        reason: StoredWaitingReason,
    },
    CommitmentRecoveryPending {
        commitment_id: String,
    },
    CommitmentCompletionProposed {
        commitment_id: String,
    },
    CommitmentCompleted {
        commitment_id: String,
    },
    CommitmentCancelled {
        commitment_id: String,
    },
    CommitmentAbandoned {
        commitment_id: String,
    },
    HeartbeatAccepted {
        commitment_id: String,
        worker_id: String,
        epoch: u64,
    },
    LeaseExpired {
        commitment_id: String,
        worker_id: String,
        epoch: u64,
    },
    GuardIssued {
        guard_id: String,
        commitment_id: String,
        claim_epoch: u64,
    },
    GuardAdmitted {
        guard_id: String,
        commitment_id: String,
        action_ref: String,
    },
    GuardInvalidated {
        guard_id: String,
    },
    GuardReservationExpired {
        guard_id: String,
    },
    TethersOutcomeRecorded {
        outcome: StoredTethersOutcome,
    },
    RecoveryCompleted {
        commitment_id: String,
        action_ref: String,
    },
    StructuralProposalApplied {
        target: String,
    },
    AttentionRaised {
        attention_id: String,
        commitment_id: Option<String>,
    },
    AttentionCleared {
        attention_id: String,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", content = "action_ref")]
enum StoredTethersOutcome {
    Succeeded(String),
    Failed(String),
    Uncertain(String),
}

fn decode_guard_state(
    state: &str,
    action_ref: Option<String>,
    outcome_json: Option<String>,
    reservation_expires_at: Option<i64>,
) -> Result<GuardState, StoreError> {
    match state {
        "issued" => {
            if action_ref.is_some() || outcome_json.is_some() || reservation_expires_at.is_none() {
                return Err(StoreError::InvalidPersistedData(
                    "issued guard has inconsistent lifecycle fields".to_owned(),
                ));
            }
            Ok(GuardState::Issued)
        }
        "admitted" => {
            let action_ref = action_ref
                .ok_or_else(|| invalid_guard_fields("admitted guard is missing action_ref"))?;
            if outcome_json.is_some() || reservation_expires_at.is_some() {
                return Err(invalid_guard_fields(
                    "admitted guard has inconsistent lifecycle fields",
                ));
            }
            Ok(GuardState::Admitted {
                action_ref: parse_id(action_ref, "Tethers action", TethersActionRef::try_new)?,
            })
        }
        "resolved" => {
            let action_ref = action_ref
                .ok_or_else(|| invalid_guard_fields("resolved guard is missing action_ref"))?;
            let outcome_json = outcome_json
                .ok_or_else(|| invalid_guard_fields("resolved guard is missing outcome"))?;
            if reservation_expires_at.is_some() {
                return Err(invalid_guard_fields(
                    "resolved guard has a reservation expiration",
                ));
            }
            let outcome: StoredTethersOutcome = serde_json::from_str(&outcome_json)?;
            let outcome = outcome.into_domain()?;
            let action_ref = parse_id(action_ref, "Tethers action", TethersActionRef::try_new)?;
            if outcome.action_ref() != &action_ref {
                return Err(invalid_guard_fields(
                    "resolved guard action and outcome disagree",
                ));
            }
            Ok(GuardState::Resolved {
                action_ref,
                outcome,
            })
        }
        "uncertain" => {
            let action_ref = action_ref
                .ok_or_else(|| invalid_guard_fields("uncertain guard is missing action_ref"))?;
            if outcome_json.is_some() || reservation_expires_at.is_some() {
                return Err(invalid_guard_fields(
                    "uncertain guard has inconsistent lifecycle fields",
                ));
            }
            Ok(GuardState::Uncertain {
                action_ref: parse_id(action_ref, "Tethers action", TethersActionRef::try_new)?,
            })
        }
        "invalidated" => {
            if action_ref.is_some() || outcome_json.is_some() || reservation_expires_at.is_some() {
                return Err(invalid_guard_fields(
                    "invalidated guard has lifecycle fields",
                ));
            }
            Ok(GuardState::Invalidated)
        }
        _ => Err(invalid_guard_fields("unknown guard state")),
    }
}

fn invalid_guard_fields(message: &str) -> StoreError {
    StoreError::InvalidPersistedData(message.to_owned())
}

impl StoredTethersOutcome {
    fn into_domain(self) -> Result<TethersOutcome, StoreError> {
        Ok(match self {
            Self::Succeeded(value) => TethersOutcome::Succeeded {
                action_ref: parse_id(value, "Tethers action", TethersActionRef::try_new)?,
            },
            Self::Failed(value) => TethersOutcome::Failed {
                action_ref: parse_id(value, "Tethers action", TethersActionRef::try_new)?,
            },
            Self::Uncertain(value) => TethersOutcome::Uncertain {
                action_ref: parse_id(value, "Tethers action", TethersActionRef::try_new)?,
            },
        })
    }
}

fn encode_event_payload(event: &WorkEvent) -> StoredEventPayload {
    match event {
        WorkEvent::GoalCreated { goal_id, revision } => StoredEventPayload::GoalCreated {
            goal_id: goal_id.as_ref().to_owned(),
            revision: revision.value(),
        },
        WorkEvent::GoalRevised { goal_id, revision } => StoredEventPayload::GoalRevised {
            goal_id: goal_id.as_ref().to_owned(),
            revision: revision.value(),
        },
        WorkEvent::CommitmentCreated {
            commitment_id,
            goal_id,
        } => StoredEventPayload::CommitmentCreated {
            commitment_id: commitment_id.as_ref().to_owned(),
            goal_id: goal_id.as_ref().to_owned(),
        },
        WorkEvent::CommitmentActivated { commitment_id } => {
            StoredEventPayload::CommitmentActivated {
                commitment_id: commitment_id.as_ref().to_owned(),
            }
        }
        WorkEvent::CommitmentClaimed {
            commitment_id,
            worker_id,
            epoch,
        } => StoredEventPayload::CommitmentClaimed {
            commitment_id: commitment_id.as_ref().to_owned(),
            worker_id: worker_id.as_ref().to_owned(),
            epoch: epoch.value(),
        },
        WorkEvent::CommitmentStarted { commitment_id } => StoredEventPayload::CommitmentStarted {
            commitment_id: commitment_id.as_ref().to_owned(),
        },
        WorkEvent::CommitmentWaiting {
            commitment_id,
            reason,
        } => StoredEventPayload::CommitmentWaiting {
            commitment_id: commitment_id.as_ref().to_owned(),
            reason: reason.into(),
        },
        WorkEvent::CommitmentRecoveryPending { commitment_id } => {
            StoredEventPayload::CommitmentRecoveryPending {
                commitment_id: commitment_id.as_ref().to_owned(),
            }
        }
        WorkEvent::CommitmentCompletionProposed { commitment_id } => {
            StoredEventPayload::CommitmentCompletionProposed {
                commitment_id: commitment_id.as_ref().to_owned(),
            }
        }
        WorkEvent::CommitmentCompleted { commitment_id } => {
            StoredEventPayload::CommitmentCompleted {
                commitment_id: commitment_id.as_ref().to_owned(),
            }
        }
        WorkEvent::CommitmentCancelled { commitment_id } => {
            StoredEventPayload::CommitmentCancelled {
                commitment_id: commitment_id.as_ref().to_owned(),
            }
        }
        WorkEvent::CommitmentAbandoned { commitment_id } => {
            StoredEventPayload::CommitmentAbandoned {
                commitment_id: commitment_id.as_ref().to_owned(),
            }
        }
        WorkEvent::HeartbeatAccepted {
            commitment_id,
            worker_id,
            epoch,
        } => StoredEventPayload::HeartbeatAccepted {
            commitment_id: commitment_id.as_ref().to_owned(),
            worker_id: worker_id.as_ref().to_owned(),
            epoch: epoch.value(),
        },
        WorkEvent::LeaseExpired {
            commitment_id,
            worker_id,
            epoch,
        } => StoredEventPayload::LeaseExpired {
            commitment_id: commitment_id.as_ref().to_owned(),
            worker_id: worker_id.as_ref().to_owned(),
            epoch: epoch.value(),
        },
        WorkEvent::GuardIssued {
            guard_id,
            commitment_id,
            claim_epoch,
        } => StoredEventPayload::GuardIssued {
            guard_id: guard_id.as_ref().to_owned(),
            commitment_id: commitment_id.as_ref().to_owned(),
            claim_epoch: claim_epoch.value(),
        },
        WorkEvent::GuardAdmitted {
            guard_id,
            commitment_id,
            action_ref,
        } => StoredEventPayload::GuardAdmitted {
            guard_id: guard_id.as_ref().to_owned(),
            commitment_id: commitment_id.as_ref().to_owned(),
            action_ref: action_ref.as_ref().to_owned(),
        },
        WorkEvent::GuardInvalidated { guard_id } => StoredEventPayload::GuardInvalidated {
            guard_id: guard_id.as_ref().to_owned(),
        },
        WorkEvent::GuardReservationExpired { guard_id } => {
            StoredEventPayload::GuardReservationExpired {
                guard_id: guard_id.as_ref().to_owned(),
            }
        }
        WorkEvent::TethersOutcomeRecorded { outcome } => {
            StoredEventPayload::TethersOutcomeRecorded {
                outcome: match outcome {
                    TethersOutcome::Succeeded { action_ref } => {
                        StoredTethersOutcome::Succeeded(action_ref.as_ref().to_owned())
                    }
                    TethersOutcome::Failed { action_ref } => {
                        StoredTethersOutcome::Failed(action_ref.as_ref().to_owned())
                    }
                    TethersOutcome::Uncertain { action_ref } => {
                        StoredTethersOutcome::Uncertain(action_ref.as_ref().to_owned())
                    }
                },
            }
        }
        WorkEvent::RecoveryCompleted {
            commitment_id,
            action_ref,
        } => StoredEventPayload::RecoveryCompleted {
            commitment_id: commitment_id.as_ref().to_owned(),
            action_ref: action_ref.as_ref().to_owned(),
        },
        WorkEvent::StructuralProposalApplied { target } => {
            StoredEventPayload::StructuralProposalApplied {
                target: target.as_ref().to_owned(),
            }
        }
        WorkEvent::AttentionRaised {
            attention_id,
            commitment_id,
        } => StoredEventPayload::AttentionRaised {
            attention_id: attention_id.as_ref().to_owned(),
            commitment_id: commitment_id.as_ref().map(|id| id.as_ref().to_owned()),
        },
        WorkEvent::AttentionCleared { attention_id } => StoredEventPayload::AttentionCleared {
            attention_id: attention_id.as_ref().to_owned(),
        },
    }
}

fn event_type(event: &WorkEvent) -> &'static str {
    match event {
        WorkEvent::GoalCreated { .. } => "goal_created",
        WorkEvent::GoalRevised { .. } => "goal_revised",
        WorkEvent::CommitmentCreated { .. } => "commitment_created",
        WorkEvent::CommitmentActivated { .. } => "commitment_activated",
        WorkEvent::CommitmentClaimed { .. } => "commitment_claimed",
        WorkEvent::CommitmentStarted { .. } => "commitment_started",
        WorkEvent::CommitmentWaiting { .. } => "commitment_waiting",
        WorkEvent::CommitmentRecoveryPending { .. } => "commitment_recovery_pending",
        WorkEvent::CommitmentCompletionProposed { .. } => "commitment_completion_proposed",
        WorkEvent::CommitmentCompleted { .. } => "commitment_completed",
        WorkEvent::CommitmentCancelled { .. } => "commitment_cancelled",
        WorkEvent::CommitmentAbandoned { .. } => "commitment_abandoned",
        WorkEvent::HeartbeatAccepted { .. } => "heartbeat_accepted",
        WorkEvent::LeaseExpired { .. } => "lease_expired",
        WorkEvent::GuardIssued { .. } => "guard_issued",
        WorkEvent::GuardAdmitted { .. } => "guard_admitted",
        WorkEvent::GuardInvalidated { .. } => "guard_invalidated",
        WorkEvent::GuardReservationExpired { .. } => "guard_reservation_expired",
        WorkEvent::TethersOutcomeRecorded { .. } => "tethers_outcome_recorded",
        WorkEvent::RecoveryCompleted { .. } => "recovery_completed",
        WorkEvent::StructuralProposalApplied { .. } => "structural_proposal_applied",
        WorkEvent::AttentionRaised { .. } => "attention_raised",
        WorkEvent::AttentionCleared { .. } => "attention_cleared",
    }
}

fn event_commitment_id(event: &WorkEvent) -> Option<&CommitmentId> {
    match event {
        WorkEvent::CommitmentCreated { commitment_id, .. }
        | WorkEvent::CommitmentActivated { commitment_id }
        | WorkEvent::CommitmentClaimed { commitment_id, .. }
        | WorkEvent::CommitmentStarted { commitment_id }
        | WorkEvent::CommitmentWaiting { commitment_id, .. }
        | WorkEvent::CommitmentRecoveryPending { commitment_id }
        | WorkEvent::CommitmentCompletionProposed { commitment_id }
        | WorkEvent::CommitmentCompleted { commitment_id }
        | WorkEvent::CommitmentCancelled { commitment_id }
        | WorkEvent::CommitmentAbandoned { commitment_id }
        | WorkEvent::HeartbeatAccepted { commitment_id, .. }
        | WorkEvent::LeaseExpired { commitment_id, .. }
        | WorkEvent::GuardIssued { commitment_id, .. }
        | WorkEvent::GuardAdmitted { commitment_id, .. }
        | WorkEvent::RecoveryCompleted { commitment_id, .. } => Some(commitment_id),
        WorkEvent::AttentionRaised { commitment_id, .. } => commitment_id.as_ref(),
        WorkEvent::GoalCreated { .. }
        | WorkEvent::GoalRevised { .. }
        | WorkEvent::GuardInvalidated { .. }
        | WorkEvent::GuardReservationExpired { .. }
        | WorkEvent::TethersOutcomeRecorded { .. }
        | WorkEvent::StructuralProposalApplied { .. }
        | WorkEvent::AttentionCleared { .. } => None,
    }
}
