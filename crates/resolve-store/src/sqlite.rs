use crate::error::StoreError;
use crate::schema;
use resolve_core::planning::{validate_acyclic, validate_proposal_shape};
use resolve_core::{
    AcceptanceRef, AttentionId, AttentionItem, AttentionState, BootGeneration, ClaimEpoch,
    ClaimLease, Commitment, CommitmentId, CommitmentState, DomainError, ExecutionGuard, GoalId,
    GoalSpec, GoalState, GuardId, GuardState, HumanDecisionRef, MonotonicDuration,
    MonotonicInstant, NewCommitment, Prerequisite, ScopeKey, ScopeSet, StructuralChange,
    StructuralProposal, TethersActionRef, TethersContractRef, TethersOutcome, WaitingReason,
    WorkEvent, WorkerId,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    last_claim_epoch: Option<u64>,
    outstanding_action: Option<TethersActionRef>,
    replacement_terminal_id: Option<CommitmentId>,
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

    pub fn last_claim_epoch(&self) -> Option<u64> {
        self.last_claim_epoch
    }

    pub fn outstanding_action(&self) -> Option<&TethersActionRef> {
        self.outstanding_action.as_ref()
    }

    pub fn replacement_terminal_id(&self) -> Option<&CommitmentId> {
        self.replacement_terminal_id.as_ref()
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
    last_claim_epoch: Option<i64>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeRecording {
    Recorded,
    AlreadyRecorded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatResult {
    Recorded,
    AlreadyCurrent,
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
        let mut store = Self::from_connection(connection, true)?;
        store.startup_recovery()?;
        Ok(store)
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
        if !matches!(commitment.state(), CommitmentState::Proposed)
            || commitment.claim().is_some()
            || commitment.outstanding_action().is_some()
        {
            return Err(StoreError::InvalidMutation(
                "new commitments must begin Proposed without a claim or action".to_owned(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let boot_generation = current_boot_generation(&transaction)?;
        let event_id = append_event(&transaction, commitment.goal_id(), event, created_at)?;
        insert_commitment_snapshot(&transaction, commitment, boot_generation)?;
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
        let (
            current_version,
            current_goal_id,
            current_parent_id,
            current_description,
            current_state_json,
            current_replacement_terminal_id,
            current_outstanding_action,
        ): (
            i64,
            String,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT state_version, goal_id, parent_id, description, state_json,
                        replacement_terminal_id, outstanding_action
                 FROM commitments WHERE commitment_id = ?1",
                params![commitment.commitment_id().as_ref()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| not_found("commitment", commitment.commitment_id().to_string()))?;
        let current_state = decode_commitment_state(&current_state_json)?;
        ensure_version(
            "commitment",
            commitment.commitment_id().to_string(),
            expected_state_version,
            current_version,
        )?;
        if current_goal_id != commitment.goal_id().as_ref()
            || current_parent_id.as_deref() != commitment.parent_id().map(AsRef::as_ref)
            || current_description != commitment.description()
        {
            return Err(StoreError::InvalidMutation(
                "generic commitment persistence cannot change identity or topology".to_owned(),
            ));
        }
        if query_prerequisites_connection(&transaction, commitment.commitment_id())?
            != commitment.prerequisites()
            || query_acceptance_refs_connection(&transaction, commitment.commitment_id())?
                != commitment.acceptance_refs()
        {
            return Err(StoreError::InvalidMutation(
                "generic commitment persistence cannot change structural references".to_owned(),
            ));
        }
        if let Some(action_ref) = current_outstanding_action {
            return Err(StoreError::OutstandingAction {
                commitment_id: commitment.commitment_id().to_string(),
                action_ref,
            });
        }
        if current_replacement_terminal_id.is_some()
            && !current_state.is_terminal()
            && !matches!(
                commitment.state(),
                CommitmentState::Cancelled | CommitmentState::Abandoned
            )
        {
            return Err(DomainError::InvalidStructuralProposal {
                reason: "ordinary persistence cannot bypass an active composite barrier",
            }
            .into());
        }
        if commitment.outstanding_action().is_some() {
            return Err(StoreError::InvalidMutation(
                "generic commitment persistence cannot create an outstanding action".to_owned(),
            ));
        }
        current_state.validate_transition_to(commitment.state())?;
        validate_commitment_event_state(event, commitment.state())?;
        let current_high_water =
            current_last_claim_epoch(&transaction, commitment.commitment_id())?;
        validate_claim_persistence(&transaction, commitment, &current_state, current_high_water)?;
        let next_state_version = next_state_version(expected_state_version)?;
        let event_id = append_event(&transaction, commitment.goal_id(), event, created_at)?;
        transaction.execute(
            "UPDATE commitments
                SET goal_id = ?1, parent_id = ?2, description = ?3, state_json = ?4,
                    last_claim_epoch = CASE
                        WHEN ?5 IS NULL THEN last_claim_epoch
                        WHEN last_claim_epoch IS NULL OR last_claim_epoch < ?5 THEN ?5
                        ELSE last_claim_epoch END,
                    outstanding_action = ?6, state_version = ?7
             WHERE commitment_id = ?8 AND state_version = ?9",
            params![
                commitment.goal_id().as_ref(),
                commitment.parent_id().map(AsRef::as_ref),
                commitment.description(),
                state_json,
                commitment.last_claim_epoch().map(|epoch| epoch.value()),
                commitment.outstanding_action().map(AsRef::as_ref),
                next_state_version,
                commitment.commitment_id().as_ref(),
                expected_state_version,
            ],
        )?;
        sync_commitment_children(&transaction, commitment, boot_generation)?;
        if matches!(commitment.state(), CommitmentState::Completed) {
            complete_composite_barrier_cascade(
                &transaction,
                commitment.commitment_id(),
                created_at,
            )?;
        }
        transaction.commit()?;
        Ok(event_id)
    }

    /// Apply one closed, claim-fenced structural proposal atomically.
    pub fn apply_structural_proposal(
        &mut self,
        proposal: StructuralProposal,
        worker_id: &WorkerId,
        boot_generation: BootGeneration,
        now: MonotonicInstant,
        claim_lease_duration: MonotonicDuration,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        validate_proposal_shape(&proposal)?;
        let (target, epoch) = match &proposal {
            StructuralProposal::Decompose { target, epoch, .. }
            | StructuralProposal::AddPrerequisite { target, epoch, .. }
            | StructuralProposal::Abandon { target, epoch, .. } => (target, *epoch),
        };
        let now_ticks = checked_i64(now.ticks(), "current monotonic instant")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_boot = current_boot_generation(&transaction)?;
        if boot_generation != current_boot {
            return Err(StoreError::ClaimMismatch {
                commitment_id: target.to_string(),
                reason: "proposal is not from the current boot generation".to_owned(),
            });
        }
        let context = query_structural_target(&transaction, target)?;
        let state = decode_commitment_state(&context.state_json)?;
        if state.is_terminal() {
            return Err(DomainError::TerminalCommitmentImmutable { state }.into());
        }
        if matches!(state, CommitmentState::RecoveryPending)
            || matches!(
                state,
                CommitmentState::Waiting(WaitingReason::UncertainAction(_))
            )
        {
            return Err(DomainError::InvalidStructuralProposal {
                reason: "structural mutation is not valid during recovery or uncertainty",
            }
            .into());
        }
        if !state.requires_active_claim() {
            return Err(StoreError::ClaimMismatch {
                commitment_id: target.to_string(),
                reason: format!("structural mutation is not valid in {state:?}"),
            });
        }
        if matches!(&proposal, StructuralProposal::Decompose { .. })
            && !matches!(state, CommitmentState::Working)
        {
            return Err(DomainError::InvalidStructuralProposal {
                reason: "decomposition requires a WORKING commitment",
            }
            .into());
        }
        if !matches!(&proposal, StructuralProposal::Abandon { .. })
            && context.replacement_terminal_id.is_some()
        {
            return Err(DomainError::InvalidStructuralProposal {
                reason: "an active composite barrier accepts only explicit abandonment",
            }
            .into());
        }
        if let Some(action_ref) = context.outstanding_action {
            return Err(StoreError::OutstandingAction {
                commitment_id: target.to_string(),
                action_ref,
            });
        }
        let stored_epoch = context
            .claim_epoch
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: target.to_string(),
                reason: "commitment has no current claim".to_owned(),
            })?;
        let epoch_ticks = checked_i64(epoch.value(), "claim epoch")?;
        if stored_epoch != epoch_ticks || context.last_claim_epoch != Some(stored_epoch) {
            return Err(StoreError::ClaimMismatch {
                commitment_id: target.to_string(),
                reason: "proposal epoch is not the current durable claim epoch".to_owned(),
            });
        }
        let current_boot_ticks = checked_i64(current_boot.value(), "boot generation")?;
        if context.worker_id.as_deref() != Some(worker_id.as_ref())
            || context.claim_boot_generation != Some(current_boot_ticks)
        {
            return Err(StoreError::ClaimMismatch {
                commitment_id: target.to_string(),
                reason: "proposal worker or claim boot generation does not match".to_owned(),
            });
        }
        let heartbeat_ticks = context
            .last_heartbeat
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: target.to_string(),
                reason: "commitment has no persisted claim heartbeat".to_owned(),
            })?;
        let heartbeat = MonotonicInstant::try_from_raw(heartbeat_ticks)?;
        let claim_deadline = claim_lease_duration.deadline_from(heartbeat)?;
        if claim_deadline <= now {
            return Err(StoreError::LeaseExpired {
                commitment_id: target.to_string(),
            });
        }
        expire_target_reservations(&transaction, target, now_ticks)?;
        if query_active_guard(&transaction, target)? {
            return Err(StoreError::GuardInvalid {
                guard_id: target.to_string(),
                reason: "structural mutation requires no live or admitted guard".to_owned(),
            });
        }
        let graph = load_dependency_graph(&transaction)?;
        let event_id = match proposal {
            StructuralProposal::Decompose {
                children,
                replacement_terminal_id,
                ..
            } => apply_decomposition(
                &transaction,
                graph,
                &context,
                children,
                replacement_terminal_id,
                current_boot,
                created_at,
            )?,
            StructuralProposal::AddPrerequisite { prerequisite, .. } => {
                apply_add_prerequisite(&transaction, graph, &context, prerequisite, created_at)?
            }
            StructuralProposal::Abandon { rationale, .. } => {
                apply_abandon(&transaction, &context, rationale, created_at)?
            }
        };
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

    /// Explicitly clear an open attention item and append its audit event.
    pub fn clear_attention(
        &mut self,
        goal_id: &GoalId,
        attention_id: &AttentionId,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: String = transaction
            .query_row(
                "SELECT state FROM attention_items WHERE attention_id = ?1",
                params![attention_id.as_ref()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| not_found("attention", attention_id.to_string()))?;
        if decode_attention_state(&state)? != AttentionState::Open {
            return Err(StoreError::InvalidMutation(
                "attention item is already cleared".to_owned(),
            ));
        }
        let event_id = append_event(
            &transaction,
            goal_id,
            &WorkEvent::AttentionCleared {
                attention_id: attention_id.clone(),
            },
            created_at,
        )?;
        if transaction.execute(
            "UPDATE attention_items SET state = 'cleared' WHERE attention_id = ?1 AND state = 'open'",
            params![attention_id.as_ref()],
        )? != 1
        {
            return Err(StoreError::InvalidMutation(
                "attention item changed while clearing".to_owned(),
            ));
        }
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
        let (goal_id, state_json, last_claim_epoch, outstanding_action): (
            String,
            String,
            Option<i64>,
            Option<String>,
        ) = transaction
            .query_row(
                "SELECT goal_id, state_json, last_claim_epoch, outstanding_action
                     FROM commitments WHERE commitment_id = ?1",
                params![commitment_id.as_ref()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
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
        let stored_last_claim_epoch = last_claim_epoch.map(ClaimEpoch::try_from_raw).transpose()?;
        if stored_last_claim_epoch != Some(claim_epoch) {
            return Err(StoreError::InvalidPersistedData(
                "active claim epoch does not equal the durable high-water mark".to_owned(),
            ));
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
                        c.last_claim_epoch, cl.claim_epoch, cl.worker_id, cl.boot_generation
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
                        last_claim_epoch: row.get(4)?,
                        claim_epoch: row.get(5)?,
                        worker: row.get(6)?,
                        boot_generation: row.get(7)?,
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
        let last_claim_epoch = commitment
            .last_claim_epoch
            .map(ClaimEpoch::try_from_raw)
            .transpose()?;
        if last_claim_epoch != Some(guard.claim_epoch) {
            return Err(StoreError::InvalidPersistedData(
                "active claim epoch does not equal the durable high-water mark".to_owned(),
            ));
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
        claim_lease_duration: MonotonicDuration,
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
        let (goal_id, stored_worker, stored_epoch, stored_heartbeat, stored_boot_generation): (
            String,
            String,
            i64,
            i64,
            i64,
        ) = transaction
            .query_row(
                "SELECT c.goal_id, cl.worker_id, cl.claim_epoch, cl.last_heartbeat,
                        cl.boot_generation
                 FROM commitments c JOIN claims cl ON cl.commitment_id = c.commitment_id
                 WHERE c.commitment_id = ?1",
                params![guard.commitment_id.as_ref()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: guard.commitment_id.to_string(),
                reason: "commitment has no current claim".to_owned(),
            })?;
        let claim_lease_deadline = claim_lease_duration
            .deadline_from(MonotonicInstant::try_from_raw(stored_heartbeat)?)?;
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

    /// Record a monotonic heartbeat for the current claim atomically with its event.
    pub fn record_heartbeat(
        &mut self,
        commitment_id: &CommitmentId,
        worker_id: &WorkerId,
        epoch: ClaimEpoch,
        now: MonotonicInstant,
        claim_lease_duration: MonotonicDuration,
    ) -> Result<HeartbeatResult, StoreError> {
        let now_ticks = checked_i64(now.ticks(), "current monotonic instant")?;
        let epoch_ticks = checked_i64(epoch.value(), "claim epoch")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (
            goal_id,
            state_json,
            last_claim_epoch,
            stored_worker,
            stored_epoch,
            heartbeat,
            boot,
        ): (
            String,
            String,
            Option<i64>,
            String,
            i64,
            i64,
            i64,
        ) = transaction
            .query_row(
                "SELECT c.goal_id, c.state_json, c.last_claim_epoch,
                        cl.worker_id, cl.claim_epoch, cl.last_heartbeat,
                        cl.boot_generation
                 FROM commitments c JOIN claims cl ON cl.commitment_id = c.commitment_id
                 WHERE c.commitment_id = ?1",
                params![commitment_id.as_ref()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::ClaimMismatch {
                commitment_id: commitment_id.to_string(),
                reason: "commitment has no current claim".to_owned(),
            })?;
        let state = decode_commitment_state(&state_json)?;
        if !state.requires_active_claim() {
            return Err(StoreError::ClaimMismatch {
                commitment_id: commitment_id.to_string(),
                reason: format!("heartbeats are not valid in {state:?}"),
            });
        }
        let current_boot = current_boot_generation(&transaction)?;
        if stored_worker != worker_id.as_ref()
            || stored_epoch != epoch_ticks
            || boot != checked_i64(current_boot.value(), "boot generation")?
        {
            return Err(StoreError::ClaimMismatch {
                commitment_id: commitment_id.to_string(),
                reason: "heartbeat does not match the current worker, epoch, or boot".to_owned(),
            });
        }
        let last_claim_epoch = last_claim_epoch.map(ClaimEpoch::try_from_raw).transpose()?;
        if last_claim_epoch != Some(epoch) {
            return Err(StoreError::InvalidPersistedData(
                "active claim epoch does not equal the durable high-water mark".to_owned(),
            ));
        }
        let heartbeat_ticks = heartbeat;
        let heartbeat = MonotonicInstant::try_from_raw(heartbeat_ticks)?;
        let claim_lease_deadline = claim_lease_duration.deadline_from(heartbeat)?;
        if claim_lease_deadline <= now {
            return Err(StoreError::LeaseExpired {
                commitment_id: commitment_id.to_string(),
            });
        }
        if now_ticks < heartbeat_ticks {
            return Err(StoreError::InvalidMutation(
                "claim heartbeat cannot move backwards".to_owned(),
            ));
        }
        if now == heartbeat {
            return Ok(HeartbeatResult::AlreadyCurrent);
        }
        let changed = transaction.execute(
            "UPDATE claims SET last_heartbeat = ?1
             WHERE commitment_id = ?2 AND worker_id = ?3 AND claim_epoch = ?4
               AND last_heartbeat = ?5",
            params![
                now_ticks,
                commitment_id.as_ref(),
                worker_id.as_ref(),
                epoch_ticks,
                heartbeat_ticks
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidMutation(
                "claim changed while recording heartbeat".to_owned(),
            ));
        }
        let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
        append_event(
            &transaction,
            &goal_id,
            &WorkEvent::HeartbeatAccepted {
                commitment_id: commitment_id.clone(),
                worker_id: worker_id.clone(),
                epoch,
            },
            now_ticks,
        )?;
        transaction.commit()?;
        Ok(HeartbeatResult::Recorded)
    }

    /// Move expired claimed work into recovery pending and invalidate only its
    /// unadmitted reservations. Held locks and admitted actions are preserved.
    pub fn process_expired_claims(
        &mut self,
        now: MonotonicInstant,
        claim_lease_duration: MonotonicDuration,
    ) -> Result<usize, StoreError> {
        let now_ticks = checked_i64(now.ticks(), "current monotonic instant")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_boot = current_boot_generation(&transaction)?;
        let current_boot_ticks = checked_i64(current_boot.value(), "boot generation")?;
        let mut statement = transaction.prepare(
            "SELECT c.commitment_id, c.goal_id, c.state_json, c.outstanding_action,
                    c.state_version, c.last_claim_epoch, cl.worker_id, cl.claim_epoch,
                    cl.last_heartbeat, cl.boot_generation
             FROM commitments c JOIN claims cl ON cl.commitment_id = c.commitment_id
             ORDER BY c.commitment_id",
        )?;
        let mut rows = statement.query([])?;
        let mut claims = Vec::new();
        while let Some(row) = rows.next()? {
            claims.push((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
            ));
        }
        drop(rows);
        drop(statement);
        let mut expired_count = 0;
        for (
            commitment_id,
            goal_id,
            state_json,
            outstanding_action,
            version,
            last_claim_epoch,
            worker_id,
            epoch,
            heartbeat,
            boot,
        ) in claims
        {
            if boot != current_boot_ticks {
                return Err(StoreError::RecoveryBlocked {
                    commitment_id,
                    reason: "claim belongs to an older boot generation".to_owned(),
                });
            }
            let state = decode_commitment_state(&state_json)?;
            if !state.requires_active_claim() {
                return Err(StoreError::InvalidPersistedData(format!(
                    "claim exists in invalid commitment state {state:?}"
                )));
            }
            let epoch = ClaimEpoch::try_from_raw(epoch)?;
            let last_claim_epoch = last_claim_epoch.map(ClaimEpoch::try_from_raw).transpose()?;
            if last_claim_epoch != Some(epoch) {
                return Err(StoreError::InvalidPersistedData(
                    "active claim epoch does not equal the durable high-water mark".to_owned(),
                ));
            }
            let heartbeat = MonotonicInstant::try_from_raw(heartbeat)?;
            let deadline = claim_lease_duration.deadline_from(heartbeat)?;
            if deadline > now {
                continue;
            }
            if outstanding_action.is_some() && !matches!(state, CommitmentState::Working) {
                return Err(StoreError::InvalidPersistedData(
                    "claim-bearing commitment has an outstanding action outside WORKING".to_owned(),
                ));
            }
            let next_version = next_state_version(version)?;
            let recovery_state = encode_commitment_state(&CommitmentState::RecoveryPending)?;
            let changed = transaction.execute(
                "UPDATE commitments SET state_json = ?1, state_version = ?2
                 WHERE commitment_id = ?3 AND state_version = ?4",
                params![recovery_state, next_version, commitment_id, version],
            )?;
            if changed != 1 {
                return Err(StoreError::InvalidMutation(
                    "commitment changed while expiring claim".to_owned(),
                ));
            }
            transaction.execute(
                "DELETE FROM claims WHERE commitment_id = ?1 AND claim_epoch = ?2",
                params![commitment_id, epoch.value()],
            )?;
            let commitment_id_typed =
                parse_id(commitment_id.clone(), "commitment", CommitmentId::try_new)?;
            let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
            let worker_id = parse_id(worker_id, "worker", WorkerId::try_new)?;
            append_event(
                &transaction,
                &goal_id,
                &WorkEvent::LeaseExpired {
                    commitment_id: commitment_id_typed.clone(),
                    worker_id,
                    epoch,
                },
                now_ticks,
            )?;
            append_event(
                &transaction,
                &goal_id,
                &WorkEvent::CommitmentRecoveryPending {
                    commitment_id: commitment_id_typed.clone(),
                },
                now_ticks,
            )?;
            let mut guards = transaction.prepare(
                "SELECT guard_id, state_version FROM execution_guards
                 WHERE commitment_id = ?1 AND claim_epoch = ?2 AND state = 'issued'",
            )?;
            let mut issued = Vec::new();
            let mut guard_rows =
                guards.query(params![commitment_id_typed.as_ref(), epoch.value()])?;
            while let Some(row) = guard_rows.next()? {
                issued.push((row.get::<_, String>(0)?, row.get::<_, i64>(1)?));
            }
            drop(guard_rows);
            drop(guards);
            for (guard_id, guard_version) in issued {
                transaction.execute(
                    "DELETE FROM scope_locks WHERE guard_id = ?1 AND lock_state = 'reserved'",
                    params![guard_id],
                )?;
                transaction.execute(
                    "UPDATE execution_guards SET state = 'invalidated',
                            reservation_expires_at = NULL, state_version = ?1
                     WHERE guard_id = ?2 AND state = 'issued' AND state_version = ?3",
                    params![next_state_version(guard_version)?, guard_id, guard_version],
                )?;
                let guard_id = parse_id(guard_id, "guard", GuardId::try_new)?;
                append_event(
                    &transaction,
                    &goal_id,
                    &WorkEvent::GuardInvalidated { guard_id },
                    now_ticks,
                )?;
            }
            expired_count += 1;
        }
        transaction.commit()?;
        Ok(expired_count)
    }

    /// Release a recovery-pending commitment with no outstanding action.
    pub fn recover_without_action(
        &mut self,
        commitment_id: &CommitmentId,
        created_at: i64,
    ) -> Result<(), StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (goal_id, state_json, outstanding_action, replacement_terminal_id, version): (
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
        ) = transaction
            .query_row(
                "SELECT goal_id, state_json, outstanding_action,
                        replacement_terminal_id, state_version
                 FROM commitments WHERE commitment_id = ?1",
                params![commitment_id.as_ref()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| not_found("commitment", commitment_id.to_string()))?;
        let state = decode_commitment_state(&state_json)?;
        let held_locks: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM scope_locks
             WHERE commitment_id = ?1 AND lock_state = 'held'",
            params![commitment_id.as_ref()],
            |row| row.get(0),
        )?;
        let has_claim: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM claims WHERE commitment_id = ?1)",
            params![commitment_id.as_ref()],
            |row| row.get(0),
        )?;
        if !matches!(state, CommitmentState::RecoveryPending)
            || outstanding_action.is_some()
            || held_locks != 0
            || has_claim
        {
            return Err(StoreError::RecoveryBlocked {
                commitment_id: commitment_id.to_string(),
                reason: "recovery requires RECOVERY_PENDING with no claim, action, or held lock"
                    .to_owned(),
            });
        }
        if let Some(replacement_terminal_id) = replacement_terminal_id {
            let replacement_state: String = transaction
                .query_row(
                    "SELECT state_json FROM commitments WHERE commitment_id = ?1",
                    params![replacement_terminal_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| {
                    StoreError::InvalidPersistedData(
                        "replacement terminal commitment is missing".to_owned(),
                    )
                })?;
            if !matches!(
                decode_commitment_state(&replacement_state)?,
                CommitmentState::Completed
            ) {
                return Err(StoreError::RecoveryBlocked {
                    commitment_id: commitment_id.to_string(),
                    reason: "composite barrier is incomplete".to_owned(),
                });
            }
            let replacement_terminal_id = parse_id(
                replacement_terminal_id,
                "replacement terminal",
                CommitmentId::try_new,
            )?;
            complete_composite_barrier_cascade(&transaction, &replacement_terminal_id, created_at)?;
            transaction.commit()?;
            return Ok(());
        }
        let next_version = next_state_version(version)?;
        let ready = encode_commitment_state(&CommitmentState::Ready)?;
        if transaction.execute(
            "UPDATE commitments SET state_json = ?1, state_version = ?2
             WHERE commitment_id = ?3 AND state_version = ?4",
            params![ready, next_version, commitment_id.as_ref(), version],
        )? != 1
        {
            return Err(StoreError::InvalidMutation(
                "commitment changed while releasing recovery".to_owned(),
            ));
        }
        let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
        append_event(
            &transaction,
            &goal_id,
            &WorkEvent::RecoveryReleased {
                commitment_id: commitment_id.clone(),
            },
            created_at,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Record exactly one Tethers outcome for the uniquely correlated action.
    pub fn record_tethers_outcome(
        &mut self,
        outcome: TethersOutcome,
        created_at: i64,
    ) -> Result<OutcomeRecording, StoreError> {
        let action_ref = outcome.action_ref().clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let guard_id: String = transaction
            .query_row(
                "SELECT guard_id FROM execution_guards WHERE action_ref = ?1",
                params![action_ref.as_ref()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| not_found("Tethers action", action_ref.to_string()))?;
        let guard_id = parse_id(guard_id, "guard", GuardId::try_new)?;
        let guard = query_guard(&transaction, &guard_id)?;
        match &guard.state {
            GuardState::Resolved {
                action_ref: existing_action,
                outcome: existing_outcome,
            } if existing_action == &action_ref && existing_outcome == &outcome => {
                return Ok(OutcomeRecording::AlreadyRecorded);
            }
            GuardState::Uncertain {
                action_ref: existing_action,
            } if existing_action == &action_ref && outcome.is_uncertain() => {
                return Ok(OutcomeRecording::AlreadyRecorded);
            }
            GuardState::Resolved { .. } | GuardState::Uncertain { .. } => {
                return Err(StoreError::OutcomeConflict {
                    action_ref: action_ref.to_string(),
                });
            }
            GuardState::Admitted {
                action_ref: existing_action,
            } if existing_action == &action_ref => {}
            _ => {
                return Err(StoreError::GuardInvalid {
                    guard_id: guard_id.to_string(),
                    reason: "outcome requires an admitted guard".to_owned(),
                });
            }
        }
        let (goal_id, state_json, outstanding, commitment_version): (
            String,
            String,
            Option<String>,
            i64,
        ) = transaction.query_row(
            "SELECT goal_id, state_json, outstanding_action, state_version
                 FROM commitments WHERE commitment_id = ?1",
            params![guard.commitment_id.as_ref()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if outstanding.as_deref() != Some(action_ref.as_ref()) {
            return Err(StoreError::OutcomeConflict {
                action_ref: action_ref.to_string(),
            });
        }
        let state = decode_commitment_state(&state_json)?;
        if !matches!(
            state,
            CommitmentState::Working | CommitmentState::RecoveryPending
        ) {
            return Err(StoreError::GuardInvalid {
                guard_id: guard_id.to_string(),
                reason: format!("outcome is not valid in {state:?}"),
            });
        }
        let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
        append_event(
            &transaction,
            &goal_id,
            &WorkEvent::TethersOutcomeRecorded {
                outcome: outcome.clone(),
            },
            created_at,
        )?;
        let scope_keys = query_guard_scopes(&transaction, &guard_id)?;
        if outcome.is_uncertain() {
            let waiting = encode_commitment_state(&CommitmentState::Waiting(
                WaitingReason::UncertainAction(action_ref.clone()),
            ))?;
            if transaction.execute(
                "UPDATE commitments SET state_json = ?1, state_version = ?2
                 WHERE commitment_id = ?3 AND state_version = ?4
                   AND outstanding_action = ?5",
                params![
                    waiting,
                    next_state_version(commitment_version)?,
                    guard.commitment_id.as_ref(),
                    commitment_version,
                    action_ref.as_ref(),
                ],
            )? != 1
            {
                return Err(StoreError::InvalidMutation(
                    "commitment changed while recording uncertain outcome".to_owned(),
                ));
            }
            transaction.execute(
                "DELETE FROM claims WHERE commitment_id = ?1",
                params![guard.commitment_id.as_ref()],
            )?;
            if transaction.execute(
                "UPDATE execution_guards SET state = 'uncertain',
                        outcome_json = NULL, reservation_expires_at = NULL,
                        state_version = ?1
                 WHERE guard_id = ?2 AND state = 'admitted' AND state_version = ?3",
                params![
                    next_state_version(guard.state_version)?,
                    guard_id.as_ref(),
                    guard.state_version
                ],
            )? != 1
            {
                return Err(StoreError::InvalidMutation(
                    "guard changed while recording uncertain outcome".to_owned(),
                ));
            }
            append_event(
                &transaction,
                &goal_id,
                &WorkEvent::CommitmentWaiting {
                    commitment_id: guard.commitment_id,
                    reason: WaitingReason::UncertainAction(action_ref),
                },
                created_at,
            )?;
        } else {
            let stored_outcome = encode_stored_outcome(&outcome);
            let outcome_json = serde_json::to_string(&stored_outcome)?;
            if transaction.execute(
                "UPDATE execution_guards SET state = 'resolved', outcome_json = ?1,
                        reservation_expires_at = NULL, state_version = ?2
                 WHERE guard_id = ?3 AND state = 'admitted' AND state_version = ?4",
                params![
                    outcome_json,
                    next_state_version(guard.state_version)?,
                    guard_id.as_ref(),
                    guard.state_version
                ],
            )? != 1
            {
                return Err(StoreError::InvalidMutation(
                    "guard changed while recording outcome".to_owned(),
                ));
            }
            if transaction.execute(
                "DELETE FROM scope_locks WHERE guard_id = ?1 AND lock_state = 'held'",
                params![guard_id.as_ref()],
            )? != scope_keys.len()
            {
                return Err(StoreError::InvalidMutation(
                    "held scope locks changed while recording outcome".to_owned(),
                ));
            }
            let was_recovery_pending = matches!(state, CommitmentState::RecoveryPending);
            let next_commitment_state = if was_recovery_pending {
                CommitmentState::Ready
            } else {
                state.clone()
            };
            let next_action = None::<String>;
            if transaction.execute(
                "UPDATE commitments SET state_json = ?1, outstanding_action = ?2,
                        state_version = ?3
                 WHERE commitment_id = ?4 AND state_version = ?5
                   AND outstanding_action = ?6",
                params![
                    encode_commitment_state(&next_commitment_state)?,
                    next_action,
                    next_state_version(commitment_version)?,
                    guard.commitment_id.as_ref(),
                    commitment_version,
                    action_ref.as_ref(),
                ],
            )? != 1
            {
                return Err(StoreError::InvalidMutation(
                    "commitment changed while completing outcome".to_owned(),
                ));
            }
            if was_recovery_pending {
                append_event(
                    &transaction,
                    &goal_id,
                    &WorkEvent::RecoveryCompleted {
                        commitment_id: guard.commitment_id,
                        action_ref,
                    },
                    created_at,
                )?;
            }
        }
        transaction.commit()?;
        Ok(OutcomeRecording::Recorded)
    }

    fn startup_recovery(&mut self) -> Result<(), StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_persisted_database(&transaction)?;
        let previous_boot = current_boot_generation(&transaction)?;
        let next_boot =
            BootGeneration::from_raw(previous_boot.value().checked_add(1).ok_or_else(|| {
                StoreError::InvalidMutation("boot generation exhausted".to_owned())
            })?);
        let previous_boot_ticks = checked_i64(previous_boot.value(), "boot generation")?;
        let mut claim_statement = transaction.prepare(
            "SELECT c.commitment_id, c.goal_id, c.state_json, c.state_version,
                    c.last_claim_epoch, c.outstanding_action, cl.worker_id, cl.claim_epoch,
                    cl.boot_generation
             FROM commitments c JOIN claims cl ON cl.commitment_id = c.commitment_id
             ORDER BY c.commitment_id",
        )?;
        let mut claim_rows = claim_statement.query([])?;
        let mut active_claims = Vec::new();
        while let Some(row) = claim_rows.next()? {
            active_claims.push((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
            ));
        }
        drop(claim_rows);
        drop(claim_statement);
        for (
            commitment_id,
            goal_id,
            state_json,
            version,
            last_claim_epoch,
            outstanding_action,
            worker_id,
            epoch,
            claim_boot,
        ) in active_claims
        {
            if claim_boot != previous_boot_ticks {
                return Err(StoreError::InvalidPersistedData(format!(
                    "claim for commitment {commitment_id:?} has unexpected boot generation"
                )));
            }
            let state = decode_commitment_state(&state_json)?;
            if !state.requires_active_claim() {
                return Err(StoreError::InvalidPersistedData(format!(
                    "claim for commitment {commitment_id:?} exists in {state:?}"
                )));
            }
            if outstanding_action.is_some() && !matches!(state, CommitmentState::Working) {
                return Err(StoreError::InvalidPersistedData(
                    "claim-bearing commitment has an outstanding action outside WORKING".to_owned(),
                ));
            }
            let commitment_id = parse_id(commitment_id, "commitment", CommitmentId::try_new)?;
            let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
            let worker_id = parse_id(worker_id, "worker", WorkerId::try_new)?;
            let epoch = ClaimEpoch::try_from_raw(epoch)?;
            let last_claim_epoch = last_claim_epoch.map(ClaimEpoch::try_from_raw).transpose()?;
            if last_claim_epoch != Some(epoch) {
                return Err(StoreError::InvalidPersistedData(
                    "active claim epoch does not equal the claim epoch high-water mark".to_owned(),
                ));
            }
            let recovery_state = encode_commitment_state(&CommitmentState::RecoveryPending)?;
            if transaction.execute(
                "UPDATE commitments SET state_json = ?1, state_version = ?2
                 WHERE commitment_id = ?3 AND state_version = ?4",
                params![
                    recovery_state,
                    next_state_version(version)?,
                    commitment_id.as_ref(),
                    version
                ],
            )? != 1
            {
                return Err(StoreError::InvalidMutation(
                    "commitment changed during startup recovery".to_owned(),
                ));
            }
            transaction.execute(
                "DELETE FROM claims WHERE commitment_id = ?1",
                params![commitment_id.as_ref()],
            )?;
            append_event(
                &transaction,
                &goal_id,
                &WorkEvent::CommitmentRecoveryPending {
                    commitment_id: commitment_id.clone(),
                },
                0,
            )?;
            let _ = (worker_id, epoch);
        }

        let mut commitment_statement = transaction.prepare(
            "SELECT c.commitment_id, c.state_json, c.outstanding_action,
                    cl.commitment_id IS NOT NULL
             FROM commitments c
             LEFT JOIN claims cl ON cl.commitment_id = c.commitment_id
             ORDER BY c.commitment_id",
        )?;
        let mut commitment_rows = commitment_statement.query([])?;
        let mut restored_commitments = Vec::new();
        while let Some(row) = commitment_rows.next()? {
            restored_commitments.push((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, bool>(3)?,
            ));
        }
        drop(commitment_rows);
        drop(commitment_statement);
        for (commitment_id, state_json, outstanding_action, has_claim) in restored_commitments {
            if has_claim {
                return Err(StoreError::InvalidPersistedData(format!(
                    "active claim for commitment {commitment_id:?} survived startup recovery"
                )));
            }
            let state = decode_commitment_state(&state_json)?;
            if state.requires_active_claim() {
                return Err(StoreError::InvalidPersistedData(format!(
                    "commitment {commitment_id:?} in {state:?} has no active claim"
                )));
            }
            if outstanding_action.is_some()
                && !matches!(
                    &state,
                    CommitmentState::RecoveryPending
                        | CommitmentState::Waiting(WaitingReason::UncertainAction(_))
                )
            {
                return Err(StoreError::InvalidPersistedData(format!(
                    "commitment {commitment_id:?} has an outstanding action in {state:?}"
                )));
            }
        }

        let mut issued_statement = transaction.prepare(
            "SELECT g.guard_id, g.state_version, c.goal_id
             FROM execution_guards g JOIN commitments c ON c.commitment_id = g.commitment_id
             WHERE g.state = 'issued' ORDER BY g.guard_id",
        )?;
        let mut issued_rows = issued_statement.query([])?;
        let mut issued_guards = Vec::new();
        while let Some(row) = issued_rows.next()? {
            issued_guards.push((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ));
        }
        drop(issued_rows);
        drop(issued_statement);
        for (guard_id, version, goal_id) in issued_guards {
            if transaction.execute(
                "UPDATE execution_guards SET state = 'invalidated',
                        reservation_expires_at = NULL, state_version = ?1
                 WHERE guard_id = ?2 AND state = 'issued' AND state_version = ?3",
                params![next_state_version(version)?, guard_id, version],
            )? != 1
            {
                return Err(StoreError::InvalidMutation(
                    "guard changed during startup recovery".to_owned(),
                ));
            }
            let guard_id = parse_id(guard_id, "guard", GuardId::try_new)?;
            let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
            append_event(
                &transaction,
                &goal_id,
                &WorkEvent::GuardInvalidated { guard_id },
                0,
            )?;
        }

        let mut active_statement = transaction.prepare(
            "SELECT guard_id, commitment_id FROM execution_guards
             WHERE state IN ('admitted', 'uncertain') ORDER BY guard_id",
        )?;
        let mut active_rows = active_statement.query([])?;
        let mut active_guards = Vec::new();
        while let Some(row) = active_rows.next()? {
            active_guards.push((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
        }
        drop(active_rows);
        drop(active_statement);
        let mut expected_locks = HashMap::new();
        let mut action_by_commitment = HashMap::new();
        for (guard_id, commitment_id) in active_guards {
            let guard_id_typed = parse_id(guard_id, "guard", GuardId::try_new)?;
            let commitment_id_typed = parse_id(commitment_id, "commitment", CommitmentId::try_new)?;
            let guard = query_guard(&transaction, &guard_id_typed)?;
            let scope_keys = query_guard_scopes(&transaction, &guard_id_typed)?;
            let action_ref = match &guard.state {
                GuardState::Admitted { action_ref } | GuardState::Uncertain { action_ref } => {
                    action_ref.clone()
                }
                _ => {
                    return Err(StoreError::InvalidPersistedData(
                        "active guard query returned a non-active guard".to_owned(),
                    ));
                }
            };
            for scope_key in scope_keys.as_slice() {
                if let Some(existing_guard) = expected_locks.insert(
                    scope_key.as_ref().to_owned(),
                    (
                        guard_id_typed.clone(),
                        commitment_id_typed.clone(),
                        action_ref.clone(),
                    ),
                ) {
                    return Err(StoreError::RecoveryBlocked {
                        commitment_id: commitment_id_typed.to_string(),
                        reason: format!(
                            "scope {:?} is claimed by both guards {} and {}",
                            scope_key, existing_guard.0, guard_id_typed
                        ),
                    });
                }
            }
            let (state_json, outstanding, last_claim_epoch): (String, Option<String>, Option<i64>) =
                transaction.query_row(
                    "SELECT state_json, outstanding_action, last_claim_epoch
                 FROM commitments WHERE commitment_id = ?1",
                    params![commitment_id_typed.as_ref()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?;
            let high_water = last_claim_epoch.map(ClaimEpoch::try_from_raw).transpose()?;
            if high_water != Some(guard.claim_epoch) {
                return Err(StoreError::InvalidPersistedData(
                    "active guard epoch does not equal the claim epoch high-water mark".to_owned(),
                ));
            }
            let state = decode_commitment_state(&state_json)?;
            if outstanding.as_deref() != Some(action_ref.as_ref()) {
                return Err(StoreError::RecoveryBlocked {
                    commitment_id: commitment_id_typed.to_string(),
                    reason: "active guard and commitment outstanding action disagree".to_owned(),
                });
            }
            match guard.state {
                GuardState::Admitted { .. }
                    if !matches!(state, CommitmentState::RecoveryPending) =>
                {
                    return Err(StoreError::RecoveryBlocked {
                        commitment_id: commitment_id_typed.to_string(),
                        reason: format!("admitted action cannot be reconstructed from {state:?}"),
                    });
                }
                GuardState::Uncertain { .. }
                    if !matches!(
                        state,
                        CommitmentState::Waiting(WaitingReason::UncertainAction(ref waiting))
                            if waiting == &action_ref
                    ) =>
                {
                    return Err(StoreError::RecoveryBlocked {
                        commitment_id: commitment_id_typed.to_string(),
                        reason: "uncertain guard and commitment state disagree".to_owned(),
                    });
                }
                _ => {}
            }
            if action_by_commitment
                .insert(commitment_id_typed.clone(), action_ref)
                .is_some()
            {
                return Err(StoreError::RecoveryBlocked {
                    commitment_id: commitment_id_typed.to_string(),
                    reason: "multiple active guards exist for one commitment".to_owned(),
                });
            }
        }

        let mut outstanding_statement = transaction.prepare(
            "SELECT commitment_id, outstanding_action FROM commitments
             WHERE outstanding_action IS NOT NULL ORDER BY commitment_id",
        )?;
        let mut outstanding_rows = outstanding_statement.query([])?;
        let mut outstanding_commitments = Vec::new();
        while let Some(row) = outstanding_rows.next()? {
            outstanding_commitments.push((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
        }
        drop(outstanding_rows);
        drop(outstanding_statement);
        for (commitment_id, action_ref) in outstanding_commitments {
            if action_by_commitment.get(&parse_id(
                commitment_id.clone(),
                "commitment",
                CommitmentId::try_new,
            )?) != Some(&parse_id(
                action_ref.clone(),
                "Tethers action",
                TethersActionRef::try_new,
            )?) {
                return Err(StoreError::RecoveryBlocked {
                    commitment_id,
                    reason: "outstanding action has no unique active guard".to_owned(),
                });
            }
        }

        transaction.execute("DELETE FROM scope_locks", [])?;
        for (scope_key, (guard_id, commitment_id, action_ref)) in expected_locks {
            transaction.execute(
                "INSERT INTO scope_locks
                    (scope_key, guard_id, commitment_id, lock_state,
                     action_ref, reservation_expires_at)
                 VALUES (?1, ?2, ?3, 'held', ?4, NULL)",
                params![
                    scope_key,
                    guard_id.as_ref(),
                    commitment_id.as_ref(),
                    action_ref.as_ref()
                ],
            )?;
        }
        transaction.execute(
            "UPDATE metadata SET value = ?1 WHERE key = 'boot_generation'",
            params![next_boot.value().to_string()],
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
                        last_claim_epoch, outstanding_action, replacement_terminal_id,
                        state_version
                 FROM commitments WHERE commitment_id = ?1",
                params![commitment_id.as_ref()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, i64>(8)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| not_found("commitment", commitment_id.to_string()))?;
        let (
            stored_id,
            goal_id,
            parent_id,
            description,
            state_json,
            last_claim_epoch,
            outstanding_action,
            replacement_terminal_id,
            version,
        ) = row;
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
        let state = decode_commitment_state(&state_json)?;
        let last_claim_epoch = last_claim_epoch
            .map(|value| positive_u64(value, "last claim epoch"))
            .transpose()?;
        if claim
            .as_ref()
            .is_some_and(|active_claim| Some(active_claim.epoch()) != last_claim_epoch)
        {
            return Err(StoreError::InvalidPersistedData(
                "active claim epoch does not equal the durable high-water mark".to_owned(),
            ));
        }
        if state.requires_active_claim() != claim.is_some() {
            return Err(StoreError::InvalidPersistedData(
                "commitment state and active claim disagree".to_owned(),
            ));
        }
        if outstanding_action.is_some()
            && !matches!(
                &state,
                CommitmentState::Working
                    | CommitmentState::RecoveryPending
                    | CommitmentState::Waiting(WaitingReason::UncertainAction(_))
            )
        {
            return Err(StoreError::InvalidPersistedData(
                "outstanding action disagrees with commitment state".to_owned(),
            ));
        }
        let commitment_id = parse_id(stored_id, "commitment", CommitmentId::try_new)?;
        let goal_id = parse_id(goal_id, "goal", GoalId::try_new)?;
        let parent_id = parent_id
            .map(|value| parse_id(value, "parent commitment", CommitmentId::try_new))
            .transpose()?;
        let replacement_terminal_id = replacement_terminal_id
            .map(|value| parse_id(value, "replacement terminal", CommitmentId::try_new))
            .transpose()?;
        if let Some(replacement_terminal_id) = replacement_terminal_id.as_ref() {
            let child = self
                .connection
                .query_row(
                    "SELECT parent_id, goal_id FROM commitments WHERE commitment_id = ?1",
                    params![replacement_terminal_id.as_ref()],
                    |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?
                .ok_or_else(|| {
                    StoreError::InvalidPersistedData(
                        "replacement terminal commitment is missing".to_owned(),
                    )
                })?;
            if child.0.as_deref() != Some(commitment_id.as_ref()) || child.1 != goal_id.as_ref() {
                return Err(StoreError::InvalidPersistedData(
                    "replacement terminal is not a direct same-goal child".to_owned(),
                ));
            }
        }
        Ok(CommitmentRecord {
            commitment_id: commitment_id.clone(),
            goal_id,
            parent_id,
            description,
            state,
            prerequisites: self.load_prerequisites(&commitment_id)?,
            acceptance_refs: self.load_acceptance_refs(&commitment_id)?,
            claim,
            last_claim_epoch,
            outstanding_action: outstanding_action
                .map(|value| parse_id(value, "Tethers action", TethersActionRef::try_new))
                .transpose()?,
            replacement_terminal_id,
            state_version: version,
        })
    }

    /// Rehydrate a domain commitment through the validated restoration boundary.
    pub fn load_commitment(&self, commitment_id: &CommitmentId) -> Result<Commitment, StoreError> {
        let record = self.load_commitment_record(commitment_id)?;
        let claim = record
            .claim
            .map(
                |claim| -> Result<(WorkerId, ClaimEpoch, MonotonicInstant), StoreError> {
                    Ok((
                        claim.worker_id,
                        ClaimEpoch::try_from_raw(checked_i64(claim.epoch, "claim epoch")?)?,
                        MonotonicInstant::from_ticks(claim.last_heartbeat),
                    ))
                },
            )
            .transpose()?;
        let last_claim_epoch = record
            .last_claim_epoch
            .map(|epoch| checked_i64(epoch, "last claim epoch"))
            .transpose()?
            .map(ClaimEpoch::try_from_raw)
            .transpose()?;
        Ok(Commitment::restore(
            record.commitment_id,
            record.goal_id,
            record.parent_id,
            record.description,
            record.state,
            record.prerequisites,
            record.acceptance_refs,
            claim,
            record.outstanding_action,
            last_claim_epoch,
        )?)
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
                if description.trim().is_empty() {
                    return Err(StoreError::InvalidPersistedData(
                        "attention description must not be empty".to_owned(),
                    ));
                }
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

fn validate_persisted_database(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut foreign_keys = transaction.prepare("PRAGMA foreign_key_check")?;
    let mut rows = foreign_keys.query([])?;
    if let Some(row) = rows.next()? {
        let table: String = row.get(0)?;
        let row_id: i64 = row.get(1)?;
        let parent_table: String = row.get(2)?;
        return Err(StoreError::InvalidPersistedData(format!(
            "foreign-key violation in {table} row {row_id} referencing {parent_table}"
        )));
    }
    drop(rows);
    drop(foreign_keys);

    let integrity: String =
        transaction.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if !integrity.eq_ignore_ascii_case("ok") {
        return Err(StoreError::InvalidPersistedData(format!(
            "SQLite integrity_check failed: {integrity}"
        )));
    }
    validate_persisted_events(transaction)?;
    validate_persisted_topology(transaction)?;
    validate_guard_lock_consistency(transaction)
}

fn validate_persisted_events(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut statement = transaction
        .prepare("SELECT event_id, event_type, payload_json FROM work_events ORDER BY event_id")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let event_id: i64 = row.get(0)?;
        let event_type: String = row.get(1)?;
        let payload_json: String = row.get(2)?;
        let payload: StoredEventPayload = serde_json::from_str(&payload_json).map_err(|error| {
            StoreError::InvalidPersistedData(format!(
                "event {event_id} has invalid payload JSON: {error}"
            ))
        })?;
        if event_type != stored_event_type(&payload) {
            return Err(StoreError::InvalidPersistedData(format!(
                "event {event_id} type {event_type:?} disagrees with its payload"
            )));
        }
    }
    Ok(())
}

fn stored_event_type(event: &StoredEventPayload) -> &'static str {
    match event {
        StoredEventPayload::GoalCreated { .. } => "goal_created",
        StoredEventPayload::GoalRevised { .. } => "goal_revised",
        StoredEventPayload::CommitmentCreated { .. } => "commitment_created",
        StoredEventPayload::CommitmentActivated { .. } => "commitment_activated",
        StoredEventPayload::CommitmentClaimed { .. } => "commitment_claimed",
        StoredEventPayload::CommitmentStarted { .. } => "commitment_started",
        StoredEventPayload::CommitmentWaiting { .. } => "commitment_waiting",
        StoredEventPayload::CommitmentRecoveryPending { .. } => "commitment_recovery_pending",
        StoredEventPayload::CommitmentCompletionProposed { .. } => "commitment_completion_proposed",
        StoredEventPayload::CommitmentCompleted { .. } => "commitment_completed",
        StoredEventPayload::CommitmentCancelled { .. } => "commitment_cancelled",
        StoredEventPayload::CommitmentAbandoned { .. } => "commitment_abandoned",
        StoredEventPayload::HeartbeatAccepted { .. } => "heartbeat_accepted",
        StoredEventPayload::LeaseExpired { .. } => "lease_expired",
        StoredEventPayload::GuardIssued { .. } => "guard_issued",
        StoredEventPayload::GuardAdmitted { .. } => "guard_admitted",
        StoredEventPayload::GuardInvalidated { .. } => "guard_invalidated",
        StoredEventPayload::GuardReservationExpired { .. } => "guard_reservation_expired",
        StoredEventPayload::TethersOutcomeRecorded { .. } => "tethers_outcome_recorded",
        StoredEventPayload::RecoveryCompleted { .. } => "recovery_completed",
        StoredEventPayload::RecoveryReleased { .. } => "recovery_released",
        StoredEventPayload::StructuralProposalApplied { .. } => "structural_proposal_applied",
        StoredEventPayload::AttentionRaised { .. } => "attention_raised",
        StoredEventPayload::AttentionCleared { .. } => "attention_cleared",
    }
}

fn validate_persisted_topology(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let graph = load_dependency_graph(transaction)?;
    let mut statement = transaction.prepare(
        "SELECT commitment_id, goal_id, parent_id, replacement_terminal_id, state_json
         FROM commitments ORDER BY commitment_id",
    )?;
    let mut rows = statement.query([])?;
    let mut commitments = Vec::new();
    while let Some(row) = rows.next()? {
        commitments.push((
            parse_id(row.get(0)?, "commitment", CommitmentId::try_new)?,
            parse_id(row.get(1)?, "goal", GoalId::try_new)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            decode_commitment_state(&row.get::<_, String>(4)?)?,
        ));
    }
    drop(rows);
    drop(statement);

    let known: BTreeSet<_> = commitments.iter().map(|(id, ..)| id.clone()).collect();
    for (commitment_id, goal_id, parent_id, replacement, state) in commitments {
        if let Some(parent_id) = parent_id {
            let parent_goal: String = transaction
                .query_row(
                    "SELECT goal_id FROM commitments WHERE commitment_id = ?1",
                    params![parent_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| {
                    StoreError::InvalidPersistedData(
                        "commitment parent reference is missing".to_owned(),
                    )
                })?;
            if parent_goal != goal_id.as_ref() {
                return Err(StoreError::InvalidPersistedData(
                    "parent and child commitments have different goals".to_owned(),
                ));
            }
        }
        if let Some(replacement) = replacement {
            let replacement_id =
                parse_id(replacement, "replacement terminal", CommitmentId::try_new)?;
            if !known.contains(&replacement_id) {
                return Err(StoreError::InvalidPersistedData(
                    "replacement terminal commitment is missing".to_owned(),
                ));
            }
            let child = transaction.query_row(
                "SELECT parent_id, goal_id FROM commitments WHERE commitment_id = ?1",
                params![replacement_id.as_ref()],
                |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
            )?;
            if child.0.as_deref() != Some(commitment_id.as_ref()) || child.1 != goal_id.as_ref() {
                return Err(StoreError::InvalidPersistedData(
                    "replacement terminal is not a direct same-goal child".to_owned(),
                ));
            }
            if !state.is_terminal()
                && !matches!(
                    &state,
                    CommitmentState::Waiting(WaitingReason::Prerequisite(child))
                        if child == &replacement_id
                )
                && !matches!(state, CommitmentState::RecoveryPending)
            {
                return Err(StoreError::InvalidPersistedData(
                    "active composite barrier is not waiting on its replacement terminal"
                        .to_owned(),
                ));
            }
        }
    }
    let _ = graph;
    Ok(())
}

fn validate_guard_lock_consistency(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut statement =
        transaction.prepare("SELECT guard_id FROM execution_guards ORDER BY guard_id")?;
    let mut rows = statement.query([])?;
    let mut guard_ids = Vec::new();
    while let Some(row) = rows.next()? {
        guard_ids.push(parse_id(row.get(0)?, "guard", GuardId::try_new)?);
    }
    drop(rows);
    drop(statement);

    for guard_id in guard_ids {
        let guard = query_guard(transaction, &guard_id)?;
        let scope_keys = query_guard_scopes(transaction, &guard_id)?;
        let mut raw_scopes = Vec::new();
        let mut scope_statement = transaction.prepare(
            "SELECT scope_key FROM execution_guard_scopes
             WHERE guard_id = ?1 ORDER BY ordinal",
        )?;
        let mut scope_rows = scope_statement.query(params![guard_id.as_ref()])?;
        while let Some(row) = scope_rows.next()? {
            raw_scopes.push(parse_id(row.get(0)?, "scope", ScopeKey::try_new)?);
        }
        drop(scope_rows);
        drop(scope_statement);
        if raw_scopes.as_slice() != scope_keys.as_slice() {
            return Err(StoreError::InvalidPersistedData(format!(
                "guard {guard_id} has a non-canonical or duplicate scope set"
            )));
        }

        let mut lock_statement = transaction.prepare(
            "SELECT scope_key, lock_state, action_ref, reservation_expires_at,
                    commitment_id
             FROM scope_locks WHERE guard_id = ?1 ORDER BY scope_key",
        )?;
        let mut lock_rows = lock_statement.query(params![guard_id.as_ref()])?;
        let mut locks = Vec::new();
        while let Some(row) = lock_rows.next()? {
            locks.push((
                parse_id(row.get(0)?, "scope", ScopeKey::try_new)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, String>(4)?,
            ));
        }
        drop(lock_rows);
        drop(lock_statement);
        if locks.len() != scope_keys.len()
            || locks
                .iter()
                .any(|(scope, _, _, _, _)| !scope_keys.as_slice().contains(scope))
        {
            return Err(StoreError::InvalidPersistedData(format!(
                "guard {guard_id} scope locks do not exactly match its scope set"
            )));
        }
        for scope_key in scope_keys.as_slice() {
            let lock = locks
                .iter()
                .find(|(scope, ..)| scope == scope_key)
                .ok_or_else(|| {
                    StoreError::InvalidPersistedData(format!(
                        "guard {guard_id} is missing a lock for scope {scope_key:?}"
                    ))
                })?;
            let expected = match &guard.state {
                GuardState::Issued => {
                    if lock.1 != "reserved"
                        || lock.2.is_some()
                        || lock.3
                            != guard
                                .reservation_expires_at
                                .map(|deadline| deadline.ticks() as i64)
                    {
                        return Err(StoreError::InvalidPersistedData(format!(
                            "issued guard {guard_id} has an inconsistent reserved lock"
                        )));
                    }
                    guard.commitment_id.as_ref()
                }
                GuardState::Admitted { action_ref } | GuardState::Uncertain { action_ref } => {
                    if lock.1 != "held"
                        || lock.2.as_deref() != Some(action_ref.as_ref())
                        || lock.3.is_some()
                    {
                        return Err(StoreError::InvalidPersistedData(format!(
                            "active guard {guard_id} has an inconsistent held lock"
                        )));
                    }
                    guard.commitment_id.as_ref()
                }
                GuardState::Resolved { .. } | GuardState::Invalidated => {
                    return Err(StoreError::InvalidPersistedData(format!(
                        "terminal guard {guard_id} still has a scope lock"
                    )));
                }
            };
            if lock.4 != expected {
                return Err(StoreError::InvalidPersistedData(format!(
                    "guard {guard_id} lock has the wrong commitment"
                )));
            }
        }
    }
    Ok(())
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

fn insert_commitment_snapshot(
    transaction: &Transaction<'_>,
    commitment: &Commitment,
    boot_generation: BootGeneration,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO commitments
            (commitment_id, goal_id, parent_id, description, state_json,
             last_claim_epoch, outstanding_action, replacement_terminal_id,
             state_version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8)",
        params![
            commitment.commitment_id().as_ref(),
            commitment.goal_id().as_ref(),
            commitment.parent_id().map(AsRef::as_ref),
            commitment.description(),
            encode_commitment_state(commitment.state())?,
            commitment.last_claim_epoch().map(|epoch| epoch.value()),
            commitment.outstanding_action().map(AsRef::as_ref),
            INITIAL_STATE_VERSION,
        ],
    )?;
    sync_commitment_children(transaction, commitment, boot_generation)
}

#[derive(Debug)]
struct StructuralTargetContext {
    commitment_id: CommitmentId,
    goal_id: String,
    state_json: String,
    outstanding_action: Option<String>,
    state_version: i64,
    last_claim_epoch: Option<i64>,
    worker_id: Option<String>,
    claim_epoch: Option<i64>,
    last_heartbeat: Option<i64>,
    claim_boot_generation: Option<i64>,
    replacement_terminal_id: Option<String>,
}

#[derive(Debug)]
struct DependencyGraph {
    edges: BTreeMap<CommitmentId, BTreeSet<CommitmentId>>,
    prerequisites: BTreeMap<CommitmentId, Vec<Prerequisite>>,
}

fn query_structural_target(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
) -> Result<StructuralTargetContext, StoreError> {
    transaction
        .query_row(
            "SELECT c.goal_id, c.state_json, c.outstanding_action, c.state_version,
                    c.last_claim_epoch, c.replacement_terminal_id,
                    cl.worker_id, cl.claim_epoch, cl.last_heartbeat, cl.boot_generation
             FROM commitments c
             LEFT JOIN claims cl ON cl.commitment_id = c.commitment_id
             WHERE c.commitment_id = ?1",
            params![commitment_id.as_ref()],
            |row| {
                Ok(StructuralTargetContext {
                    commitment_id: commitment_id.clone(),
                    goal_id: row.get(0)?,
                    state_json: row.get(1)?,
                    outstanding_action: row.get(2)?,
                    state_version: row.get(3)?,
                    last_claim_epoch: row.get(4)?,
                    replacement_terminal_id: row.get(5)?,
                    worker_id: row.get(6)?,
                    claim_epoch: row.get(7)?,
                    last_heartbeat: row.get(8)?,
                    claim_boot_generation: row.get(9)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| not_found("commitment", commitment_id.to_string()))
}

fn query_active_guard(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
) -> Result<bool, StoreError> {
    Ok(transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM execution_guards
            WHERE commitment_id = ?1 AND state IN ('issued', 'admitted', 'uncertain')
        )",
        params![commitment_id.as_ref()],
        |row| row.get(0),
    )?)
}

fn expire_target_reservations(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
    now_ticks: i64,
) -> Result<(), StoreError> {
    let mut statement = transaction.prepare(
        "SELECT guard_id FROM execution_guards
         WHERE commitment_id = ?1 AND state = 'issued'",
    )?;
    let mut rows = statement.query(params![commitment_id.as_ref()])?;
    let mut guards = Vec::new();
    while let Some(row) = rows.next()? {
        guards.push(parse_id(row.get(0)?, "guard", GuardId::try_new)?);
    }
    drop(rows);
    drop(statement);
    for guard_id in guards {
        let scopes = query_guard_scopes(transaction, &guard_id)?;
        expire_relevant_reservations(transaction, scopes.as_slice(), now_ticks)?;
    }
    Ok(())
}

fn load_dependency_graph(transaction: &Transaction<'_>) -> Result<DependencyGraph, StoreError> {
    let mut graph = DependencyGraph {
        edges: BTreeMap::new(),
        prerequisites: BTreeMap::new(),
    };
    let mut statement = transaction.prepare(
        "SELECT commitment_id, goal_id, parent_id
         FROM commitments ORDER BY commitment_id",
    )?;
    let mut rows = statement.query([])?;
    let mut commitments = Vec::new();
    while let Some(row) = rows.next()? {
        commitments.push((
            parse_id(row.get(0)?, "commitment", CommitmentId::try_new)?,
            parse_id(row.get(1)?, "goal", GoalId::try_new)?,
            row.get::<_, Option<String>>(2)?,
        ));
    }
    drop(rows);
    drop(statement);
    let known: BTreeSet<_> = commitments.iter().map(|(id, _, _)| id.clone()).collect();
    for (id, _goal, parent) in commitments {
        let _parent = parent
            .map(|value| parse_id(value, "parent commitment", CommitmentId::try_new))
            .transpose()?;
        let prerequisites = query_prerequisites_transaction(transaction, &id)?;
        let mut edges = BTreeSet::new();
        for prerequisite in &prerequisites {
            if let Prerequisite::Commitment(dependency) = prerequisite {
                if !known.contains(dependency) {
                    return Err(StoreError::InvalidPersistedData(format!(
                        "commitment {id} has a missing prerequisite {dependency}"
                    )));
                }
                edges.insert(dependency.clone());
            }
        }
        graph.edges.insert(id.clone(), edges);
        graph.prerequisites.insert(id, prerequisites);
    }
    validate_acyclic(&graph.edges)?;
    Ok(graph)
}

fn query_prerequisites_transaction(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
) -> Result<Vec<Prerequisite>, StoreError> {
    let mut statement = transaction.prepare(
        "SELECT prerequisite_type, reference_id, prerequisite_commitment_id
         FROM commitment_prerequisites
         WHERE commitment_id = ?1 ORDER BY ordinal",
    )?;
    let mut rows = statement.query(params![commitment_id.as_ref()])?;
    let mut prerequisites = Vec::new();
    while let Some(row) = rows.next()? {
        prerequisites.push(decode_prerequisite(row.get(0)?, row.get(1)?, row.get(2)?)?);
    }
    Ok(prerequisites)
}

fn query_prerequisites_connection(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
) -> Result<Vec<Prerequisite>, StoreError> {
    query_prerequisites_transaction(transaction, commitment_id)
}

fn query_acceptance_refs_connection(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
) -> Result<Vec<AcceptanceRef>, StoreError> {
    let mut statement = transaction.prepare(
        "SELECT reference_id FROM commitment_acceptance_refs
         WHERE commitment_id = ?1 ORDER BY ordinal",
    )?;
    let mut rows = statement.query(params![commitment_id.as_ref()])?;
    let mut references = Vec::new();
    while let Some(row) = rows.next()? {
        references.push(parse_id(row.get(0)?, "acceptance", AcceptanceRef::try_new)?);
    }
    Ok(references)
}

fn current_last_claim_epoch(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
) -> Result<Option<ClaimEpoch>, StoreError> {
    transaction
        .query_row(
            "SELECT last_claim_epoch FROM commitments WHERE commitment_id = ?1",
            params![commitment_id.as_ref()],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(StoreError::from)
        .and_then(|value| {
            value
                .map(ClaimEpoch::try_from_raw)
                .transpose()
                .map_err(Into::into)
        })
}

fn validate_claim_persistence(
    transaction: &Transaction<'_>,
    commitment: &Commitment,
    current_state: &CommitmentState,
    current_high_water: Option<ClaimEpoch>,
) -> Result<(), StoreError> {
    let current_claim = transaction
        .query_row(
            "SELECT worker_id, claim_epoch, last_heartbeat
             FROM claims WHERE commitment_id = ?1",
            params![commitment.commitment_id().as_ref()],
            |row| {
                Ok((
                    parse_id(row.get(0)?, "worker", WorkerId::try_new),
                    ClaimEpoch::try_from_raw(row.get(1)?),
                    MonotonicInstant::try_from_raw(row.get(2)?),
                ))
            },
        )
        .optional()?
        .map(|(worker, epoch, heartbeat)| Ok::<_, StoreError>((worker?, epoch?, heartbeat?)))
        .transpose()?;

    if commitment.state().requires_active_claim() != commitment.claim().is_some() {
        return Err(StoreError::InvalidMutation(
            "commitment state and candidate claim disagree".to_owned(),
        ));
    }

    match (current_claim, commitment.claim()) {
        (Some((worker, epoch, heartbeat)), Some(candidate)) => {
            if worker != *candidate.worker_id()
                || epoch != candidate.epoch()
                || heartbeat != candidate.last_heartbeat()
            {
                return Err(StoreError::ClaimMismatch {
                    commitment_id: commitment.commitment_id().to_string(),
                    reason: "generic persistence cannot rewrite the current claim".to_owned(),
                });
            }
            if current_high_water != Some(epoch)
                || commitment.last_claim_epoch() != current_high_water
            {
                return Err(StoreError::InvalidPersistedData(
                    "active claim epoch does not equal the durable high-water mark".to_owned(),
                ));
            }
        }
        (Some(_), None) => {
            if commitment.state().requires_active_claim() {
                return Err(StoreError::InvalidMutation(
                    "claim-bearing state cannot persist without its current claim".to_owned(),
                ));
            }
            if commitment.last_claim_epoch() != current_high_water {
                return Err(StoreError::InvalidMutation(
                    "generic persistence cannot rewrite the claim epoch high-water mark".to_owned(),
                ));
            }
        }
        (None, Some(candidate)) => {
            if !matches!(current_state, CommitmentState::Ready)
                || !matches!(commitment.state(), CommitmentState::Claimed)
            {
                return Err(StoreError::ClaimMismatch {
                    commitment_id: commitment.commitment_id().to_string(),
                    reason: "a new claim may only claim a READY commitment".to_owned(),
                });
            }
            let expected_epoch = match current_high_water {
                Some(epoch) => {
                    let next = epoch
                        .value()
                        .checked_add(1)
                        .ok_or(DomainError::EpochExhausted)?;
                    ClaimEpoch::try_from_raw(i64::try_from(next).map_err(|_| {
                        StoreError::InvalidMutation(
                            "claim epoch does not fit in SQLite INTEGER".to_owned(),
                        )
                    })?)?
                }
                None => ClaimEpoch::initial(),
            };
            if candidate.epoch() != expected_epoch
                || commitment.last_claim_epoch() != Some(expected_epoch)
            {
                return Err(DomainError::StaleEpoch {
                    expected: expected_epoch,
                    actual: candidate.epoch(),
                }
                .into());
            }
        }
        (None, None) => {
            if commitment.last_claim_epoch() != current_high_water {
                return Err(StoreError::InvalidMutation(
                    "generic persistence cannot rewrite the claim epoch high-water mark".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_commitment_event_state(
    event: &WorkEvent,
    state: &CommitmentState,
) -> Result<(), StoreError> {
    let matches = match event {
        WorkEvent::CommitmentActivated { .. } => matches!(state, CommitmentState::Ready),
        WorkEvent::CommitmentClaimed { .. } => matches!(state, CommitmentState::Claimed),
        WorkEvent::CommitmentStarted { .. } => matches!(state, CommitmentState::Working),
        WorkEvent::CommitmentWaiting { reason, .. } => {
            matches!(state, CommitmentState::Waiting(candidate) if candidate == reason)
        }
        WorkEvent::CommitmentRecoveryPending { .. } => {
            matches!(state, CommitmentState::RecoveryPending)
        }
        WorkEvent::CommitmentCompletionProposed { .. } => {
            matches!(state, CommitmentState::CompletionProposed)
        }
        WorkEvent::CommitmentCompleted { .. } => matches!(state, CommitmentState::Completed),
        WorkEvent::CommitmentCancelled { .. } => matches!(state, CommitmentState::Cancelled),
        WorkEvent::CommitmentAbandoned { .. } => matches!(state, CommitmentState::Abandoned),
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(StoreError::InvalidMutation(
            "commitment event does not describe the candidate state".to_owned(),
        ))
    }
}

fn apply_decomposition(
    transaction: &Transaction<'_>,
    graph: DependencyGraph,
    context: &StructuralTargetContext,
    children: Vec<NewCommitment>,
    replacement_terminal_id: CommitmentId,
    boot_generation: BootGeneration,
    created_at: i64,
) -> Result<i64, StoreError> {
    let target = context.commitment_id.clone();
    if context.replacement_terminal_id.is_some() {
        return Err(DomainError::InvalidStructuralProposal {
            reason: "commitment has already been decomposed",
        }
        .into());
    }
    let target_goal = parse_id(context.goal_id.clone(), "goal", GoalId::try_new)?;
    let proposed_ids: BTreeSet<_> = children
        .iter()
        .map(|child| child.commitment_id().clone())
        .collect();
    if proposed_ids
        .iter()
        .any(|id| graph.edges.contains_key(id) || id == &target)
    {
        return Err(DomainError::InvalidStructuralProposal {
            reason: "decomposition child ID already exists",
        }
        .into());
    }
    let mut candidate = graph.edges.clone();
    for child in &children {
        let mut dependencies = BTreeSet::new();
        for prerequisite in child.prerequisites() {
            if let Prerequisite::Commitment(dependency) = prerequisite {
                if !graph.edges.contains_key(dependency) && !proposed_ids.contains(dependency) {
                    return Err(DomainError::InvalidStructuralProposal {
                        reason: "child prerequisite does not identify an existing or proposed commitment",
                    }
                    .into());
                }
                dependencies.insert(dependency.clone());
            }
        }
        candidate.insert(child.commitment_id().clone(), dependencies);
    }
    candidate
        .entry(target.clone())
        .or_default()
        .insert(replacement_terminal_id.clone());
    validate_acyclic(&candidate)?;

    for child in &children {
        let commitment = Commitment::new(
            child.commitment_id().clone(),
            target_goal.clone(),
            Some(target.clone()),
            child.description(),
            child.prerequisites().to_vec(),
            child.acceptance_refs().to_vec(),
        )?;
        append_event(
            transaction,
            &target_goal,
            &WorkEvent::CommitmentCreated {
                commitment_id: commitment.commitment_id().clone(),
                goal_id: target_goal.clone(),
            },
            created_at,
        )?;
        insert_commitment_snapshot(transaction, &commitment, boot_generation)?;
    }

    let current_prerequisites = graph.prerequisites.get(&target).ok_or_else(|| {
        StoreError::InvalidPersistedData("target prerequisites are missing".to_owned())
    })?;
    transaction.execute(
        "INSERT INTO commitment_prerequisites
            (commitment_id, ordinal, prerequisite_type, reference_id,
             prerequisite_commitment_id)
         VALUES (?1, ?2, 'commitment', ?3, ?3)",
        params![
            target.as_ref(),
            current_prerequisites.len() as i64,
            replacement_terminal_id.as_ref()
        ],
    )?;
    let waiting =
        CommitmentState::Waiting(WaitingReason::Prerequisite(replacement_terminal_id.clone()));
    let next_version = next_state_version(context.state_version)?;
    transaction.execute(
        "UPDATE commitments
         SET state_json = ?1, replacement_terminal_id = ?2, state_version = ?3
         WHERE commitment_id = ?4 AND state_version = ?5 AND outstanding_action IS NULL",
        params![
            encode_commitment_state(&waiting)?,
            replacement_terminal_id.as_ref(),
            next_version,
            target.as_ref(),
            context.state_version
        ],
    )?;
    append_event(
        transaction,
        &target_goal,
        &WorkEvent::CommitmentWaiting {
            commitment_id: target.clone(),
            reason: WaitingReason::Prerequisite(replacement_terminal_id.clone()),
        },
        created_at,
    )?;
    let event_id = append_event(
        transaction,
        &target_goal,
        &WorkEvent::StructuralProposalApplied {
            target,
            change: StructuralChange::Decomposed {
                children: children
                    .iter()
                    .map(|child| child.commitment_id().clone())
                    .collect(),
                replacement_terminal_id,
            },
        },
        created_at,
    )?;
    Ok(event_id)
}

fn apply_add_prerequisite(
    transaction: &Transaction<'_>,
    graph: DependencyGraph,
    context: &StructuralTargetContext,
    prerequisite: Prerequisite,
    created_at: i64,
) -> Result<i64, StoreError> {
    let target = context.commitment_id.clone();
    let current = graph.prerequisites.get(&target).ok_or_else(|| {
        StoreError::InvalidPersistedData("target prerequisites are missing".to_owned())
    })?;
    if current.contains(&prerequisite) {
        return Err(DomainError::InvalidStructuralProposal {
            reason: "prerequisite is already present",
        }
        .into());
    }
    let mut candidate = graph.edges.clone();
    if let Prerequisite::Commitment(dependency) = &prerequisite {
        if !candidate.contains_key(dependency) {
            return Err(DomainError::InvalidStructuralProposal {
                reason: "commitment prerequisite does not exist",
            }
            .into());
        }
        candidate
            .entry(target.clone())
            .or_default()
            .insert(dependency.clone());
    }
    validate_acyclic(&candidate)?;
    let goal_id = parse_id(context.goal_id.clone(), "goal", GoalId::try_new)?;
    let (kind, reference_id, prerequisite_commitment_id) = prerequisite_columns(&prerequisite);
    transaction.execute(
        "INSERT INTO commitment_prerequisites
            (commitment_id, ordinal, prerequisite_type, reference_id,
             prerequisite_commitment_id)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            target.as_ref(),
            current.len() as i64,
            kind,
            reference_id,
            prerequisite_commitment_id
        ],
    )?;
    let next_version = next_state_version(context.state_version)?;
    transaction.execute(
        "UPDATE commitments SET state_version = ?1
         WHERE commitment_id = ?2 AND state_version = ?3 AND outstanding_action IS NULL",
        params![next_version, target.as_ref(), context.state_version],
    )?;
    append_event(
        transaction,
        &goal_id,
        &WorkEvent::StructuralProposalApplied {
            target,
            change: StructuralChange::PrerequisiteAdded { prerequisite },
        },
        created_at,
    )
}

fn apply_abandon(
    transaction: &Transaction<'_>,
    context: &StructuralTargetContext,
    rationale: String,
    created_at: i64,
) -> Result<i64, StoreError> {
    let target = context.commitment_id.clone();
    let goal_id = parse_id(context.goal_id.clone(), "goal", GoalId::try_new)?;
    let next_version = next_state_version(context.state_version)?;
    transaction.execute(
        "UPDATE commitments SET state_json = ?1, state_version = ?2
         WHERE commitment_id = ?3 AND state_version = ?4 AND outstanding_action IS NULL",
        params![
            encode_commitment_state(&CommitmentState::Abandoned)?,
            next_version,
            target.as_ref(),
            context.state_version
        ],
    )?;
    transaction.execute(
        "DELETE FROM claims WHERE commitment_id = ?1",
        params![target.as_ref()],
    )?;
    append_event(
        transaction,
        &goal_id,
        &WorkEvent::CommitmentAbandoned {
            commitment_id: target.clone(),
        },
        created_at,
    )?;
    append_event(
        transaction,
        &goal_id,
        &WorkEvent::StructuralProposalApplied {
            target,
            change: StructuralChange::Abandoned { rationale },
        },
        created_at,
    )
}

fn prerequisite_columns(prerequisite: &Prerequisite) -> (&'static str, &str, Option<&str>) {
    match prerequisite {
        Prerequisite::Commitment(id) => ("commitment", id.as_ref(), Some(id.as_ref())),
        Prerequisite::TethersVerification(reference) => {
            ("tethers_verification", reference.as_ref(), None)
        }
        Prerequisite::HumanDecision(reference) => ("human_decision", reference.as_ref(), None),
    }
}

fn complete_composite_barrier_cascade(
    transaction: &Transaction<'_>,
    completed_id: &CommitmentId,
    created_at: i64,
) -> Result<(), StoreError> {
    let total: i64 =
        transaction.query_row("SELECT COUNT(*) FROM commitments", [], |row| row.get(0))?;
    let mut pending = vec![completed_id.clone()];
    let mut processed = 0_i64;
    while let Some(child_id) = pending.pop() {
        let child_state_json: String = transaction
            .query_row(
                "SELECT state_json FROM commitments WHERE commitment_id = ?1",
                params![child_id.as_ref()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::InvalidPersistedData(
                    "composite barrier cascade child is missing".to_owned(),
                )
            })?;
        if !matches!(
            decode_commitment_state(&child_state_json)?,
            CommitmentState::Completed
        ) {
            return Err(StoreError::InvalidPersistedData(
                "composite barrier cascade child is not completed".to_owned(),
            ));
        }
        let mut statement = transaction.prepare(
            "SELECT commitment_id, goal_id, state_json, state_version,
                    outstanding_action
             FROM commitments WHERE replacement_terminal_id = ?1
             ORDER BY commitment_id",
        )?;
        let mut rows = statement.query(params![child_id.as_ref()])?;
        let mut parents = Vec::new();
        while let Some(row) = rows.next()? {
            parents.push((
                parse_id(row.get(0)?, "parent commitment", CommitmentId::try_new)?,
                parse_id(row.get(1)?, "goal", GoalId::try_new)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
            ));
        }
        drop(rows);
        drop(statement);
        for (parent_id, goal_id, state_json, version, outstanding_action) in parents {
            processed = processed.checked_add(1).ok_or_else(|| {
                StoreError::InvalidMutation("composite barrier cascade count overflowed".to_owned())
            })?;
            if processed > total {
                return Err(StoreError::InvalidPersistedData(
                    "composite barrier cascade exceeded commitment count".to_owned(),
                ));
            }
            let child_parent: Option<String> = transaction.query_row(
                "SELECT parent_id FROM commitments WHERE commitment_id = ?1",
                params![child_id.as_ref()],
                |row| row.get(0),
            )?;
            let child_goal: String = transaction.query_row(
                "SELECT goal_id FROM commitments WHERE commitment_id = ?1",
                params![child_id.as_ref()],
                |row| row.get(0),
            )?;
            if child_parent.as_deref() != Some(parent_id.as_ref()) || child_goal != goal_id.as_ref()
            {
                return Err(StoreError::InvalidPersistedData(
                    "composite barrier child is not a direct same-goal child".to_owned(),
                ));
            }
            let state = decode_commitment_state(&state_json)?;
            if state.is_terminal() {
                continue;
            }
            if outstanding_action.is_some() {
                return Err(StoreError::RecoveryBlocked {
                    commitment_id: parent_id.to_string(),
                    reason: "composite barrier parent has an outstanding action".to_owned(),
                });
            }
            let is_waiting_on_child = matches!(
                &state,
                CommitmentState::Waiting(WaitingReason::Prerequisite(prerequisite))
                    if prerequisite == &child_id
            );
            let is_recovery_pending = matches!(state, CommitmentState::RecoveryPending);
            if !is_waiting_on_child && !is_recovery_pending {
                if matches!(state, CommitmentState::Completed) {
                    continue;
                }
                return Err(StoreError::InvalidPersistedData(
                    "composite barrier parent is not waiting on its replacement terminal"
                        .to_owned(),
                ));
            }
            let held_locks: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM scope_locks
                 WHERE commitment_id = ?1 AND lock_state = 'held'",
                params![parent_id.as_ref()],
                |row| row.get(0),
            )?;
            if held_locks != 0 {
                return Err(StoreError::RecoveryBlocked {
                    commitment_id: parent_id.to_string(),
                    reason: "composite barrier parent still owns a held lock".to_owned(),
                });
            }
            let next_version = next_state_version(version)?;
            transaction.execute(
                "UPDATE commitments SET state_json = ?1, state_version = ?2
                 WHERE commitment_id = ?3 AND state_version = ?4
                   AND outstanding_action IS NULL",
                params![
                    encode_commitment_state(&CommitmentState::Completed)?,
                    next_version,
                    parent_id.as_ref(),
                    version
                ],
            )?;
            transaction.execute(
                "DELETE FROM claims WHERE commitment_id = ?1",
                params![parent_id.as_ref()],
            )?;
            append_event(
                transaction,
                &goal_id,
                &WorkEvent::CommitmentCompleted {
                    commitment_id: parent_id.clone(),
                },
                created_at,
            )?;
            pending.push(parent_id);
        }
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
    RecoveryReleased {
        commitment_id: String,
    },
    StructuralProposalApplied {
        target: String,
        change: StoredStructuralChange,
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

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", content = "data")]
enum StoredStructuralChange {
    Decomposed {
        children: Vec<String>,
        replacement_terminal_id: String,
    },
    PrerequisiteAdded {
        prerequisite: StoredPrerequisite,
    },
    Abandoned {
        rationale: String,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", content = "value")]
enum StoredPrerequisite {
    Commitment(String),
    TethersVerification(String),
    HumanDecision(String),
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

fn encode_stored_outcome(outcome: &TethersOutcome) -> StoredTethersOutcome {
    match outcome {
        TethersOutcome::Succeeded { action_ref } => {
            StoredTethersOutcome::Succeeded(action_ref.as_ref().to_owned())
        }
        TethersOutcome::Failed { action_ref } => {
            StoredTethersOutcome::Failed(action_ref.as_ref().to_owned())
        }
        TethersOutcome::Uncertain { action_ref } => {
            StoredTethersOutcome::Uncertain(action_ref.as_ref().to_owned())
        }
    }
}

fn encode_structural_change(change: &StructuralChange) -> StoredStructuralChange {
    match change {
        StructuralChange::Decomposed {
            children,
            replacement_terminal_id,
        } => StoredStructuralChange::Decomposed {
            children: children
                .iter()
                .map(|child| child.as_ref().to_owned())
                .collect(),
            replacement_terminal_id: replacement_terminal_id.as_ref().to_owned(),
        },
        StructuralChange::PrerequisiteAdded { prerequisite } => {
            StoredStructuralChange::PrerequisiteAdded {
                prerequisite: match prerequisite {
                    Prerequisite::Commitment(id) => {
                        StoredPrerequisite::Commitment(id.as_ref().to_owned())
                    }
                    Prerequisite::TethersVerification(reference) => {
                        StoredPrerequisite::TethersVerification(reference.as_ref().to_owned())
                    }
                    Prerequisite::HumanDecision(reference) => {
                        StoredPrerequisite::HumanDecision(reference.as_ref().to_owned())
                    }
                },
            }
        }
        StructuralChange::Abandoned { rationale } => StoredStructuralChange::Abandoned {
            rationale: rationale.clone(),
        },
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
        WorkEvent::RecoveryReleased { commitment_id } => StoredEventPayload::RecoveryReleased {
            commitment_id: commitment_id.as_ref().to_owned(),
        },
        WorkEvent::StructuralProposalApplied { target, change } => {
            StoredEventPayload::StructuralProposalApplied {
                target: target.as_ref().to_owned(),
                change: encode_structural_change(change),
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
        WorkEvent::RecoveryReleased { .. } => "recovery_released",
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
        | WorkEvent::RecoveryCompleted { commitment_id, .. }
        | WorkEvent::RecoveryReleased { commitment_id } => Some(commitment_id),
        WorkEvent::AttentionRaised { commitment_id, .. } => commitment_id.as_ref(),
        WorkEvent::GoalCreated { .. }
        | WorkEvent::GoalRevised { .. }
        | WorkEvent::GuardInvalidated { .. }
        | WorkEvent::GuardReservationExpired { .. }
        | WorkEvent::TethersOutcomeRecorded { .. }
        | WorkEvent::AttentionCleared { .. } => None,
        WorkEvent::StructuralProposalApplied { target, .. } => Some(target),
    }
}

#[cfg(test)]
mod structural_barrier_tests {
    use super::*;

    #[test]
    fn barrier_cascade_rejects_a_non_completed_queue_item() {
        let mut store = SqliteStore::open_in_memory_for_tests().expect("store opens");
        let goal_id = GoalId::try_new("goal-test").expect("goal");
        let goal = GoalSpec::new(
            goal_id.clone(),
            resolve_core::GoalRevision::initial(),
            "barrier test",
            GoalState::Active,
            resolve_core::Timestamp::from_unix_seconds(0),
        )
        .expect("goal value");
        store
            .insert_goal(
                &goal,
                &WorkEvent::GoalCreated {
                    goal_id: goal_id.clone(),
                    revision: resolve_core::GoalRevision::initial(),
                },
            )
            .expect("goal persists");
        let child_id = CommitmentId::try_new("child-test").expect("child");
        let child = Commitment::new(
            child_id.clone(),
            goal_id.clone(),
            None,
            "child",
            Vec::new(),
            Vec::new(),
        )
        .expect("child value");
        store
            .insert_commitment(
                &child,
                &WorkEvent::CommitmentCreated {
                    commitment_id: child_id.clone(),
                    goal_id,
                },
                1,
            )
            .expect("child persists");
        let transaction = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("transaction opens");
        let error = complete_composite_barrier_cascade(&transaction, &child_id, 2)
            .expect_err("non-completed child cannot satisfy a barrier");
        assert!(matches!(error, StoreError::InvalidPersistedData(_)));
        transaction.rollback().expect("rollback succeeds");
    }
}
