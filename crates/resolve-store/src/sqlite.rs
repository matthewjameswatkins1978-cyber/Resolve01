use crate::error::StoreError;
use crate::schema;
use resolve_core::{
    AcceptanceRef, AttentionId, AttentionItem, AttentionState, ClaimLease, Commitment,
    CommitmentId, CommitmentState, GoalId, GoalSpec, GoalState, HumanDecisionRef, Prerequisite,
    TethersActionRef, TethersContractRef, TethersOutcome, WaitingReason, WorkEvent, WorkerId,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
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
        sync_commitment_children(&transaction, commitment)?;
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
        sync_commitment_children(&transaction, commitment)?;
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

    fn from_connection(connection: Connection, file_backed: bool) -> Result<Self, StoreError> {
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
        schema::initialize(&connection)?;
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

fn sync_commitment_children(
    transaction: &Transaction<'_>,
    commitment: &Commitment,
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
        insert_claim(transaction, commitment.commitment_id(), claim)?;
    }
    Ok(())
}

fn insert_claim(
    transaction: &Transaction<'_>,
    commitment_id: &CommitmentId,
    claim: &ClaimLease,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO claims
            (commitment_id, worker_id, claim_epoch, last_heartbeat)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            commitment_id.as_ref(),
            claim.worker_id().as_ref(),
            claim.epoch().value(),
            claim.last_heartbeat().ticks(),
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
            action_ref,
        } => StoredEventPayload::GuardAdmitted {
            guard_id: guard_id.as_ref().to_owned(),
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
        | WorkEvent::RecoveryCompleted { commitment_id, .. } => Some(commitment_id),
        WorkEvent::AttentionRaised { commitment_id, .. } => commitment_id.as_ref(),
        WorkEvent::GoalCreated { .. }
        | WorkEvent::GoalRevised { .. }
        | WorkEvent::GuardAdmitted { .. }
        | WorkEvent::GuardInvalidated { .. }
        | WorkEvent::GuardReservationExpired { .. }
        | WorkEvent::TethersOutcomeRecorded { .. }
        | WorkEvent::StructuralProposalApplied { .. }
        | WorkEvent::AttentionCleared { .. } => None,
    }
}
