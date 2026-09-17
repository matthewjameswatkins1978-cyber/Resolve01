use crate::GuardAdmission;
use crate::error::StoreError;
use crate::sqlite::SqliteStore;
use resolve_core::{
    AttentionId, AttentionItem, BootGeneration, ClaimEpoch, Commitment, CommitmentId,
    ExecutionGuard, GoalId, GoalSpec, GuardId, MonotonicDuration, MonotonicInstant, ScopeSet,
    StructuralProposal, TethersActionRef, TethersOutcome, WorkEvent, WorkerId,
};

/// Typed service boundary wrapping store operations.
///
/// This layer provides a clean API for external processes to interact with
/// Resolve's operational state. It reuses existing store authority without
/// adding new semantic capabilities.
///
/// # Ownership
///
/// - Owner: Resolve (live coordination)
/// - Inputs: Typed domain values
/// - Outputs: Typed domain records
/// - Authority: Reuses existing store authority
/// - Failure semantics: All errors are typed and deterministic
///
/// # What this does NOT own
///
/// - Policy decisions (Tethers authority)
/// - Execution (Tethers authority)
/// - Historical memory (Lantern authority)
/// - Transport (future concern)
pub struct ResolveService {
    store: SqliteStore,
}

/// Typed response for guard admission operations.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum GuardAdmissionResponse {
    Admitted,
    AlreadyAdmitted,
    Rejected { reason: GuardRejectionReason },
}

/// Closed vocabulary for guard admission rejections.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum GuardRejectionReason {
    GuardNotFound,
    GuardRevoked,
    GuardAlreadyBound,
    GuardExpired,
    TaskNotActive,
    OwnershipChanged,
    FenceChanged,
    ActionMismatch,
    PreparationMismatch,
    ScopeKeysMismatch,
    StateUnavailable,
    InternalIntegrity,
}

/// Typed response for outcome delivery operations.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum OutcomeDeliveryResponse {
    Recorded,
    AlreadyRecorded,
    Conflict,
}

impl ResolveService {
    /// Create a new service wrapping the provided store.
    pub fn new(store: SqliteStore) -> Self {
        Self { store }
    }

    /// Consume the service and return the inner store.
    pub fn into_store(self) -> SqliteStore {
        self.store
    }

    /// Borrow the inner store immutably.
    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    /// Borrow the inner store mutably.
    pub fn store_mut(&mut self) -> &mut SqliteStore {
        &mut self.store
    }

    // --- Goal operations ---

    /// Create a new goal with its initial revision.
    pub fn create_goal(&mut self, goal: &GoalSpec, event: &WorkEvent) -> Result<i64, StoreError> {
        self.store.insert_goal(goal, event)
    }

    /// Revise an existing goal with append-only semantics.
    pub fn revise_goal(
        &mut self,
        goal: &GoalSpec,
        expected_state_version: i64,
        event: &WorkEvent,
    ) -> Result<i64, StoreError> {
        self.store
            .persist_goal_change(goal, expected_state_version, event)
    }

    /// Load a goal record by identifier.
    pub fn load_goal(&self, goal_id: &GoalId) -> Result<crate::GoalRecord, StoreError> {
        self.store.load_goal_record(goal_id)
    }

    // --- Commitment operations ---

    /// Create a new commitment in Proposed state.
    pub fn create_commitment(
        &mut self,
        commitment: &Commitment,
        event: &WorkEvent,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        self.store.insert_commitment(commitment, event, created_at)
    }

    /// Persist a commitment state change with optimistic concurrency.
    pub fn change_commitment(
        &mut self,
        commitment: &Commitment,
        expected_state_version: i64,
        event: &WorkEvent,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        self.store
            .persist_commitment_change(commitment, expected_state_version, event, created_at)
    }

    /// Load a commitment through the validated restoration boundary.
    pub fn load_commitment(&self, commitment_id: &CommitmentId) -> Result<Commitment, StoreError> {
        self.store.load_commitment(commitment_id)
    }

    /// Load a commitment record for inspection.
    pub fn load_commitment_record(
        &self,
        commitment_id: &CommitmentId,
    ) -> Result<crate::CommitmentRecord, StoreError> {
        self.store.load_commitment_record(commitment_id)
    }

    // --- Guard operations ---

    /// Issue a guard and reserve scope keys atomically.
    pub fn issue_guard(
        &mut self,
        request: crate::GuardIssueRequest,
    ) -> Result<ExecutionGuard, StoreError> {
        self.store.issue_guard(request)
    }

    /// Admit a guard using the exact scope set from the Tethers boundary.
    pub fn admit_guard(
        &mut self,
        guard_id: &GuardId,
        scope_keys: ScopeSet,
        action_ref: TethersActionRef,
        now: MonotonicInstant,
    ) -> Result<GuardAdmission, StoreError> {
        self.store
            .admit_guard(guard_id, scope_keys, action_ref, now)
    }

    /// Explicitly invalidate an unadmitted guard.
    pub fn invalidate_guard(
        &mut self,
        guard_id: &GuardId,
        worker_id: &WorkerId,
        claim_epoch: ClaimEpoch,
        now: MonotonicInstant,
        claim_lease_duration: MonotonicDuration,
    ) -> Result<(), StoreError> {
        self.store
            .invalidate_guard(guard_id, worker_id, claim_epoch, now, claim_lease_duration)
    }

    /// Load a guard record by identifier.
    pub fn load_guard(&self, guard_id: &GuardId) -> Result<crate::GuardRecord, StoreError> {
        self.store.load_guard_record(guard_id)
    }

    // --- Heartbeat operations ---

    /// Record a monotonic heartbeat for the current claim.
    pub fn record_heartbeat(
        &mut self,
        commitment_id: &CommitmentId,
        worker_id: &WorkerId,
        epoch: ClaimEpoch,
        now: MonotonicInstant,
        claim_lease_duration: MonotonicDuration,
    ) -> Result<crate::HeartbeatResult, StoreError> {
        self.store
            .record_heartbeat(commitment_id, worker_id, epoch, now, claim_lease_duration)
    }

    // --- Outcome operations ---

    /// Record a Tethers outcome for the uniquely correlated action.
    pub fn record_outcome(
        &mut self,
        outcome: TethersOutcome,
        created_at: i64,
    ) -> Result<crate::OutcomeRecording, StoreError> {
        self.store.record_tethers_outcome(outcome, created_at)
    }

    // --- Recovery operations ---

    /// Process expired claims and move them to recovery pending.
    pub fn process_expired_claims(
        &mut self,
        now: MonotonicInstant,
        claim_lease_duration: MonotonicDuration,
    ) -> Result<usize, StoreError> {
        self.store.process_expired_claims(now, claim_lease_duration)
    }

    /// Release a recovery-pending commitment with no outstanding action.
    pub fn recover_without_action(
        &mut self,
        commitment_id: &CommitmentId,
        created_at: i64,
    ) -> Result<(), StoreError> {
        self.store.recover_without_action(commitment_id, created_at)
    }

    // --- Structural proposal operations ---

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
        self.store.apply_structural_proposal(
            proposal,
            worker_id,
            boot_generation,
            now,
            claim_lease_duration,
            created_at,
        )
    }

    // --- Attention operations ---

    /// Insert a new attention item.
    pub fn raise_attention(
        &mut self,
        goal_id: &GoalId,
        attention: &AttentionItem,
        event: &WorkEvent,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        self.store
            .insert_attention(goal_id, attention, event, created_at)
    }

    /// Clear an open attention item.
    pub fn clear_attention(
        &mut self,
        goal_id: &GoalId,
        attention_id: &AttentionId,
        created_at: i64,
    ) -> Result<i64, StoreError> {
        self.store
            .clear_attention(goal_id, attention_id, created_at)
    }

    /// Load an attention record by identifier.
    pub fn load_attention(
        &self,
        attention_id: &AttentionId,
    ) -> Result<crate::AttentionRecord, StoreError> {
        self.store.load_attention_record(attention_id)
    }

    // --- Event operations ---

    /// List all events in order.
    pub fn list_events(&self) -> Result<Vec<crate::EventRecord>, StoreError> {
        self.store.list_events()
    }

    // --- Store metadata ---

    /// Get the current schema version.
    pub fn schema_version(&self) -> Result<i64, StoreError> {
        self.store.schema_version()
    }

    /// Get the current boot generation.
    pub fn boot_generation(&self) -> Result<BootGeneration, StoreError> {
        self.store.boot_generation()
    }

    /// Check if WAL journal mode is enabled.
    pub fn journal_mode(&self) -> Result<String, StoreError> {
        self.store.journal_mode()
    }

    /// Check if foreign keys are enabled.
    pub fn foreign_keys_enabled(&self) -> Result<bool, StoreError> {
        self.store.foreign_keys_enabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resolve_core::{CommitmentState, GoalRevision, GoalState, Timestamp};

    fn test_service() -> ResolveService {
        let store = SqliteStore::open_in_memory_for_tests().expect("store opens");
        ResolveService::new(store)
    }

    fn test_goal_id() -> GoalId {
        GoalId::try_new("test-goal").expect("goal id is valid")
    }

    fn test_commitment_id() -> CommitmentId {
        CommitmentId::try_new("test-commitment").expect("commitment id is valid")
    }

    #[test]
    fn service_wraps_store_and_exposes_goal_operations() {
        let mut service = test_service();
        let goal_id = test_goal_id();
        let goal = GoalSpec::new(
            goal_id.clone(),
            GoalRevision::initial(),
            "test goal",
            GoalState::Active,
            Timestamp::from_unix_seconds(0),
        )
        .expect("goal is valid");

        let event = WorkEvent::GoalCreated {
            goal_id: goal_id.clone(),
            revision: GoalRevision::initial(),
        };

        service
            .create_goal(&goal, &event)
            .expect("goal creation succeeds");

        let record = service.load_goal(&goal_id).expect("goal loads");
        assert_eq!(record.goal_id(), &goal_id);
        assert_eq!(record.revision(), 1);
    }

    #[test]
    fn service_exposes_commitment_operations() {
        let mut service = test_service();
        let goal_id = test_goal_id();
        let commitment_id = test_commitment_id();

        let goal = GoalSpec::new(
            goal_id.clone(),
            GoalRevision::initial(),
            "test goal",
            GoalState::Active,
            Timestamp::from_unix_seconds(0),
        )
        .expect("goal is valid");

        service
            .create_goal(
                &goal,
                &WorkEvent::GoalCreated {
                    goal_id: goal_id.clone(),
                    revision: GoalRevision::initial(),
                },
            )
            .expect("goal creation succeeds");

        let commitment = Commitment::new(
            commitment_id.clone(),
            goal_id.clone(),
            None,
            "test commitment",
            Vec::new(),
            Vec::new(),
        )
        .expect("commitment is valid");

        service
            .create_commitment(
                &commitment,
                &WorkEvent::CommitmentCreated {
                    commitment_id: commitment_id.clone(),
                    goal_id,
                },
                1,
            )
            .expect("commitment creation succeeds");

        let loaded = service
            .load_commitment(&commitment_id)
            .expect("commitment loads");
        assert_eq!(loaded.commitment_id(), &commitment_id);
        assert_eq!(loaded.state(), &CommitmentState::Proposed);
    }

    #[test]
    fn service_exposes_metadata_operations() {
        let service = test_service();

        let schema_version = service.schema_version().expect("schema version loads");
        assert_eq!(schema_version, 4);

        // In-memory stores don't auto-recover, so boot generation starts at 0
        let boot_gen = service.boot_generation().expect("boot generation loads");
        assert_eq!(boot_gen.value(), 0);

        let journal = service.journal_mode().expect("journal mode loads");
        assert_eq!(journal, "memory");

        let fk = service.foreign_keys_enabled().expect("foreign keys check");
        assert!(fk);
    }
}
