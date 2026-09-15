use crate::error::DomainError;
use crate::state_machine::{ResumeTarget, validate_transition};
use std::fmt;

macro_rules! string_id {
    ($name:ident, $kind:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn try_new(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(DomainError::InvalidIdentifier { kind: $kind });
                }
                Ok(Self(value))
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(GoalId, "goal");
string_id!(CommitmentId, "commitment");
string_id!(WorkerId, "worker");
string_id!(TethersActionRef, "Tethers action");
string_id!(TethersContractRef, "Tethers contract");
string_id!(HumanDecisionRef, "human decision");
string_id!(AcceptanceRef, "acceptance");
string_id!(AttentionId, "attention");
string_id!(GuardId, "guard");

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GoalRevision(u64);

impl GoalRevision {
    pub const fn initial() -> Self {
        Self(1)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::RevisionExhausted)
    }
}

impl fmt::Display for GoalRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClaimEpoch(u64);

impl ClaimEpoch {
    pub const fn initial() -> Self {
        Self(1)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn try_from_raw(value: i64) -> Result<Self, DomainError> {
        if value > 0 {
            Ok(Self(value as u64))
        } else {
            Err(DomainError::InvalidNumericValue {
                kind: "claim epoch",
            })
        }
    }

    pub(crate) fn next(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::EpochExhausted)
    }
}

impl fmt::Display for ClaimEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const fn from_unix_seconds(value: i64) -> Self {
        Self(value)
    }

    pub const fn as_unix_seconds(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MonotonicInstant(u64);

impl MonotonicInstant {
    pub const fn from_ticks(value: u64) -> Self {
        Self(value)
    }

    pub const fn ticks(self) -> u64 {
        self.0
    }

    pub fn try_from_raw(value: i64) -> Result<Self, DomainError> {
        if value >= 0 {
            Ok(Self(value as u64))
        } else {
            Err(DomainError::InvalidNumericValue {
                kind: "monotonic instant",
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BootGeneration(u64);

impl BootGeneration {
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn try_from_raw(value: i64) -> Result<Self, DomainError> {
        if value >= 0 {
            Ok(Self(value as u64))
        } else {
            Err(DomainError::InvalidNumericValue {
                kind: "boot generation",
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoalState {
    Active,
    Completed,
    Cancelled,
    Abandoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalSpec {
    goal_id: GoalId,
    revision: GoalRevision,
    description: String,
    state: GoalState,
    created_at: Timestamp,
}

impl GoalSpec {
    pub fn new(
        goal_id: GoalId,
        revision: GoalRevision,
        description: impl Into<String>,
        state: GoalState,
        created_at: Timestamp,
    ) -> Result<Self, DomainError> {
        let description = nonempty_description(description)?;
        Ok(Self {
            goal_id,
            revision,
            description,
            state,
            created_at,
        })
    }

    /// Create the next append-only goal revision without rewriting this value.
    pub fn revised(
        &self,
        revision: GoalRevision,
        description: impl Into<String>,
    ) -> Result<Self, DomainError> {
        let expected = self.revision.next()?;
        if revision != expected {
            return Err(DomainError::InvalidGoalRevision {
                expected,
                actual: revision,
            });
        }

        Ok(Self {
            goal_id: self.goal_id.clone(),
            revision,
            description: nonempty_description(description)?,
            state: self.state,
            created_at: self.created_at,
        })
    }

    pub fn goal_id(&self) -> &GoalId {
        &self.goal_id
    }

    pub fn revision(&self) -> GoalRevision {
        self.revision
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn state(&self) -> &GoalState {
        &self.state
    }

    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Prerequisite {
    Commitment(CommitmentId),
    TethersVerification(TethersContractRef),
    HumanDecision(HumanDecisionRef),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimLease {
    worker_id: WorkerId,
    epoch: ClaimEpoch,
    last_heartbeat: MonotonicInstant,
}

impl ClaimLease {
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn epoch(&self) -> ClaimEpoch {
        self.epoch
    }

    pub fn last_heartbeat(&self) -> MonotonicInstant {
        self.last_heartbeat
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitingReason {
    Prerequisite(CommitmentId),
    Authority(TethersActionRef),
    HumanDecision(HumanDecisionRef),
    Verification(TethersContractRef),
    UncertainAction(TethersActionRef),
    External(String),
}

impl WaitingReason {
    pub fn is_uncertain_action(&self) -> bool {
        matches!(self, Self::UncertainAction(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitmentState {
    Proposed,
    Ready,
    Claimed,
    Working,
    Waiting(WaitingReason),
    RecoveryPending,
    CompletionProposed,
    Completed,
    Cancelled,
    Abandoned,
}

impl CommitmentState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Abandoned)
    }

    /// States whose operational meaning includes a live worker claim.
    pub fn requires_active_claim(&self) -> bool {
        matches!(
            self,
            Self::Claimed | Self::Working | Self::CompletionProposed
        ) || matches!(self, Self::Waiting(reason) if !reason.is_uncertain_action())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Commitment {
    commitment_id: CommitmentId,
    goal_id: GoalId,
    parent_id: Option<CommitmentId>,
    description: String,
    state: CommitmentState,
    prerequisites: Vec<Prerequisite>,
    acceptance_refs: Vec<AcceptanceRef>,
    claim: Option<ClaimLease>,
    outstanding_action: Option<TethersActionRef>,
    last_claim_epoch: Option<ClaimEpoch>,
}

impl Commitment {
    pub fn new(
        commitment_id: CommitmentId,
        goal_id: GoalId,
        parent_id: Option<CommitmentId>,
        description: impl Into<String>,
        prerequisites: Vec<Prerequisite>,
        acceptance_refs: Vec<AcceptanceRef>,
    ) -> Result<Self, DomainError> {
        Ok(Self {
            commitment_id,
            goal_id,
            parent_id,
            description: nonempty_description(description)?,
            state: CommitmentState::Proposed,
            prerequisites,
            acceptance_refs,
            claim: None,
            outstanding_action: None,
            last_claim_epoch: None,
        })
    }

    /// Rehydrate a commitment only after validating its persisted invariants.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        commitment_id: CommitmentId,
        goal_id: GoalId,
        parent_id: Option<CommitmentId>,
        description: impl Into<String>,
        state: CommitmentState,
        prerequisites: Vec<Prerequisite>,
        acceptance_refs: Vec<AcceptanceRef>,
        claim: Option<(WorkerId, ClaimEpoch, MonotonicInstant)>,
        outstanding_action: Option<TethersActionRef>,
        last_claim_epoch: Option<ClaimEpoch>,
    ) -> Result<Self, DomainError> {
        let description = nonempty_description(description)?;
        let claim = claim.map(|(worker_id, epoch, last_heartbeat)| ClaimLease {
            worker_id,
            epoch,
            last_heartbeat,
        });
        if claim
            .as_ref()
            .is_some_and(|active_claim| Some(active_claim.epoch()) != last_claim_epoch)
        {
            return Err(DomainError::InvalidRestoredState {
                reason: "active claim epoch must equal the durable high-water mark",
            });
        }
        if state.requires_active_claim() != claim.is_some() {
            return Err(DomainError::InvalidRestoredState {
                reason: "commitment state and active claim disagree",
            });
        }
        if let Some(action_ref) = outstanding_action.as_ref() {
            let action_state_is_valid = match &state {
                CommitmentState::Working | CommitmentState::RecoveryPending => true,
                CommitmentState::Waiting(WaitingReason::UncertainAction(waiting_action)) => {
                    waiting_action == action_ref
                }
                _ => false,
            };
            if !action_state_is_valid {
                return Err(DomainError::InvalidRestoredState {
                    reason: "outstanding action disagrees with commitment state",
                });
            }
        }
        if matches!(
            state,
            CommitmentState::Waiting(WaitingReason::UncertainAction(_))
        ) && claim.is_some()
        {
            return Err(DomainError::InvalidRestoredState {
                reason: "uncertain waiting must not retain an active claim",
            });
        }

        Ok(Self {
            commitment_id,
            goal_id,
            parent_id,
            description,
            state,
            prerequisites,
            acceptance_refs,
            claim,
            outstanding_action,
            last_claim_epoch,
        })
    }

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

    pub fn prerequisites(&self) -> &[Prerequisite] {
        &self.prerequisites
    }

    pub fn acceptance_refs(&self) -> &[AcceptanceRef] {
        &self.acceptance_refs
    }

    pub fn state(&self) -> &CommitmentState {
        &self.state
    }

    pub fn claim(&self) -> Option<&ClaimLease> {
        self.claim.as_ref()
    }

    pub fn outstanding_action(&self) -> Option<&TethersActionRef> {
        self.outstanding_action.as_ref()
    }

    pub fn last_claim_epoch(&self) -> Option<ClaimEpoch> {
        self.last_claim_epoch
    }

    pub fn activate(&mut self) -> Result<(), DomainError> {
        self.transition_to(CommitmentState::Ready)
    }

    pub fn claim_for(
        &mut self,
        worker_id: WorkerId,
        last_heartbeat: MonotonicInstant,
    ) -> Result<ClaimLease, DomainError> {
        if let Some(claim) = &self.claim {
            return Err(DomainError::CommitmentAlreadyClaimed {
                worker_id: claim.worker_id().clone(),
                epoch: claim.epoch(),
            });
        }
        validate_transition(&self.state, &CommitmentState::Claimed)?;

        let epoch = match self.last_claim_epoch {
            Some(epoch) => epoch.next()?,
            None => ClaimEpoch::initial(),
        };
        let claim = ClaimLease {
            worker_id,
            epoch,
            last_heartbeat,
        };
        self.last_claim_epoch = Some(epoch);
        self.claim = Some(claim.clone());
        self.state = CommitmentState::Claimed;
        Ok(claim)
    }

    pub fn start(&mut self, worker_id: &WorkerId, epoch: ClaimEpoch) -> Result<(), DomainError> {
        self.require_current_claim(worker_id, epoch)?;
        self.transition_to(CommitmentState::Working)
    }

    pub fn wait(
        &mut self,
        worker_id: &WorkerId,
        epoch: ClaimEpoch,
        reason: WaitingReason,
    ) -> Result<(), DomainError> {
        self.require_current_claim(worker_id, epoch)?;
        let uncertain = reason.is_uncertain_action();
        self.transition_to(CommitmentState::Waiting(reason))?;
        if uncertain {
            self.claim = None;
        }
        Ok(())
    }

    pub fn resume(
        &mut self,
        worker_id: &WorkerId,
        epoch: ClaimEpoch,
        target: ResumeTarget,
    ) -> Result<(), DomainError> {
        if let CommitmentState::Waiting(WaitingReason::UncertainAction(action_ref)) = &self.state {
            return Err(DomainError::RecoveryRequired {
                action_ref: action_ref.clone(),
            });
        }
        self.require_current_claim(worker_id, epoch)?;
        let next_state = match target {
            ResumeTarget::Ready => CommitmentState::Ready,
            ResumeTarget::Working => CommitmentState::Working,
        };
        self.transition_to(next_state.clone())?;
        if target == ResumeTarget::Ready {
            self.claim = None;
        }
        Ok(())
    }

    pub fn propose_completion(
        &mut self,
        worker_id: &WorkerId,
        epoch: ClaimEpoch,
    ) -> Result<(), DomainError> {
        self.require_current_claim(worker_id, epoch)?;
        self.transition_to(CommitmentState::CompletionProposed)
    }

    pub fn complete(&mut self, worker_id: &WorkerId, epoch: ClaimEpoch) -> Result<(), DomainError> {
        self.require_current_claim(worker_id, epoch)?;
        self.transition_to(CommitmentState::Completed)?;
        self.claim = None;
        Ok(())
    }

    pub fn cancel(&mut self) -> Result<(), DomainError> {
        self.transition_to(CommitmentState::Cancelled)?;
        self.claim = None;
        Ok(())
    }

    pub fn abandon(&mut self) -> Result<(), DomainError> {
        self.transition_to(CommitmentState::Abandoned)?;
        self.claim = None;
        Ok(())
    }

    fn require_current_claim(
        &self,
        worker_id: &WorkerId,
        epoch: ClaimEpoch,
    ) -> Result<(), DomainError> {
        let claim = self.claim.as_ref().ok_or(DomainError::NotClaimed)?;
        if claim.worker_id() != worker_id {
            return Err(DomainError::ClaimOwnerMismatch {
                expected: claim.worker_id().clone(),
                actual: worker_id.clone(),
            });
        }
        if claim.epoch() != epoch {
            return Err(DomainError::StaleEpoch {
                expected: claim.epoch(),
                actual: epoch,
            });
        }
        Ok(())
    }

    fn transition_to(&mut self, next: CommitmentState) -> Result<(), DomainError> {
        validate_transition(&self.state, &next)?;
        self.state = next;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttentionState {
    Open,
    Cleared,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttentionItem {
    attention_id: AttentionId,
    commitment_id: Option<CommitmentId>,
    description: String,
    state: AttentionState,
}

impl AttentionItem {
    pub fn new(
        attention_id: AttentionId,
        commitment_id: Option<CommitmentId>,
        description: impl Into<String>,
    ) -> Result<Self, DomainError> {
        Ok(Self {
            attention_id,
            commitment_id,
            description: nonempty_description(description)?,
            state: AttentionState::Open,
        })
    }

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
pub enum TethersOutcome {
    Succeeded { action_ref: TethersActionRef },
    Failed { action_ref: TethersActionRef },
    Uncertain { action_ref: TethersActionRef },
}

impl TethersOutcome {
    pub fn action_ref(&self) -> &TethersActionRef {
        match self {
            Self::Succeeded { action_ref }
            | Self::Failed { action_ref }
            | Self::Uncertain { action_ref } => action_ref,
        }
    }

    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Uncertain { .. })
    }
}

/// Closed event vocabulary for accepted Resolve mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkEvent {
    GoalCreated {
        goal_id: GoalId,
        revision: GoalRevision,
    },
    GoalRevised {
        goal_id: GoalId,
        revision: GoalRevision,
    },
    CommitmentCreated {
        commitment_id: CommitmentId,
        goal_id: GoalId,
    },
    CommitmentActivated {
        commitment_id: CommitmentId,
    },
    CommitmentClaimed {
        commitment_id: CommitmentId,
        worker_id: WorkerId,
        epoch: ClaimEpoch,
    },
    CommitmentStarted {
        commitment_id: CommitmentId,
    },
    CommitmentWaiting {
        commitment_id: CommitmentId,
        reason: WaitingReason,
    },
    CommitmentRecoveryPending {
        commitment_id: CommitmentId,
    },
    CommitmentCompletionProposed {
        commitment_id: CommitmentId,
    },
    CommitmentCompleted {
        commitment_id: CommitmentId,
    },
    CommitmentCancelled {
        commitment_id: CommitmentId,
    },
    CommitmentAbandoned {
        commitment_id: CommitmentId,
    },
    HeartbeatAccepted {
        commitment_id: CommitmentId,
        worker_id: WorkerId,
        epoch: ClaimEpoch,
    },
    LeaseExpired {
        commitment_id: CommitmentId,
        worker_id: WorkerId,
        epoch: ClaimEpoch,
    },
    GuardIssued {
        guard_id: GuardId,
        commitment_id: CommitmentId,
        claim_epoch: ClaimEpoch,
    },
    GuardAdmitted {
        guard_id: GuardId,
        commitment_id: CommitmentId,
        action_ref: TethersActionRef,
    },
    GuardInvalidated {
        guard_id: GuardId,
    },
    GuardReservationExpired {
        guard_id: GuardId,
    },
    TethersOutcomeRecorded {
        outcome: TethersOutcome,
    },
    RecoveryCompleted {
        commitment_id: CommitmentId,
        action_ref: TethersActionRef,
    },
    RecoveryReleased {
        commitment_id: CommitmentId,
    },
    StructuralProposalApplied {
        target: CommitmentId,
    },
    AttentionRaised {
        attention_id: AttentionId,
        commitment_id: Option<CommitmentId>,
    },
    AttentionCleared {
        attention_id: AttentionId,
    },
}

fn nonempty_description(description: impl Into<String>) -> Result<String, DomainError> {
    let description = description.into();
    if description.trim().is_empty() {
        Err(DomainError::EmptyDescription)
    } else {
        Ok(description)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn goal_id() -> GoalId {
        GoalId::try_new("goal-1").expect("test identifier is valid")
    }

    fn commitment() -> Commitment {
        Commitment::new(
            CommitmentId::try_new("commitment-1").expect("test identifier is valid"),
            goal_id(),
            None,
            "implement the bounded change",
            Vec::new(),
            Vec::new(),
        )
        .expect("test commitment is valid")
    }

    fn worker(value: &str) -> WorkerId {
        WorkerId::try_new(value).expect("test identifier is valid")
    }

    #[test]
    fn terminal_commitments_are_immutable() {
        for terminal in [
            CommitmentState::Completed,
            CommitmentState::Cancelled,
            CommitmentState::Abandoned,
        ] {
            for destination in [
                CommitmentState::Proposed,
                CommitmentState::Ready,
                CommitmentState::Completed,
            ] {
                let error = validate_transition(&terminal, &destination)
                    .expect_err("terminal states must not transition");
                assert_eq!(
                    error,
                    DomainError::TerminalCommitmentImmutable {
                        state: terminal.clone(),
                    }
                );
            }
        }
    }

    #[test]
    fn illegal_transition_is_typed_and_non_panicking() {
        let mut commitment = commitment();
        let error = commitment
            .start(&worker("worker-1"), ClaimEpoch::initial())
            .expect_err("unclaimed work cannot start");

        assert_eq!(error, DomainError::NotClaimed);
        assert_eq!(commitment.state(), &CommitmentState::Proposed);
    }

    #[test]
    fn claim_epochs_are_typed_monotonic_and_stale_epochs_cannot_mutate() {
        let mut commitment = commitment();
        commitment.activate().expect("proposed can become ready");
        let first = commitment
            .claim_for(worker("worker-1"), MonotonicInstant::from_ticks(10))
            .expect("first claim succeeds");
        assert_eq!(first.worker_id(), &worker("worker-1"));
        assert_eq!(first.epoch(), ClaimEpoch::initial());
        assert_eq!(first.last_heartbeat(), MonotonicInstant::from_ticks(10));
        commitment
            .start(first.worker_id(), first.epoch())
            .expect("current claim can start");
        commitment
            .wait(
                first.worker_id(),
                first.epoch(),
                WaitingReason::External("worker needs input".to_owned()),
            )
            .expect("current claim can wait");
        commitment
            .resume(first.worker_id(), first.epoch(), ResumeTarget::Ready)
            .expect("current claim can release back to ready");

        let second = commitment
            .claim_for(worker("worker-2"), MonotonicInstant::from_ticks(20))
            .expect("second claim succeeds");
        assert_eq!(second.epoch().value(), first.epoch().value() + 1);

        let error = commitment
            .start(first.worker_id(), first.epoch())
            .expect_err("old worker epoch must be fenced");
        assert_eq!(
            error,
            DomainError::ClaimOwnerMismatch {
                expected: worker("worker-2"),
                actual: worker("worker-1"),
            }
        );

        let error = commitment
            .start(second.worker_id(), first.epoch())
            .expect_err("old epoch must not mutate current worker claim");
        assert_eq!(
            error,
            DomainError::StaleEpoch {
                expected: second.epoch(),
                actual: first.epoch(),
            }
        );
    }

    #[test]
    fn restored_active_claim_must_match_epoch_high_water_exactly() {
        let commitment = commitment();
        let claim = (
            worker("worker-1"),
            ClaimEpoch::initial(),
            MonotonicInstant::from_ticks(10),
        );
        let restore = |claim: Option<(WorkerId, ClaimEpoch, MonotonicInstant)>, high_water| {
            Commitment::restore(
                commitment.commitment_id().clone(),
                commitment.goal_id().clone(),
                commitment.parent_id().cloned(),
                commitment.description(),
                CommitmentState::Claimed,
                commitment.prerequisites().to_vec(),
                commitment.acceptance_refs().to_vec(),
                claim,
                None,
                high_water,
            )
        };

        assert!(restore(Some(claim.clone()), Some(ClaimEpoch::initial())).is_ok());
        assert!(matches!(
            restore(Some(claim.clone()), None),
            Err(DomainError::InvalidRestoredState { .. })
        ));
        assert!(matches!(
            restore(
                Some(claim.clone()),
                Some(ClaimEpoch::try_from_raw(2).expect("epoch is valid")),
            ),
            Err(DomainError::InvalidRestoredState { .. })
        ));
        assert!(matches!(
            restore(
                Some((
                    claim.0,
                    ClaimEpoch::try_from_raw(2).expect("epoch is valid"),
                    claim.2,
                )),
                Some(ClaimEpoch::initial())
            ),
            Err(DomainError::InvalidRestoredState { .. })
        ));
    }

    #[test]
    fn waiting_reasons_and_uncertain_outcomes_remain_distinct() {
        let prerequisite = WaitingReason::Prerequisite(
            CommitmentId::try_new("commitment-2").expect("test identifier is valid"),
        );
        let uncertain = WaitingReason::UncertainAction(
            TethersActionRef::try_new("action-1").expect("test identifier is valid"),
        );
        assert_ne!(prerequisite, uncertain);
        assert!(!prerequisite.is_uncertain_action());
        assert!(uncertain.is_uncertain_action());

        let outcome = TethersOutcome::Uncertain {
            action_ref: TethersActionRef::try_new("action-1").expect("test identifier is valid"),
        };
        assert!(outcome.is_uncertain());
        assert!(!matches!(outcome, TethersOutcome::Failed { .. }));
    }

    #[test]
    fn uncertain_waiting_requires_explicit_recovery() {
        let mut commitment = commitment();
        commitment.activate().expect("commitment activates");
        let claim = commitment
            .claim_for(worker("worker-1"), MonotonicInstant::from_ticks(1))
            .expect("claim succeeds");
        commitment
            .start(claim.worker_id(), claim.epoch())
            .expect("claim starts work");
        commitment
            .wait(
                claim.worker_id(),
                claim.epoch(),
                WaitingReason::UncertainAction(
                    TethersActionRef::try_new("action-1").expect("action is valid"),
                ),
            )
            .expect("uncertain waiting is representable");
        assert!(commitment.claim().is_none());

        let error = commitment
            .resume(claim.worker_id(), claim.epoch(), ResumeTarget::Working)
            .expect_err("uncertain work cannot use normal resume");
        assert_eq!(
            error,
            DomainError::RecoveryRequired {
                action_ref: TethersActionRef::try_new("action-1").expect("action is valid"),
            }
        );
    }

    #[test]
    fn goal_revisions_are_append_only_values() {
        let original = GoalSpec::new(
            goal_id(),
            GoalRevision::initial(),
            "original goal",
            GoalState::Active,
            Timestamp::from_unix_seconds(0),
        )
        .expect("test goal is valid");
        let next_revision = original.revision().next().expect("revision can advance");
        let revised = original
            .revised(next_revision, "revised goal")
            .expect("exact next revision is accepted");

        assert_eq!(original.revision(), GoalRevision::initial());
        assert_eq!(original.description(), "original goal");
        assert_eq!(revised.revision(), next_revision);
        assert_eq!(revised.description(), "revised goal");

        let error = original
            .revised(
                next_revision.next().expect("test revision can advance"),
                "skip",
            )
            .expect_err("revision gaps are not append-only");
        assert!(matches!(error, DomainError::InvalidGoalRevision { .. }));
    }

    #[test]
    fn malformed_identifiers_and_descriptions_return_errors() {
        assert_eq!(
            GoalId::try_new("   ").expect_err("blank identifier is malformed"),
            DomainError::InvalidIdentifier { kind: "goal" }
        );
        assert_eq!(
            GoalSpec::new(
                goal_id(),
                GoalRevision::initial(),
                "  ",
                GoalState::Active,
                Timestamp::from_unix_seconds(0),
            )
            .expect_err("blank goal description is malformed"),
            DomainError::EmptyDescription
        );
    }

    #[test]
    fn attention_items_use_a_validated_constructor_and_read_only_values() {
        let attention_id = AttentionId::try_new("attention-1").expect("test identifier is valid");
        let item = AttentionItem::new(attention_id.clone(), None, "human input required")
            .expect("test attention item is valid");

        assert_eq!(item.attention_id(), &attention_id);
        assert_eq!(item.commitment_id(), None);
        assert_eq!(item.description(), "human input required");
        assert_eq!(item.state(), AttentionState::Open);
        assert_eq!(
            AttentionItem::new(attention_id, None, "  ")
                .expect_err("blank attention description is malformed"),
            DomainError::EmptyDescription
        );
    }
}
