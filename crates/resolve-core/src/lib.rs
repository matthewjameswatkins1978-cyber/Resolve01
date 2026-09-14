//! Core domain model and state-machine boundary for Resolve R0.

pub mod domain;
pub mod error;
pub mod state_machine;

pub use domain::{
    AcceptanceRef, AttentionId, AttentionItem, AttentionState, BootGeneration, ClaimEpoch,
    ClaimLease, Commitment, CommitmentId, CommitmentState, GoalId, GoalRevision, GoalSpec,
    GoalState, GuardId, HumanDecisionRef, MonotonicInstant, Prerequisite, TethersActionRef,
    TethersContractRef, TethersOutcome, Timestamp, WaitingReason, WorkEvent, WorkerId,
};
pub use error::DomainError;
pub use state_machine::{ResumeTarget, validate_transition};
