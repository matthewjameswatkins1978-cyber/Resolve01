//! Core domain model and state-machine boundary for Resolve R0.

pub mod domain;
pub mod error;
pub mod guards;
pub mod planning;
mod state_machine;

pub use domain::{
    AcceptanceRef, AttentionId, AttentionItem, AttentionState, BootGeneration, ClaimEpoch,
    ClaimLease, Commitment, CommitmentId, CommitmentState, GoalId, GoalRevision, GoalSpec,
    GoalState, GuardId, HumanDecisionRef, MonotonicInstant, Prerequisite, StructuralChange,
    TethersActionRef, TethersContractRef, TethersOutcome, Timestamp, WaitingReason, WorkEvent,
    WorkerId,
};
pub use error::DomainError;
pub use guards::{ExecutionGuard, GuardState, MonotonicDuration, ScopeKey, ScopeSet};
pub use planning::{NewCommitment, StructuralProposal};
pub use state_machine::ResumeTarget;
