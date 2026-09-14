use crate::domain::{ClaimEpoch, CommitmentState, GoalRevision, WorkerId};
use std::fmt;

/// Errors raised when a domain request would violate a Resolve invariant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomainError {
    InvalidIdentifier {
        kind: &'static str,
    },
    EmptyDescription,
    InvalidGoalRevision {
        expected: GoalRevision,
        actual: GoalRevision,
    },
    InvalidTransition {
        from: CommitmentState,
        to: CommitmentState,
    },
    TerminalCommitmentImmutable {
        state: CommitmentState,
    },
    CommitmentAlreadyClaimed {
        worker_id: WorkerId,
        epoch: ClaimEpoch,
    },
    NotClaimed,
    ClaimOwnerMismatch {
        expected: WorkerId,
        actual: WorkerId,
    },
    StaleEpoch {
        expected: ClaimEpoch,
        actual: ClaimEpoch,
    },
    EpochExhausted,
    RevisionExhausted,
}

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier { kind } => {
                write!(formatter, "{kind} identifier must not be empty")
            }
            Self::EmptyDescription => formatter.write_str("description must not be empty"),
            Self::InvalidGoalRevision { expected, actual } => write!(
                formatter,
                "goal revision must be {expected}, received {actual}"
            ),
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "invalid commitment transition from {from:?} to {to:?}"
                )
            }
            Self::TerminalCommitmentImmutable { state } => {
                write!(
                    formatter,
                    "terminal commitment state {state:?} is immutable"
                )
            }
            Self::CommitmentAlreadyClaimed { worker_id, epoch } => {
                write!(
                    formatter,
                    "commitment is already claimed by {worker_id} at epoch {epoch}"
                )
            }
            Self::NotClaimed => formatter.write_str("commitment has no active claim"),
            Self::ClaimOwnerMismatch { expected, actual } => write!(
                formatter,
                "claim belongs to {expected}, not requesting worker {actual}"
            ),
            Self::StaleEpoch { expected, actual } => write!(
                formatter,
                "claim epoch {actual} is stale; current epoch is {expected}"
            ),
            Self::EpochExhausted => formatter.write_str("claim epoch space is exhausted"),
            Self::RevisionExhausted => formatter.write_str("goal revision space is exhausted"),
        }
    }
}

impl std::error::Error for DomainError {}
