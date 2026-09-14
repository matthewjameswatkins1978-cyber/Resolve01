use crate::domain::CommitmentState;
use crate::error::DomainError;

/// The destination a worker may explicitly select when resuming work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResumeTarget {
    Ready,
    Working,
}

/// Validate the frozen R0 commitment-state topology without mutating state.
pub(crate) fn validate_transition(
    from: &CommitmentState,
    to: &CommitmentState,
) -> Result<(), DomainError> {
    let legal = match (from, to) {
        (CommitmentState::Proposed, CommitmentState::Ready)
        | (CommitmentState::Proposed, CommitmentState::Cancelled)
        | (CommitmentState::Proposed, CommitmentState::Abandoned)
        | (CommitmentState::Ready, CommitmentState::Claimed)
        | (CommitmentState::Ready, CommitmentState::Cancelled)
        | (CommitmentState::Ready, CommitmentState::Abandoned)
        | (CommitmentState::Claimed, CommitmentState::Working)
        | (CommitmentState::Claimed, CommitmentState::Cancelled)
        | (CommitmentState::Claimed, CommitmentState::Abandoned)
        | (CommitmentState::Working, CommitmentState::Waiting(_))
        | (CommitmentState::Working, CommitmentState::RecoveryPending)
        | (CommitmentState::Working, CommitmentState::CompletionProposed)
        | (CommitmentState::Working, CommitmentState::Cancelled)
        | (CommitmentState::Working, CommitmentState::Abandoned)
        | (CommitmentState::Waiting(_), CommitmentState::Ready)
        | (CommitmentState::Waiting(_), CommitmentState::Working)
        | (CommitmentState::Waiting(_), CommitmentState::Cancelled)
        | (CommitmentState::Waiting(_), CommitmentState::Abandoned)
        | (CommitmentState::RecoveryPending, CommitmentState::Ready)
        | (CommitmentState::RecoveryPending, CommitmentState::Waiting(_))
        | (CommitmentState::RecoveryPending, CommitmentState::Cancelled)
        | (CommitmentState::RecoveryPending, CommitmentState::Abandoned)
        | (CommitmentState::CompletionProposed, CommitmentState::Completed)
        | (CommitmentState::CompletionProposed, CommitmentState::Cancelled)
        | (CommitmentState::CompletionProposed, CommitmentState::Abandoned) => true,
        (CommitmentState::Completed, _)
        | (CommitmentState::Cancelled, _)
        | (CommitmentState::Abandoned, _) => false,
        _ => false,
    };

    if legal {
        Ok(())
    } else if from.is_terminal() {
        Err(DomainError::TerminalCommitmentImmutable {
            state: from.clone(),
        })
    } else {
        Err(DomainError::InvalidTransition {
            from: from.clone(),
            to: to.clone(),
        })
    }
}
