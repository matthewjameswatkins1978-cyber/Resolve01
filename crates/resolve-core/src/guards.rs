use crate::domain::{
    BootGeneration, ClaimEpoch, CommitmentId, GuardId, MonotonicInstant, TethersActionRef,
    TethersOutcome,
};
use crate::error::DomainError;

/// An opaque Tethers-derived scope identifier. Resolve compares keys exactly
/// and deliberately does not interpret paths, prefixes, or resource meaning.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScopeKey(String);

impl ScopeKey {
    pub fn try_new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::InvalidIdentifier { kind: "scope" });
        }
        Ok(Self(value))
    }
}

impl AsRef<str> for ScopeKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A duration in Resolve-owned monotonic ticks.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MonotonicDuration(u64);

impl MonotonicDuration {
    pub fn try_from_ticks(value: u64) -> Result<Self, DomainError> {
        if value == 0 {
            return Err(DomainError::InvalidDuration);
        }
        Ok(Self(value))
    }

    pub const fn ticks(self) -> u64 {
        self.0
    }

    fn deadline_from(self, now: MonotonicInstant) -> Result<MonotonicInstant, DomainError> {
        now.ticks()
            .checked_add(self.0)
            .map(MonotonicInstant::from_ticks)
            .ok_or(DomainError::TimeOverflow)
    }
}

/// Canonicalize exact opaque keys for deterministic locking and persistence.
pub fn canonicalize_scope_keys(mut scope_keys: Vec<ScopeKey>) -> Vec<ScopeKey> {
    scope_keys.sort_unstable();
    scope_keys.dedup();
    scope_keys
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardState {
    Issued,
    Admitted {
        action_ref: TethersActionRef,
    },
    Resolved {
        action_ref: TethersActionRef,
        outcome: TethersOutcome,
    },
    Uncertain {
        action_ref: TethersActionRef,
    },
    Invalidated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionGuard {
    guard_id: GuardId,
    commitment_id: CommitmentId,
    claim_epoch: ClaimEpoch,
    scope_keys: Vec<ScopeKey>,
    boot_generation: BootGeneration,
    state: GuardState,
    reservation_expires_at: MonotonicInstant,
}

impl ExecutionGuard {
    /// Construct the issued guard after the store has validated current claim
    /// ownership and exact-scope availability in its transaction.
    pub fn issue(
        guard_id: GuardId,
        commitment_id: CommitmentId,
        claim_epoch: ClaimEpoch,
        scope_keys: Vec<ScopeKey>,
        boot_generation: BootGeneration,
        reservation_expires_at: MonotonicInstant,
    ) -> Self {
        Self {
            guard_id,
            commitment_id,
            claim_epoch,
            scope_keys: canonicalize_scope_keys(scope_keys),
            boot_generation,
            state: GuardState::Issued,
            reservation_expires_at,
        }
    }

    pub fn reservation_deadline(
        now: MonotonicInstant,
        guard_ttl: MonotonicDuration,
        claim_lease_deadline: MonotonicInstant,
    ) -> Result<MonotonicInstant, DomainError> {
        if claim_lease_deadline < now {
            return Err(DomainError::InvalidLeaseDeadline);
        }
        Ok(guard_ttl.deadline_from(now)?.min(claim_lease_deadline))
    }

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
        &self.scope_keys
    }

    pub fn boot_generation(&self) -> BootGeneration {
        self.boot_generation
    }

    pub fn state(&self) -> &GuardState {
        &self.state
    }

    pub fn reservation_expires_at(&self) -> MonotonicInstant {
        self.reservation_expires_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard_id() -> GuardId {
        GuardId::try_new("guard-1").expect("test identifier is valid")
    }

    fn commitment_id() -> CommitmentId {
        CommitmentId::try_new("commitment-1").expect("test identifier is valid")
    }

    #[test]
    fn scope_canonicalization_is_exact_and_deterministic() {
        let scopes = canonicalize_scope_keys(vec![
            ScopeKey::try_new("z").expect("scope is valid"),
            ScopeKey::try_new("a").expect("scope is valid"),
            ScopeKey::try_new("z").expect("scope is valid"),
        ]);
        assert_eq!(
            scopes,
            vec![
                ScopeKey::try_new("a").expect("scope is valid"),
                ScopeKey::try_new("z").expect("scope is valid"),
            ]
        );
    }

    #[test]
    fn reservation_deadline_uses_the_earlier_validated_bound() {
        let deadline = ExecutionGuard::reservation_deadline(
            MonotonicInstant::from_ticks(10),
            MonotonicDuration::try_from_ticks(20).expect("duration is valid"),
            MonotonicInstant::from_ticks(25),
        )
        .expect("deadline is valid");
        assert_eq!(deadline, MonotonicInstant::from_ticks(25));
        assert_eq!(
            MonotonicDuration::try_from_ticks(0).expect_err("zero duration is invalid"),
            DomainError::InvalidDuration
        );
        assert_eq!(
            ExecutionGuard::reservation_deadline(
                MonotonicInstant::from_ticks(10),
                MonotonicDuration::try_from_ticks(1).expect("duration is valid"),
                MonotonicInstant::from_ticks(9),
            )
            .expect_err("past claim deadline is invalid"),
            DomainError::InvalidLeaseDeadline
        );
    }

    #[test]
    fn issued_guard_exposes_metadata_without_mutable_fields() {
        let guard = ExecutionGuard::issue(
            guard_id(),
            commitment_id(),
            ClaimEpoch::initial(),
            vec![ScopeKey::try_new("scope").expect("scope is valid")],
            BootGeneration::from_raw(3),
            MonotonicInstant::from_ticks(20),
        );
        assert_eq!(guard.state(), &GuardState::Issued);
        assert_eq!(guard.scope_keys().len(), 1);
    }
}
