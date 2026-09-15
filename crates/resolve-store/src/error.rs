use resolve_core::DomainError;
use std::fmt;

/// Errors returned by the synchronous SQLite persistence boundary.
#[derive(Debug)]
pub enum StoreError {
    Domain(DomainError),
    Sqlite(rusqlite::Error),
    Serialization(serde_json::Error),
    InvalidPersistedData(String),
    InvalidMutation(String),
    NotFound {
        entity: &'static str,
        id: String,
    },
    VersionConflict {
        entity: &'static str,
        id: String,
        expected: i64,
        actual: i64,
    },
    DuplicateImmutableRevision {
        goal_id: String,
        revision: u64,
    },
    UnsupportedSchemaVersion {
        version: i64,
    },
    ScopeLocked {
        scope_key: String,
        guard_id: String,
    },
    GuardInvalid {
        guard_id: String,
        reason: String,
    },
    ClaimMismatch {
        commitment_id: String,
        reason: String,
    },
    LeaseExpired {
        commitment_id: String,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain(error) => write!(formatter, "domain error: {error}"),
            Self::Sqlite(error) => write!(formatter, "SQLite error: {error}"),
            Self::Serialization(error) => write!(formatter, "serialization error: {error}"),
            Self::InvalidPersistedData(message) => {
                write!(formatter, "invalid persisted data: {message}")
            }
            Self::InvalidMutation(message) => write!(formatter, "invalid mutation: {message}"),
            Self::NotFound { entity, id } => write!(formatter, "{entity} {id} was not found"),
            Self::VersionConflict {
                entity,
                id,
                expected,
                actual,
            } => write!(
                formatter,
                "{entity} {id} has version {actual}, expected {expected}"
            ),
            Self::DuplicateImmutableRevision { goal_id, revision } => write!(
                formatter,
                "goal {goal_id} revision {revision} already exists"
            ),
            Self::UnsupportedSchemaVersion { version } => write!(
                formatter,
                "schema version {version} is newer than this store supports"
            ),
            Self::ScopeLocked {
                scope_key,
                guard_id,
            } => {
                write!(
                    formatter,
                    "scope {scope_key:?} is locked by guard {guard_id}"
                )
            }
            Self::GuardInvalid { guard_id, reason } => {
                write!(formatter, "guard {guard_id} is not admissible: {reason}")
            }
            Self::ClaimMismatch {
                commitment_id,
                reason,
            } => write!(
                formatter,
                "commitment {commitment_id} claim mismatch: {reason}"
            ),
            Self::LeaseExpired { commitment_id } => {
                write!(
                    formatter,
                    "commitment {commitment_id} claim lease has expired"
                )
            }
        }
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<DomainError> for StoreError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}
