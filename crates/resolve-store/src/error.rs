use std::fmt;

/// Errors returned by the synchronous SQLite persistence boundary.
#[derive(Debug)]
pub enum StoreError {
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
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
        }
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}
