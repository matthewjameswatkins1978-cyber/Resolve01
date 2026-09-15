//! SQLite persistence boundary for Resolve R0.

mod error;
mod schema;
mod sqlite;

pub use error::StoreError;
pub use sqlite::{
    AttentionRecord, ClaimRecord, CommitmentRecord, EventRecord, GoalRecord, GuardAdmission,
    GuardIssueRequest, GuardRecord, SqliteStore,
};
