//! SQLite persistence boundary for Resolve R0.

mod error;
mod schema;
pub mod service;
mod sqlite;

pub use error::StoreError;
pub use service::ResolveService;
pub use sqlite::{
    AttentionRecord, ClaimRecord, CommitmentRecord, EventRecord, GoalRecord, GuardAdmission,
    GuardIssueRequest, GuardRecord, HeartbeatResult, OutcomeRecording, SqliteStore,
};
