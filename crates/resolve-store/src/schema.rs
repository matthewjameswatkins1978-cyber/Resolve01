use rusqlite::Connection;

pub(crate) const SCHEMA_VERSION: i64 = 1;

pub(crate) fn initialize(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );

        INSERT INTO metadata(key, value)
        VALUES ('schema_version', '1')
        ON CONFLICT(key) DO NOTHING;

        CREATE TABLE IF NOT EXISTS goals (
            goal_id TEXT PRIMARY KEY NOT NULL,
            revision INTEGER NOT NULL CHECK (revision > 0),
            description TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            state_version INTEGER NOT NULL CHECK (state_version > 0)
        );

        CREATE TABLE IF NOT EXISTS goal_revisions (
            goal_id TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK (revision > 0),
            description TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (goal_id, revision),
            FOREIGN KEY (goal_id) REFERENCES goals(goal_id)
        );

        CREATE TABLE IF NOT EXISTS commitments (
            commitment_id TEXT PRIMARY KEY NOT NULL,
            goal_id TEXT NOT NULL,
            parent_id TEXT,
            description TEXT NOT NULL,
            state_json TEXT NOT NULL,
            outstanding_action TEXT,
            state_version INTEGER NOT NULL CHECK (state_version > 0),
            FOREIGN KEY (goal_id) REFERENCES goals(goal_id),
            FOREIGN KEY (parent_id) REFERENCES commitments(commitment_id)
        );

        CREATE TABLE IF NOT EXISTS commitment_prerequisites (
            commitment_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
            prerequisite_type TEXT NOT NULL,
            reference_id TEXT NOT NULL,
            prerequisite_commitment_id TEXT,
            PRIMARY KEY (commitment_id, ordinal),
            FOREIGN KEY (commitment_id) REFERENCES commitments(commitment_id),
            FOREIGN KEY (prerequisite_commitment_id)
                REFERENCES commitments(commitment_id)
        );

        CREATE TABLE IF NOT EXISTS commitment_acceptance_refs (
            commitment_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
            reference_id TEXT NOT NULL,
            PRIMARY KEY (commitment_id, ordinal),
            FOREIGN KEY (commitment_id) REFERENCES commitments(commitment_id)
        );

        CREATE TABLE IF NOT EXISTS claims (
            commitment_id TEXT PRIMARY KEY NOT NULL,
            worker_id TEXT NOT NULL,
            claim_epoch INTEGER NOT NULL CHECK (claim_epoch > 0),
            last_heartbeat INTEGER NOT NULL,
            FOREIGN KEY (commitment_id) REFERENCES commitments(commitment_id)
        );

        CREATE TABLE IF NOT EXISTS attention_items (
            attention_id TEXT PRIMARY KEY NOT NULL,
            commitment_id TEXT,
            description TEXT NOT NULL,
            state TEXT NOT NULL,
            FOREIGN KEY (commitment_id) REFERENCES commitments(commitment_id)
        );

        CREATE TABLE IF NOT EXISTS work_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            goal_id TEXT NOT NULL,
            commitment_id TEXT,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS work_events_goal_id_idx
            ON work_events(goal_id, event_id);
        CREATE INDEX IF NOT EXISTS work_events_commitment_id_idx
            ON work_events(commitment_id, event_id);
        ",
    )
}
