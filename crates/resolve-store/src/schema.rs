use crate::error::StoreError;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde::Deserialize;

pub(crate) const SCHEMA_VERSION: i64 = 3;

pub(crate) fn initialize(connection: &mut Connection) -> Result<(), StoreError> {
    let has_metadata: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'metadata')",
        [],
        |row| row.get(0),
    )?;
    if !has_metadata {
        create_schema_v1(connection)?;
    }

    let version: i64 = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )?
        .parse()
        .map_err(|error| {
            StoreError::InvalidPersistedData(format!(
                "schema_version is not an integer during open: {error}"
            ))
        })?;

    match version {
        1 => {
            migrate_v1_to_v2(connection)?;
            migrate_v2_to_v3(connection)?;
        }
        2 => migrate_v2_to_v3(connection)?,
        SCHEMA_VERSION => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            ensure_claim_boot_generation(&transaction)?;
            create_fencing_tables(&transaction)?;
            ensure_unique_action_refs(&transaction)?;
            transaction.commit()?;
        }
        version if version > SCHEMA_VERSION => {
            return Err(StoreError::UnsupportedSchemaVersion { version });
        }
        version => {
            return Err(StoreError::InvalidPersistedData(format!(
                "unsupported schema version {version}"
            )));
        }
    }
    Ok(())
}

fn create_schema_v1(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );

        INSERT INTO metadata(key, value)
        VALUES ('schema_version', '1')
        ON CONFLICT(key) DO NOTHING;
        INSERT INTO metadata(key, value)
        VALUES ('boot_generation', '0')
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
            boot_generation INTEGER NOT NULL DEFAULT 0 CHECK (boot_generation >= 0),
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

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), StoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_claim_boot_generation(&transaction)?;
    create_fencing_tables(&transaction)?;
    transaction.execute(
        "INSERT INTO metadata(key, value) VALUES ('boot_generation', '0')
         ON CONFLICT(key) DO NOTHING",
        [],
    )?;
    transaction.execute(
        "UPDATE metadata SET value = '2' WHERE key = 'schema_version' AND value = '1'",
        [],
    )?;
    if transaction.changes() != 1 {
        return Err(StoreError::InvalidPersistedData(
            "schema version changed while migrating from v1".to_owned(),
        ));
    }
    transaction.commit()?;
    Ok(())
}

fn migrate_v2_to_v3(connection: &mut Connection) -> Result<(), StoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        "ALTER TABLE commitments ADD COLUMN last_claim_epoch INTEGER
         CHECK (last_claim_epoch > 0)",
        [],
    )?;
    backfill_claim_epoch_high_water(&transaction)?;
    ensure_unique_action_refs(&transaction)?;
    transaction.execute(
        "UPDATE metadata SET value = '3' WHERE key = 'schema_version' AND value = '2'",
        [],
    )?;
    if transaction.changes() != 1 {
        return Err(StoreError::InvalidPersistedData(
            "schema version changed while migrating from v2".to_owned(),
        ));
    }
    transaction.commit()?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "data")]
enum HistoricalClaimedEvent {
    CommitmentClaimed {
        commitment_id: String,
        worker_id: String,
        epoch: u64,
    },
}

fn backfill_claim_epoch_high_water(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), StoreError> {
    let mut statement = transaction.prepare(
        "SELECT event_id, commitment_id, payload_json
         FROM work_events WHERE event_type = 'commitment_claimed' ORDER BY event_id",
    )?;
    let mut rows = statement.query([])?;
    let mut historical = Vec::new();
    while let Some(row) = rows.next()? {
        historical.push((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
        ));
    }
    drop(rows);
    drop(statement);

    for (event_id, event_commitment_id, payload_json) in historical {
        let claimed =
            serde_json::from_str::<HistoricalClaimedEvent>(&payload_json).map_err(|error| {
                StoreError::InvalidPersistedData(format!(
                    "claim event {event_id} has invalid payload: {error}"
                ))
            })?;
        let HistoricalClaimedEvent::CommitmentClaimed {
            commitment_id,
            worker_id,
            epoch,
        } = claimed;
        if event_commitment_id.as_deref() != Some(commitment_id.as_str())
            || commitment_id.trim().is_empty()
            || worker_id.trim().is_empty()
            || epoch == 0
        {
            return Err(StoreError::InvalidPersistedData(format!(
                "claim event {event_id} contains inconsistent claim data"
            )));
        }
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM commitments WHERE commitment_id = ?1
            )",
            [&commitment_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidPersistedData(format!(
                "claim event {event_id} references missing commitment {commitment_id:?}"
            )));
        }
        let epoch = i64::try_from(epoch).map_err(|_| {
            StoreError::InvalidPersistedData(format!(
                "claim event {event_id} epoch does not fit in SQLite INTEGER"
            ))
        })?;
        transaction.execute(
            "UPDATE commitments
             SET last_claim_epoch = CASE
                 WHEN last_claim_epoch IS NULL OR last_claim_epoch < ?1 THEN ?1
                 ELSE last_claim_epoch END
             WHERE commitment_id = ?2",
            rusqlite::params![epoch, commitment_id],
        )?;
    }

    let mut statement = transaction
        .prepare("SELECT commitment_id, claim_epoch FROM claims ORDER BY commitment_id")?;
    let mut rows = statement.query([])?;
    let mut current_claims = Vec::new();
    while let Some(row) = rows.next()? {
        current_claims.push((row.get::<_, String>(0)?, row.get::<_, i64>(1)?));
    }
    drop(rows);
    drop(statement);
    for (commitment_id, epoch) in current_claims {
        if epoch <= 0 {
            return Err(StoreError::InvalidPersistedData(format!(
                "current claim for commitment {commitment_id:?} has invalid epoch {epoch}"
            )));
        }
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM commitments WHERE commitment_id = ?1
            )",
            [&commitment_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidPersistedData(format!(
                "current claim references missing commitment {commitment_id:?}"
            )));
        }
        transaction.execute(
            "UPDATE commitments
             SET last_claim_epoch = CASE
                 WHEN last_claim_epoch IS NULL OR last_claim_epoch < ?1 THEN ?1
                 ELSE last_claim_epoch END
             WHERE commitment_id = ?2",
            rusqlite::params![epoch, commitment_id],
        )?;
    }
    Ok(())
}

fn ensure_unique_action_refs(connection: &rusqlite::Transaction<'_>) -> Result<(), StoreError> {
    let duplicate: Option<String> = connection
        .query_row(
            "SELECT action_ref FROM execution_guards
             WHERE action_ref IS NOT NULL
             GROUP BY action_ref HAVING COUNT(*) > 1 LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(action_ref) = duplicate {
        return Err(StoreError::InvalidPersistedData(format!(
            "multiple guards use Tethers action {action_ref:?}"
        )));
    }
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS execution_guards_action_ref_idx
         ON execution_guards(action_ref) WHERE action_ref IS NOT NULL",
        [],
    )?;
    Ok(())
}

fn ensure_claim_boot_generation(connection: &Connection) -> rusqlite::Result<()> {
    let has_column: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM pragma_table_info('claims') WHERE name = 'boot_generation'
        )",
        [],
        |row| row.get(0),
    )?;
    if !has_column {
        connection.execute(
            "ALTER TABLE claims ADD COLUMN boot_generation INTEGER NOT NULL DEFAULT 0
             CHECK (boot_generation >= 0)",
            [],
        )?;
    }
    Ok(())
}

fn create_fencing_tables(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS execution_guards (
            guard_id TEXT PRIMARY KEY NOT NULL,
            commitment_id TEXT NOT NULL,
            claim_epoch INTEGER NOT NULL CHECK (claim_epoch > 0),
            boot_generation INTEGER NOT NULL CHECK (boot_generation >= 0),
            state TEXT NOT NULL CHECK (state IN
                ('issued', 'admitted', 'resolved', 'uncertain', 'invalidated')),
            action_ref TEXT,
            outcome_json TEXT,
            reservation_expires_at INTEGER,
            state_version INTEGER NOT NULL CHECK (state_version > 0),
            UNIQUE (guard_id, commitment_id),
            FOREIGN KEY (commitment_id) REFERENCES commitments(commitment_id),
            CHECK (
                (state = 'issued' AND action_ref IS NULL
                    AND outcome_json IS NULL AND reservation_expires_at IS NOT NULL)
                OR (state = 'admitted' AND action_ref IS NOT NULL
                    AND outcome_json IS NULL AND reservation_expires_at IS NULL)
                OR (state = 'resolved' AND action_ref IS NOT NULL
                    AND outcome_json IS NOT NULL AND reservation_expires_at IS NULL)
                OR (state = 'uncertain' AND action_ref IS NOT NULL
                    AND outcome_json IS NULL AND reservation_expires_at IS NULL)
                OR (state = 'invalidated' AND action_ref IS NULL
                    AND outcome_json IS NULL AND reservation_expires_at IS NULL)
            )
        );

        CREATE TABLE IF NOT EXISTS execution_guard_scopes (
            guard_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
            scope_key TEXT NOT NULL,
            PRIMARY KEY (guard_id, ordinal),
            UNIQUE (guard_id, scope_key),
            FOREIGN KEY (guard_id) REFERENCES execution_guards(guard_id)
        );

        CREATE TABLE IF NOT EXISTS scope_locks (
            scope_key TEXT PRIMARY KEY NOT NULL,
            guard_id TEXT NOT NULL,
            commitment_id TEXT NOT NULL,
            lock_state TEXT NOT NULL CHECK (lock_state IN ('reserved', 'held')),
            action_ref TEXT,
            reservation_expires_at INTEGER,
            FOREIGN KEY (guard_id) REFERENCES execution_guards(guard_id),
            FOREIGN KEY (guard_id, commitment_id)
                REFERENCES execution_guards(guard_id, commitment_id),
            CHECK (
                (lock_state = 'reserved' AND action_ref IS NULL
                    AND reservation_expires_at IS NOT NULL)
                OR (lock_state = 'held' AND action_ref IS NOT NULL
                    AND reservation_expires_at IS NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS scope_locks_guard_id_idx
            ON scope_locks(guard_id);
        ",
    )
}
