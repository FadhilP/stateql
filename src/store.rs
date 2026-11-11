use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

use crate::driver::{Column, iso_millis, now_millis};
use crate::error::{Error, Result};

const SCHEMA_VERSION: i64 = 1;
const HISTORY_CAP: i64 = 10_000;
const OPERATIONS_CAP: i64 = 10_000;
pub const RESULT_TTL_MS: i64 = 24 * 3600 * 1000;
const STORE_CAP_BYTES: i64 = 1024 * 1024 * 1024;
const ORPHAN_AGE: Duration = Duration::from_secs(3600);

const SCHEMA: &str = "
CREATE TABLE sessions (
  name TEXT PRIMARY KEY,
  target TEXT,
  profile TEXT,
  env_var TEXT,
  read_only INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  used_at TEXT NOT NULL,
  CHECK ((target IS NOT NULL) + (profile IS NOT NULL) + (env_var IS NOT NULL) = 1)
);
CREATE TABLE results (
  id TEXT PRIMARY KEY,
  session TEXT NOT NULL,
  name TEXT,
  source TEXT NOT NULL,
  columns_json TEXT NOT NULL,
  total INTEGER NOT NULL,
  bytes INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  UNIQUE (session, name)
);
CREATE TABLE operations (
  id TEXT PRIMARY KEY,
  session TEXT NOT NULL,
  target TEXT NOT NULL,
  stmt_json TEXT NOT NULL,
  hash TEXT NOT NULL,
  idempotency_key TEXT,
  flags TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'committed', 'rolled_back', 'failed')),
  affected_rows INTEGER,
  error_json TEXT,
  deadline_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  UNIQUE (session, idempotency_key)
);
CREATE INDEX operations_hash ON operations (session, hash);
CREATE TABLE history (
  id INTEGER PRIMARY KEY,
  session TEXT NOT NULL,
  command TEXT NOT NULL,
  target TEXT,
  ok INTEGER NOT NULL,
  error_code TEXT,
  duration_ms INTEGER NOT NULL,
  stmt_head TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX history_session ON history (session, id);
";

#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    Target(String),
    Profile(String),
    Env(String),
}

#[derive(Clone, Debug)]
pub struct SessionRow {
    pub name: String,
    pub source: Source,
    pub read_only: bool,
    pub used_at: String,
}

#[derive(Clone, Debug)]
pub struct ResultMeta {
    pub id: String,
    pub session: String,
    pub name: Option<String>,
    pub source: String,
    pub columns: Vec<Column>,
    pub total: u64,
    pub bytes: u64,
    pub created_at: String,
    pub expires_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Pending,
    Committed,
    RolledBack,
    Failed,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Committed => "committed",
            Status::RolledBack => "rolled_back",
            Status::Failed => "failed",
        }
    }

    fn parse(text: &str) -> Status {
        match text {
            "committed" => Status::Committed,
            "rolled_back" => Status::RolledBack,
            "failed" => Status::Failed,
            _ => Status::Pending,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Operation {
    pub id: String,
    pub stmt: Value,
    pub hash: String,
    pub idempotency_key: Option<String>,
    pub status: Status,
    pub affected_rows: Option<u64>,
    pub error: Option<Value>,
    pub deadline_at: String,
    pub created_at: String,
    pub finished_at: Option<String>,
}

impl Operation {
    pub fn outcome_unknown(&self, now: &str) -> bool {
        self.status == Status::Pending && self.deadline_at.as_str() < now
    }
}

pub enum Claim {
    Inserted,
    Existing(Operation),
    Unknown(Operation),
}

pub struct NewOperation<'a> {
    pub id: &'a str,
    pub session: &'a str,
    pub target: &'a str,
    pub stmt: Value,
    pub hash: &'a str,
    pub idempotency_key: Option<&'a str>,
    pub flags: Value,
    pub deadline_at: String,
}

pub struct Store {
    conn: Connection,
    results_dir: PathBuf,
}

pub fn now() -> String {
    iso_millis(now_millis())
}

pub fn later(milliseconds: i64) -> String {
    iso_millis(now_millis() + milliseconds)
}

impl Store {
    pub fn open(home: &Path) -> Result<Store> {
        let results_dir = home.join("results");
        let db_path = home.join("stql.db");
        let conn = Connection::open(&db_path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != 0 && version != SCHEMA_VERSION {
            drop(conn);
            reset(home, &db_path, &results_dir)?;
            return Store::open(home);
        }
        crate::config::restrict_file(&db_path)?;
        enable_wal(&conn)?;
        // In WAL mode, NORMAL keeps the store consistent after a crash; write_durable covers what must survive power loss.
        conn.execute_batch("PRAGMA foreign_keys = ON; PRAGMA synchronous = NORMAL")?;
        // Each command is a new process; a checkpoint at every close made each command pay a sync.
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )?;
        crate::config::create_private_dir(&results_dir)?;
        let mut store = Store { conn, results_dir };
        store.write(|tx| {
            let version: i64 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if version == 0 {
                tx.execute_batch(SCHEMA)?;
                tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
            }
            Ok(())
        })?;
        Ok(store)
    }

    pub fn result_path(&self, id: &str) -> PathBuf {
        self.results_dir.join(format!("{id}.db"))
    }

    fn write<T>(&mut self, work: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = work(&tx)?;
        tx.commit()?;
        Ok(value)
    }

    // Write claims and outcomes decide if a retry may run a write again, so they must survive power loss.
    fn write_durable<T>(&mut self, work: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        self.conn.execute_batch("PRAGMA synchronous = FULL")?;
        let value = self.write(work);
        self.conn.execute_batch("PRAGMA synchronous = NORMAL")?;
        value
    }

    pub fn quick_check(&self) -> Result<String> {
        Ok(self
            .conn
            .query_row("PRAGMA quick_check", [], |row| row.get(0))?)
    }

    pub fn session(&self, name: &str) -> Result<Option<SessionRow>> {
        let sql = "SELECT name, target, profile, env_var, read_only, used_at FROM sessions WHERE name = ?1";
        Ok(self.conn.query_row(sql, [name], session_row).optional()?)
    }

    pub fn sessions(&self) -> Result<Vec<SessionRow>> {
        let sql = "SELECT name, target, profile, env_var, read_only, used_at FROM sessions ORDER BY used_at DESC";
        let mut statement = self.conn.prepare(sql)?;
        let rows = statement.query_map([], session_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn set_session(&mut self, name: &str, source: &Source, read_only: bool) -> Result<()> {
        let (target, profile, env_var) = match source {
            Source::Target(target) => (Some(target), None, None),
            Source::Profile(profile) => (None, Some(profile), None),
            Source::Env(var) => (None, None, Some(var)),
        };
        let now = now();
        self.write(|tx| {
            tx.execute(
                "INSERT INTO sessions (name, target, profile, env_var, read_only, created_at, used_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
                 ON CONFLICT (name) DO UPDATE SET target = ?2, profile = ?3, env_var = ?4, read_only = ?5, used_at = ?6",
                params![name, target, profile, env_var, read_only, now],
            )?;
            Ok(())
        })
    }

    pub fn touch_session(&mut self, name: &str) -> Result<()> {
        let now = now();
        self.write(|tx| {
            Ok(tx.execute(
                "UPDATE sessions SET used_at = ?2 WHERE name = ?1",
                params![name, now],
            )?)
        })?;
        Ok(())
    }

    pub fn delete_session(&mut self, name: &str) -> Result<bool> {
        Ok(self.write(|tx| Ok(tx.execute("DELETE FROM sessions WHERE name = ?1", [name])?))? > 0)
    }

    pub fn insert_result(&mut self, meta: &ResultMeta) -> Result<Vec<String>> {
        let columns = serde_json::to_string(&meta.columns).map_err(Error::internal)?;
        let now = now();
        let doomed = self.write(|tx| {
            let mut doomed: Vec<String> = Vec::new();
            if let Some(name) = &meta.name {
                let sql = "SELECT id FROM results WHERE session = ?1 AND name = ?2";
                doomed.extend(tx.query_row(sql, params![meta.session, name], |row| row.get(0)).optional()?);
            }
            doomed.extend(ids(tx, "SELECT id FROM results WHERE expires_at < ?1", params![now])?);
            delete_results(tx, &doomed)?;
            tx.execute(
                "INSERT INTO results (id, session, name, source, columns_json, total, bytes, created_at, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![meta.id, meta.session, meta.name, meta.source, columns, meta.total, meta.bytes, meta.created_at, meta.expires_at],
            )?;
            let evicted = ids(
                tx,
                "SELECT id FROM (SELECT id, sum(bytes) OVER (ORDER BY created_at DESC, id DESC) AS running FROM results)
                 WHERE running > ?1",
                params![STORE_CAP_BYTES],
            )?;
            delete_results(tx, &evicted)?;
            doomed.extend(evicted);
            Ok(doomed)
        })?;
        self.remove_files(&doomed);
        Ok(doomed)
    }

    pub fn result(&self, session: &str, handle: &str) -> Result<Option<ResultMeta>> {
        let sql = "SELECT id, session, name, source, columns_json, total, bytes, created_at, expires_at FROM results
                   WHERE session = ?1 AND (id = ?2 OR name = ?2) AND expires_at >= ?3";
        Ok(self
            .conn
            .query_row(sql, params![session, handle, now()], result_row)
            .optional()?)
    }

    pub fn results(&self, session: &str, limit: u64) -> Result<Vec<ResultMeta>> {
        let sql = "SELECT id, session, name, source, columns_json, total, bytes, created_at, expires_at FROM results
                   WHERE session = ?1 AND expires_at >= ?2 ORDER BY created_at DESC LIMIT ?3";
        let mut statement = self.conn.prepare(sql)?;
        let rows = statement.query_map(params![session, now(), limit], result_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn purge(&mut self, scope: &PurgeScope) -> Result<usize> {
        let now = now();
        let doomed = self.write(|tx| {
            let doomed = match scope {
                PurgeScope::Session(name) => ids(
                    tx,
                    "SELECT id FROM results WHERE session = ?1",
                    params![name],
                )?,
                PurgeScope::Expired => ids(
                    tx,
                    "SELECT id FROM results WHERE expires_at < ?1",
                    params![now],
                )?,
                PurgeScope::All => ids(tx, "SELECT id FROM results", [])?,
            };
            delete_results(tx, &doomed)?;
            Ok(doomed)
        })?;
        self.remove_files(&doomed);
        Ok(doomed.len() + self.remove_orphans()?)
    }

    fn remove_files(&self, ids: &[String]) {
        for id in ids {
            let _ = fs::remove_file(self.result_path(id));
        }
    }

    pub fn remove_orphans(&self) -> Result<usize> {
        let mut removed = 0;
        for entry in fs::read_dir(&self.results_dir)? {
            let path = entry?.path();
            let Some(id) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let old = fs::metadata(&path)?
                .modified()?
                .elapsed()
                .is_ok_and(|age| age > ORPHAN_AGE);
            if !old {
                continue;
            }
            let known: bool = self.conn.query_row(
                "SELECT EXISTS (SELECT 1 FROM results WHERE id = ?1)",
                [id],
                |row| row.get(0),
            )?;
            if !known && fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    // One transaction, so two processes with the same key cannot both insert and run.
    pub fn claim_operation(
        &mut self,
        op: &NewOperation<'_>,
        block_unknown: bool,
        running: &[String],
    ) -> Result<Claim> {
        let now = now();
        self.write_durable(|tx| {
            if let Some(key) = op.idempotency_key
                && let Some(previous) = find_operation(tx, "idempotency_key = ?2", op.session, key)?
            {
                let nothing_kept = matches!(previous.status, Status::Failed | Status::RolledBack);
                if previous.hash != op.hash || !nothing_kept {
                    return Ok(Claim::Existing(previous));
                }
                tx.execute("DELETE FROM operations WHERE id = ?1", [&previous.id])?;
            }
            if block_unknown
                && let Some(previous) = unknown_by_hash(tx, op.session, op.hash, &now, running)?
            {
                return Ok(Claim::Unknown(previous));
            }
            tx.execute(
                "INSERT INTO operations (id, session, target, stmt_json, hash, idempotency_key, flags, status, deadline_at, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', ?8, ?9)",
                params![op.id, op.session, op.target, op.stmt.to_string(), op.hash, op.idempotency_key, op.flags.to_string(), op.deadline_at, now],
            )?;
            tx.execute(
                "DELETE FROM operations WHERE session = ?1 AND status != 'pending' AND id IN
                   (SELECT id FROM operations WHERE session = ?1 ORDER BY created_at DESC, id DESC LIMIT -1 OFFSET ?2)",
                params![op.session, OPERATIONS_CAP],
            )?;
            Ok(Claim::Inserted)
        })
    }

    pub fn finish_operations(
        &mut self,
        ids: &[String],
        status: Status,
        affected_rows: Option<u64>,
        error: Option<&crate::error::Error>,
    ) -> Result<()> {
        let error_json = error.map(|error| json!(error).to_string());
        let now = now();
        self.write_durable(|tx| {
            for id in ids {
                tx.execute(
                    "UPDATE operations SET status = ?2, affected_rows = coalesce(?3, affected_rows), error_json = ?4, finished_at = ?5
                     WHERE id = ?1",
                    params![id, status.as_str(), affected_rows, error_json, now],
                )?;
            }
            Ok(())
        })
    }

    pub fn record_affected(&mut self, id: &str, affected_rows: u64) -> Result<()> {
        self.write_durable(|tx| {
            tx.execute(
                "UPDATE operations SET affected_rows = ?2 WHERE id = ?1",
                params![id, affected_rows],
            )?;
            Ok(())
        })
    }

    pub fn set_deadline(&mut self, ids: &[String], deadline_at: &str) -> Result<()> {
        self.write_durable(|tx| {
            for id in ids {
                tx.execute(
                    "UPDATE operations SET deadline_at = ?2 WHERE id = ?1",
                    params![id, deadline_at],
                )?;
            }
            Ok(())
        })
    }

    pub fn operation(&self, session: &str, id: &str) -> Result<Option<Operation>> {
        find_operation(&self.conn, "id = ?2", session, id)
    }

    pub fn recent_operations(&self, session: &str, limit: u64) -> Result<Vec<Operation>> {
        let sql = format!(
            "SELECT {OPERATION_COLUMNS} FROM operations WHERE session = ?1 ORDER BY created_at DESC LIMIT ?2"
        );
        let mut statement = self.conn.prepare(&sql)?;
        let rows = statement.query_map(params![session, limit], operation_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn add_history(&mut self, entry: &HistoryEntry<'_>) -> Result<()> {
        let now = now();
        self.write(|tx| {
            tx.execute(
                "INSERT INTO history (session, command, target, ok, error_code, duration_ms, stmt_head, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![entry.session, entry.command, entry.target, entry.ok, entry.error_code, entry.duration_ms, entry.stmt_head, now],
            )?;
            tx.execute(
                "DELETE FROM history WHERE session = ?1 AND id <= (SELECT id FROM history WHERE session = ?1 ORDER BY id DESC LIMIT 1 OFFSET ?2)",
                params![entry.session, HISTORY_CAP],
            )?;
            Ok(())
        })
    }

    pub fn history(&self, session: &str, limit: u64) -> Result<Vec<Value>> {
        let sql = "SELECT command, target, ok, error_code, duration_ms, stmt_head, created_at FROM history
                   WHERE session = ?1 ORDER BY id DESC LIMIT ?2";
        let mut statement = self.conn.prepare(sql)?;
        let rows = statement.query_map(params![session, limit], |row| {
            let error_code: Option<String> = row.get(3)?;
            let stmt: Option<String> = row.get(5)?;
            let mut entry = json!({
                "command": row.get::<_, String>(0)?,
                "ok": row.get::<_, bool>(2)?,
                "duration_ms": row.get::<_, i64>(4)?,
                "created_at": row.get::<_, String>(6)?,
            });
            let object = entry.as_object_mut().expect("history entry is an object");
            if let Some(target) = row.get::<_, Option<String>>(1)? {
                object.insert("target".into(), target.into());
            }
            if let Some(code) = error_code {
                object.insert("error_code".into(), code.into());
            }
            if let Some(stmt) = stmt {
                object.insert("stmt".into(), stmt.into());
            }
            Ok(entry)
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

pub enum PurgeScope {
    Session(String),
    Expired,
    All,
}

pub struct HistoryEntry<'a> {
    pub session: &'a str,
    pub command: &'a str,
    pub target: Option<&'a str>,
    pub ok: bool,
    pub error_code: Option<String>,
    pub duration_ms: i64,
    pub stmt_head: Option<String>,
}

fn reset(home: &Path, db_path: &Path, results_dir: &Path) -> Result<()> {
    for suffix in ["", "-wal", "-shm"] {
        let path = home.join(format!(
            "{}{suffix}",
            db_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("stql.db")
        ));
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    if results_dir.exists() {
        fs::remove_dir_all(results_dir)?;
    }
    Ok(())
}

// Switching to WAL ignores the busy timeout, so parallel first opens retry here.
fn enable_wal(conn: &Connection) -> Result<()> {
    let started = std::time::Instant::now();
    loop {
        let switched = conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        });
        match switched {
            Ok(_) => return Ok(()),
            Err(error)
                if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy)
                    && started.elapsed() < Duration::from_secs(5) =>
            {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

const OPERATION_COLUMNS: &str = "id, stmt_json, hash, idempotency_key, status, affected_rows, error_json, deadline_at, created_at, finished_at";

fn find_operation(
    conn: &Connection,
    condition: &str,
    session: &str,
    value: &str,
) -> Result<Option<Operation>> {
    let sql = format!(
        "SELECT {OPERATION_COLUMNS} FROM operations WHERE session = ?1 AND {condition} ORDER BY created_at DESC LIMIT 1"
    );
    Ok(conn
        .query_row(&sql, params![session, value], operation_row)
        .optional()?)
}

fn unknown_by_hash(
    conn: &Connection,
    session: &str,
    hash: &str,
    now: &str,
    running: &[String],
) -> Result<Option<Operation>> {
    let sql = format!(
        "SELECT {OPERATION_COLUMNS} FROM operations
         WHERE session = ?1 AND hash = ?2 AND status = 'pending' AND deadline_at < ?3 ORDER BY created_at DESC"
    );
    let mut statement = conn.prepare(&sql)?;
    for operation in statement.query_map(params![session, hash, now], operation_row)? {
        let operation = operation?;
        if !running.contains(&operation.id) {
            return Ok(Some(operation));
        }
    }
    Ok(None)
}

fn ids(tx: &Transaction<'_>, sql: &str, params: impl rusqlite::Params) -> Result<Vec<String>> {
    let mut statement = tx.prepare(sql)?;
    let rows = statement.query_map(params, |row| row.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn delete_results(tx: &Transaction<'_>, ids: &[String]) -> Result<()> {
    for id in ids {
        tx.execute("DELETE FROM results WHERE id = ?1", [id])?;
    }
    Ok(())
}

fn session_row(row: &Row<'_>) -> rusqlite::Result<SessionRow> {
    let target: Option<String> = row.get(1)?;
    let profile: Option<String> = row.get(2)?;
    let env_var: Option<String> = row.get(3)?;
    let source = match (target, profile, env_var) {
        (Some(target), _, _) => Source::Target(target),
        (None, Some(profile), _) => Source::Profile(profile),
        (None, None, var) => Source::Env(var.unwrap_or_default()),
    };
    Ok(SessionRow {
        name: row.get(0)?,
        source,
        read_only: row.get(4)?,
        used_at: row.get(5)?,
    })
}

fn result_row(row: &Row<'_>) -> rusqlite::Result<ResultMeta> {
    let columns: String = row.get(4)?;
    Ok(ResultMeta {
        id: row.get(0)?,
        session: row.get(1)?,
        name: row.get(2)?,
        source: row.get(3)?,
        columns: serde_json::from_str(&columns).unwrap_or_default(),
        total: row.get(5)?,
        bytes: row.get(6)?,
        created_at: row.get(7)?,
        expires_at: row.get(8)?,
    })
}

fn operation_row(row: &Row<'_>) -> rusqlite::Result<Operation> {
    let stmt: String = row.get(1)?;
    let error: Option<String> = row.get(6)?;
    let status: String = row.get(4)?;
    Ok(Operation {
        id: row.get(0)?,
        stmt: serde_json::from_str(&stmt).unwrap_or(Value::Null),
        hash: row.get(2)?,
        idempotency_key: row.get(3)?,
        status: Status::parse(&status),
        affected_rows: row.get(5)?,
        error: error.and_then(|text| serde_json::from_str(&text).ok()),
        deadline_at: row.get(7)?,
        created_at: row.get(8)?,
        finished_at: row.get(9)?,
    })
}
