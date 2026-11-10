use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, ErrorCode, OpenFlags, params_from_iter};
use serde_json::{Value, json};

use super::{Cell, Column, Driver, ObjectQuery, Statement, WriteKind, read_only_error, remaining};
use crate::command::Isolation;
use crate::error::{Code, Error, Result};
use crate::guard::{self, Dialect};
use crate::results::{RowSink, json_param};

pub struct SqliteDriver {
    conn: Connection,
    read_only: bool,
    in_transaction: bool,
}

impl SqliteDriver {
    pub fn open(path: &Path, read_only: bool) -> Result<SqliteDriver> {
        let mode = if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        };
        let conn = Connection::open_with_flags(path, mode | OpenFlags::SQLITE_OPEN_NO_MUTEX)
            .map_err(|error| super::connection_failed(format!("{}: {error}", path.display())))?;
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(super::connection_failed)?;
        conn.query_row("PRAGMA schema_version", [], |_| Ok(()))
            .map_err(|error| super::connection_failed(format!("{}: {error}", path.display())))?;
        Ok(SqliteDriver {
            conn,
            read_only,
            in_transaction: false,
        })
    }

    // query_only alone lets PRAGMA journal_mode rewrite the file, so reads also run under an authorizer.
    fn query_only<T>(&mut self, work: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        if !self.read_only {
            self.conn.execute_batch("PRAGMA query_only = ON")?;
        }
        self.conn.authorizer(Some(read_authorizer))?;
        let result = work(&self.conn);
        self.conn
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
        if !self.read_only {
            self.conn.execute_batch("PRAGMA query_only = OFF")?;
        }
        result
    }
}

const READ_PRAGMAS_WITH_ARGUMENT: [&str; 9] = [
    "table_info",
    "table_xinfo",
    "table_list",
    "index_list",
    "index_info",
    "index_xinfo",
    "foreign_key_list",
    "integrity_check",
    "quick_check",
];

fn read_authorizer(context: AuthContext<'_>) -> Authorization {
    let allowed = match context.action {
        AuthAction::Select | AuthAction::Read { .. } | AuthAction::Recursive => true,
        AuthAction::Function { function_name } => {
            !function_name.eq_ignore_ascii_case("load_extension")
        }
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => {
            pragma_value.is_none()
                || READ_PRAGMAS_WITH_ARGUMENT
                    .iter()
                    .any(|name| pragma_name.eq_ignore_ascii_case(name))
        }
        _ => false,
    };
    if allowed {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

// Interrupts the connection when the deadline passes; dropping the guard cancels the timer.
struct Interrupter(Option<mpsc::Sender<()>>);

impl Interrupter {
    fn arm(conn: &Connection, deadline: Instant) -> Result<Interrupter> {
        let handle = conn.get_interrupt_handle();
        let wait = remaining(deadline)?;
        let (sender, receiver) = mpsc::channel::<()>();
        thread::spawn(move || {
            if receiver.recv_timeout(wait) == Err(mpsc::RecvTimeoutError::Timeout) {
                handle.interrupt();
            }
        });
        Ok(Interrupter(Some(sender)))
    }
}

impl Drop for Interrupter {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

// Connection::prepare compiles the tail to find a second statement, so the read authorizer
// would report a denied DROP in the tail. Batch reads one statement at a time.
fn prepare_one<'c>(conn: &'c Connection, sql: &str) -> Result<rusqlite::Statement<'c>> {
    use rusqlite::fallible_iterator::FallibleIterator;
    let one_statement = || Error::new(Code::InvalidSql, "Send exactly one statement.");
    let mut batch = rusqlite::Batch::new(conn, sql);
    let first = batch.next().map_err(|error| map_error(error, true))?;
    if !matches!(batch.next(), Ok(None)) {
        return Err(one_statement());
    }
    first.ok_or_else(one_statement)
}

fn map_error(error: rusqlite::Error, preparing: bool) -> Error {
    // SQLite has no error code for a missing name, only the message text.
    let message = error.to_string();
    let hint = if message.starts_with("no such column") {
        Some(super::MISSING_COLUMN_HINT)
    } else if message.starts_with("no such table") {
        Some(super::MISSING_TABLE_HINT)
    } else {
        None
    };
    let mapped = match error.sqlite_error_code() {
        Some(ErrorCode::ReadOnly) => read_only_error(),
        Some(ErrorCode::OperationInterrupted) => Error::new(
            Code::DeadlineExceeded,
            "The statement passed its deadline and was stopped.",
        ),
        Some(ErrorCode::Unknown) if preparing => Error::new(Code::InvalidSql, message),
        Some(ErrorCode::AuthorizationForStatementDenied) => {
            Error::new(Code::PermissionDenied, message)
        }
        _ => Error::query(message),
    };
    match hint {
        Some(hint) => mapped.with_hint(hint),
        None => mapped,
    }
}

fn bind(params: &[Value]) -> Vec<Cell> {
    params.iter().map(json_param).collect()
}

fn read_into(conn: &Connection, sql: &str, params: &[Value], sink: &mut RowSink) -> Result<()> {
    let mut statement = prepare_one(conn, sql)?;
    let columns: Vec<Column> = statement
        .columns()
        .map(|column| {
            Column::new(
                column.name(),
                column.decl_type().unwrap_or("").to_lowercase(),
            )
        })
        .collect();
    sink.set_columns(columns)?;
    let width = statement.column_count();
    let mut rows = statement
        .query(params_from_iter(bind(params)))
        .map_err(|error| map_error(error, false))?;
    while let Some(row) = rows.next().map_err(|error| map_error(error, false))? {
        let cells = (0..width)
            .map(|index| row.get::<_, Cell>(index))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        sink.push(cells)?;
    }
    Ok(())
}

impl Driver for SqliteDriver {
    fn read(
        &mut self,
        stmt: &Statement,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        guard::check_read(Dialect::Sqlite, &stmt.text)?;
        let _interrupter = Interrupter::arm(&self.conn, deadline)?;
        self.query_only(|conn| read_into(conn, &stmt.text, &stmt.params, sink))?;
        Ok(None)
    }

    fn classify(&self, stmt: &Statement) -> Result<WriteKind> {
        guard::classify(Dialect::Sqlite, &stmt.text)
    }

    fn execute(&mut self, stmt: &Statement, deadline: Instant) -> Result<u64> {
        let _interrupter = Interrupter::arm(&self.conn, deadline)?;
        let changes_before = self.conn.total_changes();
        let mut statement = prepare_one(&self.conn, &stmt.text)?;
        let mut rows = statement
            .query(params_from_iter(bind(&stmt.params)))
            .map_err(|error| map_error(error, false))?;
        while rows
            .next()
            .map_err(|error| map_error(error, false))?
            .is_some()
        {}
        // changes() keeps the count of the last INSERT, UPDATE, or DELETE, so DDL would report it again.
        if self.conn.total_changes() == changes_before {
            return Ok(0);
        }
        Ok(self.conn.changes())
    }

    fn begin(&mut self, _isolation: Option<Isolation>) -> Result<()> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|error| map_error(error, false))?;
        self.in_transaction = true;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.in_transaction = false;
        self.conn
            .execute_batch("COMMIT")
            .map_err(|error| map_error(error, false))
    }

    fn rollback(&mut self) -> Result<()> {
        self.in_transaction = false;
        self.conn
            .execute_batch("ROLLBACK")
            .map_err(|error| map_error(error, false))
    }

    fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    fn objects(
        &mut self,
        query: &ObjectQuery,
        sink: &mut RowSink,
        _deadline: Instant,
    ) -> Result<Option<String>> {
        if let Some(kind) = &query.kind
            && !["table", "view", "index", "trigger"].contains(&kind.as_str())
        {
            return Err(Error::invalid(
                "SQLite object kinds are table, view, index, and trigger.",
            ));
        }
        let offset = super::offset_cursor(query.cursor.as_deref())?;
        let sql = "SELECT type AS kind, 'main' AS schema, name FROM sqlite_schema
                   WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND (?1 IS NULL OR type = ?1)
                     AND (?2 IS NULL OR name LIKE ?2 ESCAPE '\\')
                   ORDER BY type, name LIMIT ?3 OFFSET ?4";
        let params = vec![
            json!(query.kind),
            json!(query.search.as_deref().map(super::like_pattern)),
            json!(query.limit),
            json!(offset),
        ];
        self.query_only(|conn| read_into(conn, sql, &params, sink))?;
        Ok(super::next_offset_cursor(offset, query.limit, sink.total()))
    }

    fn describe(&mut self, name: &str, _deadline: Instant) -> Result<Value> {
        self.query_only(|conn| describe(conn, name))
    }
}

fn describe(conn: &Connection, name: &str) -> Result<Value> {
    let found = conn.query_row(
        "SELECT type, sql FROM sqlite_schema WHERE name = ?1",
        [name],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
    );
    let (kind, ddl) = match found {
        Ok(found) => found,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            return Err(
                Error::new(Code::ResultNotFound, format!("No object named \"{name}\"."))
                    .with_hint("use objects"),
            );
        }
        Err(error) => return Err(map_error(error, false)),
    };
    let mut statement = conn.prepare(
        "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_xinfo(?1) ORDER BY cid",
    )?;
    let columns = statement
        .query_map([name], |row| {
            Ok(super::column_json(
                &row.get::<_, String>(0)?,
                &row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? == 0,
                row.get::<_, Option<String>>(3)?.as_deref(),
                row.get::<_, i64>(4)? > 0,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut statement = conn.prepare(
        "SELECT il.name, il.\"unique\", group_concat(ii.name, ', ') FROM pragma_index_list(?1) AS il
         LEFT JOIN pragma_index_info(il.name) AS ii GROUP BY il.name, il.\"unique\" ORDER BY il.name",
    )?;
    let indexes = statement
        .query_map([name], |row| {
            let mut index = json!({
                "name": row.get::<_, String>(0)?,
                "columns": row.get::<_, Option<String>>(2)?,
            });
            if row.get::<_, i64>(1)? == 1 {
                index["unique"] = json!(true);
            }
            Ok(index)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut statement = conn.prepare(
        "SELECT \"from\", \"table\", \"to\" FROM pragma_foreign_key_list(?1) ORDER BY id, seq",
    )?;
    let foreign_keys = statement
        .query_map([name], |row| {
            Ok(json!({
                "column": row.get::<_, String>(0)?,
                "references": format!("{}({})", row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?.unwrap_or_default()),
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "name": name,
        "kind": kind,
        "columns": columns,
        "indexes": indexes,
        "foreign_keys": foreign_keys,
        "ddl": ddl,
    }))
}
