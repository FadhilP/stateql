use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags, params_from_iter};
use serde_json::{Map, Value, json};

use crate::command::ExportFormat;
use crate::driver::{Cell, Column};
use crate::error::{Code, Error, Result};

pub const DEFAULT_MAX_ROWS: u64 = 100_000;
const MAX_BYTES: u64 = 64 * 1024 * 1024;
// Above this result size, summarize sorts in temp files, so its memory stays small on any machine.
const MEMORY_SORT_MAX_BYTES: u64 = 16 * 1024 * 1024;
// Each thread has its own page cache, so more threads cost memory for little time.
const SUMMARIZE_MAX_THREADS: usize = 4;
pub const MAX_CELL_CHARS: usize = 200;
const MAX_INLINE_BYTES: usize = 150;

const BATCH_ROWS: usize = 1024;

enum Job {
    Execute(String),
    Insert(String, Vec<Vec<Cell>>),
}

// SQLite writes run on their own thread, so they overlap with reading rows from the database.
struct Writer {
    jobs: SyncSender<Job>,
    thread: JoinHandle<rusqlite::Result<Connection>>,
}

impl Writer {
    fn spawn(conn: Connection) -> Writer {
        let (jobs, queue) = mpsc::sync_channel(4);
        let thread = thread::spawn(move || write_jobs(conn, &queue));
        Writer { jobs, thread }
    }

    fn join(self) -> Result<Connection> {
        drop(self.jobs);
        match self.thread.join() {
            Ok(conn) => Ok(conn?),
            Err(_) => Err(Error::internal("The result writer stopped.")),
        }
    }
}

fn write_jobs(conn: Connection, queue: &Receiver<Job>) -> rusqlite::Result<Connection> {
    for job in queue {
        match job {
            Job::Execute(sql) => conn.execute_batch(&sql)?,
            Job::Insert(sql, rows) => {
                let mut statement = conn.prepare_cached(&sql)?;
                for row in rows {
                    statement.execute(params_from_iter(row.iter()))?;
                }
            }
        }
    }
    Ok(conn)
}

pub struct RowSink {
    writer: Option<Writer>,
    path: PathBuf,
    columns: Vec<Column>,
    insert_sql: String,
    pending: Vec<Vec<Cell>>,
    total: u64,
    bytes: u64,
    max_rows: u64,
    preview: Vec<Vec<Cell>>,
    preview_rows: usize,
}

pub struct Finished {
    pub columns: Vec<Column>,
    pub total: u64,
    pub bytes: u64,
    pub preview: Vec<Vec<Cell>>,
}

impl RowSink {
    pub fn create(path: PathBuf, max_rows: u64, preview_rows: usize) -> Result<RowSink> {
        File::create_new(&path)?;
        let mut sink = RowSink {
            writer: None,
            path,
            columns: Vec::new(),
            insert_sql: String::new(),
            pending: Vec::with_capacity(BATCH_ROWS),
            total: 0,
            bytes: 0,
            max_rows,
            preview: Vec::new(),
            preview_rows,
        };
        crate::config::restrict_file(&sink.path)?;
        let conn = Connection::open(&sink.path)?;
        conn.execute_batch("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; BEGIN")?;
        sink.writer = Some(Writer::spawn(conn));
        Ok(sink)
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    fn send(&mut self, job: Job) -> Result<()> {
        let sent = self
            .writer
            .as_ref()
            .is_some_and(|writer| writer.jobs.send(job).is_ok());
        if sent {
            return Ok(());
        }
        match self.stop_writer() {
            Err(error) => Err(error),
            Ok(_) => Err(Error::internal("The result writer stopped.")),
        }
    }

    fn stop_writer(&mut self) -> Result<Connection> {
        match self.writer.take() {
            Some(writer) => writer.join(),
            None => Err(Error::internal("The result writer stopped.")),
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let rows = std::mem::replace(&mut self.pending, Vec::with_capacity(BATCH_ROWS));
        self.send(Job::Insert(self.insert_sql.clone(), rows))
    }

    pub fn add_column(&mut self, mut column: Column) -> Result<()> {
        column.name = unique_name(&column.name, &self.columns);
        let quoted = quote(&column.name);
        let sql = if self.columns.is_empty() {
            format!("CREATE TABLE r ({quoted})")
        } else {
            format!("ALTER TABLE r ADD COLUMN {quoted}")
        };
        // Pending rows have the old column count, so they go before the new column.
        self.flush()?;
        self.send(Job::Execute(sql))?;
        self.columns.push(column);
        let placeholders = vec!["?"; self.columns.len()].join(", ");
        self.insert_sql = format!("INSERT INTO r VALUES ({placeholders})");
        Ok(())
    }

    pub fn update_column(&mut self, index: usize, type_name: &str, json: bool) {
        let column = &mut self.columns[index];
        column.type_name = type_name.to_string();
        column.json = json;
    }

    pub fn set_columns(&mut self, columns: Vec<Column>) -> Result<()> {
        for column in columns {
            self.add_column(column)?;
        }
        Ok(())
    }

    pub fn push(&mut self, row: Vec<Cell>) -> Result<()> {
        if self.total >= self.max_rows {
            return Err(Error::new(
                Code::ResultTooLarge,
                format!("The result has more than {} rows.", self.max_rows),
            )
            .with_hint("add a LIMIT, or raise --max-rows"));
        }
        self.bytes += row.iter().map(cell_bytes).sum::<u64>();
        if self.bytes > MAX_BYTES {
            return Err(
                Error::new(Code::ResultTooLarge, "The result is larger than 64 MiB.")
                    .with_hint("select fewer columns or add a LIMIT"),
            );
        }
        self.total += 1;
        if self.preview.len() < self.preview_rows {
            self.preview.push(row.clone());
        }
        self.pending.push(row);
        if self.pending.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<Finished> {
        if self.columns.is_empty() {
            return Err(Error::query("The statement returned no columns."));
        }
        self.flush()?;
        let conn = self.stop_writer()?;
        conn.execute_batch("COMMIT")?;
        conn.close().map_err(|(_, error)| error)?;
        let bytes = fs::metadata(&self.path)?.len();
        // The caller owns the file from here on and deletes it if its metadata row fails.
        self.path = PathBuf::new();
        // Rows pushed before a later column was added are shorter; the table has NULL there.
        for row in &mut self.preview {
            row.resize(self.columns.len(), Cell::Null);
        }
        Ok(Finished {
            columns: std::mem::take(&mut self.columns),
            total: self.total,
            bytes,
            preview: std::mem::take(&mut self.preview),
        })
    }
}

impl Drop for RowSink {
    fn drop(&mut self) {
        // Windows cannot delete the file while the writer holds it open.
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
        if !self.path.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn cell_bytes(cell: &Cell) -> u64 {
    match cell {
        Cell::Text(text) => text.len() as u64,
        Cell::Blob(bytes) => bytes.len() as u64,
        _ => 8,
    }
}

fn unique_name(name: &str, columns: &[Column]) -> String {
    let taken: HashSet<String> = columns
        .iter()
        .map(|column| column.name.to_lowercase())
        .collect();
    if !taken.contains(&name.to_lowercase()) {
        return name.to_string();
    }
    (2..)
        .map(|suffix| format!("{name}_{suffix}"))
        .find(|candidate| !taken.contains(&candidate.to_lowercase()))
        .expect("an unused suffix exists")
}

pub fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn open_read(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    Ok(conn)
}

pub fn select_columns(columns: &[Column], selection: Option<&str>) -> Result<Vec<Column>> {
    let Some(selection) = selection else {
        return Ok(columns.to_vec());
    };
    selection
        .split(',')
        .map(str::trim)
        .map(|name| {
            columns
                .iter()
                .find(|column| column.name == name)
                .cloned()
                .ok_or_else(|| Error::invalid(format!("Unknown column \"{name}\".")))
        })
        .collect()
}

fn select_list(columns: &[Column]) -> String {
    columns
        .iter()
        .map(|column| quote(&column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

// Exact decimals are stored as text, and SQLite orders all text after all numbers.
fn is_decimal_text(column: &Column) -> bool {
    matches!(
        column.type_name.as_str(),
        "numeric" | "decimal" | "decimal128" | "bigint unsigned"
    )
}

fn compare_key(column: &Column) -> String {
    let name = quote(&column.name);
    if is_decimal_text(column) {
        // CAST makes these words 0. PostgreSQL sorts NaN above all numbers, so it compares as infinity.
        // The outer CAST keeps numeric affinity, so a quoted number in a predicate compares as a number.
        return format!(
            "CAST(CASE {name} WHEN 'NaN' THEN '9e999' WHEN 'Infinity' THEN '9e999' \
             WHEN '-Infinity' THEN '-9e999' ELSE {name} END AS NUMERIC)"
        );
    }
    name
}

// A column named rowid hides the row id, so this picks a name for it that no column has.
fn row_id(columns: &[Column]) -> &'static str {
    ["rowid", "_rowid_", "oid"]
        .into_iter()
        .find(|alias| {
            !columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(alias))
        })
        .unwrap_or("rowid")
}

fn compare_list(columns: &[Column]) -> String {
    columns
        .iter()
        .map(|column| format!("{} AS {}", compare_key(column), quote(&column.name)))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn read_page(
    path: &Path,
    columns: &[Column],
    offset: u64,
    limit: u64,
) -> Result<Vec<Vec<Cell>>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let conn = open_read(path)?;
    let id = row_id(columns);
    let sql = format!(
        "SELECT {} FROM r WHERE {id} > ?1 ORDER BY {id} LIMIT ?2",
        select_list(columns)
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([offset, limit], |row| {
        (0..columns.len())
            .map(|index| row.get::<_, Cell>(index))
            .collect()
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub fn cell_json(cell: &Cell, column: &Column, max_chars: usize) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Integer(number) => json!(number),
        Cell::Real(number) => json!(number),
        Cell::Text(text) => {
            let length = text.chars().count();
            if length > max_chars {
                let preview: String = text.chars().take(max_chars).collect();
                let kind = if column.json { "json" } else { "text" };
                return json!({"type": kind, "length": length, "preview": preview});
            }
            full_text_json(text, column)
        }
        Cell::Blob(bytes) if bytes.len() <= MAX_INLINE_BYTES => {
            json!({"type": "bytes", "length": bytes.len(), "base64": BASE64.encode(bytes)})
        }
        Cell::Blob(bytes) => json!({"type": "bytes", "length": bytes.len()}),
    }
}

fn full_text_json(text: &str, column: &Column) -> Value {
    if column.json {
        return serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()));
    }
    Value::String(text.to_string())
}

pub fn rows_json(rows: &[Vec<Cell>], columns: &[Column], max_chars: usize) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .zip(columns)
                .map(|(cell, column)| cell_json(cell, column, max_chars))
                .collect()
        })
        .collect()
}

pub fn create_filtered(
    source: &Path,
    target: &Path,
    columns: &[Column],
    predicate: &str,
    params: &[Value],
) -> Result<u64> {
    File::create_new(target)?;
    crate::config::restrict_file(target)?;
    let result = fill_filtered(source, target, columns, predicate, params);
    if result.is_err() {
        let _ = fs::remove_file(target);
    }
    result
}

fn fill_filtered(
    source: &Path,
    target: &Path,
    columns: &[Column],
    predicate: &str,
    params: &[Value],
) -> Result<u64> {
    let mut conn = Connection::open_with_flags(
        target,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let names = select_list(columns);
    conn.execute_batch(&format!(
        "PRAGMA journal_mode = OFF; CREATE TABLE r ({names})"
    ))?;
    conn.execute("ATTACH DATABASE ?1 AS src", [sqlite_uri(source)])?;
    conn.authorizer(Some(filter_authorizer))?;
    // The predicate sees decimals as numbers; the copied rows keep their exact text.
    let compared = compare_list(columns);
    let id = row_id(columns);
    let sql = format!(
        "INSERT INTO main.r SELECT {names} FROM src.r WHERE {id} IN \
         (SELECT {id} FROM (SELECT {id} AS {id}, {compared} FROM src.r) WHERE ({predicate})) ORDER BY {id}"
    );
    let mut statement = conn.prepare(&sql).map_err(filter_error)?;
    let values = params.iter().map(json_param).collect::<Vec<_>>();
    let inserted = statement
        .execute(params_from_iter(values.iter()))
        .map_err(filter_error)?;
    drop(statement);
    conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
    conn.execute_batch("DETACH DATABASE src")?;
    conn.close().map_err(|(_, error)| error)?;
    Ok(inserted as u64)
}

fn filter_authorizer(context: AuthContext<'_>) -> Authorization {
    let allowed = match context.action {
        AuthAction::Select => true,
        AuthAction::Read { table_name, .. } => {
            table_name == "r" && context.database_name == Some("src")
        }
        AuthAction::Insert { table_name } => {
            table_name == "r" && context.database_name == Some("main")
        }
        AuthAction::Function { function_name } => {
            !function_name.eq_ignore_ascii_case("load_extension")
        }
        _ => false,
    };
    if allowed {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

fn filter_error(error: rusqlite::Error) -> Error {
    let message = error.to_string();
    if message.contains("not authorized") || matches!(error, rusqlite::Error::MultipleStatement) {
        return Error::new(
            Code::PermissionDenied,
            format!("The predicate is not allowed: {message}"),
        )
        .with_hint("use columns of this result, literals, operators, and built-in functions");
    }
    Error::new(Code::InvalidSql, format!("Invalid predicate: {message}"))
}

fn sqlite_uri(path: &Path) -> String {
    let text = path.to_string_lossy().replace('\\', "/");
    let encoded = text
        .replace('%', "%25")
        .replace('?', "%3f")
        .replace('#', "%23");
    let prefix = if encoded.starts_with('/') {
        "file://"
    } else {
        "file:///"
    };
    format!("{prefix}{encoded}?mode=ro")
}

pub fn json_param(value: &Value) -> Cell {
    match value {
        Value::Null => Cell::Null,
        Value::Bool(flag) => Cell::Integer(i64::from(*flag)),
        Value::Number(number) => match number.as_i64() {
            Some(integer) => Cell::Integer(integer),
            None => Cell::Real(number.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(text) => Cell::Text(text.clone()),
        other => Cell::Text(other.to_string()),
    }
}

// Each column is a full scan with its own DISTINCT sort, so threads take the next column until none is left.
pub fn summarize(path: &Path, columns: &[Column]) -> Result<Vec<Value>> {
    let temp_store = if fs::metadata(path)?.len() <= MEMORY_SORT_MAX_BYTES {
        "MEMORY"
    } else {
        "DEFAULT"
    };
    let threads = thread::available_parallelism()
        .map_or(1, usize::from)
        .min(SUMMARIZE_MAX_THREADS)
        .min(columns.len());
    let next = AtomicUsize::new(0);
    let take_columns = || -> Result<Vec<(usize, Value)>> {
        let conn = open_read(path)?;
        conn.pragma_update(None, "temp_store", temp_store)?;
        let mut done = Vec::new();
        loop {
            let index = next.fetch_add(1, Ordering::Relaxed);
            let Some(column) = columns.get(index) else {
                return Ok(done);
            };
            done.push((index, summarize_column(&conn, column)?));
        }
    };
    let mut summary = thread::scope(|scope| {
        let workers: Vec<_> = (0..threads).map(|_| scope.spawn(take_columns)).collect();
        let mut summary = Vec::with_capacity(columns.len());
        for worker in workers {
            let part = worker
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
            summary.extend(part);
        }
        Ok::<_, Error>(summary)
    })?;
    summary.sort_unstable_by_key(|(index, _)| *index);
    Ok(summary.into_iter().map(|(_, value)| value).collect())
}

fn summarize_column(conn: &Connection, column: &Column) -> Result<Value> {
    let name = quote(&column.name);
    let key = compare_key(column);
    let (min, max) = if is_decimal_text(column) {
        let extreme = |order| {
            format!(
                "(SELECT {name} FROM r WHERE {name} IS NOT NULL ORDER BY {key} {order} LIMIT 1)"
            )
        };
        (extreme("ASC"), extreme("DESC"))
    } else {
        (format!("min({name})"), format!("max({name})"))
    };
    let sql =
        format!("SELECT count(*) - count({name}), count(DISTINCT {key}), {min}, {max} FROM r");
    let (nulls, distinct, min, max) = conn.query_row(&sql, [], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Cell>(2)?,
            row.get::<_, Cell>(3)?,
        ))
    })?;
    Ok(json!({
        "name": column.name,
        "type": column.type_name,
        "nulls": nulls,
        "distinct": distinct,
        "min": cell_json(&min, column, MAX_CELL_CHARS),
        "max": cell_json(&max, column, MAX_CELL_CHARS),
    }))
}

pub fn export(path: &Path, columns: &[Column], output: &Path, format: ExportFormat) -> Result<u64> {
    let file = File::create_new(output).map_err(|error| match error.kind() {
        std::io::ErrorKind::AlreadyExists => {
            Error::invalid(format!("{} already exists.", output.display()))
                .with_hint("choose a new file name")
        }
        _ => Error::from(error),
    })?;
    let result = write_export(path, columns, BufWriter::new(file), format);
    if result.is_err() {
        let _ = fs::remove_file(output);
    }
    result
}

fn write_export(
    path: &Path,
    columns: &[Column],
    mut out: BufWriter<File>,
    format: ExportFormat,
) -> Result<u64> {
    let conn = open_read(path)?;
    let mut statement = conn.prepare(&format!(
        "SELECT {} FROM r ORDER BY {}",
        select_list(columns),
        row_id(columns)
    ))?;
    let mut rows = statement.query([])?;
    let mut count = 0;
    match format {
        ExportFormat::Csv => writeln!(
            out,
            "{}",
            columns
                .iter()
                .map(|c| csv_field(&c.name))
                .collect::<Vec<_>>()
                .join(",")
        )?,
        ExportFormat::Json => write!(out, "[")?,
        ExportFormat::Jsonl => {}
    }
    while let Some(row) = rows.next()? {
        let cells = (0..columns.len())
            .map(|index| row.get::<_, Cell>(index))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        match format {
            ExportFormat::Csv => {
                let fields: Vec<String> = cells
                    .iter()
                    .map(|cell| csv_field(&cell_text(cell)))
                    .collect();
                writeln!(out, "{}", fields.join(","))?;
            }
            ExportFormat::Json | ExportFormat::Jsonl => {
                let object: Map<String, Value> = columns
                    .iter()
                    .zip(&cells)
                    .map(|(column, cell)| (column.name.clone(), export_json(cell, column)))
                    .collect();
                let separator = match (format, count) {
                    (ExportFormat::Json, 0) => "",
                    (ExportFormat::Json, _) => ",",
                    _ => "",
                };
                write!(out, "{separator}{}", Value::Object(object))?;
                if format == ExportFormat::Jsonl {
                    writeln!(out)?;
                }
            }
        }
        count += 1;
    }
    if format == ExportFormat::Json {
        writeln!(out, "]")?;
    }
    out.flush()?;
    Ok(count)
}

fn export_json(cell: &Cell, column: &Column) -> Value {
    match cell {
        Cell::Text(text) => full_text_json(text, column),
        Cell::Blob(bytes) => Value::String(BASE64.encode(bytes)),
        other => cell_json(other, column, MAX_CELL_CHARS),
    }
}

fn cell_text(cell: &Cell) -> String {
    match cell {
        Cell::Null => String::new(),
        Cell::Integer(number) => number.to_string(),
        Cell::Real(number) => number.to_string(),
        Cell::Text(text) => text.clone(),
        Cell::Blob(bytes) => BASE64.encode(bytes),
    }
}

fn csv_field(text: &str) -> String {
    if text.contains([',', '"', '\n', '\r']) {
        return format!("\"{}\"", text.replace('"', "\"\""));
    }
    text.to_string()
}
