use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::command::Isolation;
use crate::error::{Code, Error, Result};
use crate::results::RowSink;

pub mod mongo;
pub mod mysql;
pub mod postgres;
pub mod redis;
pub mod sqlite;

pub type Cell = rusqlite::types::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub json: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Column {
            name: name.into(),
            type_name: type_name.into(),
            json: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Statement {
    pub text: String,
    pub params: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteKind {
    pub statement_type: String,
    pub unbounded: bool,
    pub destructive: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectQuery {
    pub kind: Option<String>,
    pub search: Option<String>,
    pub limit: u64,
    pub cursor: Option<String>,
}

pub trait Driver {
    fn read(
        &mut self,
        stmt: &Statement,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>>;
    fn classify(&self, stmt: &Statement) -> Result<WriteKind>;
    fn execute(&mut self, stmt: &Statement, deadline: Instant) -> Result<u64>;
    fn begin(&mut self, isolation: Option<Isolation>) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn in_transaction(&self) -> bool;
    fn objects(
        &mut self,
        query: &ObjectQuery,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>>;
    fn describe(&mut self, name: &str, deadline: Instant) -> Result<Value>;

    fn write_expecting(
        &mut self,
        stmt: &Statement,
        expected: u64,
        deadline: Instant,
    ) -> Result<u64> {
        let own_transaction = !self.in_transaction();
        if own_transaction {
            self.begin(None)?;
        }
        let affected = match self.execute(stmt, deadline) {
            Ok(affected) => affected,
            Err(error) => {
                if own_transaction {
                    let _ = self.rollback();
                }
                return Err(error);
            }
        };
        if affected != expected {
            if own_transaction {
                self.rollback()?;
            }
            return Err(row_count_mismatch(expected, affected));
        }
        if own_transaction {
            self.commit()?;
        }
        Ok(affected)
    }
}

pub fn row_count_mismatch(expected: u64, actual: u64) -> Error {
    let mut error = Error::new(
        Code::RowCountMismatch,
        format!("Expected {expected} affected rows, got {actual}. No change was kept."),
    )
    .with_hint("check the rows with query, then run exec again");
    error.actual_rows = Some(actual);
    error
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Sqlite,
    Postgres,
    Mysql,
    Mongo,
    Redis,
}

impl Backend {
    pub fn of(target: &str) -> Backend {
        let scheme = target
            .split_once("://")
            .map(|(scheme, _)| scheme.to_ascii_lowercase());
        match scheme.as_deref() {
            Some("postgres" | "postgresql") => Backend::Postgres,
            Some("mysql") => Backend::Mysql,
            Some("mongodb" | "mongodb+srv") => Backend::Mongo,
            Some("redis" | "rediss") => Backend::Redis,
            _ => Backend::Sqlite,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Backend::Sqlite => "sqlite",
            Backend::Postgres => "postgres",
            Backend::Mysql => "mysql",
            Backend::Mongo => "mongodb",
            Backend::Redis => "redis",
        }
    }
}

pub fn sqlite_path(target: &str) -> Result<PathBuf> {
    let path = target.strip_prefix("sqlite:").unwrap_or(target);
    if path.is_empty() || path == ":memory:" || path.contains("://") {
        return Err(Error::invalid(format!(
            "Unsupported database target \"{target}\"."
        )));
    }
    Ok(std::path::absolute(Path::new(path))?)
}

pub fn open(target: &str, read_only: bool, deadline: Instant) -> Result<Box<dyn Driver>> {
    Ok(match Backend::of(target) {
        Backend::Sqlite => Box::new(sqlite::SqliteDriver::open(
            &sqlite_path(target)?,
            read_only,
        )?),
        Backend::Postgres => Box::new(postgres::PostgresDriver::open(target, read_only, deadline)?),
        Backend::Mysql => Box::new(mysql::MysqlDriver::open(target, read_only, deadline)?),
        Backend::Mongo => Box::new(mongo::MongoDriver::open(target, deadline)?),
        Backend::Redis => Box::new(redis::RedisDriver::open(target, deadline)?),
    })
}

pub fn take_query_param(target: &str, name: &str) -> Result<(String, Option<String>)> {
    let mut url = url::Url::parse(target).map_err(connection_failed)?;
    let value = url
        .query_pairs()
        .filter(|(key, _)| key == name)
        .last()
        .map(|(_, value)| value.into_owned());
    let rest: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| key != name)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    if rest.is_empty() {
        url.set_query(None);
    } else {
        url.query_pairs_mut().clear().extend_pairs(rest);
    }
    Ok((url.to_string(), value))
}

pub fn remaining(deadline: Instant) -> Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(Error::new(
            Code::DeadlineExceeded,
            "The command passed its deadline.",
        ));
    }
    Ok(left)
}

pub fn connection_failed(error: impl std::fmt::Display) -> Error {
    Error::new(Code::ConnectionFailed, format!("Cannot connect: {error}"))
}

pub fn read_ca_file(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls::pki_types::pem::PemObject;
    let bytes = std::fs::read(path)
        .map_err(|error| connection_failed(format!("cannot read the CA file {path}: {error}")))?;
    let certs = rustls::pki_types::CertificateDer::pem_slice_iter(&bytes)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| connection_failed(format!("the CA file {path} is not PEM: {error}")))?;
    if certs.is_empty() {
        return Err(connection_failed(format!(
            "the CA file {path} holds no certificate"
        )));
    }
    Ok(certs)
}

pub fn read_only_error() -> Error {
    Error::new(
        Code::ReadOnly,
        "The database refused the change because the statement runs read-only.",
    )
    .with_hint("use exec on a read-write connection")
}

pub fn unsupported(message: impl Into<String>) -> Error {
    Error::new(Code::Unsupported, message)
}

pub const MISSING_COLUMN_HINT: &str = "stql describe TABLE";
pub const MISSING_TABLE_HINT: &str = "stql objects --search TEXT";

// Leaves out the common values (nullable, no default, not a key) to keep describe short.
pub fn column_json(
    name: &str,
    type_name: &str,
    nullable: bool,
    default: Option<&str>,
    primary_key: bool,
) -> Value {
    let mut column = json!({"name": name, "type": type_name});
    if !nullable {
        column["nullable"] = json!(false);
    }
    if let Some(default) = default {
        column["default"] = json!(default);
    }
    if primary_key {
        column["primary_key"] = json!(true);
    }
    column
}

pub fn like_pattern(search: &str) -> String {
    let escaped = search
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("%{escaped}%")
}

pub fn offset_cursor(cursor: Option<&str>) -> Result<u64> {
    let Some(cursor) = cursor else { return Ok(0) };
    cursor
        .parse()
        .map_err(|_| Error::invalid("The cursor must come from an earlier objects response."))
}

pub fn next_offset_cursor(offset: u64, limit: u64, returned: u64) -> Option<String> {
    (returned >= limit).then(|| (offset + limit).to_string())
}

pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

pub fn format_date(days: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

pub fn format_time(micros: i64) -> String {
    let seconds = micros.div_euclid(1_000_000);
    let fraction = micros.rem_euclid(1_000_000);
    let text = format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    );
    if fraction == 0 {
        return text;
    }
    let digits = format!("{fraction:06}");
    format!("{text}.{}", digits.trim_end_matches('0'))
}

pub fn format_timestamp(unix_micros: i64) -> String {
    let days = unix_micros.div_euclid(86_400_000_000);
    let micros = unix_micros.rem_euclid(86_400_000_000);
    format!("{}T{}", format_date(days), format_time(micros))
}

pub fn now_millis() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

pub fn iso_millis(unix_millis: i64) -> String {
    let days = unix_millis.div_euclid(86_400_000);
    let millis = unix_millis.rem_euclid(86_400_000);
    let seconds = millis / 1000;
    format!(
        "{}T{:02}:{:02}:{:02}.{:03}Z",
        format_date(days),
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
        millis % 1000
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_dates_across_eras() {
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(-1), "1969-12-31");
        assert_eq!(format_date(19_723), "2024-01-01");
        assert_eq!(format_date(11_016), "2000-02-29");
        assert_eq!(iso_millis(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
        assert_eq!(format_timestamp(-1), "1969-12-31T23:59:59.999999");
        assert_eq!(format_time(3_723_000_001), "01:02:03.000001");
        assert_eq!(format_time(3_723_500_000), "01:02:03.5");
    }
}
