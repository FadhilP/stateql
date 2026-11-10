use std::path::PathBuf;
use std::time::{Duration, Instant};

use mysql::consts::{CapabilityFlags, ColumnFlags, ColumnType};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Params, SslOpts, Value as MyValue};
use serde_json::{Value, json};

use super::{Cell, Column, Driver, ObjectQuery, Statement, WriteKind, format_time, remaining};
use crate::command::Isolation;
use crate::error::{Code, Error, Executed, Result};
use crate::guard::{self, Dialect};
use crate::results::RowSink;

const BINARY_CHARSET: u16 = 63;
const STATEMENTS_WITH_IMPLICIT_COMMIT: [&str; 5] =
    ["create", "alter", "drop", "truncate", "unparsed"];

pub struct MysqlDriver {
    conn: Conn,
    read_only: bool,
    in_transaction: bool,
}

impl MysqlDriver {
    pub fn open(target: &str, read_only: bool, deadline: Instant) -> Result<MysqlDriver> {
        let (url, tls) = split_ssl_mode(target)?;
        let opts = Opts::from_url(&url).map_err(super::connection_failed)?;
        let tls = ssl_opts(tls, opts.get_ssl_opts())?;
        let timeout = remaining(deadline)?;
        let mut init = vec!["SET time_zone = '+00:00'"];
        if read_only {
            init.push("SET SESSION transaction_read_only = ON");
        }
        let builder = OptsBuilder::from_opts(opts)
            .tcp_connect_timeout(Some(timeout))
            // One second past the deadline, so the server-side max_execution_time fires first.
            .read_timeout(Some(timeout + Duration::from_secs(1)))
            .write_timeout(Some(timeout + Duration::from_secs(1)))
            .additional_capabilities(CapabilityFlags::CLIENT_FOUND_ROWS)
            .ssl_opts(tls)
            .init(init);
        let conn = Conn::new(builder).map_err(super::connection_failed)?;
        Ok(MysqlDriver {
            conn,
            read_only,
            in_transaction: false,
        })
    }

    fn read_only_scope<T>(
        &mut self,
        deadline: Instant,
        work: impl FnOnce(&mut Conn) -> Result<T>,
    ) -> Result<T> {
        let timeout = remaining(deadline)?.as_millis();
        let conn = &mut self.conn;
        conn.query_drop(format!("SET SESSION max_execution_time = {timeout}"))
            .map_err(read_error)?;
        // MySQL has no read-only savepoint, so a read inside pipe --atomic undoes its own changes.
        if self.in_transaction {
            conn.query_drop("SAVEPOINT stql_read").map_err(read_error)?;
            let result = finish_before(deadline, work(conn));
            let closed = conn
                .query_drop("ROLLBACK TO SAVEPOINT stql_read")
                .and_then(|()| conn.query_drop("RELEASE SAVEPOINT stql_read"))
                .map_err(read_error);
            let value = result?;
            closed?;
            return Ok(value);
        }
        conn.query_drop("SET SESSION transaction_read_only = ON")
            .map_err(read_error)?;
        conn.query_drop("START TRANSACTION READ ONLY")
            .map_err(read_error)?;
        let result = finish_before(deadline, work(conn));
        let mut closed = conn.query_drop("ROLLBACK").map_err(read_error);
        if !self.read_only && closed.is_ok() {
            closed = conn
                .query_drop("SET SESSION transaction_read_only = OFF")
                .map_err(read_error);
        }
        let value = result?;
        closed?;
        Ok(value)
    }

    fn text_rows(
        &mut self,
        deadline: Instant,
        sql: &str,
        params: Vec<MyValue>,
    ) -> Result<Vec<Vec<Option<String>>>> {
        self.read_only_scope(deadline, |conn| {
            let rows: Vec<mysql::Row> = conn.exec(sql, params).map_err(read_error)?;
            Ok(rows
                .into_iter()
                .map(|row| row.unwrap().into_iter().map(value_text).collect())
                .collect())
        })
    }
}

// The MySQL client meanings. A CA path is Some only when ssl-ca is set; else the webpki roots apply.
#[derive(Debug, PartialEq)]
enum Tls {
    Unset,
    Off,
    Unverified,
    VerifyCa(Option<String>),
    VerifyIdentity(Option<String>),
}

fn split_ssl_mode(target: &str) -> Result<(String, Tls)> {
    let (url, mode) = super::take_query_param(target, "ssl-mode")?;
    let (url, ca) = super::take_query_param(&url, "ssl-ca")?;
    let mode = mode.map(|mode| mode.to_ascii_uppercase());
    let tls = match mode.as_deref() {
        None if ca.is_some() => {
            return Err(Error::invalid(
                "ssl-ca needs ssl-mode=VERIFY_CA or VERIFY_IDENTITY.",
            ));
        }
        None => Tls::Unset,
        Some("DISABLED") => Tls::Off,
        // Like PostgreSQL require with a root CA: a given CA is always checked.
        Some("REQUIRED") if ca.is_some() => Tls::VerifyCa(ca),
        Some("REQUIRED") => Tls::Unverified,
        Some("VERIFY_CA") => Tls::VerifyCa(ca),
        Some("VERIFY_IDENTITY") => Tls::VerifyIdentity(ca),
        Some("PREFERRED") => {
            return Err(Error::invalid(
                "ssl-mode PREFERRED can fall back to plain TCP, so stql does not support it.",
            )
            .with_hint("use ssl-mode=DISABLED or ssl-mode=REQUIRED"));
        }
        Some(other) => {
            return Err(Error::invalid(format!(
                "ssl-mode \"{other}\" is not supported."
            )));
        }
    };
    Ok((url, tls))
}

// Starts from the crate's own URL options, but sets every check, so no URL option weakens a VERIFY mode.
fn ssl_opts(tls: Tls, from_url: Option<&SslOpts>) -> Result<Option<SslOpts>> {
    let base = from_url.cloned().unwrap_or_default();
    let (accept_invalid_certs, skip_domain_validation, ca) = match tls {
        Tls::Unset => return Ok(from_url.cloned()),
        Tls::Off => return Ok(None),
        Tls::Unverified => (true, true, None),
        Tls::VerifyCa(ca) => (false, true, ca),
        Tls::VerifyIdentity(ca) => (false, false, ca),
    };
    let mut opts = base
        .with_danger_accept_invalid_certs(accept_invalid_certs)
        .with_danger_skip_domain_validation(skip_domain_validation);
    if let Some(ca) = ca {
        super::read_ca_file(&ca)?;
        opts = opts.with_root_cert_path(Some(PathBuf::from(ca)));
    }
    Ok(Some(opts))
}

fn read_error(error: mysql::Error) -> Error {
    let mysql::Error::MySqlError(db) = &error else {
        return Error::query(error);
    };
    match db.code {
        1792 => super::read_only_error(),
        3024 => Error::new(
            Code::DeadlineExceeded,
            "The statement passed its deadline and was cancelled.",
        ),
        1044 | 1142 | 1143 | 1227 | 1370 => Error::new(Code::PermissionDenied, db.message.clone()),
        1054 => {
            Error::new(Code::InvalidSql, db.message.clone()).with_hint(super::MISSING_COLUMN_HINT)
        }
        1146 => {
            Error::new(Code::InvalidSql, db.message.clone()).with_hint(super::MISSING_TABLE_HINT)
        }
        1064 | 1149 | 1295 => Error::new(Code::InvalidSql, db.message.clone()),
        _ => Error::query(db.message.clone()),
    }
}

// max_execution_time can end SLEEP or BENCHMARK with a normal value, and a socket timeout is a
// generic I/O error. A read that ends past its deadline is reported as late in both cases.
fn finish_before<T>(deadline: Instant, result: Result<T>) -> Result<T> {
    let late = remaining(deadline).is_err();
    match result {
        Ok(_) if late => Err(Error::new(
            Code::DeadlineExceeded,
            "The statement passed its deadline.",
        )),
        Err(error) if late && error.code == Code::QueryFailed => Err(Error::new(
            Code::DeadlineExceeded,
            "The statement passed its deadline.",
        )),
        other => other,
    }
}

fn write_error(error: mysql::Error) -> Error {
    if matches!(error, mysql::Error::MySqlError(_)) {
        return read_error(error);
    }
    Error::query(error).with_executed(Executed::Unknown)
}

fn params(values: &[Value]) -> Params {
    if values.is_empty() {
        return Params::Empty;
    }
    let values = values
        .iter()
        .map(|value| match value {
            Value::Null => MyValue::NULL,
            Value::Bool(flag) => MyValue::Int(i64::from(*flag)),
            Value::Number(number) => match (number.as_i64(), number.as_u64()) {
                (Some(int), _) => MyValue::Int(int),
                (None, Some(unsigned)) => MyValue::UInt(unsigned),
                _ => MyValue::Double(number.as_f64().unwrap_or_default()),
            },
            Value::String(text) => MyValue::Bytes(text.clone().into_bytes()),
            other => MyValue::Bytes(other.to_string().into_bytes()),
        })
        .collect();
    Params::Positional(values)
}

fn check_param_count(statement: &mysql::Statement, values: &[Value]) -> Result<()> {
    let expected = usize::from(statement.num_params());
    if expected == values.len() {
        return Ok(());
    }
    Err(Error::invalid(format!(
        "The statement takes {expected} parameters, but {} were given.",
        values.len()
    )))
}

fn is_binary(column: &mysql::Column) -> bool {
    column.character_set() == BINARY_CHARSET
}

fn type_label(column: &mysql::Column) -> String {
    use ColumnType::*;
    let flags = column.flags();
    let name = match column.column_type() {
        MYSQL_TYPE_TINY => "tinyint",
        MYSQL_TYPE_SHORT => "smallint",
        MYSQL_TYPE_INT24 => "mediumint",
        MYSQL_TYPE_LONG => "int",
        MYSQL_TYPE_LONGLONG => "bigint",
        MYSQL_TYPE_FLOAT => "float",
        MYSQL_TYPE_DOUBLE => "double",
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => "decimal",
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => "date",
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => "time",
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => "datetime",
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => "timestamp",
        MYSQL_TYPE_YEAR => "year",
        MYSQL_TYPE_BIT => "bit",
        MYSQL_TYPE_JSON => "json",
        MYSQL_TYPE_GEOMETRY => "geometry",
        MYSQL_TYPE_VECTOR => "vector",
        MYSQL_TYPE_NULL => "null",
        _ if flags.contains(ColumnFlags::ENUM_FLAG) => "enum",
        _ if flags.contains(ColumnFlags::SET_FLAG) => "set",
        MYSQL_TYPE_STRING if is_binary(column) => "binary",
        MYSQL_TYPE_STRING => "char",
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING if is_binary(column) => "varbinary",
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => "varchar",
        _ if is_binary(column) => "blob",
        _ => "text",
    };
    let numeric = matches!(
        column.column_type(),
        MYSQL_TYPE_TINY
            | MYSQL_TYPE_SHORT
            | MYSQL_TYPE_INT24
            | MYSQL_TYPE_LONG
            | MYSQL_TYPE_LONGLONG
    );
    if numeric && flags.contains(ColumnFlags::UNSIGNED_FLAG) {
        return format!("{name} unsigned");
    }
    name.to_string()
}

fn column_of(column: &mysql::Column) -> Column {
    Column {
        name: column.name_str().into_owned(),
        type_name: type_label(column),
        json: column.column_type() == ColumnType::MYSQL_TYPE_JSON,
    }
}

fn day_micros(hours: u32, minutes: u8, seconds: u8, micros: u32) -> i64 {
    let seconds = (i64::from(hours) * 60 + i64::from(minutes)) * 60 + i64::from(seconds);
    seconds * 1_000_000 + i64::from(micros)
}

fn decode(column: &mysql::Column, value: MyValue) -> Cell {
    use ColumnType::*;
    match value {
        MyValue::NULL => Cell::Null,
        MyValue::Int(int) => Cell::Integer(int),
        MyValue::UInt(unsigned) => match i64::try_from(unsigned) {
            Ok(int) => Cell::Integer(int),
            Err(_) => Cell::Text(unsigned.to_string()),
        },
        // Through its decimal text, so FLOAT 1.1 stays 1.1 and does not become 1.100000023841858.
        MyValue::Float(float) => Cell::Real(float.to_string().parse().unwrap_or(f64::from(float))),
        MyValue::Double(double) => Cell::Real(double),
        MyValue::Date(year, month, day, hour, minute, second, micros) => {
            let date = format!("{year:04}-{month:02}-{day:02}");
            match column.column_type() {
                MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => Cell::Text(date),
                kind => {
                    let zone = if matches!(kind, MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2) {
                        "Z"
                    } else {
                        ""
                    };
                    let time = format_time(day_micros(u32::from(hour), minute, second, micros));
                    Cell::Text(format!("{date}T{time}{zone}"))
                }
            }
        }
        MyValue::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if negative { "-" } else { "" };
            let time = format_time(day_micros(
                days * 24 + u32::from(hours),
                minutes,
                seconds,
                micros,
            ));
            Cell::Text(format!("{sign}{time}"))
        }
        MyValue::Bytes(bytes) if column.column_type() == MYSQL_TYPE_BIT => bit_value(&bytes),
        MyValue::Bytes(bytes) if column.column_type() == MYSQL_TYPE_JSON => {
            Cell::Text(String::from_utf8_lossy(&bytes).into_owned())
        }
        MyValue::Bytes(bytes) if is_binary(column) && !is_numeric_text(column) => Cell::Blob(bytes),
        MyValue::Bytes(bytes) => match String::from_utf8(bytes) {
            Ok(text) => Cell::Text(text),
            Err(error) => Cell::Blob(error.into_bytes()),
        },
    }
}

fn is_numeric_text(column: &mysql::Column) -> bool {
    matches!(
        column.column_type(),
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL
    )
}

fn bit_value(bytes: &[u8]) -> Cell {
    let value = bytes
        .iter()
        .fold(0_u128, |sum, byte| (sum << 8) | u128::from(*byte));
    match i64::try_from(value) {
        Ok(int) => Cell::Integer(int),
        Err(_) => Cell::Text(value.to_string()),
    }
}

fn value_text(value: MyValue) -> Option<String> {
    match value {
        MyValue::NULL => None,
        MyValue::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
        MyValue::Int(int) => Some(int.to_string()),
        MyValue::UInt(unsigned) => Some(unsigned.to_string()),
        other => Some(other.as_sql(true)),
    }
}

fn read_into(conn: &mut Conn, sql: &str, values: &[Value], sink: &mut RowSink) -> Result<()> {
    let statement = conn.prep(sql).map_err(read_error)?;
    check_param_count(&statement, values)?;
    let mut result = conn
        .exec_iter(&statement, params(values))
        .map_err(read_error)?;
    let columns: Vec<mysql::Column> = result.columns().as_ref().to_vec();
    sink.set_columns(columns.iter().map(column_of).collect())?;
    for row in result.by_ref() {
        let row = row.map_err(read_error)?;
        let cells = row
            .unwrap()
            .into_iter()
            .zip(&columns)
            .map(|(value, column)| decode(column, value))
            .collect();
        sink.push(cells)?;
    }
    Ok(())
}

fn split_name(name: &str) -> (Option<String>, String) {
    match name.split_once('.') {
        Some((schema, table)) => (Some(schema.to_string()), table.to_string()),
        None => (None, name.to_string()),
    }
}

fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn text(value: Option<String>) -> MyValue {
    value.map_or(MyValue::NULL, |text| MyValue::Bytes(text.into_bytes()))
}

impl Driver for MysqlDriver {
    fn read(
        &mut self,
        stmt: &Statement,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        guard::check_read(Dialect::Mysql, &stmt.text)?;
        // Inside pipe --atomic MySQL cannot make one statement read-only, and a savepoint does not undo
        // MyISAM writes, so a read there must parse and contain no write.
        if self.in_transaction && guard::may_write(Dialect::Mysql, &stmt.text) {
            return Err(super::read_only_error());
        }
        self.read_only_scope(deadline, |conn| {
            read_into(conn, &stmt.text, &stmt.params, sink)
        })?;
        Ok(None)
    }

    fn classify(&self, stmt: &Statement) -> Result<WriteKind> {
        guard::classify(Dialect::Mysql, &stmt.text)
    }

    fn execute(&mut self, stmt: &Statement, deadline: Instant) -> Result<u64> {
        remaining(deadline)?;
        if self.in_transaction {
            let kind = self.classify(stmt)?;
            if STATEMENTS_WITH_IMPLICIT_COMMIT.contains(&kind.statement_type.as_str()) {
                return Err(super::unsupported(
                    "MySQL commits this statement at once, so it cannot run inside a transaction.",
                )
                .with_hint("run it with exec outside pipe --atomic and without --expect-rows"));
            }
        }
        let prepared = self.conn.prep(&stmt.text).map_err(read_error)?;
        check_param_count(&prepared, &stmt.params)?;
        self.conn
            .exec_drop(&prepared, params(&stmt.params))
            .map_err(write_error)?;
        Ok(self.conn.affected_rows())
    }

    fn begin(&mut self, isolation: Option<Isolation>) -> Result<()> {
        let level = match isolation {
            None => None,
            Some(Isolation::ReadCommitted) => Some("READ COMMITTED"),
            Some(Isolation::RepeatableRead) => Some("REPEATABLE READ"),
            Some(Isolation::Serializable) => Some("SERIALIZABLE"),
        };
        if let Some(level) = level {
            self.conn
                .query_drop(format!("SET TRANSACTION ISOLATION LEVEL {level}"))
                .map_err(read_error)?;
        }
        self.conn
            .query_drop("START TRANSACTION")
            .map_err(read_error)?;
        self.in_transaction = true;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.in_transaction = false;
        self.conn.query_drop("COMMIT").map_err(write_error)
    }

    fn rollback(&mut self) -> Result<()> {
        self.in_transaction = false;
        self.conn.query_drop("ROLLBACK").map_err(read_error)
    }

    fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    fn objects(
        &mut self,
        query: &ObjectQuery,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        if let Some(kind) = &query.kind
            && !["table", "view", "function", "trigger"].contains(&kind.as_str())
        {
            return Err(Error::invalid(
                "MySQL object kinds are table, view, function, and trigger.",
            ));
        }
        let offset = super::offset_cursor(query.cursor.as_deref())?;
        let sql = "SELECT kind, schema_name AS `schema`, name FROM (
              SELECT CASE WHEN TABLE_TYPE = 'VIEW' THEN 'view' ELSE 'table' END AS kind, TABLE_SCHEMA AS schema_name, TABLE_NAME AS name
              FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE()
              UNION ALL
              SELECT 'function', ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.routines
              WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'FUNCTION'
              UNION ALL
              SELECT 'trigger', TRIGGER_SCHEMA, TRIGGER_NAME FROM information_schema.triggers WHERE TRIGGER_SCHEMA = DATABASE()
            ) objects
            WHERE (? IS NULL OR kind = ?) AND (? IS NULL OR name LIKE ? ESCAPE '\\\\')
            ORDER BY schema_name, kind, name LIMIT ? OFFSET ?";
        let pattern = query.search.as_deref().map(super::like_pattern);
        let values = vec![
            json!(query.kind),
            json!(query.kind),
            json!(pattern),
            json!(pattern),
            json!(query.limit),
            json!(offset),
        ];
        self.read_only_scope(deadline, |conn| read_into(conn, sql, &values, sink))?;
        Ok(super::next_offset_cursor(offset, query.limit, sink.total()))
    }

    fn describe(&mut self, name: &str, deadline: Instant) -> Result<Value> {
        let (schema, table) = split_name(name);
        let found = self.text_rows(
            deadline,
            "SELECT TABLE_SCHEMA, TABLE_TYPE FROM information_schema.tables WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ?",
            vec![text(schema), text(Some(table.clone()))],
        )?;
        let Some([Some(schema), Some(kind)]) = found
            .into_iter()
            .next()
            .and_then(|row| <[_; 2]>::try_from(row).ok())
        else {
            return Err(
                Error::new(Code::ResultNotFound, format!("No object named \"{name}\"."))
                    .with_hint("use objects"),
            );
        };
        let key = || vec![text(Some(schema.clone())), text(Some(table.clone()))];
        let columns = self.text_rows(
            deadline,
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY FROM information_schema.columns
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            key(),
        )?;
        let indexes = self.text_rows(
            deadline,
            "SELECT INDEX_NAME, MIN(NON_UNIQUE), GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ')
             FROM information_schema.statistics WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? GROUP BY INDEX_NAME ORDER BY INDEX_NAME",
            key(),
        )?;
        let ddl = self.text_rows(
            deadline,
            &format!(
                "SHOW CREATE TABLE {}.{}",
                quote_identifier(&schema),
                quote_identifier(&table)
            ),
            Vec::new(),
        )?;
        Ok(json!({
            "name": table,
            "schema": schema,
            "kind": if kind == "VIEW" { "view" } else { "table" },
            "columns": columns.iter().map(|row| super::column_json(
                row[0].as_deref().unwrap_or_default(),
                row[1].as_deref().unwrap_or_default(),
                row[2].as_deref() == Some("YES"),
                row[3].as_deref(),
                row[4].as_deref() == Some("PRI"),
            )).collect::<Vec<_>>(),
            "indexes": indexes.iter().map(|row| {
                let mut index = json!({"name": row[0], "columns": row[2]});
                if row[1].as_deref() == Some("0") {
                    index["unique"] = json!(true);
                }
                index
            }).collect::<Vec<_>>(),
            "ddl": ddl.into_iter().next().and_then(|row| row.into_iter().nth(1).flatten()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssl_modes_follow_the_mysql_client() {
        let ca = || Some("/ca.pem".to_string());
        let cases = [
            ("", Tls::Unset),
            ("?ssl-mode=DISABLED", Tls::Off),
            ("?ssl-mode=disabled&ssl-ca=/ca.pem", Tls::Off),
            ("?ssl-mode=REQUIRED", Tls::Unverified),
            ("?ssl-mode=REQUIRED&ssl-ca=/ca.pem", Tls::VerifyCa(ca())),
            ("?ssl-mode=VERIFY_CA", Tls::VerifyCa(None)),
            ("?ssl-mode=verify_ca&ssl-ca=/ca.pem", Tls::VerifyCa(ca())),
            ("?ssl-mode=VERIFY_IDENTITY", Tls::VerifyIdentity(None)),
            (
                "?ssl-ca=/ca.pem&ssl-mode=VERIFY_IDENTITY",
                Tls::VerifyIdentity(ca()),
            ),
        ];
        for (query, tls) in cases {
            let target = format!("mysql://u@h/db{query}");
            assert_eq!(
                split_ssl_mode(&target).unwrap(),
                ("mysql://u@h/db".to_string(), tls),
                "{query}"
            );
        }
        for query in ["ssl-mode=PREFERRED", "ssl-mode=x", "ssl-ca=/ca.pem"] {
            let error = split_ssl_mode(&format!("mysql://u@h/db?{query}")).unwrap_err();
            assert_eq!(error.code, Code::InvalidCommand, "{query}");
        }
    }

    #[test]
    fn ssl_opts_set_every_check_and_read_the_ca_file_first() {
        let weakened = SslOpts::default().with_danger_accept_invalid_certs(true);
        let checks = |tls| {
            let opts = ssl_opts(tls, Some(&weakened)).unwrap().unwrap();
            (opts.accept_invalid_certs(), opts.skip_domain_validation())
        };
        assert_eq!(checks(Tls::Unverified), (true, true));
        assert_eq!(checks(Tls::VerifyCa(None)), (false, true));
        assert_eq!(checks(Tls::VerifyIdentity(None)), (false, false));
        assert!(ssl_opts(Tls::Off, Some(&weakened)).unwrap().is_none());
        assert!(ssl_opts(Tls::Unset, None).unwrap().is_none());
        let missing = std::env::temp_dir()
            .join("stql-no-such-ca.pem")
            .display()
            .to_string();
        let error = ssl_opts(Tls::VerifyIdentity(Some(missing)), None).unwrap_err();
        assert_eq!(error.code, Code::ConnectionFailed);
    }

    #[test]
    fn bit_values_and_dates_decode() {
        assert_eq!(bit_value(&[1, 0]), Cell::Integer(256));
        assert_eq!(bit_value(&[0xff; 8]), Cell::Text(u64::MAX.to_string()));
        assert_eq!(day_micros(838, 59, 59, 0), 3_020_399_000_000);
    }
}
