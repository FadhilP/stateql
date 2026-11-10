use std::time::Instant;

use redis::Value as RedisValue;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Cell, Column, Driver, ObjectQuery, Statement, WriteKind, remaining};
use crate::command::Isolation;
use crate::error::{Code, Error, Executed, Result};
use crate::results::RowSink;

const MAX_ARGUMENTS: usize = 100;
const MAX_ARGUMENT_BYTES: usize = 256 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_COLLECTION_VALUES: usize = 200;
const READ_COMMANDS: [&str; 25] = [
    "GET",
    "MGET",
    "TYPE",
    "EXISTS",
    "TTL",
    "PTTL",
    "HGET",
    "HMGET",
    "LRANGE",
    "SCAN",
    "HSCAN",
    "SSCAN",
    "ZSCAN",
    "HGETALL",
    "HLEN",
    "SMEMBERS",
    "SCARD",
    "SISMEMBER",
    "ZRANGE",
    "ZCARD",
    "ZSCORE",
    "LLEN",
    "STRLEN",
    "DBSIZE",
    "INFO",
];
const WRITE_COMMANDS: [&str; 19] = [
    "SET", "DEL", "UNLINK", "HSET", "HDEL", "LPUSH", "RPUSH", "SADD", "SREM", "ZADD", "ZREM",
    "EXPIRE", "PEXPIRE", "PERSIST", "INCR", "INCRBY", "DECR", "DECRBY", "HINCRBY",
];
const COUNTERS: [&str; 5] = ["INCR", "INCRBY", "DECR", "DECRBY", "HINCRBY"];
const DESTRUCTIVE: [&str; 5] = ["DEL", "UNLINK", "HDEL", "SREM", "ZREM"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCommand {
    command: String,
    #[serde(default)]
    args: Vec<String>,
}

struct RedisCommand {
    name: String,
    args: Vec<String>,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::invalid(message)
}

fn parse_command(stmt: &Statement) -> Result<RedisCommand> {
    if !stmt.params.is_empty() {
        return Err(invalid(
            "Redis commands take no --param or --params; put values in \"args\".",
        ));
    }
    let raw: RawCommand = serde_json::from_str(&stmt.text).map_err(|error| {
        invalid(format!(
            "A Redis command is {{\"command\":\"GET\",\"args\":[\"key\"]}}: {error}"
        ))
    })?;
    if raw.command.is_empty() || !raw.command.chars().all(|char| char.is_ascii_alphabetic()) {
        return Err(invalid("The Redis command name is not valid."));
    }
    if raw.args.iter().any(|arg| arg.contains('\0')) {
        return Err(invalid("Redis arguments cannot contain NUL bytes."));
    }
    let bytes = serde_json::to_string(&raw.args)
        .map_err(Error::internal)?
        .len();
    if raw.args.len() > MAX_ARGUMENTS + 4 || bytes > MAX_ARGUMENT_BYTES {
        return Err(invalid("The Redis arguments pass the size limits."));
    }
    Ok(RedisCommand {
        name: raw.command.to_ascii_uppercase(),
        args: raw.args,
    })
}

fn count(args: &[String], expected: usize) -> Result<()> {
    if args.len() == expected {
        return Ok(());
    }
    Err(invalid(format!(
        "This Redis command takes {expected} arguments."
    )))
}

fn range(args: &[String], minimum: usize, maximum: usize) -> Result<()> {
    if (minimum..=maximum).contains(&args.len()) {
        return Ok(());
    }
    Err(invalid(format!(
        "This Redis command takes {minimum}-{maximum} arguments."
    )))
}

fn integer(value: &str, name: &str) -> Result<i64> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    let parsed = value
        .parse::<i64>()
        .ok()
        .filter(|number| number.unsigned_abs() <= 9_007_199_254_740_991);
    match parsed {
        Some(number) if !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) => {
            Ok(number)
        }
        _ => Err(invalid(format!("Redis {name} needs an integer."))),
    }
}

fn positive(value: &str, name: &str) -> Result<()> {
    if integer(value, name)? < 1 {
        return Err(invalid(format!("Redis {name} needs a positive integer.")));
    }
    Ok(())
}

fn bounded_range(start: &str, stop: &str) -> Result<()> {
    let (Ok(start), Ok(stop)) = (start.parse::<i64>(), stop.parse::<i64>()) else {
        return Err(invalid("Redis ranges need integer bounds."));
    };
    if start < 0 || stop < start || stop - start >= MAX_COLLECTION_VALUES as i64 {
        return Err(invalid(format!(
            "A Redis range may ask for at most {MAX_COLLECTION_VALUES} elements from index 0 or later."
        )));
    }
    Ok(())
}

fn validate_scan(args: &[String], keyed: bool) -> Result<()> {
    let base = if keyed { 2 } else { 1 };
    range(args, base, base + 4)?;
    let cursor = &args[base - 1];
    if cursor.is_empty() || !cursor.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("The Redis SCAN cursor must be a number."));
    }
    for pair in args[base..].chunks(2) {
        let option = pair[0].to_ascii_uppercase();
        let value = pair.get(1).map(String::as_str).unwrap_or_default();
        let count_ok = value
            .parse::<usize>()
            .is_ok_and(|count| (1..=MAX_COLLECTION_VALUES).contains(&count));
        let valid = match option.as_str() {
            "MATCH" => !value.is_empty(),
            "COUNT" => count_ok,
            _ => false,
        };
        if !valid {
            return Err(invalid(format!(
                "Redis SCAN takes MATCH and COUNT (1-{MAX_COLLECTION_VALUES}) only."
            )));
        }
    }
    Ok(())
}

fn validate_set(args: &[String]) -> Result<()> {
    range(args, 2, 5)?;
    let mut expiry = false;
    let mut condition = false;
    let mut index = 2;
    while index < args.len() {
        let option = args[index].to_ascii_uppercase();
        if matches!(option.as_str(), "NX" | "XX") && !condition {
            condition = true;
        } else if matches!(option.as_str(), "EX" | "PX") && !expiry && index + 1 < args.len() {
            expiry = true;
            index += 1;
            positive(&args[index], &format!("SET {option}"))?;
        } else {
            return Err(invalid(
                "Redis SET takes one of EX or PX and one of NX or XX.",
            ));
        }
        index += 1;
    }
    Ok(())
}

fn validate_read(command: &RedisCommand) -> Result<()> {
    let args = &command.args;
    match command.name.as_str() {
        "GET" | "TYPE" | "TTL" | "PTTL" | "HGETALL" | "HLEN" | "SMEMBERS" | "SCARD" | "ZCARD"
        | "LLEN" | "STRLEN" => count(args, 1),
        "MGET" | "EXISTS" => range(args, 1, MAX_ARGUMENTS),
        "HGET" | "SISMEMBER" | "ZSCORE" => count(args, 2),
        "HMGET" => range(args, 2, MAX_ARGUMENTS + 1),
        "LRANGE" => count(args, 3).and_then(|()| bounded_range(&args[1], &args[2])),
        "ZRANGE" => {
            range(args, 3, 4)?;
            bounded_range(&args[1], &args[2])?;
            if args.len() == 4 && !args[3].eq_ignore_ascii_case("WITHSCORES") {
                return Err(invalid(
                    "Redis ZRANGE takes index ranges with an optional WITHSCORES.",
                ));
            }
            Ok(())
        }
        "DBSIZE" => count(args, 0),
        "INFO" => {
            range(args, 0, 1)?;
            if args
                .first()
                .is_some_and(|section| !section.chars().all(|char| char.is_ascii_alphabetic()))
            {
                return Err(invalid("The Redis INFO section is not valid."));
            }
            Ok(())
        }
        "SCAN" => validate_scan(args, false),
        _ => validate_scan(args, true),
    }
}

fn validate_write(command: &RedisCommand) -> Result<()> {
    let args = &command.args;
    let name = command.name.as_str();
    match name {
        "SET" => validate_set(args),
        "DEL" | "UNLINK" => range(args, 1, MAX_ARGUMENTS),
        "HSET" => {
            range(args, 3, MAX_ARGUMENTS + 1)?;
            if args.len().is_multiple_of(2) {
                return Err(invalid(
                    "Redis HSET takes a key, then field and value pairs.",
                ));
            }
            Ok(())
        }
        "EXPIRE" | "PEXPIRE" => count(args, 2).and_then(|()| positive(&args[1], name)),
        "PERSIST" | "INCR" | "DECR" => count(args, 1),
        "INCRBY" | "DECRBY" => count(args, 2).and_then(|()| integer(&args[1], name).map(drop)),
        "HINCRBY" => count(args, 3).and_then(|()| integer(&args[2], name).map(drop)),
        "ZADD" => {
            count(args, 3)?;
            if !args[1].parse::<f64>().is_ok_and(f64::is_finite) {
                return Err(invalid("The Redis ZADD score must be a finite number."));
            }
            Ok(())
        }
        _ => count(args, 2),
    }
}

fn read_command(stmt: &Statement) -> Result<RedisCommand> {
    let command = parse_command(stmt)?;
    if WRITE_COMMANDS.contains(&command.name.as_str()) {
        return Err(Error::new(
            Code::ReadOnly,
            format!("Redis {} is a write.", command.name),
        )
        .with_hint("use exec"));
    }
    if !READ_COMMANDS.contains(&command.name.as_str()) {
        return Err(invalid(format!(
            "Redis {} is not supported. Reads: {}.",
            command.name,
            READ_COMMANDS.join(", ")
        )));
    }
    validate_read(&command)?;
    Ok(command)
}

fn write_command(stmt: &Statement) -> Result<RedisCommand> {
    let command = parse_command(stmt)?;
    if READ_COMMANDS.contains(&command.name.as_str()) {
        return Err(Error::new(
            Code::InvalidSql,
            format!("Redis {} is a read.", command.name),
        )
        .with_hint("use query"));
    }
    if !WRITE_COMMANDS.contains(&command.name.as_str()) {
        return Err(invalid(format!(
            "Redis {} is not supported. Writes: {}.",
            command.name,
            WRITE_COMMANDS.join(", ")
        )));
    }
    validate_write(&command)?;
    Ok(command)
}

fn read_error(error: redis::RedisError) -> Error {
    if error.is_timeout() {
        return Error::new(
            Code::DeadlineExceeded,
            "The Redis command passed its deadline.",
        );
    }
    if error.is_connection_dropped() || error.is_connection_refusal() {
        return super::connection_failed(error);
    }
    Error::query(error)
}

fn write_error(error: redis::RedisError) -> Error {
    let unknown = error.is_io_error() || error.is_timeout() || error.is_connection_dropped();
    let mapped = read_error(error);
    if unknown {
        mapped.with_executed(Executed::Unknown)
    } else {
        mapped
    }
}

fn json_value(value: &RedisValue) -> Value {
    match value {
        RedisValue::Nil => Value::Null,
        RedisValue::Int(number) => json!(number),
        RedisValue::BulkString(bytes) => json!(String::from_utf8_lossy(bytes)),
        RedisValue::SimpleString(text) => json!(text),
        RedisValue::Okay => json!("OK"),
        RedisValue::Double(number) => json!(number),
        RedisValue::Boolean(flag) => json!(flag),
        RedisValue::Array(items) | RedisValue::Set(items) => {
            Value::Array(items.iter().map(json_value).collect())
        }
        RedisValue::Map(pairs) => Value::Array(
            pairs
                .iter()
                .map(|(key, value)| json!([json_value(key), json_value(value)]))
                .collect(),
        ),
        other => json!(format!("{other:?}")),
    }
}

fn cell(value: &RedisValue) -> Cell {
    match value {
        RedisValue::Nil => Cell::Null,
        RedisValue::Int(number) => Cell::Integer(*number),
        RedisValue::Boolean(flag) => Cell::Integer((*flag).into()),
        RedisValue::Double(number) => Cell::Real(*number),
        RedisValue::BulkString(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => Cell::Text(text.to_string()),
            Err(_) => Cell::Blob(bytes.clone()),
        },
        RedisValue::SimpleString(text) => Cell::Text(text.clone()),
        RedisValue::Okay => Cell::Text("OK".to_string()),
        other => Cell::Text(json_value(other).to_string()),
    }
}

fn type_name(value: &RedisValue) -> &'static str {
    match value {
        RedisValue::Nil => "null",
        RedisValue::Int(_) | RedisValue::Double(_) => "number",
        RedisValue::Boolean(_) => "boolean",
        RedisValue::Array(_) | RedisValue::Set(_) | RedisValue::Map(_) => "array",
        _ => "string",
    }
}

fn items(value: RedisValue) -> Vec<RedisValue> {
    match value {
        RedisValue::Array(items) | RedisValue::Set(items) => items,
        RedisValue::Map(pairs) => pairs
            .into_iter()
            .flat_map(|(key, value)| [key, value])
            .collect(),
        other => vec![other],
    }
}

fn store_rows(sink: &mut RowSink, names: &[&str], rows: Vec<Vec<RedisValue>>) -> Result<()> {
    let first = rows.first();
    let columns = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            Column::new(
                *name,
                first
                    .and_then(|row| row.get(index))
                    .map_or("string", type_name),
            )
        })
        .collect();
    sink.set_columns(columns)?;
    for row in rows {
        sink.push(row.iter().map(cell).collect())?;
    }
    Ok(())
}

fn pairs(values: Vec<RedisValue>) -> Vec<Vec<RedisValue>> {
    let mut rows = Vec::new();
    let mut values = values.into_iter();
    while let Some(first) = values.next() {
        rows.push(vec![first, values.next().unwrap_or(RedisValue::Nil)]);
    }
    rows
}

fn info_rows(text: &str) -> Vec<Vec<RedisValue>> {
    let mut section = String::new();
    let mut rows = Vec::new();
    for line in text.lines() {
        if let Some(name) = line.strip_prefix("# ") {
            section = name.trim().to_lowercase();
        }
        let Some((field, value)) = line
            .split_once(':')
            .filter(|(field, _)| !field.is_empty() && !line.starts_with('#'))
        else {
            continue;
        };
        let text = |text: &str| RedisValue::BulkString(text.as_bytes().to_vec());
        rows.push(vec![text(&section), text(field), text(value)]);
    }
    rows
}

pub struct RedisDriver {
    conn: redis::Connection,
}

impl RedisDriver {
    pub fn open(target: &str, deadline: Instant) -> Result<RedisDriver> {
        let client = redis::Client::open(target).map_err(super::connection_failed)?;
        let conn = client
            .get_connection_with_timeout(remaining(deadline)?)
            .map_err(super::connection_failed)?;
        let mut driver = RedisDriver { conn };
        driver.run(&["PING".to_string()], deadline, super::connection_failed)?;
        Ok(driver)
    }

    fn run(
        &mut self,
        parts: &[String],
        deadline: Instant,
        failed: fn(redis::RedisError) -> Error,
    ) -> Result<RedisValue> {
        let timeout = Some(remaining(deadline)?);
        self.conn.set_read_timeout(timeout).map_err(failed)?;
        self.conn.set_write_timeout(timeout).map_err(failed)?;
        let mut command = redis::cmd(&parts[0]);
        command.arg(&parts[1..]);
        command.query(&mut self.conn).map_err(failed)
    }

    fn query(&mut self, deadline: Instant, parts: &[&str]) -> Result<RedisValue> {
        let parts: Vec<String> = parts.iter().map(|part| part.to_string()).collect();
        self.run(&parts, deadline, read_error)
    }

    fn size_check(&mut self, command: &RedisCommand, deadline: Instant) -> Result<()> {
        let (size_command, alternative) = match command.name.as_str() {
            "HGETALL" => ("HLEN", "HSCAN"),
            "SMEMBERS" => ("SCARD", "SSCAN"),
            _ => return Ok(()),
        };
        let size = self.query(deadline, &[size_command, &command.args[0]])?;
        if matches!(size, RedisValue::Int(size) if size > MAX_COLLECTION_VALUES as i64) {
            return Err(Error::new(
                Code::ResultTooLarge,
                format!(
                    "Redis {} would return more than {MAX_COLLECTION_VALUES} values.",
                    command.name
                ),
            )
            .with_hint(format!("use {alternative}")));
        }
        Ok(())
    }
}

impl Driver for RedisDriver {
    fn read(
        &mut self,
        stmt: &Statement,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        let command = read_command(stmt)?;
        self.size_check(&command, deadline)?;
        let mut parts = vec![command.name.clone()];
        parts.extend(command.args.iter().cloned());
        let raw = self.run(&parts, deadline, read_error)?;
        if json_value(&raw).to_string().len() > MAX_RESPONSE_BYTES {
            return Err(Error::new(
                Code::ResultTooLarge,
                "The Redis response is larger than 1 MiB.",
            ));
        }
        let name = command.name.as_str();
        if name.ends_with("SCAN") {
            let mut parts = items(raw).into_iter();
            let cursor = parts
                .next()
                .map(|cursor| json_value(&cursor))
                .unwrap_or_default();
            let cursor = cursor.as_str().unwrap_or("0").to_string();
            let values = parts.next().map(items).unwrap_or_default();
            match name {
                "SCAN" => store_rows(
                    sink,
                    &["key"],
                    values.into_iter().map(|value| vec![value]).collect(),
                )?,
                "SSCAN" => store_rows(
                    sink,
                    &["value"],
                    values.into_iter().map(|value| vec![value]).collect(),
                )?,
                "HSCAN" => store_rows(sink, &["field", "value"], pairs(values))?,
                _ => store_rows(sink, &["value", "score"], pairs(values))?,
            }
            return Ok((cursor != "0").then_some(cursor));
        }
        match (name, raw) {
            ("HGETALL", raw) => store_rows(sink, &["field", "value"], pairs(items(raw)))?,
            ("ZRANGE", raw) if command.args.len() == 4 => {
                store_rows(sink, &["value", "score"], pairs(items(raw)))?
            }
            ("INFO", RedisValue::BulkString(bytes)) => store_rows(
                sink,
                &["section", "field", "value"],
                info_rows(&String::from_utf8_lossy(&bytes)),
            )?,
            (_, RedisValue::Array(values) | RedisValue::Set(values)) => {
                let rows = values
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| vec![RedisValue::Int(index as i64), value])
                    .collect();
                store_rows(sink, &["index", "value"], rows)?
            }
            (_, raw) => store_rows(sink, &["value"], vec![vec![raw]])?,
        }
        Ok(None)
    }

    fn classify(&self, stmt: &Statement) -> Result<WriteKind> {
        let command = write_command(stmt)?;
        Ok(WriteKind {
            statement_type: command.name.to_lowercase(),
            unbounded: false,
            destructive: DESTRUCTIVE.contains(&command.name.as_str()),
        })
    }

    fn execute(&mut self, stmt: &Statement, deadline: Instant) -> Result<u64> {
        let command = write_command(stmt)?;
        let mut parts = vec![command.name.clone()];
        parts.extend(command.args.iter().cloned());
        let result = self.run(&parts, deadline, write_error)?;
        // Counters return the new value, not a count.
        let affected = match result {
            _ if COUNTERS.contains(&command.name.as_str()) => 1,
            RedisValue::Int(count) => count.max(0) as u64,
            RedisValue::Okay => 1,
            _ => 0,
        };
        Ok(affected)
    }

    fn begin(&mut self, _isolation: Option<Isolation>) -> Result<()> {
        Err(super::unsupported(
            "Redis does not run inside pipe --atomic.",
        ))
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }

    fn in_transaction(&self) -> bool {
        false
    }

    fn objects(
        &mut self,
        query: &ObjectQuery,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        if query.kind.as_deref().is_some_and(|kind| kind != "key") {
            return Err(invalid("The Redis object kind is key."));
        }
        if !(1..=MAX_COLLECTION_VALUES as u64).contains(&query.limit) {
            return Err(invalid(format!(
                "Redis objects takes --limit 1-{MAX_COLLECTION_VALUES}."
            )));
        }
        let cursor = query.cursor.clone().unwrap_or_else(|| "0".to_string());
        if cursor.is_empty() || !cursor.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid(
                "The cursor must come from an earlier objects response.",
            ));
        }
        let mut parts = vec!["SCAN".to_string(), cursor];
        if let Some(search) = &query.search {
            let escaped: String = search
                .chars()
                .flat_map(|char| {
                    if "\\*?[]".contains(char) {
                        vec!['\\', char]
                    } else {
                        vec![char]
                    }
                })
                .collect();
            parts.extend(["MATCH".to_string(), format!("*{escaped}*")]);
        }
        parts.extend(["COUNT".to_string(), query.limit.to_string()]);
        let mut raw = items(self.run(&parts, deadline, read_error)?).into_iter();
        let next = raw
            .next()
            .map(|cursor| json_value(&cursor))
            .unwrap_or_default();
        let keys = raw.next().map(items).unwrap_or_default();
        sink.set_columns(vec![
            Column::new("kind", "string"),
            Column::new("name", "string"),
        ])?;
        for key in keys {
            sink.push(vec![Cell::Text("key".to_string()), cell(&key)])?;
        }
        let next = next.as_str().unwrap_or("0").to_string();
        Ok((next != "0").then_some(next))
    }

    fn describe(&mut self, name: &str, deadline: Instant) -> Result<Value> {
        let kind = json_value(&self.query(deadline, &["TYPE", name])?);
        let kind = kind.as_str().unwrap_or("none").to_string();
        if kind == "none" {
            return Err(
                Error::new(Code::ResultNotFound, format!("No key named \"{name}\"."))
                    .with_hint("use objects"),
            );
        }
        let ttl = json_value(&self.query(deadline, &["PTTL", name])?);
        let mut description = json!({"name": name, "kind": "key", "type": kind, "ttl_ms": ttl});
        let scan = |driver: &mut RedisDriver, command: &str| -> Result<Value> {
            let mut raw =
                items(driver.query(deadline, &[command, name, "0", "COUNT", "100"])?).into_iter();
            let cursor = raw
                .next()
                .map(|cursor| json_value(&cursor))
                .unwrap_or_default();
            let more = cursor.as_str().is_some_and(|cursor| cursor != "0");
            Ok(json!({"values": raw.next().map(|values| json_value(&values)), "truncated": more}))
        };
        let detail = match kind.as_str() {
            "string" => {
                let length = json_value(&self.query(deadline, &["STRLEN", name])?);
                if length
                    .as_u64()
                    .is_some_and(|length| length as usize > MAX_RESPONSE_BYTES)
                {
                    json!({"length": length, "value_omitted": true})
                } else {
                    json!({"length": length, "value": json_value(&self.query(deadline, &["GET", name])?)})
                }
            }
            "list" => {
                let length = json_value(&self.query(deadline, &["LLEN", name])?);
                let values = json_value(&self.query(deadline, &["LRANGE", name, "0", "99"])?);
                json!({"length": length, "values": values, "truncated": length.as_u64().is_some_and(|length| length > 100)})
            }
            "hash" => scan(self, "HSCAN")?,
            "set" => scan(self, "SSCAN")?,
            "zset" => scan(self, "ZSCAN")?,
            _ => json!({"value_omitted": true}),
        };
        if let (Value::Object(target), Value::Object(extra)) = (&mut description, detail) {
            target.extend(extra);
        }
        if description.to_string().len() > MAX_RESPONSE_BYTES {
            return Err(Error::new(
                Code::ResultTooLarge,
                "The Redis value is larger than 1 MiB.",
            ));
        }
        Ok(description)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statement(text: &str) -> Statement {
        Statement {
            text: text.to_string(),
            params: Vec::new(),
        }
    }

    #[test]
    fn commands_follow_the_ts_allowlists() {
        let read = |text: &str| read_command(&statement(text)).err().map(|error| error.code);
        let write = |text: &str| {
            write_command(&statement(text))
                .err()
                .map(|error| error.code)
        };
        assert_eq!(read(r#"{"command":"get","args":["a"]}"#), None);
        assert_eq!(read(r#"{"command":"LRANGE","args":["l","0","199"]}"#), None);
        assert_eq!(
            read(r#"{"command":"LRANGE","args":["l","0","200"]}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            read(r#"{"command":"SCAN","args":["0","COUNT","500"]}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            read(r#"{"command":"KEYS","args":["*"]}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            read(r#"{"command":"SET","args":["a","1"]}"#),
            Some(Code::ReadOnly)
        );
        assert_eq!(
            read(r#"{"command":"GET","args":["a"],"db":1}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            write(r#"{"command":"SET","args":["a","1","EX","10","NX"]}"#),
            None
        );
        assert_eq!(
            write(r#"{"command":"SET","args":["a","1","EX","0"]}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            write(r#"{"command":"HSET","args":["h","f"]}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            write(r#"{"command":"FLUSHALL"}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            write(r#"{"command":"GET","args":["a"]}"#),
            Some(Code::InvalidSql)
        );
        assert_eq!(
            write(r#"{"command":"ZADD","args":["z","inf","m"]}"#),
            Some(Code::InvalidCommand)
        );
    }
}
