mod common;

use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use serde_json::{Value, json};
use stateql::{Code, PipeMode};

struct Redis {
    conn: redis::Connection,
    prefix: String,
}

impl Redis {
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    fn run(&mut self, args: &[&str]) -> redis::Value {
        let mut command = redis::cmd(args[0]);
        command.arg(&args[1..]);
        command.query(&mut self.conn).expect("redis command")
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        let pattern = format!("{}*", self.prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query(&mut self.conn)
            .unwrap_or_default();
        if !keys.is_empty() {
            let _ = redis::cmd("DEL").arg(&keys).query::<i64>(&mut self.conn);
        }
    }
}

fn redis_fixture() -> Option<Redis> {
    let url = env_url("STQL_TEST_REDIS_URL")?;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let prefix = format!(
        "stql:{}:{}:",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    let conn = redis::Client::open(url)
        .unwrap()
        .get_connection()
        .expect("redis connects");
    let mut fixture = Redis { conn, prefix };
    let (hash, zset, text) = (fixture.key("h"), fixture.key("z"), fixture.key("s"));
    fixture.run(&["HSET", &hash, "a", "1", "b", "2"]);
    fixture.run(&["ZADD", &zset, "1.5", "m", "2", "n"]);
    fixture.run(&["SET", &text, "hello"]);
    Some(fixture)
}

fn connect_env(read_write: bool) -> String {
    json!({"command": "connect", "env": "STQL_TEST_REDIS_URL", "read_write": read_write})
        .to_string()
}

fn redis_command(name: &str, args: &[&str]) -> String {
    json!({"command": name, "args": args}).to_string()
}

#[test]
fn reads_return_rows_and_scan_cursors() {
    let Some(mut fixture) = redis_fixture() else {
        return;
    };
    for index in 0..25 {
        let key = fixture.key(&format!("many:{index:02}"));
        fixture.run(&["SET", &key, "x"]);
    }
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(false));
    let hash = ok(
        &mut session,
        &query_json(&redis_command("HGETALL", &[&fixture.key("h")])),
    );
    assert_eq!(hash.columns.unwrap(), ["field", "value"]);
    assert_eq!(
        hash.rows,
        Some(vec![
            vec![json!("a"), json!("1")],
            vec![json!("b"), json!("2")]
        ])
    );
    let scored = ok(
        &mut session,
        &query_json(&redis_command(
            "ZRANGE",
            &[&fixture.key("z"), "0", "10", "WITHSCORES"],
        )),
    );
    assert_eq!(
        scored.rows,
        Some(vec![
            vec![json!("m"), json!("1.5")],
            vec![json!("n"), json!("2")]
        ])
    );
    let values = ok(
        &mut session,
        &query_json(&redis_command(
            "MGET",
            &[&fixture.key("s"), &fixture.key("none")],
        )),
    );
    assert_eq!(
        values.rows,
        Some(vec![
            vec![json!(0), json!("hello")],
            vec![json!(1), Value::Null]
        ])
    );

    let pattern = format!("{}many:*", fixture.prefix);
    let mut cursor = "0".to_string();
    let mut keys = Vec::new();
    for _ in 0..100 {
        let page = ok(
            &mut session,
            &query_json(&redis_command(
                "SCAN",
                &[&cursor, "MATCH", &pattern, "COUNT", "10"],
            )),
        );
        keys.extend(
            page.rows
                .unwrap()
                .into_iter()
                .map(|row| row[0].as_str().unwrap().to_string()),
        );
        let Some(next) = page.next_cursor else { break };
        cursor = next;
    }
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 25);

    let members: Vec<String> = (0..201).map(|index| index.to_string()).collect();
    let set = fixture.key("big");
    let mut sadd = vec!["SADD", set.as_str()];
    sadd.extend(members.iter().map(String::as_str));
    fixture.run(&sadd);
    let error = expect_code(
        &mut session,
        &query_json(&redis_command("SMEMBERS", &[&set])),
        Code::ResultTooLarge,
    );
    assert_eq!(error.hint.as_deref(), Some("use SSCAN"));
    expect_code(
        &mut session,
        &query_json(&redis_command("LRANGE", &[&set, "0", "200"])),
        Code::InvalidCommand,
    );
    expect_code(
        &mut session,
        &query_json(&redis_command("KEYS", &["*"])),
        Code::InvalidCommand,
    );
    expect_code(
        &mut session,
        &query_json(&redis_command("DEL", &[&set])),
        Code::ReadOnly,
    );
}

#[test]
fn writes_pass_the_gates_and_report_counts() {
    let Some(mut fixture) = redis_fixture() else {
        return;
    };
    let home = Home::new();
    let engine = home.engine();
    let mut reader = engine.session("ro").unwrap();
    ok(&mut reader, &connect_env(false));
    let text = fixture.key("s");
    expect_code(
        &mut reader,
        &exec_json(&redis_command("SET", &[&text, "x"]), json!({})),
        Code::ReadOnly,
    );

    let mut session = engine.session("rw").unwrap();
    ok(&mut session, &connect_env(true));
    let set = ok(
        &mut session,
        &exec_json(
            &redis_command("SET", &[&text, "changed", "EX", "60"]),
            json!({}),
        ),
    );
    assert_eq!(set.affected_rows, Some(1));
    let counter = fixture.key("counter");
    let incr = ok(
        &mut session,
        &exec_json(&redis_command("INCRBY", &[&counter, "5"]), json!({})),
    );
    assert_eq!(incr.affected_rows, Some(1));
    expect_code(
        &mut session,
        &exec_json(&redis_command("DEL", &[&text]), json!({})),
        Code::DestructiveOperation,
    );
    let deleted = ok(
        &mut session,
        &exec_json(
            &redis_command("DEL", &[&text, &counter]),
            json!({"allow_destructive": true}),
        ),
    );
    assert_eq!(deleted.affected_rows, Some(2));
    expect_code(
        &mut session,
        &exec_json(
            &redis_command("SET", &[&text, "x"]),
            json!({"expect_rows": 1}),
        ),
        Code::Unsupported,
    );
    expect_code(
        &mut session,
        &exec_json(&redis_command("GET", &[&text]), json!({})),
        Code::InvalidSql,
    );
    let input = format!(
        "{}\n",
        exec_json(&redis_command("SET", &[&text, "x"]), json!({}))
    );
    let mut output = Vec::new();
    let exit = session
        .pipe(input.as_bytes(), &mut output, PipeMode::Atomic(None))
        .unwrap();
    assert_eq!(exit, Code::Unsupported.exit_code());
    assert_eq!(fixture.run(&["EXISTS", &text]), redis::Value::Int(0));
}

#[test]
fn catalog_scans_keys_and_describes_values() {
    let Some(fixture) = redis_fixture() else {
        return;
    };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(false));
    let mut names = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..1000 {
        let mut request = json!({"command": "objects", "search": fixture.prefix, "limit": 200});
        if let Some(cursor) = &cursor {
            request["cursor"] = json!(cursor);
        }
        let page = ok(&mut session, &request.to_string());
        names.extend(
            page.rows
                .unwrap()
                .into_iter()
                .map(|row| row[1].as_str().unwrap().to_string()),
        );
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    names.sort();
    assert_eq!(
        names,
        [fixture.key("h"), fixture.key("s"), fixture.key("z")]
    );
    let hash = ok(
        &mut session,
        &json!({"command": "describe", "name": fixture.key("h")}).to_string(),
    );
    assert_eq!(
        hash.data["object"],
        json!({"name": fixture.key("h"), "kind": "key", "type": "hash", "ttl_ms": -1, "values": ["a", "1", "b", "2"], "truncated": false})
    );
    let text = ok(
        &mut session,
        &json!({"command": "describe", "name": fixture.key("s")}).to_string(),
    );
    assert_eq!(text.data["object"]["value"], json!("hello"));
    expect_code(
        &mut session,
        &json!({"command": "describe", "name": fixture.key("none")}).to_string(),
        Code::ResultNotFound,
    );
    expect_code(
        &mut session,
        r#"{"command":"objects","limit":500}"#,
        Code::InvalidCommand,
    );
}
