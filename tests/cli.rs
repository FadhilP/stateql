mod common;

use std::io::Write;
use std::process::{Command, Output, Stdio};

use common::*;

fn stql(home: &Home, args: &[&str], stdin: Option<&str>) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_stql"))
        .args(args)
        .env("STQL_HOME", home.path.join("home"))
        .env("STQL_SESSION", "cli")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("stql runs");
    let mut input = child.stdin.take().expect("stdin");
    if let Some(text) = stdin {
        input.write_all(text.as_bytes()).expect("stdin write");
    }
    drop(input);
    child.wait_with_output().expect("stql output")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("utf8")
}

fn connected_home() -> Home {
    let home = Home::new();
    let db = home.file("app.db");
    sqlite_fixture(&db);
    let output = stql(
        &home,
        &["connect", &db.display().to_string(), "--read-write"],
        None,
    );
    assert_eq!(output.status.code(), Some(0), "{}", stdout(&output));
    home
}

#[test]
fn errors_print_one_json_line_and_map_to_exit_codes() {
    let home = Home::new();
    let output = stql(&home, &["query", "SELECT 1"], None);
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(
        stdout(&output),
        "{\"ok\":false,\"error\":{\"code\":\"NOT_CONNECTED\",\"message\":\"This session has no connection.\",\"retryable\":false,\"executed\":false,\"hint\":\"stql connect <target|profile>\"}}\n"
    );
    let output = stql(&home, &["rows"], None);
    assert_eq!(output.status.code(), Some(2));
    assert!(stdout(&output).starts_with("{\"ok\":false,\"error\":{\"code\":\"INVALID_COMMAND\""));
}

#[test]
fn parse_errors_drop_clap_tips_and_name_the_replacement_of_removed_names() {
    let home = Home::new();
    let cases = [
        (
            &["query", "--cache", "bypass", "SELECT 1"][..],
            "unexpected argument '--cache' found",
            "stql has no cache; remove --cache",
        ),
        (
            &["inspect", "columns", "users"][..],
            "unrecognized subcommand 'inspect'",
            "use stql describe NAME",
        ),
        (
            &["transaction", "begin"][..],
            "unrecognized subcommand 'transaction'",
            "use stql pipe --atomic",
        ),
        (
            &["describe", "plan", "--bogus"][..],
            "unexpected argument '--bogus' found",
            "stql help",
        ),
    ];
    for (args, message, hint) in cases {
        let output = stql(&home, args, None);
        assert_eq!(output.status.code(), Some(2));
        let line: serde_json::Value = serde_json::from_str(&stdout(&output)).unwrap();
        assert_eq!(
            line["error"]["message"],
            serde_json::json!(message),
            "{args:?}"
        );
        assert_eq!(line["error"]["hint"], serde_json::json!(hint), "{args:?}");
    }
}

#[test]
fn global_profile_connects_before_a_pipe_and_the_notice_stays_on_stdout() {
    let home = Home::new();
    let db = home.file("app.db");
    sqlite_fixture(&db);
    std::fs::create_dir_all(home.path.join("home")).unwrap();
    let old_store =
        rusqlite::Connection::open(home.path.join("home").join("state.sqlite")).unwrap();
    old_store
        .execute_batch(
            "CREATE TABLE profiles (name TEXT, target TEXT, secret_env TEXT, read_only INTEGER);
             INSERT INTO profiles VALUES ('old', NULL, 'OLD_DATABASE_URL', 1);",
        )
        .unwrap();
    drop(old_store);
    let output = stql(&home, &["query", "SELECT 1"], None);
    assert_eq!(output.stderr, b"");
    let line: serde_json::Value = serde_json::from_str(&stdout(&output)).unwrap();
    assert_eq!(line["error"]["code"], serde_json::json!("NOT_CONNECTED"));
    assert!(
        line["error"]["notice"]
            .as_str()
            .unwrap()
            .contains("migrated 1 profiles")
    );

    let profiles = home.path.join("home").join("profiles.toml");
    let mut toml = std::fs::read_to_string(&profiles).unwrap();
    toml.push_str(&format!(
        "[profiles.local]\nurl = '{}'\nread_only = true\n",
        db.display()
    ));
    std::fs::write(&profiles, toml).unwrap();
    let input = format!("{}\n", query_json("SELECT count(*) AS n FROM users"));
    let output = stql(&home, &["--profile", "local", "pipe"], Some(&input));
    assert_eq!(output.status.code(), Some(0), "{}", stdout(&output));
    assert_eq!(output.stderr, b"");
    let lines: Vec<String> = stdout(&output).lines().map(str::to_string).collect();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("\"rows\":[[5]]"), "{}", lines[0]);
    assert!(!lines[0].contains("notice"), "{}", lines[0]);
}

#[test]
fn query_prints_the_documented_shape() {
    let home = connected_home();
    let output = stql(
        &home,
        &[
            "query",
            "SELECT id, name FROM users WHERE id < ? ORDER BY id",
            "--param",
            "3",
        ],
        None,
    );
    assert_eq!(output.status.code(), Some(0));
    let line = stdout(&output);
    let handle = serde_json::from_str::<serde_json::Value>(&line).unwrap()["handle"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(handle.len(), 7, "a prefix and 6 random characters");
    assert_eq!(
        line.replace(&handle, "H"),
        "{\"ok\":true,\"handle\":\"H\",\"total\":2,\"columns\":[\"id\",\"name\"],\"types\":[\"integer\",\"text\"],\"rows\":[[1,\"Ada\"],[2,\"Grace\"]]}\n"
    );
    let text = stql(&home, &["rows", &handle, "--text"], None);
    assert!(
        stdout(&text).starts_with("id  name\n1   Ada\n2   Grace\n"),
        "{}",
        stdout(&text)
    );
}

#[test]
fn query_reads_the_statement_from_stdin_with_file_dash() {
    let home = connected_home();
    let output = stql(
        &home,
        &["query", "--file", "-"],
        Some("SELECT count(*) AS n FROM users"),
    );
    assert_eq!(output.status.code(), Some(0));
    assert!(stdout(&output).contains("\"rows\":[[5]]"));
}

#[test]
fn pipe_reads_json_lines_from_stdin() {
    let home = connected_home();
    let input = "{\"command\":\"query\",\"stmt\":\"SELECT 1 AS one\",\"as\":\"x\"}\n{\"command\":\"rows\",\"handle\":\"x\",\"limit\":0}\n";
    let output = stql(&home, &["pipe"], Some(input));
    assert_eq!(output.status.code(), Some(0));
    let lines: Vec<String> = stdout(&output).lines().map(str::to_string).collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[1].contains("\"name\":\"x\",\"total\":1"));
}

#[test]
fn a_commit_that_hangs_ends_at_the_watchdog_and_reads_as_unknown() {
    let home = connected_home();
    let reader = rusqlite::Connection::open(home.file("app.db")).unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM users;")
        .unwrap();
    let started = std::time::Instant::now();
    let line = "{\"command\":\"exec\",\"stmt\":\"INSERT INTO audit (note) VALUES ('x')\"}\n";
    let output = stql(
        &home,
        &["--timeout-ms", "300", "pipe", "--atomic"],
        Some(line),
    );
    let elapsed = started.elapsed();
    drop(reader);
    let lines: Vec<serde_json::Value> = stdout(&output)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(output.status.code(), Some(4), "{lines:?}");
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "the SQLite busy timeout is 5 s; the watchdog must end COMMIT first: {elapsed:?}"
    );
    assert_eq!(
        lines[1]["error"]["code"],
        serde_json::json!("DEADLINE_EXCEEDED")
    );
    assert_eq!(
        lines[1]["error"]["hint"],
        serde_json::json!("--timeout-ms is 300; raise it or narrow the query")
    );
    let handle = lines[0]["handle"].as_str().unwrap();
    let engine = home.engine();
    let mut session = engine.session("cli").unwrap();
    let receipt = ok(
        &mut session,
        &serde_json::json!({"command": "receipt", "handle": handle}).to_string(),
    );
    assert_eq!(receipt.status.as_deref(), Some("unknown"));
    assert_eq!(count(&home.file("app.db"), "audit"), 0);
}
