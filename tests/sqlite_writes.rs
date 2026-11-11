mod common;

use std::process::{Command as Process, Stdio};
use std::time::{Duration, Instant};

use common::*;
use serde_json::{Value, json};
use stateql::{Code, Executed, PipeMode};

fn setup() -> (Home, String) {
    let home = Home::new();
    let db = home.file("app.db");
    sqlite_fixture(&db);
    let target = format!("sqlite:{}", db.display());
    (home, target)
}

fn pipe_lines(
    session: &mut stateql::Session<'_>,
    lines: &[String],
    mode: PipeMode,
) -> (u8, Vec<Value>) {
    let input = lines.join("\n");
    let mut output = Vec::new();
    let exit = session.pipe(input.as_bytes(), &mut output, mode).unwrap();
    let values = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (exit, values)
}

#[test]
fn exec_commits_and_receipt_reports_it() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let written = ok(
        &mut session,
        &exec_json(
            "INSERT INTO audit (note) VALUES (?)",
            json!({"param": ["hello"]}),
        ),
    );
    assert_eq!(
        (written.status.as_deref(), written.affected_rows),
        (Some("committed"), Some(1))
    );
    assert_eq!(count(&home.file("app.db"), "audit"), 1);
    let receipt = ok(
        &mut session,
        &json!({"command":"receipt","handle":written.handle}).to_string(),
    );
    assert_eq!(receipt.status.as_deref(), Some("committed"));
    assert_eq!(receipt.statement_type.as_deref(), Some("insert"));
    assert_eq!(
        receipt.data["stmt"],
        json!("INSERT INTO audit (note) VALUES (?)")
    );

    ok(
        &mut session,
        &exec_json(
            "INSERT INTO audit (note) VALUES (?)",
            json!({"param": ["hello"]}),
        ),
    );
    assert_eq!(
        count(&home.file("app.db"), "audit"),
        2,
        "a committed identical write does not block"
    );
}

#[test]
fn read_only_sessions_and_gates_stop_writes_before_they_run() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut reader = engine.session("r").unwrap();
    ok(&mut reader, &connect_json(&target, false));
    let error = expect_code(
        &mut reader,
        &exec_json("INSERT INTO audit (note) VALUES ('x')", json!({})),
        Code::ReadOnly,
    );
    assert_eq!(error.executed, Executed::No);

    let mut writer = engine.session("w").unwrap();
    ok(&mut writer, &connect_json(&target, true));
    let cases = [
        (
            "DELETE FROM users WHERE id = 1",
            json!({}),
            Code::DestructiveOperation,
        ),
        (
            "UPDATE users SET name = 'x'",
            json!({}),
            Code::UnboundedMutation,
        ),
        (
            "DELETE FROM users",
            json!({"allow_destructive": true}),
            Code::UnboundedMutation,
        ),
        ("DROP TABLE users", json!({}), Code::DestructiveOperation),
        (
            "CREATE VIRTUAL TABLE v USING fts5(x) WITH MAGIC",
            json!({"allow_destructive": true}),
            Code::UnboundedMutation,
        ),
        ("SELECT * FROM users", json!({}), Code::InvalidSql),
        ("VACUUM", json!({}), Code::InvalidSql),
        (
            "INSERT INTO audit (note) VALUES ('a'); DELETE FROM users",
            json!({}),
            Code::InvalidSql,
        ),
    ];
    for (sql, flags, code) in cases {
        expect_code(&mut writer, &exec_json(sql, flags), code);
    }
    assert_eq!((count(&db, "users"), count(&db, "audit")), (5, 0));
    let status = ok(&mut writer, r#"{"command":"status"}"#);
    assert_eq!(
        status.data["writes"],
        json!([]),
        "gated writes leave no operation rows"
    );

    let deleted = ok(
        &mut writer,
        &exec_json(
            "DELETE FROM users WHERE id = 1",
            json!({"allow_destructive": true}),
        ),
    );
    assert_eq!(deleted.affected_rows, Some(1));
}

#[test]
fn idempotency_key_returns_the_old_receipt_or_a_conflict() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let insert = exec_json(
        "INSERT INTO audit (note) VALUES ('once')",
        json!({"idempotency_key": "k1"}),
    );
    let first = ok(&mut session, &insert);
    let second = ok(&mut session, &insert);
    assert_eq!(first.handle, second.handle);
    assert_eq!(count(&home.file("app.db"), "audit"), 1);
    let error = expect_code(
        &mut session,
        &exec_json(
            "INSERT INTO audit (note) VALUES ('other')",
            json!({"idempotency_key": "k1"}),
        ),
        Code::IdempotencyConflict,
    );
    assert_eq!(error.executed, Executed::No);
}

#[test]
fn expect_rows_commits_on_a_match_and_rolls_back_on_a_mismatch() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let matched = ok(
        &mut session,
        &exec_json(
            "UPDATE users SET status = 'gone' WHERE id = 1",
            json!({"expect_rows": 1}),
        ),
    );
    assert_eq!(matched.status.as_deref(), Some("committed"));

    let error = expect_code(
        &mut session,
        &exec_json(
            "UPDATE users SET status = 'x' WHERE status = 'active'",
            json!({"expect_rows": 1}),
        ),
        Code::RowCountMismatch,
    );
    assert_eq!((error.actual_rows, error.executed), (Some(2), Executed::No));
    let conn = rusqlite::Connection::open(&db).unwrap();
    let changed: i64 = conn
        .query_row("SELECT count(*) FROM users WHERE status = 'x'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(changed, 0);
    let status = ok(&mut session, r#"{"command":"status"}"#);
    assert_eq!(status.data["writes"][0]["status"], json!("rolled_back"));
}

#[test]
fn killed_writer_leaves_an_unknown_outcome_that_blocks_until_replay() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let stql_home = home.path.join("home");
    let engine = home.engine();
    let mut session = engine.session("cli").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let slow = "INSERT INTO audit (note) SELECT hex(randomblob(64)) FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 50000000) SELECT x FROM c)";
    let mut child = Process::new(env!("CARGO_BIN_EXE_stql"))
        .args(["exec", slow, "--timeout-ms", "60000"])
        .env("STQL_HOME", &stql_home)
        .env("STQL_SESSION", "cli")
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let store = rusqlite::Connection::open(stql_home.join("stql.db")).unwrap();
    let started = Instant::now();
    while store
        .query_row("SELECT count(*) FROM operations", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
        == 0
    {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the write never started"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(300));
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(count(&db, "audit"), 0);

    // The killed process cannot record its end; the outcome becomes unknown once deadline_at passes.
    store
        .execute(
            "UPDATE operations SET deadline_at = '2000-01-01T00:00:00.000Z'",
            [],
        )
        .unwrap();
    let error = expect_code(
        &mut session,
        &exec_json(slow, json!({})),
        Code::OutcomeUnknown,
    );
    let previous = error.previous_operation.expect("previous operation");
    let receipt = ok(
        &mut session,
        &json!({"command":"receipt","handle":previous}).to_string(),
    );
    assert_eq!(
        (receipt.status.as_deref(), &receipt.data["executed"]),
        (Some("unknown"), &json!("unknown"))
    );

    let small = "INSERT INTO audit (note) VALUES ('after')";
    ok(&mut session, &exec_json(small, json!({})));
    session.set_timeout(Duration::from_millis(500));
    let replayed = session.run(cmd(&exec_json(slow, json!({"replay": true}))));
    let error = replayed.expect_err("the slow write passes its deadline");
    assert_eq!(
        (error.code, error.executed),
        (Code::DeadlineExceeded, Executed::No)
    );
    assert_eq!(
        error.hint.as_deref(),
        Some("--timeout-ms is 500; raise it or narrow the query")
    );
    assert_eq!(count(&db, "audit"), 1);
}

#[test]
fn atomic_pipe_commits_together_and_reads_its_own_writes() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let lines = [
        exec_json("INSERT INTO audit (note) VALUES ('a')", json!({})),
        exec_json(
            "INSERT INTO audit (note) VALUES ('b')",
            json!({"expect_rows": 1}),
        ),
        query_json("SELECT count(*) AS n FROM audit"),
    ];
    let (exit, output) = pipe_lines(&mut session, &lines, PipeMode::Atomic(None));
    assert_eq!(exit, 0);
    assert_eq!(output[0]["status"], json!("pending"));
    assert_eq!(output[2]["rows"], json!([[2]]));
    assert_eq!(output[3]["status"], json!("committed"));
    assert_eq!(count(&db, "audit"), 2);
    let receipt = ok(
        &mut session,
        &json!({"command":"receipt","handle":output[0]["handle"]}).to_string(),
    );
    assert_eq!(receipt.status.as_deref(), Some("committed"));
}

#[test]
fn atomic_pipe_rolls_back_everything_on_any_error() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    for failing in [
        exec_json("INSERT INTO missing (x) VALUES (1)", json!({})),
        exec_json(
            "UPDATE users SET name = 'x' WHERE id < 3",
            json!({"expect_rows": 1}),
        ),
        exec_json("DELETE FROM users WHERE id = 1", json!({})),
        r#"{"command":"connect","target":"other.db"}"#.to_string(),
    ] {
        let lines = [
            exec_json("INSERT INTO audit (note) VALUES ('a')", json!({})),
            failing.clone(),
            query_json("SELECT 1"),
        ];
        let (exit, output) = pipe_lines(&mut session, &lines, PipeMode::Atomic(None));
        assert_ne!(exit, 0, "{failing}");
        assert_eq!(output.len(), 2, "{failing}: the pipe stops at the error");
        assert_eq!(
            (count(&db, "audit"), count(&db, "users")),
            (0, 5),
            "{failing}"
        );
        let receipt = ok(
            &mut session,
            &json!({"command":"receipt","handle":output[0]["handle"]}).to_string(),
        );
        assert_eq!(receipt.status.as_deref(), Some("rolled_back"), "{failing}");
    }
    let names: i64 = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row("SELECT count(*) FROM users WHERE name = 'x'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(names, 0);
}

#[test]
fn parallel_sessions_write_results_and_operations() {
    let (home, target) = setup();
    let home_path = home.path.join("home");
    let threads: Vec<_> = (0..6)
        .map(|index| {
            let home_path = home_path.clone();
            let target = target.clone();
            std::thread::spawn(move || {
                let engine =
                    stateql::Engine::open_with_keyring_service(&home_path, KEYRING_SERVICE)
                        .unwrap();
                let mut session = engine.session(&format!("s{index}")).unwrap();
                ok(&mut session, &connect_json(&target, true));
                for round in 0..5 {
                    ok(&mut session, &query_json("SELECT * FROM users"));
                    ok(
                        &mut session,
                        &exec_json(
                            "INSERT INTO audit (note) VALUES (?)",
                            json!({"param": [format!("{index}-{round}")]}),
                        ),
                    );
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("worker finished");
    }
    assert_eq!(count(&home.file("app.db"), "audit"), 30);
    let store = rusqlite::Connection::open(home_path.join("stql.db")).unwrap();
    let (results, committed): (i64, i64) = store
        .query_row(
            "SELECT (SELECT count(*) FROM results), (SELECT count(*) FROM operations WHERE status = 'committed')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((results, committed), (30, 30));
}

#[test]
fn gate_errors_exit_with_code_five_and_the_documented_json() {
    let (home, target) = setup();
    let run = |args: &[&str]| {
        Process::new(env!("CARGO_BIN_EXE_stql"))
            .args(args)
            .env("STQL_HOME", home.path.join("home"))
            .env("STQL_SESSION", "cli")
            .output()
            .unwrap()
    };
    assert_eq!(
        run(&["connect", &target, "--read-write"]).status.code(),
        Some(0)
    );
    let output = run(&["exec", "DELETE FROM users WHERE id = 1"]);
    assert_eq!(output.status.code(), Some(5));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{\"ok\":false,\"error\":{\"code\":\"DESTRUCTIVE_OPERATION\",\"message\":\"This delete can remove or overwrite data.\",\"retryable\":false,\"executed\":false,\"hint\":\"--allow-destructive\"}}\n"
    );
    let output = run(&[
        "exec",
        "UPDATE users SET name = 'x' WHERE id = 1",
        "--expect-rows",
        "2",
    ]);
    assert_eq!(output.status.code(), Some(7));
}

#[test]
fn atomic_pipe_rolls_back_when_stdin_breaks() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let mut input = exec_json("INSERT INTO audit (note) VALUES ('a')", json!({})).into_bytes();
    input.extend(b"\n\xff\n");
    let mut output = Vec::new();
    let exit = session
        .pipe(input.as_slice(), &mut output, PipeMode::Atomic(None))
        .unwrap();
    let lines: Vec<Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_ne!(exit, 0);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[1]["ok"], false);
    assert_eq!(count(&db, "audit"), 0);
    let receipt = ok(
        &mut session,
        &json!({"command":"receipt","handle":lines[0]["handle"]}).to_string(),
    );
    assert_eq!(receipt.status.as_deref(), Some("rolled_back"));
}

fn store(home: &Home) -> rusqlite::Connection {
    rusqlite::Connection::open(home.path.join("home").join("stql.db")).unwrap()
}

fn make_unknown(home: &Home) {
    store(home)
        .execute(
            "UPDATE operations SET status = 'pending', deadline_at = '2000-01-01T00:00:00.000Z'",
            [],
        )
        .unwrap();
}

#[test]
fn atomic_pipe_runs_a_repeated_statement_and_prints_each_error_once() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let insert = exec_json("INSERT INTO audit (note) VALUES ('x')", json!({}));
    let (exit, output) = pipe_lines(
        &mut session,
        &[insert.clone(), insert.clone()],
        PipeMode::Atomic(None),
    );
    assert_eq!(exit, 0, "{output:?}");
    assert_eq!(output.len(), 3);
    assert_eq!(output[2]["status"], json!("committed"));
    assert_eq!(count(&db, "audit"), 2);

    make_unknown(&home);
    let (exit, output) = pipe_lines(&mut session, &[insert], PipeMode::Atomic(None));
    assert_eq!(exit, Code::OutcomeUnknown.exit_code());
    assert_eq!(output.len(), 1, "{output:?}");
    assert_eq!(output[0]["error"]["code"], json!("OUTCOME_UNKNOWN"));
    assert_eq!(count(&db, "audit"), 2);
}

#[test]
fn ddl_after_dml_reports_no_affected_rows() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let lines = [
        exec_json("UPDATE users SET status = status WHERE id < 3", json!({})),
        exec_json("CREATE TABLE z (a)", json!({"expect_rows": 0})),
    ];
    let (exit, output) = pipe_lines(&mut session, &lines, PipeMode::StopOnError);
    assert_eq!(exit, 0, "{output:?}");
    assert_eq!(
        (&output[0]["affected_rows"], &output[1]["affected_rows"]),
        (&json!(2), &json!(0))
    );
}

#[test]
fn idempotency_key_runs_again_only_when_nothing_was_kept() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    let insert = exec_json(
        "INSERT INTO late (v) VALUES (1)",
        json!({"idempotency_key": "k1"}),
    );
    let first = fail(&mut session, &insert);
    let again = fail(&mut session, &insert);
    assert_eq!(again.code, first.code, "a failed write is not a success");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("CREATE TABLE late (v)")
        .unwrap();
    let written = ok(&mut session, &insert);
    assert_eq!(
        (written.status.as_deref(), written.affected_rows),
        (Some("committed"), Some(1))
    );
    let receipt = ok(&mut session, &insert);
    assert_eq!(receipt.handle, written.handle);
    assert_eq!(count(&db, "late"), 1);

    make_unknown(&home);
    let error = expect_code(&mut session, &insert, Code::OutcomeUnknown);
    assert_eq!(error.previous_operation, written.handle);
    assert_eq!(count(&db, "late"), 1);
}

#[test]
fn operations_are_capped_but_unknown_outcomes_stay() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    ok(
        &mut session,
        &exec_json("INSERT INTO audit (note) VALUES ('a')", json!({})),
    );
    store(&home)
        .execute_batch(
            "INSERT INTO operations (id, session, target, stmt_json, hash, flags, status, deadline_at, created_at)
               VALUES ('wold', 't', 'x', '{}', 'h', '{}', 'pending', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z');
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n LIMIT 10001)
             INSERT INTO operations (id, session, target, stmt_json, hash, flags, status, deadline_at, created_at)
               SELECT 'f' || i, 't', 'x', '{}', 'h', '{}', 'committed', '2001-01-01T00:00:00.000Z', '2001-01-01T00:00:00.000Z' FROM n;",
        )
        .unwrap();
    ok(
        &mut session,
        &exec_json("INSERT INTO audit (note) VALUES ('b')", json!({})),
    );
    let rows: i64 = store(&home)
        .query_row(
            "SELECT count(*) FROM operations WHERE session = 't'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 10_001, "10,000 rows plus the unknown one");
    let receipt = ok(&mut session, r#"{"command":"receipt","handle":"wold"}"#);
    assert_eq!(receipt.status.as_deref(), Some("unknown"));
}
