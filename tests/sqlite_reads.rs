mod common;

use std::fs;
use std::time::{Duration, SystemTime};

use common::*;
use serde_json::json;
use stateql::{Code, Engine, PipeMode};

fn setup() -> (Home, String) {
    let home = Home::new();
    let db = home.file("app.db");
    sqlite_fixture(&db);
    let target = format!("sqlite:{}", db.display());
    (home, target)
}

#[test]
fn query_stores_full_result_and_pages_from_the_handle() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));

    let first = ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT id, name FROM users ORDER BY id","preview_rows":2,"as":"people"}"#,
    );
    assert_eq!(first.total, Some(5));
    assert_eq!(first.next_offset, Some(2));
    assert_eq!(
        first.columns,
        Some(vec!["id".to_string(), "name".to_string()])
    );
    assert_eq!(
        first.rows,
        Some(vec![
            vec![json!(1), json!("Ada")],
            vec![json!(2), json!("Grace")]
        ])
    );

    let page = ok(
        &mut session,
        r#"{"command":"rows","handle":"people","offset":3,"limit":10}"#,
    );
    assert_eq!(
        page.rows,
        Some(vec![
            vec![json!(4), json!("Barbara")],
            vec![json!(5), json!("Edsger")]
        ])
    );
    assert_eq!(page.next_offset, None);

    let meta = ok(
        &mut session,
        &format!(
            r#"{{"command":"rows","handle":"{}","limit":0}}"#,
            first.handle.unwrap()
        ),
    );
    assert_eq!(meta.total, Some(5));
    assert_eq!(meta.rows, None);

    let narrowed = ok(
        &mut session,
        r#"{"command":"rows","handle":"people","columns":"name","limit":1}"#,
    );
    assert_eq!(narrowed.rows, Some(vec![vec![json!("Ada")]]));
}

#[test]
fn later_commands_read_the_stored_result_without_the_database() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let result = ok(&mut session, &query_json("SELECT name FROM users"));
    drop(session);
    fs::remove_file(home.file("app.db")).unwrap();
    let mut session = engine.session("t").unwrap();
    let page = ok(
        &mut session,
        &format!(
            r#"{{"command":"rows","handle":"{}"}}"#,
            result.handle.unwrap()
        ),
    );
    assert_eq!(page.total, Some(5));
}

#[test]
fn params_bind_by_position_and_values_keep_their_types() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let result = ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT id, score, avatar, NULL AS empty, ? AS echo FROM users WHERE status = ? ORDER BY id LIMIT 1","params":[7,"active"]}"#,
    );
    assert_eq!(
        result.rows.unwrap()[0],
        vec![
            json!(1),
            json!(9.5),
            json!({"type":"bytes","length":2,"base64":"AQI="}),
            json!(null),
            json!(7)
        ]
    );
    let long = ok(
        &mut session,
        &query_json("SELECT printf('%.300c', 'x') AS long_text"),
    );
    assert_eq!(long.rows.unwrap()[0][0]["length"], json!(300));
}

#[test]
fn cell_limits_change_only_the_output_of_one_call() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let full = ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT printf('%.300c', 'x') AS long_text","max_cell_characters":1000,"as":"long"}"#,
    );
    assert_eq!(full.rows.unwrap()[0][0], json!("x".repeat(300)));
    let stored = ok(&mut session, r#"{"command":"rows","handle":"long"}"#);
    assert_eq!(stored.rows.unwrap()[0][0]["length"], json!(300));
    let short = ok(
        &mut session,
        r#"{"command":"rows","handle":"long","max_cell_characters":3}"#,
    );
    assert_eq!(
        short.rows.unwrap()[0][0],
        json!({"type":"text","length":300,"preview":"xxx"})
    );
}

#[test]
fn byte_limit_drops_rows_and_rows_offset_continues_without_gap() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let all = ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT id, name FROM users ORDER BY id","as":"people"}"#,
    );
    let max_bytes = 150;
    let mut seen = Vec::new();
    let mut next = Some(0);
    while let Some(offset) = next {
        let page = ok(
            &mut session,
            &json!({"command": "rows", "handle": "people", "offset": offset, "max_bytes": max_bytes})
                .to_string(),
        );
        let line = stateql::json_line(&Ok(page.clone()));
        assert!(line.len() <= max_bytes, "{line}");
        let rows = page.rows.unwrap();
        assert!(!rows.is_empty() && rows.len() < 5, "{line}");
        seen.extend(rows);
        next = page.next_offset;
        assert!(seen.len() <= 5, "pages repeat rows");
    }
    assert_eq!(Some(seen), all.rows);

    let none_fit = ok(
        &mut session,
        r#"{"command":"rows","handle":"people","offset":2,"max_bytes":10}"#,
    );
    assert_eq!(none_fit.rows, Some(vec![]));
    assert_eq!(none_fit.next_offset, Some(2));
    assert!(
        none_fit.data["hint"]
            .as_str()
            .unwrap()
            .contains("--max-bytes")
    );
}

#[test]
fn duplicate_column_names_get_a_suffix_that_filter_can_use() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let result = ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT 1 AS id, 2 AS id, 3 AS ID","as":"dupes"}"#,
    );
    assert_eq!(
        result.columns,
        Some(vec![
            "id".to_string(),
            "id_2".to_string(),
            "ID_3".to_string()
        ])
    );
    let filtered = ok(
        &mut session,
        r#"{"command":"filter","handle":"dupes","predicate":"id_2 = 2"}"#,
    );
    assert_eq!(filtered.total, Some(1));
}

const SQLITE_CORPUS: &[&str] = &[
    "DELETE FROM users",
    "UPDATE users SET name = 'x'",
    "INSERT INTO users (name) VALUES ('x')",
    "REPLACE INTO users (id, name) VALUES (1, 'x')",
    "DROP TABLE users",
    "CREATE TABLE evil (id INTEGER)",
    "ALTER TABLE users ADD COLUMN evil TEXT",
    "WITH gone AS (SELECT 1) DELETE FROM users",
    "SELECT 1; DELETE FROM users",
    "SELECT 1; -- ok\nDELETE FROM users",
    "/* /* */ DELETE FROM users; -- */ SELECT 1",
    "ATTACH DATABASE 'other.db' AS other",
    "PRAGMA user_version = 7",
    "PRAGMA journal_mode = WAL",
    "PRAGMA query_only = OFF",
    "VACUUM",
    "ANALYZE",
    "REINDEX",
    "SELECT load_extension('evil')",
    "BEGIN",
    "EXPLAIN QUERY PLAN UPDATE users SET name = 'x'",
];

fn assert_corpus_blocked(session: &mut stateql::Session<'_>, db: &std::path::Path) {
    for sql in SQLITE_CORPUS {
        let error = fail(session, &query_json(sql));
        assert_ne!(error.code, Code::InternalError, "{sql}: {}", error.message);
        assert_eq!(count(db, "users"), 5, "{sql} changed data");
    }
    let conn = rusqlite::Connection::open(db).unwrap();
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let journal: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!((version, journal.as_str()), (0, "delete"));
    assert!(!db.with_file_name("other.db").exists());
}

#[test]
fn read_guard_corpus_blocks_writes_on_read_only_and_read_write_sessions() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("ro").unwrap();
    ok(&mut session, &connect_json(&target, false));
    assert_corpus_blocked(&mut session, &db);

    let mut writer = engine.session("rw").unwrap();
    ok(&mut writer, &connect_json(&target, true));
    assert_corpus_blocked(&mut writer, &db);
    ok(&mut writer, &query_json("PRAGMA table_info(users)"));
    ok(&mut writer, &query_json("PRAGMA user_version"));
    ok(
        &mut writer,
        &query_json("EXPLAIN QUERY PLAN SELECT * FROM users WHERE status = 'x'"),
    );
    assert_eq!(count(&db, "users"), 5);
}

#[test]
fn a_second_statement_is_refused_the_same_way_whatever_it_is() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("rw").unwrap();
    ok(&mut session, &connect_json(&target, true));
    for sql in [
        "SELECT 1; SELECT 2",
        "SELECT 1; DROP TABLE users",
        "SELECT 1; SELECT * FROM missing",
    ] {
        let error = expect_code(&mut session, &query_json(sql), Code::InvalidSql);
        assert_eq!(error.message, "Send exactly one statement.", "{sql}");
    }
    ok(&mut session, &query_json("SELECT 1; -- a comment"));
    assert_eq!(count(&db, "users"), 5);
}

#[test]
fn read_guard_corpus_holds_inside_an_atomic_pipe() {
    let (home, target) = setup();
    let db = home.file("app.db");
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, true));
    for sql in SQLITE_CORPUS {
        let input = format!("{}\n", query_json(sql));
        let mut output = Vec::new();
        let exit = session
            .pipe(input.as_bytes(), &mut output, PipeMode::Atomic(None))
            .unwrap();
        assert_ne!(exit, 0, "{sql} passed inside an atomic pipe");
        assert_eq!(
            count(&db, "users"),
            5,
            "{sql} changed data inside an atomic pipe"
        );
    }
}

#[test]
fn filter_sandbox_denies_everything_but_the_source_rows() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT id, name, status FROM users","as":"src"}"#,
    );
    let files_before = result_files(&home);
    for predicate in [
        "(SELECT count(*) FROM sqlite_schema) > 0",
        "(SELECT count(*) FROM main.r) >= 0",
        "1=1); DELETE FROM src.r; --",
        "1=1) ; ATTACH DATABASE 'x.db' AS x; --",
        "load_extension('evil') IS NULL",
        "(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c) > 0",
        "pragma_user_version IS NULL OR (SELECT * FROM pragma_user_version) = 0",
    ] {
        let error = fail(
            &mut session,
            &json!({"command":"filter","handle":"src","predicate":predicate}).to_string(),
        );
        assert!(
            matches!(error.code, Code::PermissionDenied | Code::InvalidSql),
            "{predicate}: {error:?}"
        );
    }
    assert_eq!(result_files(&home), files_before);
    let source = ok(
        &mut session,
        r#"{"command":"rows","handle":"src","limit":0}"#,
    );
    assert_eq!(source.total, Some(5));

    let filtered = ok(
        &mut session,
        r#"{"command":"filter","handle":"src","predicate":"status = ? AND upper(name) LIKE ?","params":["active","%A%"],"as":"a"}"#,
    );
    assert_eq!(filtered.total, Some(3));
    assert_eq!(
        filtered.rows.unwrap()[0],
        vec![json!(1), json!("Ada"), json!("active")]
    );
}

#[test]
fn caps_overflow_deletes_the_partial_result_file() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let error = fail(
        &mut session,
        r#"{"command":"query","stmt":"SELECT * FROM users","max_rows":3}"#,
    );
    assert_eq!(error.code, Code::ResultTooLarge);
    let error = expect_code(
        &mut session,
        &query_json("SELECT * FROM missing_table"),
        Code::InvalidSql,
    );
    assert_eq!(error.hint.as_deref(), Some("stql objects --search TEXT"));
    let error = expect_code(
        &mut session,
        &query_json("SELECT missing_column FROM users"),
        Code::QueryFailed,
    );
    assert_eq!(error.hint.as_deref(), Some("stql describe TABLE"));
    assert_eq!(result_files(&home), 0);
}

#[test]
fn summarize_and_export_read_the_stored_rows() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT name, score FROM users ORDER BY id","as":"s"}"#,
    );
    let summary = ok(&mut session, r#"{"command":"summarize","handle":"s"}"#);
    assert_eq!(
        summary.data["summary"][1],
        json!({"name":"score","type":"real","nulls":1,"distinct":4,"min":6.0,"max":9.5})
    );

    let csv = home.file("out.csv");
    let jsonl = home.file("out.jsonl");
    let exported = ok(
        &mut session,
        &json!({"command":"export","handle":"s","output":csv}).to_string(),
    );
    assert_eq!(exported.data["exported"], json!(5));
    assert!(
        fs::read_to_string(&csv)
            .unwrap()
            .starts_with("name,score\nAda,9.5\nGrace,8\n")
    );
    ok(
        &mut session,
        &json!({"command":"export","handle":"s","output":jsonl,"columns":"name"}).to_string(),
    );
    assert_eq!(
        fs::read_to_string(&jsonl).unwrap().lines().next(),
        Some(r#"{"name":"Ada"}"#)
    );
    let error = fail(
        &mut session,
        &json!({"command":"export","handle":"s","output":csv}).to_string(),
    );
    assert_eq!(error.code, Code::InvalidCommand);
}

#[test]
fn objects_and_describe_read_the_catalog() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let objects = ok(&mut session, r#"{"command":"objects","kind":"table"}"#);
    assert_eq!(
        objects.rows,
        Some(vec![
            vec![json!("table"), json!("main"), json!("audit")],
            vec![json!("table"), json!("main"), json!("users")]
        ])
    );
    let paged = ok(&mut session, r#"{"command":"objects","limit":1}"#);
    assert_eq!(paged.next_cursor.as_deref(), Some("1"));
    let searched = ok(&mut session, r#"{"command":"objects","search":"stat"}"#);
    assert_eq!(searched.total, Some(1));

    let described = ok(&mut session, r#"{"command":"describe","name":"users"}"#);
    let object = &described.data["object"];
    assert_eq!(
        object["columns"][0],
        json!({"name":"id","type":"INTEGER","primary_key":true})
    );
    assert_eq!(
        object["columns"][1],
        json!({"name":"name","type":"TEXT","nullable":false})
    );
    assert_eq!(object["columns"][2], json!({"name":"status","type":"TEXT"}));
    assert_eq!(object["indexes"][0]["name"], json!("users_status"));
    assert!(
        object["ddl"]
            .as_str()
            .unwrap()
            .starts_with("CREATE TABLE users")
    );
    expect_code(
        &mut session,
        r#"{"command":"describe","name":"nope"}"#,
        Code::ResultNotFound,
    );
}

#[test]
fn history_keeps_statements_but_never_params() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT * FROM users WHERE name = ?","param":["secret-value"]}"#,
    );
    fail(&mut session, &query_json("SELECT * FROM nope"));
    let history = ok(&mut session, r#"{"command":"history","limit":5}"#);
    let entries = history.data["history"].as_array().unwrap().clone();
    assert_eq!(entries[0]["error_code"], json!("INVALID_SQL"));
    assert_eq!(
        entries[1]["stmt"],
        json!("SELECT * FROM users WHERE name = ?")
    );
    let db = rusqlite::Connection::open(home.path.join("home").join("stql.db")).unwrap();
    let leaked: i64 = db.query_row("SELECT count(*) FROM history WHERE stmt_head LIKE '%secret-value%' OR command LIKE '%secret%'", [], |r| r.get(0)).unwrap();
    assert_eq!(leaked, 0);
}

#[test]
fn expired_results_disappear_and_purge_removes_their_files() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT 1","as":"old"}"#,
    );
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT 2","as":"fresh"}"#,
    );
    let db = rusqlite::Connection::open(home.path.join("home").join("stql.db")).unwrap();
    db.execute(
        "UPDATE results SET expires_at = '2000-01-01T00:00:00.000Z' WHERE name = 'old'",
        [],
    )
    .unwrap();
    expect_code(
        &mut session,
        r#"{"command":"rows","handle":"old"}"#,
        Code::ResultNotFound,
    );
    let purged = ok(&mut session, r#"{"command":"purge","expired":true}"#);
    assert_eq!(purged.data["purged"], json!(1));
    assert_eq!(result_files(&home), 1);
    ok(&mut session, r#"{"command":"purge"}"#);
    assert_eq!(result_files(&home), 0);
    expect_code(
        &mut session,
        r#"{"command":"purge","all":true,"expired":true}"#,
        Code::InvalidCommand,
    );
}

#[test]
fn store_evicts_the_oldest_results_past_one_gib() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT 1","as":"first"}"#,
    );
    let db = rusqlite::Connection::open(home.path.join("home").join("stql.db")).unwrap();
    db.execute("UPDATE results SET bytes = 700 * 1024 * 1024, created_at = '2001-01-01T00:00:00.000Z' WHERE name = 'first'", []).unwrap();
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT 2","as":"second"}"#,
    );
    db.execute("UPDATE results SET bytes = 700 * 1024 * 1024, created_at = '2002-01-01T00:00:00.000Z' WHERE name = 'second'", []).unwrap();
    ok(
        &mut session,
        r#"{"command":"query","stmt":"SELECT 3","as":"third"}"#,
    );
    expect_code(
        &mut session,
        r#"{"command":"rows","handle":"first"}"#,
        Code::ResultNotFound,
    );
    ok(&mut session, r#"{"command":"rows","handle":"second"}"#);
    assert_eq!(result_files(&home), 2);
}

#[test]
fn orphan_result_files_older_than_an_hour_are_removed() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let old = home.results_dir().join("rold00.db");
    let young = home.results_dir().join("ryoung.db");
    fs::write(&old, b"").unwrap();
    fs::write(&young, b"").unwrap();
    fs::File::options()
        .write(true)
        .open(&old)
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(7200))
        .unwrap();
    let doctor = ok(&mut session, r#"{"command":"doctor"}"#);
    assert!(
        doctor.data["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["detail"] == json!("1 removed"))
    );
    assert!(!old.exists());
    assert!(young.exists());
}

#[test]
fn store_with_another_schema_version_is_rebuilt() {
    let (home, target) = setup();
    {
        let engine = home.engine();
        let mut session = engine.session("t").unwrap();
        ok(&mut session, &connect_json(&target, false));
        ok(&mut session, &query_json("SELECT 1"));
    }
    let db_path = home.path.join("home").join("stql.db");
    rusqlite::Connection::open(&db_path)
        .unwrap()
        .execute_batch("PRAGMA user_version = 99")
        .unwrap();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    expect_code(&mut session, &query_json("SELECT 1"), Code::NotConnected);
    assert_eq!(result_files(&home), 0);
    let version: i64 = rusqlite::Connection::open(&db_path)
        .unwrap()
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 1);
}

#[test]
fn pipe_runs_lines_on_one_session_and_stops_at_the_first_error() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_json(&target, false));
    let input = format!(
        "{}\n\n{}\n{}\n",
        query_json("SELECT 1 AS one"),
        r#"{"command":"bogus"}"#,
        query_json("SELECT 2")
    );
    let mut output = Vec::new();
    let exit = session
        .pipe(input.as_bytes(), &mut output, PipeMode::StopOnError)
        .unwrap();
    let lines: Vec<serde_json::Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(exit, 2);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["rows"], json!([[1]]));
    assert_eq!(lines[1]["error"]["code"], json!("INVALID_COMMAND"));

    let mut output = Vec::new();
    let exit = session
        .pipe(input.as_bytes(), &mut output, PipeMode::ContinueOnError)
        .unwrap();
    assert_eq!(
        (exit, String::from_utf8(output).unwrap().lines().count()),
        (2, 3)
    );
}

#[test]
fn sessions_do_not_see_each_others_results() {
    let (home, target) = setup();
    let engine = home.engine();
    let mut first = engine.session("a").unwrap();
    ok(&mut first, &connect_json(&target, false));
    let handle = ok(&mut first, &query_json("SELECT 1")).handle.unwrap();
    let mut second = engine.session("b").unwrap();
    expect_code(
        &mut second,
        &format!(r#"{{"command":"rows","handle":"{handle}"}}"#),
        Code::ResultNotFound,
    );
    expect_code(&mut second, &query_json("SELECT 1"), Code::NotConnected);
    let status = ok(&mut first, r#"{"command":"status"}"#);
    assert_eq!(status.data["results"][0]["handle"], json!(handle));
    assert_eq!(
        status.data["results"][0].get("name"),
        None,
        "null fields are omitted"
    );
    let all = ok(&mut first, r#"{"command":"status","all":true}"#);
    assert_eq!(all.data["sessions"].as_array().unwrap().len(), 1);
    drop(engine);
    let _ = Engine::open_with_keyring_service(&home.path.join("home"), KEYRING_SERVICE).unwrap();
}
