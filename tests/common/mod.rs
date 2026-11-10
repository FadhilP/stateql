#![allow(
    dead_code,
    reason = "each test binary uses a different subset of the helpers"
)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::Value;
use stateql::{Code, Command, Engine, Error, Response, Session};

pub const KEYRING_SERVICE: &str = "stql-test-suite";

pub struct Home {
    pub path: PathBuf,
}

impl Home {
    pub fn new() -> Home {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let unique = format!(
            "stql-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        let path = std::env::temp_dir().join(unique);
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("temp dir");
        Home { path }
    }

    pub fn engine(&self) -> Engine {
        Engine::open_with_keyring_service(&self.path.join("home"), KEYRING_SERVICE)
            .expect("engine opens")
    }

    pub fn file(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    pub fn results_dir(&self) -> PathBuf {
        self.path.join("home").join("results")
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub fn sqlite_fixture(path: &Path) {
    let conn = rusqlite::Connection::open(path).expect("fixture db");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, status TEXT, score REAL, avatar BLOB);
         CREATE INDEX users_status ON users (status);
         CREATE TABLE audit (id INTEGER PRIMARY KEY, note TEXT);
         INSERT INTO users (name, status, score, avatar) VALUES
           ('Ada', 'active', 9.5, x'0102'), ('Grace', 'active', 8.0, NULL), ('Linus', 'inactive', NULL, NULL),
           ('Barbara', 'active', 7.25, NULL), ('Edsger', 'inactive', 6.0, NULL);",
    )
    .expect("fixture schema");
}

pub fn count(path: &Path, table: &str) -> i64 {
    let conn = rusqlite::Connection::open(path).expect("fixture db");
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .expect("count")
}

pub fn cmd(json: &str) -> Command {
    serde_json::from_str(json).unwrap_or_else(|error| panic!("bad command {json}: {error}"))
}

pub fn ok(session: &mut Session<'_>, json: &str) -> Response {
    session
        .run(cmd(json))
        .unwrap_or_else(|error| panic!("{json} failed: {error}"))
}

pub fn fail(session: &mut Session<'_>, json: &str) -> Error {
    match session.run(cmd(json)) {
        Ok(response) => panic!("{json} succeeded: {response:?}"),
        Err(error) => error,
    }
}

pub fn expect_code(session: &mut Session<'_>, json: &str, code: Code) -> Error {
    let error = fail(session, json);
    assert_eq!(error.code, code, "{json}: {}", error.message);
    error
}

pub fn connect_json(target: &str, read_write: bool) -> String {
    serde_json::json!({"command": "connect", "target": target, "read_write": read_write})
        .to_string()
}

pub fn query_json(stmt: &str) -> String {
    serde_json::json!({"command": "query", "stmt": stmt}).to_string()
}

pub fn exec_json(stmt: &str, extra: Value) -> String {
    let mut value = serde_json::json!({"command": "exec", "stmt": stmt});
    if let (Value::Object(object), Value::Object(more)) = (&mut value, extra) {
        object.extend(more);
    }
    value.to_string()
}

pub fn result_files(home: &Home) -> usize {
    std::fs::read_dir(home.results_dir())
        .map(|dir| dir.count())
        .unwrap_or(0)
}

// A skipped backend must be visible, and a CI job that names it in STQL_TEST_REQUIRE must not pass without it.
pub fn env_url(name: &str) -> Option<String> {
    let url = std::env::var(name).ok().filter(|value| !value.is_empty());
    if url.is_some() {
        return url;
    }
    let required = std::env::var("STQL_TEST_REQUIRE").unwrap_or_default();
    assert!(
        !required.split(',').any(|item| item.trim() == name),
        "{name} is not set, but STQL_TEST_REQUIRE names it"
    );
    // Writes to stderr directly, because the test harness hides eprintln! output of tests that pass.
    let _ = writeln!(std::io::stderr(), "skipped: {name} is not set");
    None
}
