mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use mysql::prelude::Queryable;
use serde_json::{Value, json};
use stateql::{Code, Executed, PipeMode};

struct My {
    admin: mysql::Conn,
    prefix: String,
}

impl My {
    fn count(&mut self, table: &str) -> i64 {
        let sql = format!("SELECT count(*) FROM {}{table}", self.prefix);
        self.admin
            .query_first(sql)
            .expect("count")
            .expect("one row")
    }

    fn sql(&self, template: &str) -> String {
        template.replace("{p}", &self.prefix)
    }
}

impl Drop for My {
    fn drop(&mut self) {
        let tables: Vec<String> = self
            .admin
            .exec(
                "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE ?",
                (format!("{}%", self.prefix),),
            )
            .unwrap_or_default();
        for table in tables {
            let _ = self
                .admin
                .query_drop(format!("DROP VIEW IF EXISTS `{table}`"));
            let _ = self
                .admin
                .query_drop(format!("DROP TABLE IF EXISTS `{table}`"));
        }
    }
}

// The URL may carry a password, which stql accepts only through --env or a profile.
fn connect_env(read_write: bool) -> String {
    json!({"command": "connect", "env": "STQL_TEST_MYSQL_URL", "read_write": read_write})
        .to_string()
}

fn my() -> Option<My> {
    let url = env_url("STQL_TEST_MYSQL_URL")?;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let prefix = format!(
        "p{}_{}_",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    let opts = mysql::Opts::from_url(&url).expect("mysql url");
    let admin = mysql::Conn::new(opts).expect("admin connects");
    let mut my = My { admin, prefix };
    for statement in [
        "CREATE TABLE {p}users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL, status VARCHAR(20), score DECIMAL(6,2), KEY {p}users_status (status)) ENGINE=InnoDB",
        "CREATE TABLE {p}audit (id INT AUTO_INCREMENT PRIMARY KEY, note TEXT) ENGINE=InnoDB",
        "CREATE VIEW {p}active AS SELECT id, name FROM {p}users WHERE status = 'active'",
        "INSERT INTO {p}users (name, status, score) VALUES ('Ada', 'active', 9.5), ('Grace', 'active', 8), ('Linus', 'inactive', NULL), ('Barbara', 'active', 7.25), ('Edsger', 'inactive', 6)",
    ] {
        let sql = my.sql(statement);
        my.admin.query_drop(sql).expect("fixture");
    }
    Some(my)
}

#[test]
fn values_map_to_cells_and_keep_database_type_names() {
    let Some(mut my) = my() else { return };
    let create = my.sql(
        "CREATE TABLE {p}vals (ti TINYINT, iu INT UNSIGNED, bu BIGINT UNSIGNED, dc DECIMAL(30,3), f FLOAT, d DOUBLE,
           vc VARCHAR(10), ch CHAR(3), tx TEXT, bl BLOB, vb VARBINARY(4), j JSON, dt DATE, dtm DATETIME(3), ts TIMESTAMP NULL,
           tm TIME(1), yr YEAR, bt BIT(10), en ENUM('a','b'), st SET('x','y'))",
    );
    let insert = my.sql(
        "INSERT INTO {p}vals VALUES (-1, 4000000000, 18446744073709551615, 123456789012345678901234567.125, 1.1, 2.25,
           'v', 'ab', 'long text', X'0102', X'ff', '{\"a\": [1, 2]}', '2024-02-29', '2024-02-29 01:02:03.004',
           '2024-02-29 01:02:03', '-12:30:00.5', 2024, b'1000000001', 'b', 'x,y')",
    );
    my.admin.query_drop("SET time_zone = '+00:00'").unwrap();
    my.admin.query_drop(create).unwrap();
    my.admin.query_drop(insert).unwrap();
    let insert_nulls = my.sql("INSERT INTO {p}vals (ti) VALUES (NULL)");
    my.admin.query_drop(insert_nulls).unwrap();

    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(false));
    let response = ok(
        &mut session,
        &query_json(&my.sql("SELECT * FROM {p}vals ORDER BY ti IS NULL")),
    );
    assert_eq!(
        response.types.unwrap(),
        [
            "tinyint",
            "int unsigned",
            "bigint unsigned",
            "decimal",
            "float",
            "double",
            "varchar",
            "char",
            "text",
            "blob",
            "varbinary",
            "json",
            "date",
            "datetime",
            "timestamp",
            "time",
            "year",
            "bit",
            "enum",
            "set"
        ]
    );
    let rows = response.rows.unwrap();
    assert_eq!(
        rows[0],
        vec![
            json!(-1),
            json!(4_000_000_000_i64),
            json!("18446744073709551615"),
            json!("123456789012345678901234567.125"),
            json!(1.1),
            json!(2.25),
            json!("v"),
            json!("ab"),
            json!("long text"),
            json!({"type": "bytes", "length": 2, "base64": "AQI="}),
            json!({"type": "bytes", "length": 1, "base64": "/w=="}),
            json!({"a": [1, 2]}),
            json!("2024-02-29"),
            json!("2024-02-29T01:02:03.004"),
            json!("2024-02-29T01:02:03Z"),
            json!("-12:30:00.5"),
            json!(2024),
            json!(513),
            json!("b"),
            json!("x,y"),
        ]
    );
    assert!(rows[1].iter().all(Value::is_null));
    let filtered = ok(
        &mut session,
        &json!({"command": "filter", "handle": response.handle, "predicate": "iu > 3999999999 AND bt = 513"}).to_string(),
    );
    assert_eq!(filtered.total, Some(1));
}

const MYSQL_CORPUS: &[&str] = &[
    "DELETE FROM {p}users",
    "UPDATE {p}users SET name = 'x'",
    "INSERT INTO {p}users (name) VALUES ('x')",
    "REPLACE INTO {p}users (id, name) VALUES (1, 'x')",
    "DROP TABLE {p}audit",
    "CREATE TABLE {p}evil (id INT)",
    "ALTER TABLE {p}users ADD COLUMN evil INT",
    "TRUNCATE {p}users",
    "WITH g AS (SELECT 1) DELETE FROM {p}users",
    "WITH g AS (SELECT 1) UPDATE {p}users SET name = 'x'",
    "EXPLAIN ANALYZE UPDATE {p}users u, {p}audit a SET u.name = 'x'",
    "SELECT 1; DELETE FROM {p}users",
    "SELECT 1 /*!, (SELECT 1) */",
    "/*!50000 DELETE FROM {p}users */",
    "SELECT * FROM {p}users INTO OUTFILE '/tmp/stql-evil'",
    "SELECT 1 INTO DUMPFILE '/tmp/stql-evil'",
    "# c\nDELETE FROM {p}users",
    "-- c\nDELETE FROM {p}users",
    "SET SESSION transaction_read_only = OFF",
    "SET GLOBAL max_connections = 10",
    "KILL 1",
    "LOCK TABLES {p}users WRITE",
    "CALL nothing()",
    "DO SLEEP(0)",
    "HANDLER {p}users OPEN",
    "LOAD DATA LOCAL INFILE 'x' INTO TABLE {p}users",
    "START TRANSACTION",
    "COMMIT",
    "ROLLBACK TO SAVEPOINT stql_read",
    "RELEASE SAVEPOINT stql_read",
];

fn assert_corpus_blocked(session: &mut stateql::Session<'_>, my: &mut My) {
    for template in MYSQL_CORPUS {
        let sql = my.sql(template);
        let error = fail(session, &query_json(&sql));
        assert_ne!(error.code, Code::InternalError, "{sql}: {}", error.message);
        assert_eq!(error.executed, Executed::No, "{sql}");
        assert_eq!(my.count("users"), 5, "{sql} changed data");
    }
    let names = my.sql("SELECT count(*) FROM {p}users WHERE name = 'x'");
    assert_eq!(my.admin.query_first::<i64, _>(names).unwrap(), Some(0));
    let tables = my.sql(
        "SELECT count(*) FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('{p}evil', '{p}audit')",
    );
    assert_eq!(my.admin.query_first::<i64, _>(tables).unwrap(), Some(1));
}

#[test]
fn read_guard_corpus_blocks_writes_on_every_session_kind() {
    let Some(mut my) = my() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut reader = engine.session("ro").unwrap();
    ok(&mut reader, &connect_env(false));
    assert_corpus_blocked(&mut reader, &mut my);

    let mut writer = engine.session("rw").unwrap();
    ok(&mut writer, &connect_env(true));
    assert_corpus_blocked(&mut writer, &mut my);
    ok(&mut writer, &query_json(&my.sql("TABLE {p}users")));
    ok(&mut writer, &query_json("SHOW TABLES"));
    ok(
        &mut writer,
        &query_json(&my.sql("EXPLAIN SELECT * FROM {p}users")),
    );
    ok(&mut writer, &query_json("VALUES ROW(1), ROW(2)"));

    for template in MYSQL_CORPUS {
        let sql = my.sql(template);
        let lines = format!(
            "{}\n{}\n",
            exec_json(
                &my.sql("INSERT INTO {p}audit (note) VALUES ('in pipe')"),
                json!({})
            ),
            query_json(&sql)
        );
        let mut output = Vec::new();
        let exit = writer
            .pipe(lines.as_bytes(), &mut output, PipeMode::Atomic(None))
            .unwrap();
        assert_ne!(exit, 0, "{sql} passed inside an atomic pipe");
        assert_eq!(my.count("users"), 5, "{sql} changed data in a pipe");
    }
    assert_eq!(my.count("audit"), 0);
}

#[test]
fn atomic_pipe_reads_its_writes_and_refuses_statements_that_commit() {
    let Some(mut my) = my() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(true));
    let lines = [
        exec_json(
            &my.sql("INSERT INTO {p}audit (note) VALUES (?)"),
            json!({"params": ["first"]}),
        ),
        query_json(&my.sql("SELECT count(*) AS n FROM {p}audit")),
        exec_json(
            &my.sql("UPDATE {p}users SET status = ? WHERE id = ?"),
            json!({"params": ["gone", 1], "expect_rows": 1}),
        ),
    ]
    .join("\n");
    let mut output = Vec::new();
    let exit = session
        .pipe(
            lines.as_bytes(),
            &mut output,
            PipeMode::Atomic(Some(stateql::Isolation::Serializable)),
        )
        .unwrap();
    let output = String::from_utf8(output).unwrap();
    assert_eq!(exit, 0, "{output}");
    let values: Vec<Value> = output
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(values[1]["rows"], json!([[1]]));
    assert_eq!(values.last().unwrap()["status"], json!("committed"));
    assert_eq!(my.count("audit"), 1);

    let lines = [
        exec_json(
            &my.sql("INSERT INTO {p}audit (note) VALUES ('second')"),
            json!({}),
        ),
        exec_json(&my.sql("CREATE TABLE {p}inside (id INT)"), json!({})),
    ]
    .join("\n");
    let mut output = Vec::new();
    let exit = session
        .pipe(lines.as_bytes(), &mut output, PipeMode::Atomic(None))
        .unwrap();
    assert_eq!(exit, Code::Unsupported.exit_code());
    assert_eq!(my.count("audit"), 1);
    expect_code(
        &mut session,
        &exec_json(
            &my.sql("CREATE TABLE {p}counted (id INT)"),
            json!({"expect_rows": 0}),
        ),
        Code::Unsupported,
    );
}

#[test]
fn exec_commits_and_expect_rows_counts_matched_rows() {
    let Some(mut my) = my() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut reader = engine.session("ro").unwrap();
    ok(&mut reader, &connect_env(false));
    expect_code(
        &mut reader,
        &exec_json(
            &my.sql("INSERT INTO {p}audit (note) VALUES ('x')"),
            json!({}),
        ),
        Code::ReadOnly,
    );

    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(true));
    let written = ok(
        &mut session,
        &exec_json(
            &my.sql("INSERT INTO {p}audit (note) VALUES (?), (?)"),
            json!({"params": ["a", "b"]}),
        ),
    );
    assert_eq!(
        (written.status.as_deref(), written.affected_rows),
        (Some("committed"), Some(2))
    );
    let same = ok(
        &mut session,
        &exec_json(
            &my.sql("UPDATE {p}users SET status = 'active' WHERE status = 'active'"),
            json!({"expect_rows": 3}),
        ),
    );
    assert_eq!(same.affected_rows, Some(3), "matched rows count");
    let error = expect_code(
        &mut session,
        &exec_json(
            &my.sql("UPDATE {p}users SET status = 'x' WHERE status = 'active'"),
            json!({"expect_rows": 2}),
        ),
        Code::RowCountMismatch,
    );
    assert_eq!((error.executed, error.actual_rows), (Executed::No, Some(3)));
    let active = my.sql("SELECT count(*) FROM {p}users WHERE status = 'active'");
    assert_eq!(my.admin.query_first::<i64, _>(active).unwrap(), Some(3));
    expect_code(
        &mut session,
        &exec_json(&my.sql("DELETE FROM {p}audit"), json!({})),
        Code::UnboundedMutation,
    );
    expect_code(
        &mut session,
        &exec_json(
            &my.sql("INSERT INTO {p}users (id, name) VALUES (1, 'x') ON DUPLICATE KEY UPDATE name = 'x'"),
            json!({}),
        ),
        Code::DestructiveOperation,
    );
    expect_code(
        &mut session,
        &exec_json(
            &my.sql("INSERT INTO {p}users (id, name) VALUES (1, 'dup')"),
            json!({}),
        ),
        Code::QueryFailed,
    );
    expect_code(
        &mut session,
        &json!({"command": "query", "stmt": "SELECT ? AS n", "params": [1, 2]}).to_string(),
        Code::InvalidCommand,
    );
    expect_code(
        &mut session,
        &query_json(&my.sql("SELECT missing FROM {p}users")),
        Code::InvalidSql,
    );
    assert_eq!(my.count("audit"), 2);
}

#[test]
fn deadlines_cancel_reads_and_leave_slow_writes_unknown() {
    let Some(my) = my() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    session.set_timeout(Duration::from_millis(1500));
    ok(&mut session, &connect_env(true));
    let error = expect_code(
        &mut session,
        &query_json("SELECT SLEEP(5) AS s"),
        Code::DeadlineExceeded,
    );
    assert_eq!(error.executed, Executed::No);
    let error = expect_code(
        &mut session,
        &exec_json(
            &my.sql("UPDATE {p}users SET name = 'slow' WHERE id = 1 AND SLEEP(4) = 0"),
            json!({}),
        ),
        Code::OutcomeUnknown,
    );
    assert_eq!(error.executed, Executed::Unknown);
}

#[test]
fn catalog_lists_objects_and_describes_tables_and_views() {
    let Some(my) = my() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(false));
    let objects = ok(
        &mut session,
        &json!({"command": "objects", "search": my.sql("{p}")}).to_string(),
    );
    let names: Vec<(String, String)> = objects
        .rows
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row[0].as_str().unwrap().into(),
                row[2].as_str().unwrap().into(),
            )
        })
        .collect();
    assert_eq!(
        names,
        [
            ("table".to_string(), my.sql("{p}audit")),
            ("table".to_string(), my.sql("{p}users")),
            ("view".to_string(), my.sql("{p}active")),
        ]
    );
    let page = ok(
        &mut session,
        &json!({"command": "objects", "kind": "table", "search": my.sql("{p}"), "limit": 1})
            .to_string(),
    );
    assert_eq!(page.next_cursor.as_deref(), Some("1"));

    let users = ok(
        &mut session,
        &json!({"command": "describe", "name": my.sql("{p}users")}).to_string(),
    );
    let value = &users.data["object"];
    assert_eq!(value["kind"], json!("table"));
    assert_eq!(
        value["columns"][0],
        json!({"name": "id", "type": "int", "nullable": false, "primary_key": true})
    );
    assert_eq!(value["columns"][3]["type"], json!("decimal(6,2)"));
    let indexes: Vec<&str> = value["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|index| index["name"].as_str().unwrap())
        .collect();
    assert!(indexes.contains(&"PRIMARY"), "{indexes:?}");
    assert!(value["ddl"].as_str().unwrap().starts_with("CREATE TABLE"));
    let view = ok(
        &mut session,
        &json!({"command": "describe", "name": my.sql("{p}active")}).to_string(),
    );
    assert_eq!(view.data["object"]["kind"], json!("view"));
    expect_code(
        &mut session,
        r#"{"command":"describe","name":"missing"}"#,
        Code::ResultNotFound,
    );
}
