mod common;

use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use serde_json::{Value, json};
use stateql::{Code, Executed, PipeMode};

struct Pg {
    admin: postgres::Client,
    schema: String,
    url_var: String,
}

impl Pg {
    fn connect(&self, read_write: bool) -> String {
        json!({"command": "connect", "env": self.url_var, "read_write": read_write}).to_string()
    }

    fn count(&mut self, table: &str) -> i64 {
        let sql = format!("SELECT count(*) FROM {}.{table}", self.schema);
        self.admin.query_one(&sql, &[]).expect("count").get(0)
    }
}

impl Drop for Pg {
    fn drop(&mut self) {
        let _ = self
            .admin
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema));
    }
}

fn pg() -> Option<Pg> {
    let base = env_url("STQL_TEST_POSTGRES_URL")?;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    static URL_VARS: Once = Once::new();
    URL_VARS.call_once(|| {
        for index in 0..MAX_FIXTURES {
            let separator = if base.contains('?') { '&' } else { '?' };
            let url = format!(
                "{base}{separator}options=-csearch_path%3D{}",
                schema_name(index)
            );
            // SAFETY: every test calls pg() first, so the other test threads wait on this Once and read no environment.
            unsafe { std::env::set_var(url_var(index), url) };
        }
    });
    let index = NEXT.fetch_add(1, Ordering::SeqCst);
    assert!(index < MAX_FIXTURES, "raise MAX_FIXTURES");
    let schema = schema_name(index);
    let mut admin = postgres::Client::connect(&base, postgres::NoTls).expect("admin connects");
    admin
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; SET search_path = {schema};
             CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL, status text, score numeric(6,2));
             CREATE INDEX users_status ON users (status);
             CREATE TABLE audit (id serial PRIMARY KEY, note text);
             CREATE VIEW active_users AS SELECT id, name FROM users WHERE status = 'active';
             INSERT INTO users (name, status, score) VALUES
               ('Ada', 'active', 9.5), ('Grace', 'active', 8), ('Linus', 'inactive', NULL),
               ('Barbara', 'active', 7.25), ('Edsger', 'inactive', 6);"
        ))
        .expect("fixture schema");
    Some(Pg {
        admin,
        schema,
        url_var: url_var(index),
    })
}

const MAX_FIXTURES: usize = 32;

fn schema_name(index: usize) -> String {
    format!("stql_{}_{index}", std::process::id())
}

// A URL with a password is refused as a raw target, so each fixture connects through its own variable.
fn url_var(index: usize) -> String {
    format!("STQL_TEST_POSTGRES_URL_{index}")
}

#[test]
fn values_map_to_cells_and_keep_database_type_names() {
    let Some(mut pg) = pg() else { return };
    pg.admin
        .batch_execute(&format!(
            "SET search_path = {};
             CREATE TYPE mood AS ENUM ('sad', 'happy');
             CREATE DOMAIN positive AS int4 CHECK (VALUE > 0);
             CREATE TABLE vals (b bool, s int2, i int4, l int8, f4 float4, f8 float8, n numeric, t text, v varchar(5),
               c char(3), u uuid, d date, tm time, ts timestamp, tz timestamptz, j json, jb jsonb, by bytea,
               ia int4[], ta text[], m mood, p positive, inf timestamp, empty int4[]);
             INSERT INTO vals VALUES (true, -2, 3, 9007199254740993, 1.5, 2.25, 12345678901234567890.123, 'x', 'vv', 'ab',
               '00000000-0000-0000-0000-00000000002a', '2024-02-29', '13:14:15.5', '2024-02-29 01:02:03.004',
               '2024-02-29 01:02:03+02', '{{\"a\": 1}}', '{{\"b\": [1, 2]}}', '\\x0102', '{{1,NULL,3}}', '{{{{a,b}},{{c,d}}}}',
               'happy', 7, 'infinity', '{{}}');
             INSERT INTO vals (b) VALUES (NULL);",
            pg.schema
        ))
        .expect("value fixture");
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(false));
    let response = ok(
        &mut session,
        &query_json("SELECT * FROM vals ORDER BY b NULLS LAST"),
    );
    let types = response.types.unwrap();
    assert_eq!(
        types,
        [
            "bool",
            "int2",
            "int4",
            "int8",
            "float4",
            "float8",
            "numeric",
            "text",
            "varchar",
            "bpchar",
            "uuid",
            "date",
            "time",
            "timestamp",
            "timestamptz",
            "json",
            "jsonb",
            "bytea",
            "int4[]",
            "text[]",
            "mood",
            "int4",
            "timestamp",
            "int4[]"
        ]
    );
    let rows = response.rows.unwrap();
    assert_eq!(
        rows[0],
        vec![
            json!(1),
            json!(-2),
            json!(3),
            json!(9_007_199_254_740_993_i64),
            json!(1.5),
            json!(2.25),
            json!("12345678901234567890.123"),
            json!("x"),
            json!("vv"),
            json!("ab "),
            json!("00000000-0000-0000-0000-00000000002a"),
            json!("2024-02-29"),
            json!("13:14:15.5"),
            json!("2024-02-29T01:02:03.004"),
            json!("2024-02-28T23:02:03Z"),
            json!({"a": 1}),
            json!({"b": [1, 2]}),
            json!({"type": "bytes", "length": 2, "base64": "AQI="}),
            json!([1, null, 3]),
            json!([["a", "b"], ["c", "d"]]),
            json!("happy"),
            json!(7),
            json!("infinity"),
            json!([]),
        ]
    );
    assert!(rows[1].iter().all(Value::is_null));

    let filtered = ok(
        &mut session,
        &json!({"command": "filter", "handle": response.handle, "predicate": "l > 9007199254740992 AND n = '12345678901234567890.123'"})
            .to_string(),
    );
    assert_eq!(filtered.total, Some(1));
}

#[test]
fn unsupported_types_fail_with_a_cast_hint_and_leave_no_file() {
    let Some(pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(false));
    for sql in [
        "SELECT point(1, 2) AS p",
        "SELECT ARRAY[point(1, 2)] AS ps",
        "SELECT '08:00:2b:01:02:03:04:05'::macaddr8 AS m",
    ] {
        let error = expect_code(&mut session, &query_json(sql), Code::QueryFailed);
        assert_eq!(
            error.hint.as_deref(),
            Some("cast the column to text (::text)")
        );
    }
    ok(&mut session, &query_json("SELECT point(1, 2)::text AS p"));
    assert_eq!(result_files(&home), 1);
}

#[test]
fn interval_network_and_system_types_read_as_their_postgres_text() {
    let Some(pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(false));
    let values = [
        "'-1 day -02:03:04.5'::interval",
        "'1 year 2 mons 3 days 04:05:06.789'::interval",
        "'0'::interval",
        "'-1 year +2 days'::interval",
        "'-3 days +04:00:00'::interval",
        "'100 hours 0.000001 seconds'::interval",
        "'192.168.1.5'::inet",
        "'192.168.1.5/24'::inet",
        "'10.0.0.0/8'::cidr",
        "'10.1.2.3/32'::cidr",
        "'2001:db8::1'::inet",
        "'2001:db8:0:1::/64'::cidr",
        "'::ffff:1.2.3.4'::inet",
        "'::'::inet",
        "'1:0:0:2:0:0:0:3'::inet",
        "(SELECT xmin FROM users WHERE id = 1)",
        "'7'::cid",
        "'16/B374D848'::pg_lsn",
        "'0/1A'::pg_lsn",
        "'13:14:15.25+05:30'::timetz",
        "'23:59:59-08'::timetz",
        "'01:02:03+05:30:15'::timetz",
        "'08:00:2B:01:02:03'::macaddr",
    ];
    // format uses the output function; inet::text is text(inet), which always adds the netmask.
    let select: Vec<String> = values
        .iter()
        .map(|value| format!("{value} AS v, format('%s', {value}) AS t"))
        .collect();
    let response = ok(
        &mut session,
        &query_json(&format!("SELECT {}", select.join(", "))),
    );
    let row = &response.rows.unwrap()[0];
    for (index, value) in values.iter().enumerate() {
        assert_eq!(row[2 * index], row[2 * index + 1], "{value}");
    }
    let null = ok(&mut session, &query_json("SELECT NULL::interval AS v"));
    assert_eq!(null.rows, Some(vec![vec![Value::Null]]));
    let arrays = ok(
        &mut session,
        &query_json("SELECT ARRAY['1 day'::interval, NULL] AS gaps, ARRAY['::1'::inet] AS hosts"),
    );
    assert_eq!(
        arrays.rows.unwrap()[0],
        vec![json!(["1 day", null]), json!(["::1"])]
    );
}

#[test]
fn missing_columns_and_tables_carry_a_hint() {
    let Some(pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(false));
    let near = expect_code(
        &mut session,
        &query_json("SELECT u.nme FROM users u"),
        Code::InvalidSql,
    );
    let hint = near.hint.expect("PostgreSQL suggests the column");
    assert!(hint.contains("u.name"), "{hint}");
    let far = expect_code(
        &mut session,
        &query_json("SELECT zzzz FROM users"),
        Code::InvalidSql,
    );
    assert_eq!(far.hint.as_deref(), Some("stql describe TABLE"));
    let table = expect_code(
        &mut session,
        &query_json("SELECT * FROM no_such_table"),
        Code::InvalidSql,
    );
    assert_eq!(table.hint.as_deref(), Some("stql objects --search TEXT"));
}

const POSTGRES_CORPUS: &[&str] = &[
    "DELETE FROM users",
    "UPDATE users SET name = 'x'",
    "INSERT INTO users (name) VALUES ('x')",
    "DROP TABLE audit",
    "CREATE TABLE evil (id int)",
    "TRUNCATE users",
    "WITH gone AS (DELETE FROM users RETURNING *) SELECT * FROM gone",
    "EXPLAIN ANALYZE DELETE FROM users",
    "SELECT 1; DELETE FROM users",
    "SELECT 1; COMMIT; DELETE FROM users",
    "/* c */ DELETE FROM users",
    "-- c\nDELETE FROM users",
    "(DELETE FROM users)",
    "SELECT * FROM users FOR UPDATE",
    "SELECT nextval('users_id_seq')",
    "COPY users TO PROGRAM 'echo x'",
    "DO $$ BEGIN DELETE FROM users; END $$",
    "CALL nothing()",
    "VACUUM users",
    "BEGIN",
    "COMMIT",
    "ROLLBACK TO SAVEPOINT stql_read",
    "SET transaction_read_only = off",
    "SET default_transaction_read_only = off",
    "LOCK users",
];

fn assert_corpus_blocked(session: &mut stateql::Session<'_>, pg: &mut Pg) {
    for sql in POSTGRES_CORPUS {
        let error = fail(session, &query_json(sql));
        assert_ne!(error.code, Code::InternalError, "{sql}: {}", error.message);
        assert_eq!(error.executed, Executed::No, "{sql}");
        assert_eq!(pg.count("users"), 5, "{sql} changed data");
    }
    let (sequence,): (i64,) = {
        let sql = format!("SELECT last_value FROM {}.users_id_seq", pg.schema);
        let row = pg.admin.query_one(&sql, &[]).unwrap();
        (row.get(0),)
    };
    assert_eq!(sequence, 5);
    let evil = format!("SELECT to_regclass('{}.evil') IS NULL", pg.schema);
    assert!(pg.admin.query_one(&evil, &[]).unwrap().get::<_, bool>(0));
}

#[test]
fn read_guard_corpus_blocks_writes_on_every_session_kind() {
    let Some(mut pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut reader = engine.session("ro").unwrap();
    ok(&mut reader, &pg.connect(false));
    assert_corpus_blocked(&mut reader, &mut pg);

    let mut writer = engine.session("rw").unwrap();
    ok(&mut writer, &pg.connect(true));
    assert_corpus_blocked(&mut writer, &mut pg);
    ok(&mut writer, &query_json("TABLE users"));
    ok(&mut writer, &query_json("SHOW search_path"));
    ok(&mut writer, &query_json("EXPLAIN SELECT * FROM users"));
    ok(&mut writer, &query_json("VALUES (1), (2)"));
    ok(
        &mut writer,
        &query_json("EXPLAIN ANALYZE CREATE TABLE evil AS SELECT 1"),
    );
    assert_corpus_blocked(&mut writer, &mut pg);

    for sql in POSTGRES_CORPUS {
        let lines = format!(
            "{}\n{}\n",
            exec_json("INSERT INTO audit (note) VALUES ('in pipe')", json!({})),
            query_json(sql)
        );
        let mut output = Vec::new();
        let exit = writer
            .pipe(lines.as_bytes(), &mut output, PipeMode::Atomic(None))
            .unwrap();
        assert_ne!(exit, 0, "{sql} passed inside an atomic pipe");
        assert_eq!(pg.count("users"), 5, "{sql} changed data in a pipe");
        assert_eq!(pg.count("audit"), 0, "{sql} kept the pipe's write");
    }
}

#[test]
fn reads_inside_an_atomic_pipe_see_its_writes_and_stay_read_only() {
    let Some(mut pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(true));
    let lines = [
        exec_json(
            "INSERT INTO audit (note) VALUES ($1)",
            json!({"param": ["first"]}),
        ),
        query_json("SELECT count(*) AS n FROM audit"),
        exec_json(
            "UPDATE users SET status = $1 WHERE id = $2",
            json!({"params": ["gone", 1], "expect_rows": 1}),
        ),
        query_json("SELECT status FROM users WHERE id = 1"),
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
    assert_eq!(values[3]["rows"], json!([["gone"]]));
    assert_eq!(values.last().unwrap()["status"], json!("committed"));
    assert_eq!(pg.count("audit"), 1);
}

#[test]
fn params_bind_as_text_and_their_count_is_checked() {
    let Some(pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(false));
    let response = ok(
        &mut session,
        &json!({"command": "query", "stmt": "SELECT id, name FROM users WHERE id < $1 AND name <> $2 AND $3::text IS NULL ORDER BY id", "params": [3, "Ada", null]}).to_string(),
    );
    assert_eq!(response.rows, Some(vec![vec![json!(2), json!("Grace")]]));
    expect_code(
        &mut session,
        &json!({"command": "query", "stmt": "SELECT $1::int AS n", "params": [1, 2]}).to_string(),
        Code::InvalidCommand,
    );
    expect_code(
        &mut session,
        &json!({"command": "query", "stmt": "SELECT $1::int AS n", "param": ["nope"]}).to_string(),
        Code::QueryFailed,
    );
    expect_code(
        &mut session,
        &query_json("SELECT missing FROM users"),
        Code::InvalidSql,
    );
}

#[test]
fn exec_commits_and_expect_rows_rolls_back_a_mismatch() {
    let Some(mut pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut reader = engine.session("ro").unwrap();
    ok(&mut reader, &pg.connect(false));
    expect_code(
        &mut reader,
        &exec_json("INSERT INTO audit (note) VALUES ('x')", json!({})),
        Code::ReadOnly,
    );

    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(true));
    let written = ok(
        &mut session,
        &exec_json(
            "INSERT INTO audit (note) VALUES ($1), ($2)",
            json!({"param": ["a", "b"]}),
        ),
    );
    assert_eq!(
        (written.status.as_deref(), written.affected_rows),
        (Some("committed"), Some(2))
    );
    let error = expect_code(
        &mut session,
        &exec_json(
            "UPDATE users SET status = 'x' WHERE status = 'active'",
            json!({"expect_rows": 2}),
        ),
        Code::RowCountMismatch,
    );
    assert_eq!((error.executed, error.actual_rows), (Executed::No, Some(3)));
    let active = format!(
        "SELECT count(*) FROM {}.users WHERE status = 'active'",
        pg.schema
    );
    assert_eq!(
        pg.admin.query_one(&active, &[]).unwrap().get::<_, i64>(0),
        3
    );
    expect_code(
        &mut session,
        &exec_json("DELETE FROM audit", json!({})),
        Code::UnboundedMutation,
    );
    expect_code(
        &mut session,
        &exec_json(
            "INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = 'x'",
            json!({}),
        ),
        Code::DestructiveOperation,
    );
    expect_code(
        &mut session,
        &exec_json("INSERT INTO users (id, name) VALUES (1, 'dup')", json!({})),
        Code::QueryFailed,
    );
    assert_eq!(pg.count("audit"), 2);
}

#[test]
fn statement_timeout_cancels_slow_reads_and_writes() {
    let Some(mut pg) = pg() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(true));
    session.set_timeout(Duration::from_millis(1500));
    let error = expect_code(
        &mut session,
        &query_json("SELECT pg_sleep(10)"),
        Code::DeadlineExceeded,
    );
    assert_eq!(error.executed, Executed::No);
    assert_eq!(
        error.hint.as_deref(),
        Some("--timeout-ms is 1500; raise it or narrow the query")
    );
    let error = expect_code(
        &mut session,
        &exec_json(
            "UPDATE users SET name = 'slow' WHERE pg_sleep(10) IS NOT NULL",
            json!({}),
        ),
        Code::DeadlineExceeded,
    );
    assert_eq!(error.executed, Executed::No);
    let named = format!(
        "SELECT count(*) FROM {}.users WHERE name = 'slow'",
        pg.schema
    );
    assert_eq!(pg.admin.query_one(&named, &[]).unwrap().get::<_, i64>(0), 0);
    ok(&mut session, &query_json("SELECT 1 AS still_works"));
    assert_eq!(pg.count("users"), 5);
}

#[test]
fn catalog_lists_objects_and_describes_tables_and_views() {
    let Some(mut pg) = pg() else { return };
    let probe = format!("CREATE VIEW {}.catalog_probe AS SELECT 1 AS one", pg.schema);
    pg.admin.batch_execute(&probe).unwrap();
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &pg.connect(false));
    let objects = ok(
        &mut session,
        r#"{"command":"objects","search":"catalog_probe"}"#,
    );
    assert_eq!(
        objects.rows,
        Some(vec![vec![
            json!("view"),
            json!(pg.schema),
            json!("catalog_probe")
        ]])
    );
    let tables = ok(
        &mut session,
        &json!({"command": "objects", "kind": "table", "search": "audit", "limit": 1}).to_string(),
    );
    assert_eq!(tables.total, Some(1));
    assert_eq!(tables.next_cursor.as_deref(), Some("1"));

    let users = ok(&mut session, r#"{"command":"describe","name":"users"}"#);
    let value = users.data["object"].clone();
    assert_eq!(value["schema"], json!(pg.schema));
    assert_eq!(value["kind"], json!("table"));
    assert_eq!(
        value["columns"][0],
        json!({"name": "id", "type": "integer", "nullable": false, "default": "nextval('users_id_seq'::regclass)", "primary_key": true})
    );
    assert_eq!(
        value["columns"][2],
        json!({"name": "status", "type": "text"})
    );
    assert_eq!(value["columns"][3]["type"], json!("numeric(6,2)"));
    assert_eq!(
        value["constraints"],
        json!([{"name": "users_pkey", "type": "PRIMARY KEY"}]),
        "NOT NULL columns are not listed as CHECK constraints"
    );
    let indexes: Vec<&str> = value["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|index| index["name"].as_str().unwrap())
        .collect();
    assert_eq!(indexes, ["users_pkey", "users_status"]);

    let view = ok(
        &mut session,
        &json!({"command": "describe", "name": format!("{}.active_users", pg.schema)}).to_string(),
    );
    assert!(
        view.data["object"]["ddl"]
            .as_str()
            .unwrap()
            .contains("WHERE")
    );
    expect_code(
        &mut session,
        r#"{"command":"describe","name":"missing"}"#,
        Code::ResultNotFound,
    );
}
