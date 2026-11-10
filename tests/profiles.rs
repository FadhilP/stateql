mod common;

use std::path::Path;

use common::*;
use serde_json::{Value, json};
use stateql::Code;

const DB_TARGET: &str = "postgres://app@db.example.com/sales";
// targetFingerprint(DB_TARGET) from StateQL 0.20 (TypeScript).
const DB_FINGERPRINT: &str = "1afbada60274dbebd3a36d39142f9e11a439237cf1b7b7959f40d4e697645bbb";

struct Keyring {
    accounts: Vec<String>,
}

impl Keyring {
    fn open() -> Option<Keyring> {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let unique = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let probe_account = account(&format!("probe{unique}"));
        let probe = keyring::Entry::new(KEYRING_SERVICE, &probe_account).ok()?;
        if let Err(error) = probe.set_password("probe") {
            eprintln!("skipped: the OS keyring is not available: {error}");
            return None;
        }
        let _ = probe.delete_credential();
        // A delete failed once on Windows; Drop tries again.
        Some(Keyring {
            accounts: vec![probe_account],
        })
    }

    fn set(&mut self, account: &str, secret: &str) {
        keyring::Entry::new(KEYRING_SERVICE, account)
            .unwrap()
            .set_password(secret)
            .unwrap();
        self.track(account);
    }

    fn track(&mut self, account: &str) {
        self.accounts.push(account.to_string());
    }

    fn get(&self, account: &str) -> Option<String> {
        keyring::Entry::new(KEYRING_SERVICE, account)
            .unwrap()
            .get_password()
            .ok()
    }
}

impl Drop for Keyring {
    fn drop(&mut self) {
        for account in &self.accounts {
            if let Ok(entry) = keyring::Entry::new(KEYRING_SERVICE, account) {
                let _ = entry.delete_credential();
            }
        }
    }
}

fn account(name: &str) -> String {
    format!("{name}-{}", std::process::id())
}

fn v2(fingerprint: &str, password: &str, stale: bool) -> String {
    let mut record = json!({"version": 2, "fingerprint": fingerprint, "password": password});
    if stale {
        record["stale"] = json!(true);
    }
    record.to_string()
}

fn ts_store(home: &Home, with_password_ref: bool, rows: &[(&str, Value)]) {
    std::fs::create_dir_all(home.path.join("home")).unwrap();
    let conn = rusqlite::Connection::open(home.path.join("home").join("state.sqlite")).unwrap();
    let password_ref = if with_password_ref {
        "password_ref TEXT,"
    } else {
        ""
    };
    conn.execute_batch(&format!(
        "CREATE TABLE profiles (name TEXT PRIMARY KEY, target TEXT, secret_env TEXT, credential_ref TEXT, {password_ref}
           read_only INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    ))
    .unwrap();
    for (name, row) in rows {
        let text = |key: &str| row[key].as_str().map(str::to_string);
        if with_password_ref {
            conn.execute(
                "INSERT INTO profiles VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'x', 'x')",
                rusqlite::params![
                    name,
                    text("target"),
                    text("env"),
                    text("credential_ref"),
                    text("password_ref"),
                    row["read_only"].as_bool().unwrap_or(true)
                ],
            )
            .unwrap();
        } else {
            conn.execute(
                "INSERT INTO profiles VALUES (?1, ?2, ?3, ?4, ?5, 'x', 'x')",
                rusqlite::params![
                    name,
                    text("target"),
                    text("env"),
                    text("credential_ref"),
                    row["read_only"].as_bool().unwrap_or(true)
                ],
            )
            .unwrap();
        }
    }
}

fn profiles_toml(home: &Home) -> String {
    std::fs::read_to_string(home.path.join("home").join("profiles.toml")).unwrap()
}

fn file_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

#[test]
fn ts_profiles_migrate_once_with_checked_passwords() {
    let Some(mut keyring) = Keyring::open() else {
        return;
    };
    let home = Home::new();
    let [good, other, stale, raw, missing] =
        ["good", "other", "stale", "raw", "missing"].map(account);
    keyring.set(&good, &v2(DB_FINGERPRINT, "good-pw", false));
    keyring.set(&other, &v2(DB_FINGERPRINT, "other-pw", false));
    keyring.set(&stale, &v2(DB_FINGERPRINT, "stale-pw", true));
    keyring.set(&raw, "raw-pw");
    let names = [
        "p_good",
        "p_other",
        "p_stale",
        "p_raw",
        "p_missing",
        "p_env",
        "p_cred",
        "p_plain",
    ]
    .map(account);
    for name in &names {
        keyring.track(name);
    }
    let os = |uuid: &str| json!(format!("os:{uuid}"));
    ts_store(
        &home,
        true,
        &[
            (
                &names[0],
                json!({"target": DB_TARGET, "password_ref": os(&good), "read_only": false}),
            ),
            (
                &names[1],
                json!({"target": "postgres://app@other.example.com/sales", "password_ref": os(&other)}),
            ),
            (
                &names[2],
                json!({"target": DB_TARGET, "password_ref": os(&stale)}),
            ),
            (
                &names[3],
                json!({"target": "mysql://root@127.0.0.1:3307/shop", "password_ref": os(&raw)}),
            ),
            (
                &names[4],
                json!({"target": DB_TARGET, "password_ref": os(&missing)}),
            ),
            (&names[5], json!({"env": "APP_DATABASE_URL"})),
            (&names[6], json!({"credential_ref": "vault:prod"})),
            (&names[7], json!({"target": "redis://cache:6380/0"})),
        ],
    );
    let old_store = home.path.join("home").join("state.sqlite");
    let before = file_bytes(&old_store);

    let engine = home.engine();
    let notice = engine.migration_notice().expect("a report").to_string();
    for name in &names[..6] {
        assert!(notice.contains(name.as_str()), "{notice}");
    }
    assert!(
        notice.contains(&format!(
            "Skipped (credential_ref is not supported): {}.",
            names[6]
        )),
        "{notice}"
    );
    let without = notice.split("--force: ").nth(1).unwrap();
    for name in [&names[1], &names[2], &names[4]] {
        assert!(without.contains(name.as_str()), "{notice}");
    }
    assert!(
        !without.contains(names[0].as_str()) && !without.contains(names[3].as_str()),
        "{notice}"
    );

    let saved: Value = serde_json::from_str(&keyring.get(&names[0]).unwrap()).unwrap();
    assert_eq!(
        saved,
        json!({"host": "db.example.com", "port": 5432, "user": "app", "password": "good-pw"})
    );
    let saved: Value = serde_json::from_str(&keyring.get(&names[3]).unwrap()).unwrap();
    assert_eq!(
        saved,
        json!({"host": "127.0.0.1", "port": 3307, "user": "root", "password": "raw-pw"})
    );
    for name in [&names[1], &names[2], &names[4], &names[7]] {
        assert_eq!(keyring.get(name), None, "{name}");
    }
    assert_eq!(
        keyring.get(&good).as_deref(),
        Some(v2(DB_FINGERPRINT, "good-pw", false).as_str())
    );
    assert_eq!(keyring.get(&raw).as_deref(), Some("raw-pw"));
    assert_eq!(file_bytes(&old_store), before, "state.sqlite changed");

    let mut session = engine.session("t").unwrap();
    let listed = ok(&mut session, r#"{"command":"status"}"#);
    assert!(listed.data.get("connected").is_none());
    let toml = profiles_toml(&home);
    assert!(toml.contains("env = \"APP_DATABASE_URL\""), "{toml}");
    assert!(!toml.contains(names[6].as_str()), "{toml}");
    drop(session);
    drop(engine);

    let again = home.engine();
    assert_eq!(again.migration_notice(), None);
    assert_eq!(profiles_toml(&home), toml);
}

#[test]
fn older_ts_schema_without_password_refs_migrates() {
    let home = Home::new();
    ts_store(
        &home,
        false,
        &[
            (
                "plain",
                json!({"target": "postgres://app@db.example.com/sales", "read_only": false}),
            ),
            ("from_env", json!({"env": "APP_URL"})),
        ],
    );
    let engine = home.engine();
    let notice = engine.migration_notice().unwrap();
    assert!(notice.contains("migrated 2 profiles"), "{notice}");
    let toml = profiles_toml(&home);
    assert!(
        toml.contains(
            "[profiles.plain]\nurl = \"postgres://app@db.example.com/sales\"\nread_only = false"
        ),
        "{toml}"
    );
    assert!(
        toml.contains("[profiles.from_env]\nenv = \"APP_URL\"\nread_only = true"),
        "{toml}"
    );
}

#[test]
fn profiles_refuse_passwords_and_keep_them_bound_to_one_destination() {
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    expect_code(
        &mut session,
        &connect_json("postgres://app:secret@db.example.com/sales", false),
        Code::PermissionDenied,
    );
    let name = account("bound");
    let with_password = stateql::Command::Profile {
        action: stateql::ProfileCommand::Add {
            name: name.clone(),
            url: Some("postgres://app:secret@db.example.com/sales".into()),
            env: None,
            env_file: None,
            ask_password: false,
            read_write: false,
            force: false,
        },
    };
    assert_eq!(
        session.run(with_password).unwrap_err().code,
        Code::PermissionDenied
    );

    let Some(mut keyring) = Keyring::open() else {
        return;
    };
    keyring.track(&name);
    let run = |session: &mut stateql::Session<'_>, url: &str, force: bool| {
        session.run(stateql::Command::Profile {
            action: stateql::ProfileCommand::Add {
                name: name.clone(),
                url: Some(url.into()),
                env: None,
                env_file: None,
                ask_password: false,
                read_write: false,
                force,
            },
        })
    };
    run(&mut session, "postgres://app@db.invalid/sales", false).unwrap();
    assert_eq!(
        run(&mut session, "postgres://app@db.invalid/sales", false)
            .unwrap_err()
            .code,
        Code::InvalidCommand
    );
    keyring.set(
        &name,
        &json!({"host": "db.invalid", "port": 5432, "user": "app", "password": "pw"}).to_string(),
    );

    run(&mut session, "postgres://app@db.invalid/other_db", true).unwrap();
    assert!(
        keyring.get(&name).is_some(),
        "same destination keeps the password"
    );
    std::fs::write(
        home.path.join("home").join("profiles.toml"),
        format!(
            "[profiles.{name}]\nurl = \"postgres://app@evil.invalid/sales\"\nread_only = true\n"
        ),
    )
    .unwrap();
    let error = expect_code(
        &mut session,
        &json!({"command": "connect", "target": name}).to_string(),
        Code::CredentialMismatch,
    );
    assert!(
        error.message.contains("app@db.invalid"),
        "{}",
        error.message
    );

    run(&mut session, "postgres://app@new.invalid/sales", true).unwrap();
    assert_eq!(
        keyring.get(&name),
        None,
        "a new host deletes the old password"
    );

    let removed = session
        .run(stateql::Command::Profile {
            action: stateql::ProfileCommand::Remove { name: name.clone() },
        })
        .unwrap();
    assert_eq!(removed.data["removed"], json!(true));
    let listed = session
        .run(stateql::Command::Profile {
            action: stateql::ProfileCommand::List,
        })
        .unwrap();
    assert_eq!(listed.data["profiles"], json!([]));
}

#[test]
fn env_and_sqlite_profiles_connect_without_the_keyring() {
    let home = Home::new();
    let db = home.file("app.db");
    sqlite_fixture(&db);
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    let add = |name: &str, url: Option<String>, env: Option<&str>| stateql::Command::Profile {
        action: stateql::ProfileCommand::Add {
            name: name.into(),
            url,
            env: env.map(str::to_string),
            env_file: None,
            ask_password: false,
            read_write: true,
            force: false,
        },
    };
    session
        .run(add("local", Some(db.display().to_string()), None))
        .unwrap();
    session
        .run(add("from_env", None, Some("STQL_TEST_NOT_SET_ANYWHERE")))
        .unwrap();
    assert_eq!(
        session
            .run(add("bad name!", None, Some("X")))
            .unwrap_err()
            .code,
        Code::InvalidCommand
    );
    assert_eq!(
        session
            .run(add("bad_env", None, Some("1X")))
            .unwrap_err()
            .code,
        Code::InvalidCommand
    );

    let connected = ok(&mut session, r#"{"command":"connect","target":"local"}"#);
    assert_eq!(
        connected.data.get("read_only"),
        None,
        "the profile defaults to read-write"
    );
    ok(
        &mut session,
        &exec_json("INSERT INTO audit (note) VALUES ('x')", json!({})),
    );
    expect_code(
        &mut session,
        r#"{"command":"connect","target":"from_env"}"#,
        Code::CredentialMissing,
    );
    let listed = session
        .run(stateql::Command::Profile {
            action: stateql::ProfileCommand::List,
        })
        .unwrap();
    assert_eq!(
        listed.data["profiles"],
        json!([
            {"name": "from_env", "env": "STQL_TEST_NOT_SET_ANYWHERE", "read_only": false},
            {"name": "local", "url": db.display().to_string(), "read_only": false},
        ])
    );
}

#[test]
fn read_only_wins_over_a_read_write_profile_and_with_profile_keeps_the_mode() {
    let home = Home::new();
    let db = home.file("app.db");
    sqlite_fixture(&db);
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    std::fs::write(
        home.path.join("home").join("profiles.toml"),
        format!(
            "[profiles.local]\nurl = '{}'\nread_only = false\n",
            db.display()
        ),
    )
    .unwrap();
    let insert = exec_json("INSERT INTO audit (note) VALUES ('x')", json!({}));
    let connected = ok(
        &mut session,
        r#"{"command":"connect","target":"local","read_only":true}"#,
    );
    assert_eq!(connected.data["read_only"], json!(true));
    expect_code(&mut session, &insert, Code::ReadOnly);

    let command = session.with_profile("local", cmd(&insert)).unwrap();
    assert_eq!(
        session.run(command).unwrap_err().code,
        Code::ReadOnly,
        "--profile kept the read-only connection of the same profile"
    );
    let mut fresh = engine.session("fresh").unwrap();
    let command = fresh.with_profile("local", cmd(&insert)).unwrap();
    fresh.run(command).unwrap();
    assert_eq!(count(&db, "audit"), 1);

    let error = fresh.with_profile("typo", cmd(&insert)).unwrap_err();
    assert_eq!(
        error.hint.as_deref(),
        Some("stql connect <target|profile>; profiles: local")
    );
    let mut unconnected = engine.session("none").unwrap();
    let error = expect_code(
        &mut unconnected,
        &query_json("SELECT 1"),
        Code::NotConnected,
    );
    assert_eq!(
        error.hint,
        Some("stql connect <target|profile>; profiles: local".into())
    );
}

#[test]
fn env_profiles_read_a_missing_variable_from_their_env_file() {
    let home = Home::new();
    let db = home.file("app.db");
    sqlite_fixture(&db);
    let var = format!("STQL_TEST_ENV_FILE_{}", std::process::id());
    let env_file = home.file("app.env");
    std::fs::write(
        &env_file,
        format!(
            "# a comment\nOTHER=x\nexport {var}=\"sqlite:{}\"\n",
            db.display()
        ),
    )
    .unwrap();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    let add = |name: &str, env_file| stateql::Command::Profile {
        action: stateql::ProfileCommand::Add {
            name: name.into(),
            url: None,
            env: Some(var.clone()),
            env_file,
            ask_password: false,
            read_write: false,
            force: false,
        },
    };
    session.run(add("filed", Some(env_file.clone()))).unwrap();
    session
        .run(add("empty", Some(home.file("empty.env"))))
        .unwrap();
    std::fs::write(home.file("empty.env"), "").unwrap();

    let connected = ok(&mut session, r#"{"command":"connect","target":"filed"}"#);
    assert_eq!(
        connected.data["target"],
        json!(format!("sqlite:{}", db.display()))
    );
    let error = expect_code(
        &mut session,
        r#"{"command":"connect","target":"empty"}"#,
        Code::CredentialMissing,
    );
    assert!(
        error.message.contains(&var) && error.message.contains("empty.env"),
        "{}",
        error.message
    );
    let listed = session
        .run(stateql::Command::Profile {
            action: stateql::ProfileCommand::List,
        })
        .unwrap();
    assert_eq!(
        listed.data["profiles"][1]["env_file"],
        json!(env_file.display().to_string())
    );
}
