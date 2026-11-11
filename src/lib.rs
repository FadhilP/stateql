pub mod command;
mod config;
mod driver;
mod error;
mod guard;
mod response;
mod results;
mod secret;
mod store;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub use command::{Cli, Command, ExportFormat, Isolation, JsonArray, ProfileCommand};
pub use error::{Code, Error, Executed};
pub use response::{Response, json_line, text};

use config::Profile;
use driver::{Backend, Column, Driver, ObjectQuery, Statement};
use error::Result;
use results::RowSink;
use store::{
    Claim, HistoryEntry, NewOperation, Operation, PurgeScope, ResultMeta, Source, Status, Store,
};

const DEFAULT_PREVIEW_ROWS: u64 = 20;
const MAX_PREVIEW_ROWS: u64 = 200;
const MAX_PAGE_ROWS: u64 = 1000;
const STMT_HEAD_BYTES: usize = 4096;
const WATCHDOG_GRACE: Duration = Duration::from_secs(2);

static WATCHDOG: Mutex<Option<(Instant, Duration)>> = Mutex::new(None);

pub fn spawn_watchdog() {
    std::thread::spawn(|| {
        loop {
            std::thread::sleep(Duration::from_millis(100));
            let armed = *WATCHDOG.lock().expect("watchdog lock poisoned");
            if let Some((due, timeout)) = armed
                && Instant::now() >= due
            {
                let error = Error::new(
                    Code::DeadlineExceeded,
                    "The command did not stop at its deadline, so the process exited.",
                )
                .with_executed(Executed::Unknown)
                .with_hint(timeout_hint(timeout));
                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "{}", json_line(&Err(error)));
                let _ = out.flush();
                std::process::exit(i32::from(Code::DeadlineExceeded.exit_code()));
            }
        }
    });
}

fn arm_watchdog(armed: Option<(Instant, Duration)>) {
    *WATCHDOG.lock().expect("watchdog lock poisoned") = armed;
}

fn timeout_hint(timeout: Duration) -> String {
    format!(
        "--timeout-ms is {}; raise it or narrow the query",
        timeout.as_millis()
    )
}

pub struct Engine {
    home: PathBuf,
    keyring_service: String,
    migration_notice: Option<String>,
}

impl Engine {
    pub fn default_home() -> PathBuf {
        config::default_home()
    }

    pub fn open(home: &Path) -> Result<Engine> {
        Engine::open_with_keyring_service(home, secret::SERVICE)
    }

    pub fn open_with_keyring_service(home: &Path, keyring_service: &str) -> Result<Engine> {
        config::create_private_dir(home)?;
        Store::open(home)?;
        let migration_notice = secret::migrate_ts_profiles(home, keyring_service)?;
        Ok(Engine {
            home: home.to_path_buf(),
            keyring_service: keyring_service.to_string(),
            migration_notice,
        })
    }

    pub fn migration_notice(&self) -> Option<&str> {
        self.migration_notice.as_deref()
    }

    pub fn session(&self, name: &str) -> Result<Session<'_>> {
        if name.is_empty() {
            return Err(Error::invalid("The session name is empty."));
        }
        Ok(Session {
            engine: self,
            name: name.to_string(),
            store: Store::open(&self.home)?,
            connection: None,
            timeout: Duration::from_secs(30),
            atomic: None,
            notice: None,
        })
    }
}

struct Connection {
    driver: Box<dyn Driver>,
    backend: Backend,
    identity: String,
    read_only: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipeMode {
    StopOnError,
    ContinueOnError,
    Atomic(Option<Isolation>),
}

pub struct Session<'e> {
    engine: &'e Engine,
    name: String,
    store: Store,
    connection: Option<Connection>,
    timeout: Duration,
    atomic: Option<Vec<String>>,
    notice: Option<String>,
}

impl Session<'_> {
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    // The next response or error carries the notice, so stderr stays empty.
    pub fn set_notice(&mut self, notice: Option<String>) {
        self.notice = notice;
    }

    // Applies the global --profile option before the command runs.
    pub fn with_profile(&mut self, profile: &str, command: Command) -> Result<Command> {
        let profiles = config::load_profiles(&self.engine.home)?;
        if !profiles.profiles.contains_key(profile) {
            return Err(Error::invalid(format!("No profile named \"{profile}\"."))
                .with_hint(connect_hint(&profiles)));
        }
        match command {
            Command::Connect {
                target: None,
                env: None,
                read_write,
                read_only,
            } => Ok(Command::Connect {
                target: Some(profile.to_string()),
                env: None,
                read_write,
                read_only,
            }),
            Command::Connect { .. } | Command::Profile { .. } => Err(Error::invalid(
                "--profile does not work with profile, or with connect and a target.",
            )
            .with_hint("stql connect NAME")),
            command => {
                let same_profile = self
                    .store
                    .session(&self.name)?
                    .is_some_and(|row| row.source == Source::Profile(profile.to_string()));
                if !same_profile {
                    self.run(Command::Connect {
                        target: Some(profile.to_string()),
                        env: None,
                        read_write: false,
                        read_only: false,
                    })?;
                }
                Ok(command)
            }
        }
    }

    pub fn run(&mut self, command: Command) -> Result<Response> {
        let started = Instant::now();
        let name = command_name(&command);
        let loaded = load_statement(command);
        let stmt_head = loaded.as_ref().ok().and_then(statement_head);
        let result = self.with_watchdog(|session, deadline| {
            loaded.and_then(|command| session.dispatch(command, deadline))
        });
        let target = self
            .connection
            .as_ref()
            .map(|connection| connection.identity.clone());
        let entry = HistoryEntry {
            session: &self.name,
            command: name,
            target: target.as_deref(),
            ok: result.is_ok(),
            error_code: result
                .as_ref()
                .err()
                .and_then(|error| json!(error.code).as_str().map(str::to_string)),
            duration_ms: i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
            stmt_head,
        };
        let _ = self.store.add_history(&entry);
        let notice = self.notice.take();
        match result {
            Ok(response) => Ok(response.with("notice", notice)),
            Err(mut error) => {
                error.notice = notice;
                Err(error)
            }
        }
    }

    fn dispatch(&mut self, command: Command, deadline: Instant) -> Result<Response> {
        match command {
            Command::Connect {
                target,
                env,
                read_write,
                read_only,
            } => {
                let read_only = match (read_write, read_only) {
                    (true, true) => {
                        return Err(Error::invalid("Use --read-write or --read-only, not both."));
                    }
                    (true, false) => Some(false),
                    (false, true) => Some(true),
                    (false, false) => None,
                };
                self.connect(target, env, read_only, deadline)
            }
            Command::Disconnect {} => self.disconnect(),
            Command::Status { all } => self.status(all),
            Command::Profile { action } => {
                secret::profile_command(&self.engine.home, &self.engine.keyring_service, action)
            }
            Command::Query {
                stmt,
                param,
                params,
                name,
                preview_rows,
                max_rows,
                max_cell_characters,
                max_bytes,
                ..
            } => {
                let stmt = Statement {
                    text: stmt.unwrap_or_default(),
                    params: merge_params(param, params)?,
                };
                let limits = PageLimits::new(max_cell_characters, max_bytes);
                self.query(&stmt, name, preview_rows, max_rows, limits, deadline)
            }
            Command::Exec {
                stmt,
                param,
                params,
                idempotency_key,
                replay,
                allow_unbounded,
                allow_destructive,
                expect_rows,
                ..
            } => {
                let stmt = Statement {
                    text: stmt.unwrap_or_default(),
                    params: merge_params(param, params)?,
                };
                let request = WriteRequest {
                    idempotency_key,
                    replay,
                    allow_unbounded,
                    allow_destructive,
                    expect_rows,
                };
                self.exec(&stmt, &request, deadline)
            }
            Command::Receipt { handle } => self.receipt(&handle),
            Command::Rows {
                handle,
                offset,
                limit,
                columns,
                max_cell_characters,
                max_bytes,
            } => {
                let limits = PageLimits::new(max_cell_characters, max_bytes);
                self.rows(&handle, offset, limit, columns.as_deref(), limits)
            }
            Command::Filter {
                handle,
                predicate,
                param,
                params,
                name,
                max_cell_characters,
                max_bytes,
            } => {
                let params = merge_params(param, params)?;
                let limits = PageLimits::new(max_cell_characters, max_bytes);
                self.filter(&handle, &predicate, &params, name, limits)
            }
            Command::Summarize { handle } => self.summarize(&handle),
            Command::Export {
                handle,
                output,
                format,
                columns,
            } => self.export(&handle, &output, format, columns.as_deref()),
            Command::Objects {
                kind,
                search,
                limit,
                cursor,
            } => self.objects(kind, search, limit, cursor, deadline),
            Command::Describe { name } => self.describe(&name, deadline),
            Command::History { limit } => {
                let entries = self
                    .store
                    .history(&self.name, limit.unwrap_or(20).min(MAX_PAGE_ROWS))?;
                Ok(Response::default().with("history", entries))
            }
            Command::Purge {
                expired,
                session,
                all,
            } => self.purge(expired, session, all),
            Command::Doctor {} => self.doctor(),
            Command::Pipe { .. } => Err(Error::invalid("pipe cannot run inside another command.")),
        }
    }

    fn connect(
        &mut self,
        target: Option<String>,
        env: Option<String>,
        read_only: Option<bool>,
        deadline: Instant,
    ) -> Result<Response> {
        if self.atomic.is_some() {
            return Err(Error::invalid("connect cannot run inside pipe --atomic."));
        }
        let profiles = config::load_profiles(&self.engine.home)?;
        let (source, default_read_only) = match (target, env) {
            (None, Some(var)) => (Source::Env(var), true),
            (Some(target), None) => match profiles.profiles.get(&target) {
                Some(profile) => (Source::Profile(target), profile.read_only()),
                None => (Source::Target(normalize_target(&target)?), true),
            },
            _ => {
                return Err(Error::invalid(
                    "Give a target, a profile name, or --env VAR.",
                ));
            }
        };
        let read_only = read_only.unwrap_or(default_read_only);
        self.connection = None;
        let mut connection = self.open_connection(&source, read_only, deadline)?;
        connection.driver.verify(deadline)?;
        self.store.set_session(&self.name, &source, read_only)?;
        let response = Response::default()
            .with("session", &self.name)
            .with("target", &connection.identity)
            .with("backend", connection.backend.name())
            .with("read_only", read_only);
        self.connection = Some(connection);
        Ok(response)
    }

    fn open_connection(
        &self,
        source: &Source,
        read_only: bool,
        deadline: Instant,
    ) -> Result<Connection> {
        let url = match source {
            Source::Target(target) => target.clone(),
            Source::Env(var) => config::env_value(var, None)?,
            Source::Profile(name) => {
                let profiles = config::load_profiles(&self.engine.home)?;
                match profiles.profiles.get(name) {
                    Some(Profile::Url { url, .. }) => {
                        secret::profile_url(&self.engine.keyring_service, name, url)?
                    }
                    Some(Profile::Env { env, env_file, .. }) => {
                        config::env_value(env, env_file.as_deref())?
                    }
                    None => {
                        return Err(Error::new(
                            Code::NotConnected,
                            format!("Profile \"{name}\" no longer exists."),
                        )
                        .with_hint("profile list, then connect again"));
                    }
                }
            }
        };
        let backend = Backend::of(&url);
        let driver = driver::open(&url, read_only, deadline)?;
        Ok(Connection {
            driver,
            backend,
            identity: redact(&url),
            read_only,
        })
    }

    fn connection(&mut self, deadline: Instant) -> Result<&mut Connection> {
        if self.connection.is_none() {
            let Some(row) = self.store.session(&self.name)? else {
                let profiles = config::load_profiles(&self.engine.home)?;
                return Err(
                    Error::new(Code::NotConnected, "This session has no connection.")
                        .with_hint(connect_hint(&profiles)),
                );
            };
            let connection = self.open_connection(&row.source, row.read_only, deadline)?;
            self.store.touch_session(&self.name)?;
            self.connection = Some(connection);
        }
        Ok(self
            .connection
            .as_mut()
            .expect("connection was just opened"))
    }

    fn disconnect(&mut self) -> Result<Response> {
        if self.atomic.is_some() {
            return Err(Error::invalid(
                "disconnect cannot run inside pipe --atomic.",
            ));
        }
        self.connection = None;
        let removed = self.store.delete_session(&self.name)?;
        Ok(Response::default()
            .with("session", &self.name)
            .with("disconnected", removed))
    }

    fn status(&mut self, all: bool) -> Result<Response> {
        if all {
            let sessions: Vec<Value> = self
                .store
                .sessions()?
                .into_iter()
                .map(|row| json!({"name": row.name, "target": source_label(&row.source), "read_only": row.read_only, "used_at": row.used_at}))
                .collect();
            return Ok(Response::default().with("sessions", sessions));
        }
        let row = self.store.session(&self.name)?;
        let now = store::now();
        let results: Vec<Value> = self
            .store
            .results(&self.name, 20)?
            .into_iter()
            .map(|meta| {
                let mut result =
                    json!({"handle": meta.id, "total": meta.total, "created_at": meta.created_at});
                if let Some(name) = meta.name {
                    result["name"] = json!(name);
                }
                result
            })
            .collect();
        let writes: Vec<Value> = self
            .store
            .recent_operations(&self.name, 5)?
            .iter()
            .map(|op| json!({"handle": op.id, "status": display_status(op, &now), "statement_type": op.stmt["type"], "created_at": op.created_at}))
            .collect();
        let mut response = Response::default().with("session", &self.name);
        if let Some(row) = row {
            response = response
                .with("connected", true)
                .with("target", source_label(&row.source))
                .with("read_only", row.read_only);
        }
        Ok(response.with("results", results).with("writes", writes))
    }

    fn query(
        &mut self,
        stmt: &Statement,
        name: Option<String>,
        preview_rows: Option<u64>,
        max_rows: Option<u64>,
        limits: PageLimits,
        deadline: Instant,
    ) -> Result<Response> {
        check_result_name(name.as_deref())?;
        let preview = preview_rows
            .unwrap_or(DEFAULT_PREVIEW_ROWS)
            .min(MAX_PREVIEW_ROWS);
        let id = new_id('r');
        let mut sink = RowSink::create(
            self.store.result_path(&id),
            max_rows.unwrap_or(results::DEFAULT_MAX_ROWS),
            preview as usize,
        )?;
        let connection = self.connection(deadline)?;
        let next_cursor = connection.driver.read(stmt, &mut sink, deadline)?;
        let source = connection.identity.clone();
        self.store_result(sink, id, name, source, next_cursor, limits)
    }

    fn store_result(
        &mut self,
        sink: RowSink,
        id: String,
        name: Option<String>,
        source: String,
        next_cursor: Option<String>,
        limits: PageLimits,
    ) -> Result<Response> {
        let finished = sink.finish()?;
        let meta = ResultMeta {
            id,
            session: self.name.clone(),
            name,
            source,
            columns: finished.columns,
            total: finished.total,
            bytes: finished.bytes,
            created_at: store::now(),
            expires_at: store::later(store::RESULT_TTL_MS),
        };
        if let Err(error) = self.store.insert_result(&meta) {
            let _ = fs::remove_file(self.store.result_path(&meta.id));
            return Err(error);
        }
        let mut response = page(
            &meta,
            &meta.columns,
            &finished.preview,
            0,
            limits.cell_chars,
        );
        response.next_cursor = next_cursor;
        Ok(fit_bytes(response, 0, limits.bytes))
    }

    fn result(&self, handle: &str) -> Result<ResultMeta> {
        self.store.result(&self.name, handle)?.ok_or_else(|| {
            Error::new(
                Code::ResultNotFound,
                format!("No result \"{handle}\" in session \"{}\".", self.name),
            )
            .with_hint("results expire after 24 hours; run the query again")
        })
    }

    fn rows(
        &mut self,
        handle: &str,
        offset: Option<u64>,
        limit: Option<u64>,
        columns: Option<&str>,
        limits: PageLimits,
    ) -> Result<Response> {
        let meta = self.result(handle)?;
        let columns = results::select_columns(&meta.columns, columns)?;
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(DEFAULT_PREVIEW_ROWS).min(MAX_PAGE_ROWS);
        let rows = results::read_page(&self.store.result_path(&meta.id), &columns, offset, limit)?;
        let mut response = page(&meta, &columns, &rows, offset, limits.cell_chars);
        if limit == 0 {
            response.rows = None;
            response.next_offset = None;
        }
        Ok(fit_bytes(response, offset, limits.bytes))
    }

    fn filter(
        &mut self,
        handle: &str,
        predicate: &str,
        params: &[Value],
        name: Option<String>,
        limits: PageLimits,
    ) -> Result<Response> {
        check_result_name(name.as_deref())?;
        let source = self.result(handle)?;
        let id = new_id('r');
        let path = self.store.result_path(&id);
        let total = results::create_filtered(
            &self.store.result_path(&source.id),
            &path,
            &source.columns,
            predicate,
            params,
        )?;
        let meta = ResultMeta {
            id,
            session: self.name.clone(),
            name,
            source: format!("filter:{}", source.id),
            columns: source.columns,
            total,
            bytes: fs::metadata(&path)?.len(),
            created_at: store::now(),
            expires_at: store::later(store::RESULT_TTL_MS),
        };
        if let Err(error) = self.store.insert_result(&meta) {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        let preview = results::read_page(&path, &meta.columns, 0, DEFAULT_PREVIEW_ROWS)?;
        let response = page(&meta, &meta.columns, &preview, 0, limits.cell_chars);
        Ok(fit_bytes(response, 0, limits.bytes))
    }

    fn summarize(&mut self, handle: &str) -> Result<Response> {
        let meta = self.result(handle)?;
        let summary = results::summarize(&self.store.result_path(&meta.id), &meta.columns)?;
        Ok(Response {
            handle: Some(meta.id),
            total: Some(meta.total),
            ..Response::default()
        }
        .with("summary", summary))
    }

    fn export(
        &mut self,
        handle: &str,
        output: &Path,
        format: Option<ExportFormat>,
        columns: Option<&str>,
    ) -> Result<Response> {
        let meta = self.result(handle)?;
        let columns = results::select_columns(&meta.columns, columns)?;
        let format = format.unwrap_or_else(|| export_format(output));
        let exported =
            results::export(&self.store.result_path(&meta.id), &columns, output, format)?;
        Ok(Response {
            handle: Some(meta.id),
            ..Response::default()
        }
        .with("path", std::path::absolute(output)?.display().to_string())
        .with("format", format)
        .with("exported", exported))
    }

    fn objects(
        &mut self,
        kind: Option<String>,
        search: Option<String>,
        limit: Option<u64>,
        cursor: Option<String>,
        deadline: Instant,
    ) -> Result<Response> {
        let limit = limit.unwrap_or(100).clamp(1, MAX_PAGE_ROWS);
        let query = ObjectQuery {
            kind,
            search,
            limit,
            cursor,
        };
        let id = new_id('r');
        let mut sink = RowSink::create(
            self.store.result_path(&id),
            limit,
            limit.min(MAX_PREVIEW_ROWS) as usize,
        )?;
        let connection = self.connection(deadline)?;
        let next_cursor = connection.driver.objects(&query, &mut sink, deadline)?;
        let source = format!("objects:{}", connection.identity);
        let limits = PageLimits::new(None, None);
        self.store_result(sink, id, None, source, next_cursor, limits)
    }

    fn describe(&mut self, name: &str, deadline: Instant) -> Result<Response> {
        let connection = self.connection(deadline)?;
        let description = connection.driver.describe(name, deadline)?;
        Ok(Response::default().with("object", description))
    }

    fn purge(&mut self, expired: bool, session: Option<String>, all: bool) -> Result<Response> {
        let scope = match (expired, session, all) {
            (false, None, false) => PurgeScope::Session(self.name.clone()),
            (false, Some(name), false) => PurgeScope::Session(name),
            (true, None, false) => PurgeScope::Expired,
            (false, None, true) => PurgeScope::All,
            _ => {
                return Err(Error::invalid(
                    "Use at most one of --expired, --session, and --all.",
                ));
            }
        };
        let purged = self.store.purge(&scope)?;
        Ok(Response::default().with("purged", purged))
    }

    fn doctor(&mut self) -> Result<Response> {
        let home = &self.engine.home;
        let probe = home.join(format!("doctor.{}.tmp", std::process::id()));
        let writable = fs::write(&probe, b"ok").and_then(|()| fs::remove_file(&probe));
        let store_check = self.store.quick_check()?;
        let orphans = self.store.remove_orphans()?;
        let mut checks = vec![
            json!({"name": "home", "ok": writable.is_ok() && config::home_is_private(home), "detail": home.display().to_string()}),
            json!({"name": "store", "ok": store_check == "ok", "detail": store_check}),
            json!({"name": "orphan_results", "ok": true, "detail": format!("{orphans} removed")}),
        ];
        checks.extend(secret::doctor_checks(home, &self.engine.keyring_service));
        Ok(Response::default().with("checks", checks))
    }

    fn exec(
        &mut self,
        stmt: &Statement,
        request: &WriteRequest,
        deadline: Instant,
    ) -> Result<Response> {
        let session = self.name.clone();
        let connection = self.connection(deadline)?;
        if connection.read_only {
            return Err(Error::new(Code::ReadOnly, "This session is read-only.")
                .with_hint("stql connect <target> --read-write"));
        }
        let kind = connection.driver.classify(stmt)?;
        check_gates(&kind, request)?;
        if request.expect_rows.is_some() && connection.backend == Backend::Redis {
            return Err(driver::unsupported(
                "--expect-rows does not work with Redis.",
            ));
        }
        let hash = sha256_hex(&json!([connection.identity, stmt.text, stmt.params]).to_string());
        let identity = connection.identity.clone();
        let id = new_id('w');
        let flags = json!({
            "allow_unbounded": request.allow_unbounded,
            "allow_destructive": request.allow_destructive,
            "expect_rows": request.expect_rows,
        });
        let operation = NewOperation {
            id: &id,
            session: &session,
            target: &identity,
            stmt: json!({"type": kind.statement_type, "stmt": stmt.text}),
            hash: &hash,
            idempotency_key: request.idempotency_key.as_deref(),
            flags,
            deadline_at: deadline_at(deadline),
        };
        let running = self.atomic.as_deref().unwrap_or_default();
        match self
            .store
            .claim_operation(&operation, !request.replay, running)?
        {
            Claim::Inserted => {}
            Claim::Existing(previous) => return self.repeated_key(&previous, &hash),
            Claim::Unknown(previous) => {
                return Err(earlier_unknown(
                    &previous,
                    "check the data, then run exec again with --replay",
                ));
            }
        }
        let driver = &mut self
            .connection
            .as_mut()
            .expect("connection opened above")
            .driver;
        let outcome = match request.expect_rows {
            None => driver.execute(stmt, deadline),
            Some(expected) => driver.write_expecting(stmt, expected, deadline),
        };
        let ids = [id.clone()];
        let affected = match outcome {
            Ok(affected) => affected,
            Err(error) => return Err(self.write_failed(&id, error)),
        };
        let status = match &mut self.atomic {
            Some(pending) => {
                pending.push(id.clone());
                self.store.record_affected(&id, affected)?;
                "pending"
            }
            None => {
                self.store
                    .finish_operations(&ids, Status::Committed, Some(affected), None)?;
                "committed"
            }
        };
        Ok(Response {
            handle: Some(id),
            status: Some(status.into()),
            statement_type: Some(kind.statement_type),
            affected_rows: Some(affected),
            ..Response::default()
        })
    }

    fn write_failed(&mut self, id: &str, error: Error) -> Error {
        let ids = [id.to_string()];
        let in_atomic_pipe = self.atomic.is_some();
        let stored = match (error.code, error.executed) {
            (_, Executed::Unknown) => self.store.set_deadline(&ids, &store::now()),
            (Code::RowCountMismatch, _) if !in_atomic_pipe => self.store.finish_operations(
                &ids,
                Status::RolledBack,
                error.actual_rows,
                Some(&error),
            ),
            _ => self
                .store
                .finish_operations(&ids, Status::Failed, None, Some(&error)),
        };
        if let Err(store_error) = stored {
            return store_error;
        }
        if error.executed != Executed::Unknown {
            return error;
        }
        let mut unknown = Error::new(
            Code::OutcomeUnknown,
            format!("The write may or may not have run: {}", error.message),
        )
        .with_executed(Executed::Unknown)
        .with_hint("check the data; receipt shows the operation");
        unknown.previous_operation = Some(id.to_string());
        unknown
    }

    fn receipt(&self, handle: &str) -> Result<Response> {
        let operation = self.store.operation(&self.name, handle)?.ok_or_else(|| {
            Error::new(
                Code::ResultNotFound,
                format!("No write \"{handle}\" in session \"{}\".", self.name),
            )
        })?;
        Ok(receipt(&operation))
    }

    pub fn pipe(
        &mut self,
        input: impl BufRead,
        output: &mut dyn Write,
        mode: PipeMode,
    ) -> Result<u8> {
        if let PipeMode::Atomic(isolation) = mode {
            return self.atomic_pipe(input, output, isolation);
        }
        let mut input = BufReader::new(input);
        let mut buffer = String::new();
        let mut exit = 0;
        loop {
            // A read stays open only while the next line is already here, so it holds no lock while the caller waits.
            if !input.buffer().contains(&b'\n') {
                self.close_read();
            }
            buffer.clear();
            if input.read_line(&mut buffer)? == 0 {
                break;
            }
            let line = buffer.trim_end_matches(['\n', '\r']);
            if line.trim().is_empty() {
                continue;
            }
            let result = parse_pipe_line(line).and_then(|command| self.run(command));
            writeln!(output, "{}", json_line(&result))?;
            output.flush()?;
            if let Err(error) = result {
                if exit == 0 {
                    exit = error.code.exit_code();
                }
                if mode == PipeMode::StopOnError {
                    break;
                }
            }
        }
        Ok(exit)
    }

    // A failed ROLLBACK stays owed: the next command sends it again or reports the broken connection.
    fn close_read(&mut self) {
        let _ = self.with_watchdog(|session, _| match &mut session.connection {
            Some(connection) => connection.driver.close_read(),
            None => Ok(()),
        });
    }

    fn atomic_pipe(
        &mut self,
        input: impl BufRead,
        output: &mut dyn Write,
        isolation: Option<Isolation>,
    ) -> Result<u8> {
        let begun =
            self.with_watchdog(|session, deadline| session.begin_atomic(isolation, deadline));
        if let Err(error) = begun {
            writeln!(output, "{}", json_line(&Err(error.clone())))?;
            return Ok(error.code.exit_code());
        }
        self.atomic = Some(Vec::new());
        let mut failure = None;
        for line in input.lines() {
            let line = match line {
                Ok(line) => line,
                Err(error) => {
                    let error = Error::from(error);
                    let _ = writeln!(output, "{}", json_line(&Err(error.clone())));
                    failure = Some(error);
                    break;
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            let result = self
                .extend_atomic_deadline()
                .and_then(|()| parse_pipe_line(&line))
                .and_then(|command| self.run(command));
            let written = writeln!(output, "{}", json_line(&result)).and_then(|()| output.flush());
            if let Err(error) = result {
                failure = Some(error);
                break;
            }
            if let Err(error) = written {
                failure = Some(error.into());
                break;
            }
        }
        let ended = self.end_atomic(failure.is_none());
        let exit = match (&failure, &ended) {
            (Some(error), _) | (None, Err(error)) => error.code.exit_code(),
            (None, Ok(_)) => 0,
        };
        // The failed line is already printed; a rollback that worked adds nothing to it.
        if failure.is_none() || ended.is_err() {
            writeln!(output, "{}", json_line(&ended))?;
        }
        output.flush()?;
        Ok(exit)
    }

    fn with_watchdog<T>(
        &mut self,
        work: impl FnOnce(&mut Self, Instant) -> Result<T>,
    ) -> Result<T> {
        let deadline = Instant::now() + self.timeout;
        arm_watchdog(Some((deadline + WATCHDOG_GRACE, self.timeout)));
        let result = work(self, deadline);
        arm_watchdog(None);
        result.map_err(|error| match error.code {
            Code::DeadlineExceeded => error.with_hint(timeout_hint(self.timeout)),
            _ => error,
        })
    }

    // Pipe writes stay pending until COMMIT. If the watchdog ends the process, this deadline has passed, so they read as unknown.
    fn extend_atomic_deadline(&mut self) -> Result<()> {
        let ids = self.atomic.as_deref().unwrap_or_default();
        let deadline = Instant::now() + self.timeout;
        self.store.set_deadline(ids, &deadline_at(deadline))
    }

    fn repeated_key(&self, previous: &Operation, hash: &str) -> Result<Response> {
        if previous.hash != hash {
            return Err(Error::new(
                Code::IdempotencyConflict,
                format!("This key belongs to a different write ({}).", previous.id),
            )
            .with_hint("use a new --idempotency-key"));
        }
        let in_this_pipe = self
            .atomic
            .as_ref()
            .is_some_and(|ids| ids.contains(&previous.id));
        if previous.status == Status::Committed || in_this_pipe {
            return Ok(receipt(previous));
        }
        Err(earlier_unknown(
            previous,
            "check the data; to run it again, use a new --idempotency-key",
        ))
    }

    fn begin_atomic(&mut self, isolation: Option<Isolation>, deadline: Instant) -> Result<()> {
        let connection = self.connection(deadline)?;
        if matches!(connection.backend, Backend::Mongo | Backend::Redis) {
            return Err(driver::unsupported(format!(
                "pipe --atomic does not work with {}.",
                connection.backend.name()
            )));
        }
        connection.driver.begin(isolation)
    }

    fn end_atomic(&mut self, commit: bool) -> Result<Response> {
        let pending = self.atomic.take().unwrap_or_default();
        self.with_watchdog(|session, deadline| {
            session
                .store
                .set_deadline(&pending, &deadline_at(deadline))?;
            session.finish_atomic(&pending, commit, deadline)
        })
    }

    fn finish_atomic(
        &mut self,
        pending: &[String],
        commit: bool,
        deadline: Instant,
    ) -> Result<Response> {
        let driver = &mut self
            .connection
            .as_mut()
            .expect("atomic pipe holds a connection")
            .driver;
        if !commit {
            let rolled_back = driver.rollback();
            self.store
                .finish_operations(pending, Status::RolledBack, None, None)?;
            rolled_back?;
            return Ok(Response::default());
        }
        match driver.commit(deadline) {
            Ok(()) => {
                self.store
                    .finish_operations(pending, Status::Committed, None, None)?;
                Ok(Response {
                    status: Some("committed".into()),
                    ..Response::default()
                }
                .with("operations", pending))
            }
            Err(error) if error.executed == Executed::Unknown => {
                self.store.set_deadline(pending, &store::now())?;
                let mut unknown = Error::new(
                    Code::OutcomeUnknown,
                    format!("COMMIT did not answer: {}", error.message),
                )
                .with_executed(Executed::Unknown);
                unknown.previous_operation = pending.first().cloned();
                Err(unknown)
            }
            Err(error) => {
                self.store
                    .finish_operations(pending, Status::RolledBack, None, Some(&error))?;
                Err(error)
            }
        }
    }
}

struct WriteRequest {
    idempotency_key: Option<String>,
    replay: bool,
    allow_unbounded: bool,
    allow_destructive: bool,
    expect_rows: Option<u64>,
}

fn check_gates(kind: &driver::WriteKind, request: &WriteRequest) -> Result<()> {
    if kind.unbounded && !request.allow_unbounded {
        return Err(Error::new(
            Code::UnboundedMutation,
            format!("This {} has no row filter.", kind.statement_type),
        )
        .with_hint("--allow-unbounded"));
    }
    if kind.destructive && !request.allow_destructive {
        return Err(Error::new(
            Code::DestructiveOperation,
            format!("This {} can remove or overwrite data.", kind.statement_type),
        )
        .with_hint("--allow-destructive"));
    }
    Ok(())
}

fn parse_pipe_line(line: &str) -> Result<Command> {
    let command: Command = serde_json::from_str(line)
        .map_err(|error| Error::invalid(format!("Invalid pipe line: {error}")))?;
    let reads_stdin = matches!(&command, Command::Query { file: Some(path), .. } | Command::Exec { file: Some(path), .. } if path.as_os_str() == "-");
    if reads_stdin {
        return Err(Error::invalid("--file - cannot read stdin inside pipe."));
    }
    Ok(command)
}

fn load_statement(mut command: Command) -> Result<Command> {
    if let Command::Query { stmt, file, .. } | Command::Exec { stmt, file, .. } = &mut command {
        *stmt = Some(statement_text(stmt.take(), file.take())?);
    }
    Ok(command)
}

fn statement_text(stmt: Option<String>, file: Option<PathBuf>) -> Result<String> {
    match (stmt, file) {
        (Some(text), None) => Ok(text),
        (None, Some(path)) if path.as_os_str() == "-" => {
            let mut text = String::new();
            std::io::stdin().read_to_string(&mut text)?;
            Ok(text)
        }
        (None, Some(path)) => fs::read_to_string(&path)
            .map_err(|error| Error::invalid(format!("Cannot read {}: {error}", path.display()))),
        _ => Err(Error::invalid(
            "Give the statement or --file, but not both.",
        )),
    }
}

fn statement_head(command: &Command) -> Option<String> {
    let text = match command {
        Command::Query { stmt, .. } | Command::Exec { stmt, .. } => stmt.as_deref()?,
        Command::Filter { predicate, .. } => predicate,
        _ => return None,
    };
    let mut end = text.len().min(STMT_HEAD_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Some(text[..end].to_string())
}

fn merge_params(param: Vec<String>, params: Option<JsonArray>) -> Result<Vec<Value>> {
    match params {
        None => Ok(param.into_iter().map(Value::String).collect()),
        Some(JsonArray(values)) if param.is_empty() => Ok(values),
        Some(_) => Err(Error::invalid("Use --param or --params, not both.")),
    }
}

fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Connect { .. } => "connect",
        Command::Disconnect {} => "disconnect",
        Command::Status { .. } => "status",
        Command::Profile { .. } => "profile",
        Command::Query { .. } => "query",
        Command::Exec { .. } => "exec",
        Command::Receipt { .. } => "receipt",
        Command::Rows { .. } => "rows",
        Command::Filter { .. } => "filter",
        Command::Summarize { .. } => "summarize",
        Command::Export { .. } => "export",
        Command::Objects { .. } => "objects",
        Command::Describe { .. } => "describe",
        Command::History { .. } => "history",
        Command::Purge { .. } => "purge",
        Command::Doctor {} => "doctor",
        Command::Pipe { .. } => "pipe",
    }
}

#[derive(Clone, Copy)]
struct PageLimits {
    cell_chars: usize,
    bytes: Option<usize>,
}

impl PageLimits {
    fn new(cell_chars: Option<usize>, bytes: Option<usize>) -> PageLimits {
        PageLimits {
            cell_chars: cell_chars.unwrap_or(results::MAX_CELL_CHARS),
            bytes,
        }
    }
}

fn page(
    meta: &ResultMeta,
    columns: &[Column],
    rows: &[Vec<driver::Cell>],
    offset: u64,
    cell_chars: usize,
) -> Response {
    let end = offset + rows.len() as u64;
    Response {
        handle: Some(meta.id.clone()),
        name: meta.name.clone(),
        total: Some(meta.total),
        next_offset: (end < meta.total).then_some(end),
        columns: Some(columns.iter().map(|column| column.name.clone()).collect()),
        types: Some(
            columns
                .iter()
                .map(|column| column.type_name.clone())
                .collect(),
        ),
        rows: Some(results::rows_json(rows, columns, cell_chars)),
        ..Response::default()
    }
}

// Drops rows from the end until the JSON line fits; next_offset then points at the first dropped row.
fn fit_bytes(mut response: Response, offset: u64, max_bytes: Option<usize>) -> Response {
    let Some(max_bytes) = max_bytes else {
        return response;
    };
    let fits = |response: &Response| json_line(&Ok(response.clone())).len() <= max_bytes;
    let has_rows = response.rows.as_ref().is_some_and(|rows| !rows.is_empty());
    if !has_rows || fits(&response) {
        return response;
    }
    let rows = response.rows.take().unwrap_or_default();
    let with_rows = |count: usize| Response {
        rows: Some(rows[..count].to_vec()),
        next_offset: Some(offset + count as u64),
        ..response.clone()
    };
    // Every count below the full page has a next_offset, so the line grows with the count.
    let (mut low, mut high) = (0, rows.len() - 1);
    while low < high {
        let middle = (low + high).div_ceil(2);
        if fits(&with_rows(middle)) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    if low > 0 {
        return with_rows(low);
    }
    with_rows(0).with(
        "hint",
        "no row fits in --max-bytes; use --columns, a smaller --max-cell-characters, or a larger --max-bytes",
    )
}

// The wall-clock time at which the watchdog ends a command with this deadline.
fn deadline_at(deadline: Instant) -> String {
    let left = deadline.saturating_duration_since(Instant::now()) + WATCHDOG_GRACE;
    store::later(i64::try_from(left.as_millis()).unwrap_or(i64::MAX))
}

fn earlier_unknown(previous: &Operation, hint: &str) -> Error {
    let mut error = Error::new(
        Code::OutcomeUnknown,
        format!(
            "An identical earlier write ({}) has an unknown outcome or has not finished.",
            previous.id
        ),
    )
    .with_hint(hint);
    error.previous_operation = Some(previous.id.clone());
    error
}

fn display_status(operation: &Operation, now: &str) -> &'static str {
    match operation.status {
        _ if operation.outcome_unknown(now) => "unknown",
        Status::Pending => "pending",
        Status::Committed => "committed",
        Status::RolledBack => "rolled_back",
        Status::Failed => "failed",
    }
}

fn receipt(operation: &Operation) -> Response {
    let status = display_status(operation, &store::now());
    Response {
        handle: Some(operation.id.clone()),
        status: Some(status.into()),
        statement_type: operation.stmt["type"].as_str().map(str::to_string),
        affected_rows: operation.affected_rows,
        ..Response::default()
    }
    .with(
        "executed",
        if status == "unknown" {
            json!("unknown")
        } else {
            json!(null)
        },
    )
    .with("stmt", &operation.stmt["stmt"])
    .with("idempotency_key", &operation.idempotency_key)
    .with("created_at", &operation.created_at)
    .with("finished_at", &operation.finished_at)
    .with("error", &operation.error)
}

fn check_result_name(name: Option<&str>) -> Result<()> {
    let Some(name) = name else { return Ok(()) };
    let looks_like_handle = name.len() == 7
        && name.starts_with(['r', 'w'])
        && name.chars().all(|c| c.is_ascii_alphanumeric());
    if name.is_empty() || looks_like_handle {
        return Err(Error::invalid(format!("\"{name}\" cannot name a result."))
            .with_hint("use a name that does not look like a handle"));
    }
    Ok(())
}

fn export_format(output: &Path) -> ExportFormat {
    match output.extension().and_then(|extension| extension.to_str()) {
        Some("json") => ExportFormat::Json,
        Some("jsonl" | "ndjson") => ExportFormat::Jsonl,
        _ => ExportFormat::Csv,
    }
}

fn normalize_target(target: &str) -> Result<String> {
    if Backend::of(target) != Backend::Sqlite {
        if secret::url_has_secret(target) {
            return Err(Error::new(
                Code::PermissionDenied,
                "A target URL must not contain a password or token.",
            )
            .with_hint("stql profile add NAME URL --ask-password, or connect --env VAR"));
        }
        return Ok(target.to_string());
    }
    Ok(format!("sqlite:{}", driver::sqlite_path(target)?.display()))
}

fn connect_hint(profiles: &config::Profiles) -> String {
    let hint = "stql connect <target|profile>";
    if profiles.profiles.is_empty() {
        return hint.to_string();
    }
    let names: Vec<&str> = profiles.profiles.keys().map(String::as_str).collect();
    format!("{hint}; profiles: {}", names.join(", "))
}

fn source_label(source: &Source) -> String {
    match source {
        Source::Target(target) => redact(target),
        Source::Profile(name) => format!("profile:{name}"),
        Source::Env(var) => format!("env:{var}"),
    }
}

fn redact(target: &str) -> String {
    let Some((scheme, rest)) = target.split_once("://") else {
        return target.to_string();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let authority = match authority.rsplit_once('@') {
        Some((userinfo, host)) => {
            format!("{}@{host}", userinfo.split(':').next().unwrap_or_default())
        }
        None => authority.to_string(),
    };
    let tail = match tail.split_once('?') {
        Some((path, query)) => {
            let pairs: Vec<String> = query
                .split('&')
                .map(|pair| {
                    let key = pair.split('=').next().unwrap_or_default();
                    let lower = key.to_ascii_lowercase();
                    let secret = ["pass", "token", "secret", "key"]
                        .iter()
                        .any(|word| lower.contains(word));
                    if secret {
                        format!("{key}=***")
                    } else {
                        pair.to_string()
                    }
                })
                .collect();
            format!("{path}?{}", pairs.join("&"))
        }
        None => tail.to_string(),
    };
    format!("{scheme}://{authority}{tail}")
}

fn new_id(prefix: char) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
    let mut bytes = [0u8; 6];
    getrandom::fill(&mut bytes).expect("the OS random source is available");
    std::iter::once(prefix)
        .chain(
            bytes
                .iter()
                .map(|byte| ALPHABET[usize::from(byte % 32)] as char),
        )
        .collect()
}

fn sha256_hex(text: &str) -> String {
    secret::hex(&Sha256::digest(text.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_passwords_and_secret_query_values() {
        assert_eq!(
            redact("postgres://ada:pw@db:5432/app?sslmode=require"),
            "postgres://ada@db:5432/app?sslmode=require"
        );
        assert_eq!(redact("redis://:pw@cache"), "redis://@cache");
        assert_eq!(
            redact("mongodb://a:b@h1,h2/db?authToken=x"),
            "mongodb://a@h1,h2/db?authToken=***"
        );
        assert_eq!(redact("sqlite:/tmp/a.db"), "sqlite:/tmp/a.db");
    }
}
