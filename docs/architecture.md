# Architecture

## Layout

```text
src/main.rs           arguments, the watchdog, printing, and the exit code
src/lib.rs            Engine and Session: the entry point of every command
src/command.rs        the Command enum: clap for the CLI, serde for pipe lines
src/response.rs       the response struct, JSON and --text output
src/error.rs          error codes, exit codes, retryable, executed
src/store.rs          stql.db: sessions, results, operations, history
src/results.rs        result files: write, page, filter, summarize, export
src/guard.rs          the read keyword check and the write gates
src/config.rs         the home directory, profiles.toml, env files
src/secret.rs         the OS keyring, the password dialog, the TypeScript migration
src/driver/mod.rs     the Driver trait and shared helpers
src/driver/<db>.rs    sqlite, postgres, mysql, mongo, redis
tests/                integration tests that run the engine and the binary
npm/stateql/          npm package: launcher and install script
skills/stateql/       agent skill shipped with the npm package
```

The crate is a library (`stateql`) and a binary (`stql`). The binary only parses arguments and prints. A different front end can call `Engine::open(home)?.session(name)?.run(command)`.

## Command flow

1. `main` parses the arguments with clap. A parse error becomes `INVALID_COMMAND` (exit 2), with a hint for removed TypeScript commands.
2. `Engine::open` creates the home directory, opens the store, and migrates TypeScript profiles one time.
3. `engine.session(STQL_SESSION)` opens a `Session`. With `--profile`, it connects first, unless the session is already on that profile.
4. `Session::run` arms the watchdog, calls `dispatch`, and adds a history row.
5. `main` prints one JSON line (or a `--text` table) and exits with the code of the error.

`pipe` runs step 4 for each stdin line, on the same `Session` and connection.

Each command is a new process. The session keeps only the target in `stql.db`, not an open connection. A command that needs the database connects again.

## Data model

```text
<home>/
  stql.db             the store (SQLite, WAL)
  results/<id>.db     one SQLite file per result, with one table `r`
  profiles.toml       saved profiles, without passwords
  state.sqlite        the TypeScript store, if any; read once, never changed
```

On Unix, the directories are mode 0700 and the files 0600.

Tables in `stql.db`:

- `sessions`: name, the source (a target, a profile, or an environment variable name), and `read_only`. A CHECK makes sure exactly one source is set.
- `results`: id, session, optional name (unique in a session), source, columns with type names and a `json` flag, `total`, `bytes`, and `expires_at`.
- `operations`: one row per write. Statement, `hash`, optional `idempotency_key` (unique in a session), flags, `status` (`pending`, `committed`, `rolled_back`, `failed`), affected rows, error, and `deadline_at`. At most 10,000 rows per session.
- `history`: command, target, result, error code, duration, and the first 4,096 bytes of the statement. Never parameters. At most 10,000 rows per session.

The store uses WAL with `synchronous = NORMAL`. Write claims and outcomes use `synchronous = FULL`, because they decide if a retry may run a write again. Every store write is a `BEGIN IMMEDIATE` transaction with a 5 s busy timeout, so parallel processes are safe.

When `user_version` is not the current schema version, `stql` deletes `stql.db` and all result files and starts again.

## Results

A read streams rows into a `RowSink`:

- The sink creates `results/<id>.db` with `journal_mode = OFF` and `synchronous = OFF`, because a failed result is deleted, not recovered.
- A writer thread inserts rows in batches of 1,024 in one transaction, while the driver reads the next rows.
- The sink keeps the first rows for the preview.
- Over `--max-rows` (default 100,000) or 64 MiB, the read stops with `RESULT_TOO_LARGE` and the file is deleted.
- Cells keep the SQLite storage class that the driver gives: integer, real, text, blob, or null. Exact decimals stay text.

After the read, `insert_result` adds the metadata row. In the same transaction it deletes a result with the same name, all expired results (24 h), and the oldest results while the store is over 1 GiB. `purge` also deletes result files older than 1 hour that have no metadata row.

Pages come from the result file in `rowid` order, so they are stable without `ORDER BY`. `rows`, `filter`, `summarize`, and `export` never query the database.

**`filter`** creates a new result file, attaches the source file as `src`, and runs:

```sql
INSERT INTO main.r SELECT ... FROM src.r WHERE rowid IN (SELECT ... WHERE (<predicate>)) ORDER BY rowid
```

A SQLite authorizer allows only `SELECT`, reads of `src.r`, inserts into `main.r`, and functions other than `load_extension`. It denies all else, so a predicate cannot read other files or write. In the predicate, exact decimal strings are cast to numbers; the copied rows keep the exact text.

**`summarize`** runs one query per column on up to 4 threads. Above 16 MiB, sorts use temp files, so memory stays small.

## Reads

A read has three guards. The first is in `stql`; the database enforces the others.

1. **Keyword check** (`guard::check_read`). After whitespace, comments, and `(`, the first keyword must be `SELECT`, `WITH`, `VALUES`, `TABLE`, `EXPLAIN`, `SHOW`, `DESCRIBE`, or `DESC`, or `PRAGMA` on SQLite. The comment rules follow each dialect. On MySQL, `/*!`, `OUTFILE`, and `DUMPFILE` anywhere in the text are refused.
2. **One statement.** Each driver runs the read through a prepared-statement API that refuses a second statement.
3. **Read-only on the database:**

| Backend | Guard |
|---|---|
| SQLite | A read-only connection opens the file with `SQLITE_OPEN_READ_ONLY`. A read-write connection sets `PRAGMA query_only` for the read. Every read also runs under an authorizer, because `query_only` alone lets `PRAGMA journal_mode` change the file. |
| PostgreSQL | `BEGIN READ ONLY`. Inside `pipe --atomic`: a savepoint with `SET LOCAL transaction_read_only = on`. |
| MySQL | `SET SESSION transaction_read_only = ON; START TRANSACTION READ ONLY`. Inside `pipe --atomic`, MySQL has no read-only savepoint: a read must parse and contain no write (`guard::may_write`), because a savepoint does not undo MyISAM writes. |

PostgreSQL and MySQL leave the read transaction open after the rows arrive and end it with the next statement, to save one round trip. In `pipe`, the read stays open only while the next line is already in the input buffer, so it holds no lock while the caller waits.

The database read-only error maps to `READ_ONLY`.

Known limit: a server function with side effects (for example `pg_terminate_backend`, `GET_LOCK`) is not blocked. For an agent you do not trust, use a read-only database user.

## Writes

`Session::exec` runs a write in this order:

1. **Mode.** A read-only session returns `READ_ONLY`.
2. **Classify.** SQL drivers parse the statement with `sqlparser` in their dialect (`guard::classify`). It must be one statement. Nested statements (for example a write in a CTE) count. The result is a statement type and two flags: `unbounded` and `destructive`. SQL that does not parse is `unparsed`, with both flags. MongoDB and Redis drivers classify their JSON commands.
3. **Gates.** A flag without its approval option returns `UNBOUNDED_MUTATION` or `DESTRUCTIVE_OPERATION`. Nothing has run.
4. **Claim.** `hash = sha256(target, statement, params)`. In one durable store transaction:
   - An idempotency key with a different hash returns `IDEMPOTENCY_CONFLICT`. With the same hash, a committed write returns its receipt; a failed or rolled-back write is deleted and runs again.
   - Unless `--replay`, a `pending` row with the same hash whose deadline has passed returns `OUTCOME_UNKNOWN`.
   - Else a `pending` row is inserted.
5. **Run.** `Driver::execute`, or `write_expecting` with `--expect-rows`. `write_expecting` opens a transaction, runs the write, and rolls back on a different count (`ROW_COUNT_MISMATCH`). MongoDB counts first and writes second.
6. **Finish.** The row becomes `committed`, `rolled_back`, or `failed`. If the driver cannot tell if the write ran (a timeout or a lost connection), the row stays `pending` with its deadline set to now, and the response is `OUTCOME_UNKNOWN`.

Because the row is `pending` before the write runs, a crash, a kill, or the watchdog leaves a row that later reads as `unknown`. No signal handling is needed.

**Atomic pipe.** `pipe --atomic` calls `Driver::begin` before the first line. Each write stays `pending` and its deadline moves forward with each line. After the last line, `commit` marks all rows `committed` in one store transaction. Any error rolls back and marks them `rolled_back`. If `COMMIT` does not answer, all rows read as unknown.

## Deadlines

Each command has a deadline (`--timeout-ms`, default 30 s). Drivers pass it to the database:

| Backend | Mechanism |
|---|---|
| SQLite | A timer thread calls `interrupt` on the connection. |
| PostgreSQL | `SET LOCAL statement_timeout`. |
| MySQL | `max_execution_time`, and a socket timeout 1 s after the deadline. |
| MongoDB | `maxTimeMS` on each operation. |
| Redis | A socket read timeout. |

A watchdog thread is the last guard. When a command runs 2 s past its deadline, it prints `DEADLINE_EXCEEDED` with `executed:"unknown"` and exits with code 4. A `pending` write then reads as unknown.

## Profiles and credentials

- `profiles.toml` holds each profile as a URL without a password, or as an environment variable name with an optional env file, plus `read_only` and an optional `note`.
- A password is in the OS keyring (service `stql`, account = profile name) as JSON with host, port, user, and password. At connect, `stql` compares the host, port, and user with the profile URL. A difference returns `CREDENTIAL_MISMATCH`, so an edited profile cannot send the password to a new host.
- `--ask-password` starts a system dialog, so the password never appears in the agent's terminal.
- Targets in responses and history pass through `redact`: the password is removed, and query values whose key contains `pass`, `token`, `secret`, or `key` become `***`.

**TypeScript migration.** If `<home>/state.sqlite` exists and `profiles.toml` does not, `Engine::open` copies the profiles and their keyring passwords to the new format. It does not change the old store or old keyring entries. The report is the `notice` of that first response.

## Drivers

```rust
trait Driver {
    fn read(&mut self, stmt: &Statement, sink: &mut RowSink, deadline: Instant) -> Result<Option<String>>;
    fn classify(&self, stmt: &Statement) -> Result<WriteKind>;
    fn execute(&mut self, stmt: &Statement, deadline: Instant) -> Result<u64>;
    fn begin(&mut self, isolation: Option<Isolation>) -> Result<()>;
    fn commit(&mut self, deadline: Instant) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn in_transaction(&self) -> bool;
    fn verify(&mut self, deadline: Instant) -> Result<()>;      // connect: prove the server answers
    fn close_read(&mut self) -> Result<()>;                      // end a read left open
    fn objects(&mut self, query: &ObjectQuery, sink: &mut RowSink, deadline: Instant) -> Result<Option<String>>;
    fn describe(&mut self, name: &str, deadline: Instant) -> Result<Value>;
    fn write_expecting(&mut self, stmt: &Statement, expected: u64, deadline: Instant) -> Result<u64>;
}
```

`read` and `objects` return an optional `next_cursor` (Redis `SCAN`, paged `objects`). The backend comes from the URL scheme; anything else is a SQLite path.

| Backend | Crate | Notes |
|---|---|---|
| SQLite | `rusqlite` (bundled) | Catalog from `PRAGMA`. |
| PostgreSQL | `postgres` + `tokio-postgres-rustls` | Catalog from `pg_catalog`. |
| MySQL | `mysql` with rustls | Catalog from `information_schema` and `SHOW CREATE TABLE`. Session time zone is UTC. |
| MongoDB | `mongodb` (sync) | JSON commands with an operation allowlist. Documents become rows: top-level fields in first-seen order, `_id` first. `describe` samples 100 documents. |
| Redis | `redis` with rustls | JSON commands with a command allowlist. A response is limited to 1 MiB. |

All TLS uses rustls, so there is no OpenSSL, and the Linux builds are static musl binaries.
