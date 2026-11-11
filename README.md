# StateQL

StateQL (`stql`) is a database CLI for AI agents and automation. It runs a
query one time and keeps the full result on this computer behind a short
handle. Later commands page, filter, summarize, or export the result. They do
not query the database again.

- **SQL:** SQLite, PostgreSQL, and MySQL.
- **JSON commands:** MongoDB (EJSON) and Redis.
- **Compact output:** one line of JSON for each command, with a preview of 20 rows.
- **Safe writes:** read-only by default. Unbounded and destructive statements
  need an approval flag. Row-count checks, idempotency keys, and atomic pipes.

## Install

```bash
npm install -g @fadhilp/stateql
```

The npm package holds the prebuilt binaries for Linux (x64, arm64; static, for
glibc and musl), macOS (x64, arm64), and Windows (x64, arm64). On a global
install, an install script points the `stql` command at the binary, so a call
does not start Node.js. If install scripts are off, `stql` still works through
a small Node.js launcher.

From source (Rust 1.89 or later):

```bash
cargo install --locked --git https://github.com/FadhilP/stateql
```

## Quick start

```bash
export STQL_SESSION=audit
stql connect ./app.sqlite
stql query 'SELECT id, name FROM users WHERE status = ? ORDER BY id' --param active
```

```json
{"ok":true,"handle":"rqj6d8m","total":333,"next_offset":20,"columns":["id","name"],"types":["integer","text"],"rows":[[2,"Ada"],[3,"Grace"]]}
```

Use the handle. These commands do not go to the database:

```bash
stql rows rqj6d8m --offset 20 --limit 20
stql filter rqj6d8m 'name LIKE ?' --param 'A%'
stql export rqj6d8m --output users.csv
```

For a remote database, put the URL in an environment variable
(`stql connect --env DATABASE_URL`), or save a profile
(`stql profile add NAME --env VAR [--env-file .env]`, or
`stql profile add NAME 'URL without password' --ask-password`). A URL in a
command must not contain a password. `stql --profile NAME <command>` connects
to a profile first, so one call does not need a separate `connect`.

`stql help` shows all commands. `stql doctor` checks the home directory, the
store, and the OS keyring.

## State

`stql` keeps its state in `%LOCALAPPDATA%\stql` on Windows and in
`$XDG_DATA_HOME/stql` (default `~/.local/share/stql`) on other systems.
`STQL_HOME` changes this. Results expire
after 24 hours. On the first run, `stql` copies the profiles of the TypeScript
version from `state.sqlite` to `profiles.toml`. It does not change the old
files or keyring entries. The response of that first command has a `notice`
field with the report. In normal operation, `stql` writes nothing to stderr.

## Agent skill

`skills/stateql` is an [Agent Skill](https://agentskills.io). It teaches coding
agents to use `stql` safely and with few tokens. Copy the folder into the
skills directory of your agent, for example for Claude Code:

```bash
cp -r "$(npm root -g)/@fadhilp/stateql/skills/stateql" ~/.claude/skills/
```

## Benchmarks

`stql` 1.1.0 compared with the native CLI of each database. Each engine reads
100,000 `orders` rows of the same shape (`id`, `customer`, `status`, `amount`,
a timestamp, and a text `note` with some `NULL`s). PostgreSQL reads them from
a table of 1,000,000 rows. Time is the median wall time of the whole sequence
of calls, and each call is a new process.

### Query once, then page, filter, and summarize

`stql` runs the query one time and works on the stored result. The native tool
runs the query again for each step: `LIMIT 20`, `LIMIT 20 OFFSET`, a `WHERE`
filter, and an aggregate query for the summary (`find`, `skip`, and
`aggregate` in `mongosh`).

On all 100k rows:

| Engine | Native tool | `stql` | Native | Result |
|---|---|---:|---:|---|
| SQLite | `sqlite3` | 266 ms | 340 ms | `stql` 1.3× faster |
| PostgreSQL | `psql` | 291 ms | 672 ms | `stql` 2.3× faster |
| PostgreSQL, remote | `psql` | 538 ms | 1,246 ms | `stql` 2.3× faster |
| MySQL | `mysql` | 260 ms | 401 ms | `stql` 1.5× faster |
| MongoDB | `mongosh` | 449 ms | 3,906 ms | `stql` 8.7× faster |

On a `GROUP BY customer` result (5,000 rows):

| Engine | Native tool | `stql` | Native | Result |
|---|---|---:|---:|---|
| SQLite | `sqlite3` | 123 ms | 188 ms | `stql` 1.5× faster |
| PostgreSQL | `psql` | 136 ms | 290 ms | `stql` 2.1× faster |
| PostgreSQL, remote | `psql` | 380 ms | 958 ms | `stql` 2.5× faster |
| MySQL | `mysql` | 117 ms | 297 ms | `stql` 2.5× faster |
| MongoDB | `mongosh` | 155 ms | 4,001 ms | `stql` 26× faster |

### One query

20 rows:

| Engine | Native tool | `stql` | Native | Result |
|---|---|---:|---:|---|
| SQLite | `sqlite3` | 16 ms | 7 ms | `stql` 2.3× slower |
| PostgreSQL | `psql` | 39 ms | 37 ms | About the same |
| PostgreSQL, remote | `psql` | 313 ms | 210 ms | `stql` 1.5× slower |
| MySQL | `mysql` | 22 ms | 23 ms | About the same |
| MongoDB | `mongosh` | 23 ms | 971 ms | `stql` 42× faster |

All 100k rows (the native tool prints all of them; `stql` stores them and
prints 20):

| Engine | Native tool | `stql` | Native | Result |
|---|---|---:|---:|---|
| SQLite | `sqlite3` | 86 ms | 313 ms | `stql` 3.6× faster |
| PostgreSQL | `psql` | 126 ms | 168 ms | `stql` 1.3× faster |
| PostgreSQL, remote | `psql` | 366 ms | 271 ms | `stql` 1.4× slower |
| MySQL | `mysql` | 93 ms | 320 ms | `stql` 3.4× faster |
| MongoDB | `mongosh` | 226 ms | 1,743 ms | `stql` 7.7× faster |

### Notes

- `stql` is made for the first two tables: a result that you look at more than
  one time. Its advantage grows with the cost of the query and with the
  network latency.
- For one remote read, `stql` waits for 2 more round trips than `psql`: it
  opens a read-only transaction with a deadline, and it prepares the
  statement to get the column types.
- `sqlite3` reads the database file in its own process. `stql` also opens its
  result store.
- `mongosh` takes about 0.9 s to start for each call.
- Redis commands take microseconds, so a stored result saves little. Also,
  `redis-cli` ran inside WSL, where a process starts faster than on Windows.

Method: Windows 11, Intel i7-12700, 32 GB RAM, release build. The servers run
on `127.0.0.1`: PostgreSQL 18 (`psql -X -q -At`), MySQL 9.7 (`mysql -N -B`),
SQLite 3.32 (`sqlite3`), MongoDB 6.0 (`mongosh` 2.14 with `--quiet
--json=relaxed`), and Redis 7.0 in WSL 2 (`redis-cli`, timed inside WSL).
"Remote" sends PostgreSQL through a proxy that adds about 60 ms to each round
trip. Each sequence runs 3 to 10 times after one warm-up run.

## License

[MIT](LICENSE)
