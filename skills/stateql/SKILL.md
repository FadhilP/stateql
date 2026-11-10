---
name: stateql
description: Query, inspect, and safely change SQLite, PostgreSQL, MySQL, MongoDB, and Redis databases with the `stql` CLI, which stores each result behind a short handle and returns compact JSON. Use when the user asks to look at a database schema, run SQL, read or page through rows, count or filter query results, export data, or make a guarded write.
---

# StateQL (`stql`)

`stql` runs a query one time and keeps the full result on this computer. It
returns a short preview and a handle. Use the handle to page, filter, summarize,
and export. These steps do not run the query again. Each command prints one line
of JSON on stdout, also for errors, so `2>&1` is not necessary.

If `stql --version` fails, ask the user to install the `stql` binary. Do not
install it yourself.

## Rules

1. Set a session one time for each task: `export STQL_SESSION=<short-task-name>`
   (PowerShell: `$env:STQL_SESSION = "<name>"`). If you do not, all tasks share
   the `default` session.
2. Connect read-only (the default). Use `--read-write` only when the user asked
   for a change.
3. Do not cut the output with `head -c` or `cut -c`. That makes the JSON
   invalid. Use `--max-bytes`, `--preview-rows`, or `--columns`.
4. Do not add `--allow-destructive`, `--allow-unbounded`, or `--replay` unless
   the user approved that operation. These flags are approvals, not fixes.
5. Do not put a password in a command. `stql` rejects a URL that contains a
   password (`PERMISSION_DENIED`). Use an environment variable with `--env`, or
   a profile.
6. Give values as parameters (`--param`, `--params`). Do not build SQL strings.
7. Do not run a query again to see more rows. Use its handle.

## Connect

```bash
stql status                                  # is this session connected?
stql connect ./app.sqlite                    # SQLite file, read-only. The file must exist.
stql connect --env DATABASE_URL              # postgres://, mysql://, mongodb://, or redis:// URL in the variable
stql connect --env DATABASE_URL --read-write # only for changes that the user asked for
stql connect <profile>                       # a saved profile, in the mode that it was saved with
stql connect <profile> --read-only           # read-only, also for a profile saved with --read-write
stql --profile <profile> query 'SELECT 1'    # connects first, so you do not need a separate connect
```

`--profile NAME` works before any command, also before `pipe`. If the session
is already connected to that profile, it keeps the connection and its mode. It
does not work with `profile` or with `connect TARGET`.

Profiles are in `profiles.toml` in the `stql` home directory.

- `stql profile add <name> --env VAR`: the full URL stays in the variable. Add
  `--env-file PATH` (for example `.env`) when the variable is in a file. The
  environment wins. Do not source the file yourself.
- `stql profile add <name> '<URL without password>' --ask-password`: the user
  types the password in a dialog. The password goes to the OS keyring. You do
  not see it. Use this command only when the user asks for it.
- `stql profile list` and `stql profile remove <name>`.

TLS for PostgreSQL (`sslmode`) and MySQL (`ssl-mode`) in the URL:

| PostgreSQL | MySQL | What it does |
| --- | --- | --- |
| no value, `disable` | no value, `DISABLED` | Plain TCP. No encryption. |
| `require` | `REQUIRED` | Encrypted. The certificate is not checked, so this does not protect against a server that is impersonated. |
| `verify-ca` | `VERIFY_CA` | Encrypted. Checks the certificate chain, not the host name. |
| `verify-full` | `VERIFY_IDENTITY` | Encrypted. Checks the chain and the host name. |

- For a private CA, add `sslrootcert=/path/ca.pem` (PostgreSQL) or
  `ssl-ca=/path/ca.pem` (MySQL). With a CA file, `require` and `REQUIRED` also
  check the chain. Without a CA file, the public web CAs are used.
- `prefer`, `allow`, and `PREFERRED` are refused, because they can fall back to
  plain TCP.
- MongoDB (`tls=true`, `tlsCAFile`, ...) and Redis (`rediss://`) use the
  options of their own drivers.

`connect` opens and pings the database one time. Later commands connect again
when they need the database.

## Read workflow

1. Find objects: `stql objects table --search order`
   (kinds: `table`, `view`, `index`, `trigger`, `function`, `enum`; MongoDB
   `collection`, `view`; Redis `key`).
2. Read the shape: `stql describe orders` (columns, keys, indexes, and DDL).
   A column shows `nullable:false`, `default`, and `primary_key:true` only
   when they apply. An index shows `unique:true` only when it is unique.
3. Query only the columns that you need:
   `stql query 'SELECT id, total FROM orders WHERE status = $1' --param paid`
4. Read the response (see below). `total` is the full row count, so you do not
   need a separate `COUNT(*)`.
5. More rows: `stql rows <handle> --offset <next_offset> --limit 50`
   (maximum 1000). `--columns id,email` returns only some columns.
   `--limit 0` returns only the metadata.
6. Learn about the data: `stql summarize <handle>` gives the type, null count,
   distinct count, min, and max of each column.
7. Make a smaller result: `stql filter <handle> 'CAST(total AS REAL) > ?' --params '[100]'`.
   The predicate is SQLite syntax with `?` placeholders. It runs on the stored
   rows and returns a new handle.
8. Save all rows to a new file: `stql export <handle> --output rows.csv`
   (`--format csv|json|jsonl`, optional `--columns`). `export` does not
   overwrite a file.

Notes for `filter`:

- `--param` always binds a string. For a number, use `--params '[100]'`.
  SQLite sorts every string after every number, so `total > ?` with
  `--param 100` matches no number.
- PostgreSQL `numeric`, MySQL `DECIMAL`, and MySQL `BIGINT UNSIGNED` values
  above 2^63 are stored as exact strings (`"7.50"`). Use
  `CAST(column AS REAL)` to compare them as numbers.

`query --preview-rows N` (0 to 200, default 20) sets the preview size.
`query --as NAME` gives the result a name. Use the name in place of the handle.

`query`, `rows`, and `filter` also take these options. They change only the
output of that call, not the stored result:

- `--max-cell-characters N` (default 200): read a long text cell, for example
  JSON or a query plan, in full.
- `--max-bytes N`: drop rows from the end of the page until the line fits in
  `N` bytes. `next_offset` is then the first dropped row, so
  `rows <handle> --offset <next_offset>` continues with no gap. If no row fits,
  `rows` is `[]` and `hint` tells you what to change.

## Response format

```json
{"ok":true,"handle":"rqj6d8m","total":331,"next_offset":20,"columns":["id","name"],"types":["integer","text"],"rows":[[1,"Ada"],[2,"Grace"]]}
```

| Field | Meaning |
| --- | --- |
| `handle` | Result handle (`r…`) or write handle (`w…`). Results stay for 24 hours, in this session only. |
| `columns`, `types`, `rows` | Each row is an array in `columns` order. `types` are the database type names. |
| `total` | Rows in the stored result. |
| `next_offset` | Only when more rows exist. If it is absent, you have all rows. |
| `next_cursor` | Only on `objects` and Redis `SCAN` pages. Give it to the next call. |

- Dates and times are ISO 8601 text. `timestamptz` and MySQL `TIMESTAMP` are UTC with `Z`.
- JSON columns and MongoDB nested values come back as JSON values.
- A text cell longer than the limit is an object, not a string:
  `{"type":"text","length":20000,"preview":"..."}`. Binary is
  `{"type":"bytes","length":N,"base64":"..."}`. Use `--max-cell-characters`
  or `export` to get the full value.
- Booleans are `1` and `0`.
- PostgreSQL `interval`, `inet`, `cidr`, `macaddr`, `timetz`, `pg_lsn`, `xid`,
  and `cid` come back as the text that `psql` shows (but an IPv6 address
  `::a.b.c.d` shows as hex words). For another type that
  `stql` cannot read, cast the column with `::text`.
- Null fields and `false` flags are not in the output.
- `notice` (also inside `error`) is a one-time message, for example about the
  profiles that `stql` copied from its TypeScript version. Tell the user.
- `--text` prints a table for people. Do not parse it.

## Placeholders and shell quoting

| Database | Placeholders |
| --- | --- |
| SQLite, MySQL | `?` |
| PostgreSQL | `$1`, `$2`, ... |
| `filter` predicates | `?` |

- Put SQL in single quotes. In bash and PowerShell, `"$1"` becomes an empty
  string and breaks the query.
- For long SQL, or SQL with quotes, write it to a file and use `--file query.sql`.
  `--file -` reads standard input. This works for `query` and `exec`. Git Bash
  paths (`/c/work/q.sql`) work; you do not need `cygpath`.
- `--params '[1, "Ada", null]'` binds typed JSON values. `--param` binds strings.
- PostgreSQL: `interval $1` is not valid. Write `$1::interval`.
- PostgreSQL array parameter: `--param '{a,b}'` with `= ANY($1::text[])`.
- **Windows PowerShell 5.1** removes the double quotes inside arguments. JSON
  arguments break. Send the command on standard input:
  `'{"command":"GET","args":["a"]}' | stql query --file -`, or use `stql pipe`.

## Many steps in one call

`stql pipe` reads one JSON command on each line and prints one response on each
line. All lines use one connection. It stops at the first error, unless you add
`--continue-on-error`. The exit code is the code of the first error.

```bash
printf '%s\n' \
  '{"command":"query","stmt":"SELECT id, email FROM users WHERE status = $1","params":["active"],"as":"active"}' \
  '{"command":"filter","handle":"active","predicate":"email LIKE ?","params":["%@example.com"],"as":"example"}' \
  '{"command":"rows","handle":"example","offset":20,"limit":20}' | stql pipe
```

Fields are the CLI long flag names in snake_case. The statement field is `stmt`.
`--timeout-ms` applies to each line. `stql --profile NAME pipe` connects one
time before the first line.

## Errors

```json
{"ok":false,"error":{"code":"UNBOUNDED_MUTATION","message":"This update has no row filter.","retryable":false,"executed":false,"hint":"--allow-unbounded"}}
```

Retry only when `retryable` is `true`. `executed` is `false` (nothing ran),
`true`, or `"unknown"`. Read `hint` first. For a missing column or table, it
gives the database suggestion, or the `describe` or `objects` command to run.

| Code (exit) | What to do |
| --- | --- |
| `INVALID_COMMAND`, `INVALID_SQL` (2) | Fix the command or the SQL. Check the placeholder style. |
| `CONNECTION_FAILED`, `NOT_CONNECTED` (3) | Check `stql status`, the URL variable, and the network. `hint` lists the saved profiles. |
| `CREDENTIAL_MISSING`, `CREDENTIAL_MISMATCH` (3) | The saved password is missing, or it is for a different host or user. Tell the user. |
| `QUERY_FAILED` (4) | Read the database message (missing table, type error). |
| `DEADLINE_EXCEEDED` (4) | The default limit is 30000 ms. Make the query smaller, or raise `--timeout-ms`. |
| `READ_ONLY` (5) | `query` runs only reads; use `exec`. On a read-only connection, ask the user before you connect with `--read-write`. |
| `UNBOUNDED_MUTATION`, `DESTRUCTIVE_OPERATION` (5) | Show the statement to the user. Add the flag in `hint` only after approval. |
| `OUTCOME_UNKNOWN` (6) | The write may have run. Query the data first. See [reference/writes.md](reference/writes.md). |
| `IDEMPOTENCY_CONFLICT` (6) | The key was used for a different statement. Use a new key. |
| `ROW_COUNT_MISMATCH` (7) | `--expect-rows` did not match. `actual_rows` gives the count. Check the data before you try again. |
| `RESULT_NOT_FOUND` (7) | Wrong handle or session, or the result expired. Run the query again. |
| `PERMISSION_DENIED` (8) | The command is not allowed (password in a URL, MongoDB `$out`, JavaScript). Do not try to get around it. |
| `UNSUPPORTED` (9) | This database does not support the feature. |
| `RESULT_TOO_LARGE` (1) | Results are limited to 100,000 rows and 64 MiB (`--max-rows`). Add `WHERE` or `LIMIT`, or select fewer columns. |

## Writes

Writes need a read-write connection and a request from the user. Read
[reference/writes.md](reference/writes.md) before you use `exec` or
`pipe --atomic`. In short:

```bash
stql exec 'UPDATE jobs SET claimed = 1 WHERE id = $1' --param 42 --expect-rows 1 --idempotency-key claim-job-42
```

## MongoDB and Redis

These use JSON commands, not SQL, with the same `query` and `exec` commands.
Read [reference/native.md](reference/native.md) first.

## Known limits

- Reads run in a read-only transaction, and `stql` blocks statements that are
  not reads. But a server function with side effects (for example
  `pg_terminate_backend`, `GET_LOCK`, `LOAD_FILE`) is not blocked. For an agent
  that you do not trust, connect with a read-only database user.
- `--expect-rows` on MongoDB counts the matching documents first, then writes.
  These are two steps, not one transaction. A change between them is not
  found.
- If a command does not stop at its deadline, `stql` exits with code 4 after
  2 more seconds. In `pipe`, this ends the whole pipe. Later lines do not run,
  and an atomic pipe does not commit.

## Other commands

| Command | Use |
| --- | --- |
| `stql receipt <write-handle>` | Full record of a write |
| `stql history --limit 10` | Recent commands in this session |
| `stql status --all` | All sessions |
| `stql purge` | Delete the results of this session (`--session NAME`, `--expired`, `--all`) |
| `stql doctor` | Check the home directory, the store, and the keyring |
| `stql help <command>` | Syntax of one command |
