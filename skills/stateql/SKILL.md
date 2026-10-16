---
name: stateql
description: Query, inspect, and safely change SQLite, PostgreSQL, MySQL, MongoDB, and Redis databases with the `stql` CLI, which stores each result behind a short handle and returns compact JSON. Use when the user asks to look at a database schema, run SQL, read or page through rows, count or filter query results, export data, or make a guarded write.
---

# StateQL (`stql`)

`stql` runs a query once, stores the full result locally, and returns a short
preview plus a handle. Page, filter, count, and export from the handle without
running the query again. Output is one line of JSON per command.

If `stql --version` fails, ask the user before installing
`npm install -g @fadhilp/stateql` (Node.js 22.16+).

## Rules

1. Set a session once per task: `export STQL_SESSION=<short-task-name>`
   (PowerShell: `$env:STQL_SESSION = "<name>"`). Without it, every task shares
   the `default` session.
2. Connect read-only (the default). Use `--read-write` only when the user asked
   for a change.
3. Never add `--allow-destructive`, `--allow-unbounded`, or `--replay` unless the
   user explicitly approved that operation. They are approvals, not fixes.
4. Never put a password in a command. Remote URLs go in an environment variable
   that you pass with `--env`.
5. Pass values as parameters (`--param`), never by building SQL strings.
6. Do not rerun a query to see more rows. Use its handle.

## Connect

```bash
stql status                                  # already connected in this session?
stql connect ./app.sqlite                    # SQLite path, read-only
stql connect --env DATABASE_URL              # postgres://, mysql://, mongodb://, redis:// URL in the variable
stql connect --env DATABASE_URL --read-write # only for requested changes
```

`status` without a `connection` field means no connection is open.

## Read workflow

1. Find objects: `stql objects table --search order`
2. Read the shape: `stql object table orders` (returns the DDL or column list)
3. Query only the columns you need, with `ORDER BY`:
   `stql query 'SELECT id, total FROM orders WHERE status = $1 ORDER BY id' --param paid`
4. Read the response (see below). `total` is the full row count, so you do not
   need a separate `COUNT(*)`.
5. More rows: `stql rows <handle> --offset <next_offset> --limit 50`
   (default 20, maximum 1000).
6. Narrow down: `stql filter <handle> 'CAST(total AS REAL) > ?' --param 100`.
   This runs locally on the stored rows in SQLite syntax, always with `?`, and
   returns a new handle. PostgreSQL `numeric`/`bigint` and MySQL `DECIMAL` values
   are stored as strings (`"7.50"`), and SQLite treats every string as greater
   than every number, so `total > ?` silently matches all rows. Wrap such
   columns in `CAST(... AS REAL)`, or filter in SQL with a new query.
7. Save all rows: `stql export <handle> --output rows.csv` (`--format json|jsonl|csv`).

Use `--preview-rows N` (0 to 200) on `query` to change the preview size. Use
`--cache bypass` when data may have changed outside this session; writes made
through `stql` invalidate the cache automatically.

## Response format

```json
{"ok":true,"handle":"katcitg6un","total":331,"next_offset":5,"columns":["id","name"],"rows":[[2,"Ada"],[3,"Grace"],[5,"Linus"],[6,"Alan"],[8,"Edsger"]]}
```

| Field | Meaning |
| --- | --- |
| `handle` | Stable result handle. Pass it to `rows`, `filter`, `count`, `columns`, `show`, `export`. |
| `columns` + `rows` | Each row is an array in `columns` order. If rows have different keys (MongoDB documents), rows are objects and `columns` is absent. |
| `total` | Rows in the stored result. |
| `next_offset` | Present only when more rows exist. Absent means you have every row. |
| `alias` | Present only when you named the result (batch `as`). |
| `warnings` | For example `NON_DETERMINISTIC_PAGINATION`: add `ORDER BY` before paging. |

- Exact numbers keep their precision as strings: PostgreSQL `numeric` and
  `bigint`, MySQL `DECIMAL`.
- A long cell comes back as `{"type":"text","length":20000,"preview":"..."}`.
  Use `--max-cell-characters N` or `export` for the full value.
- Null fields and false `cached`/`duplicate` flags are omitted.
- `rows`, `count`, and `columns` do not repeat the handle you passed.

## Placeholders and shell quoting

| Database | Placeholders |
| --- | --- |
| SQLite, MySQL | `?` |
| PostgreSQL | `$1`, `$2`, ... |
| `filter` predicates (all databases) | `?` |

- Put SQL in single quotes. In bash and PowerShell, `"$1"` expands to an empty
  string and breaks the query.
- Use parameters instead of string literals, so you do not need quotes inside
  quotes: `--param 'Ada Lovelace'`.
- **Windows PowerShell 5.1** removes the double quotes inside JSON arguments, so
  `--params '[1]'` is fine but `stql redis query '{"command":"GET"}'` breaks.
  Send JSON on standard input instead: `'<one JSON command>' | stql pipe` (see
  below), or escape every inner quote as `\"`.

## Several steps in one call

`stql pipe` reads one JSON command per line and prints one response per line.
It stops at the first error unless you pass `--continue-on-error`. Fields use
snake_case; `as` gives a result a name that later lines can use.

```bash
printf '%s\n' \
  '{"command":"query","sql":"SELECT id, email FROM users WHERE status = $1 ORDER BY id","params":["active"],"as":"active"}' \
  '{"command":"filter","handle":"active","where":"email LIKE ?","params":["%@example.com"],"as":"example"}' \
  '{"command":"rows","handle":"example","offset":5,"limit":20}' | stql pipe
```

Command names match the CLI: `query`, `filter`, `rows`, `count`, `columns`,
`show`, `export`, `exec`, `plan`, `apply`, `objects.list`, `object.describe`,
`inspect`, `mongo.query`, `redis.query`, `transaction.begin`, and so on.
`stql batch commands.json` runs a JSON array from a file.

## Errors

Errors look like `{"ok":false,"error":{"code":"...","message":"...","retryable":false,"executed":false}}`.
Retry only when `retryable` is `true`. `executed:false` means nothing ran.

| Code | What to do |
| --- | --- |
| `INVALID_SQL` | Fix the SQL at the reported line and column. Check the placeholder style. |
| `QUERY_FAILED` | Read the database message (missing table or column, type error). Retry only if `retryable`. |
| `READ_ONLY_CONNECTION` | Ask the user before you reconnect with `--read-write`. |
| `UNBOUNDED_MUTATION` | Add a `WHERE` clause. Use `--allow-unbounded` only with user approval. |
| `DESTRUCTIVE_OPERATION_BLOCKED` | `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, and `REPLACE` need user approval, then `--allow-destructive`. |
| `POTENTIAL_DUPLICATE_WRITE` | The same write already ran (`previous_operation_id`). Do not use `--replay` unless the user wants it to run again. |
| `OUTCOME_UNKNOWN` | The write may or may not have committed. Check the data with a query before anything else. |
| `OUTPUT_LIMIT_EXCEEDED` | Results are capped at 10,000 rows and 16 MiB. Add `WHERE`/`LIMIT` or select fewer columns; page `rows` in steps of at most 1000. |
| `RESULT_NOT_FOUND`, `RESULT_EXPIRED` | Wrong session or the result expired (24 h). Run the query again. |
| `STALE_PLAN` | Data changed after `plan`. Create a new plan. |
| `TRANSACTION_FAILED` | A staged transaction is open. Reads wait until you commit or roll back. |
| `DEADLINE_EXCEEDED` | The default timeout is 30 s. Narrow the query or pass `--timeout-ms`. |
| `CONNECTION_FAILED`, `CONNECTION_NOT_FOUND` | Check `stql status`, the URL variable, and the network. |

## Writes

Writes need a read-write connection and the user's request. Read
[reference/writes.md](reference/writes.md) before `exec`, `plan`/`apply`, or a
transaction. In short:

```bash
stql exec 'UPDATE jobs SET claimed = 1 WHERE id = $1' --param 42 --idempotency-key claim-job-42
```

A successful write returns `affected_rows` and `status` (`committed`, or
`pending` inside a transaction).

## MongoDB and Redis

These use native commands, not SQL. Read [reference/native.md](reference/native.md)
before you use `stql mongo` or `stql redis`.

## Other commands

| Command | Use |
| --- | --- |
| `stql inspect columns\|indexes\|constraints <table>` | Column, index, and constraint details |
| `stql history --limit 10` | Recent commands in this session |
| `stql receipt <operation-handle>` | Full record of a write |
| `stql session summary` | Open results and recent writes |
| `stql help` | Command syntax |
