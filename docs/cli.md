# CLI reference

```text
stql [GLOBAL OPTIONS] <COMMAND>

stql connect     [TARGET | --env VAR] [--read-write | --read-only]
stql disconnect
stql status      [--all]
stql profile add <name> (<url> [--ask-password] | --env VAR [--env-file PATH]) [--read-write] [--note TEXT] [--force]
stql profile [list]          (alias: profiles)
stql profile remove <name>
stql query       (<stmt> | --file PATH) [--param V]... | [--params JSON] [--as NAME] [--preview-rows N] [--max-rows N]
                 [--max-cell-characters N] [--max-bytes N]
stql exec        (<stmt> | --file PATH) [--param V]... | [--params JSON] [--idempotency-key K] [--replay]
                 [--allow-unbounded] [--allow-destructive] [--expect-rows N]
stql receipt     <write-handle>
stql rows        <handle> [--offset N] [--limit N] [--columns A,B] [--max-cell-characters N] [--max-bytes N]
stql filter      <handle> <predicate> [--param V]... | [--params JSON] [--as NAME] [--preview-rows N]
                 [--max-cell-characters N] [--max-bytes N]
stql summarize   <handle>
stql export      <handle> --output FILE [--format csv|json|jsonl] [--columns A,B]
stql objects     [KIND] [--search S] [--limit N] [--cursor C]
stql describe    <name>
stql history     [--limit N]
stql purge       [--expired | --session NAME | --all]
stql doctor
stql pipe        [--continue-on-error | --atomic [--isolation read-committed|repeatable-read|serializable]]
stql help        [COMMAND]
```

## Global options

| Option | Default | Meaning |
|---|---|---|
| `--timeout-ms <MS>` | `30000` | Deadline of each command. In `pipe`, the deadline of each line. |
| `--text` | off | Print a table for people instead of JSON. Do not parse it. |
| `-p`, `--profile <NAME>` | — | Connect this session to the profile first, unless it is already connected to it. When it connects, the profile note is the `notice` of the response. Does not work with `profile`, or with `connect TARGET`. |

| Environment variable | Default | Meaning |
|---|---|---|
| `STQL_SESSION` | `default` | The session of this command. Each session has its own connection, results, writes, and history. |
| `STQL_HOME` | see below | Where `stql` keeps its state. |

The home directory is `%LOCALAPPDATA%\stql` on Windows, and `$XDG_DATA_HOME/stql` (or `~/.local/share/stql`) on Linux and macOS.

`help`, `--help`, and `--version` print plain text and exit 0. All other output is JSON.

## Targets

`connect TARGET` takes one of these:

| Target | Backend |
|---|---|
| `postgres://...`, `postgresql://...` | PostgreSQL |
| `mysql://...` | MySQL |
| `mongodb://...`, `mongodb+srv://...` | MongoDB. The URL must name a database. |
| `redis://...`, `rediss://...` | Redis |
| `sqlite:PATH`, or any other text | A SQLite file path. The file must exist. `:memory:` is refused. |
| a profile name | The saved profile. A profile name wins over a file path of the same name. |

- A URL in a command must not contain a password or a token. `stql` refuses it with `PERMISSION_DENIED`. Use `--env VAR` or a profile.
- `--env VAR` reads the full URL from the variable when it connects. The session stores only the variable name.
- A new connection is read-only. A profile uses the mode it was saved with. `--read-write` and `--read-only` override it.
- TLS options for PostgreSQL and MySQL: see the TLS table in [SKILL.md](../skills/stateql/SKILL.md#connect).

## Output

Each command prints one line of JSON on stdout, also for errors. `stql` writes nothing to stderr in normal operation. Null fields and `false` flags are left out.

```json
{"ok":true,"handle":"rqj6d8m","total":333,"next_offset":20,"columns":["id","name"],"types":["integer","text"],"rows":[[2,"Ada"],[3,"Grace"]]}
{"ok":true,"handle":"wck68sz","status":"committed","statement_type":"update","affected_rows":1}
{"ok":false,"error":{"code":"UNBOUNDED_MUTATION","message":"This update has no row filter.","retryable":false,"executed":false,"hint":"--allow-unbounded"}}
```

| Field | Meaning |
|---|---|
| `handle` | `r` and 6 characters for a result, `w` and 6 characters for a write. |
| `total` | Rows in the stored result. |
| `next_offset` | Only when more rows exist. Give it to `rows --offset`. |
| `next_cursor` | Only on `objects` and Redis `SCAN` pages. Give it to the next call. |
| `columns`, `types`, `rows` | Each row is an array in `columns` order. `types` are the database type names. |
| `notice` | A message for this call: the note of a profile that `--profile` connected, a `--param` that Git Bash converted to a Windows path, or the one-time report of the TypeScript profile migration. |

Cell values:

- Dates and times are ISO 8601 text. PostgreSQL `timestamptz` and MySQL `TIMESTAMP` are UTC with `Z`.
- JSON columns, PostgreSQL arrays, and MongoDB nested values are JSON values.
- PostgreSQL `numeric`, MySQL `DECIMAL`, and MySQL `BIGINT UNSIGNED` above 2^63 are exact strings.
- Booleans are `1` and `0`.
- A text cell over `--max-cell-characters` (default 200) is `{"type":"text","length":N,"preview":"..."}`. Binary is `{"type":"bytes","length":N,"base64":"..."}`.

`--max-bytes N` drops rows from the end of the page until the line fits in `N` bytes. `next_offset` is then the first dropped row.

## Commands

### `connect [TARGET]`

Opens the database, checks that it answers, and stores the target in the session. Later commands connect again when they need the database. Response: `session`, `target` (the URL without its password and secret query values), `backend`, `read_only`.

### `disconnect`

Forgets the connection of this session. Results stay.

### `status [--all]`

Without `--all`: `session`, `connected`, `target`, `read_only`, the last 20 `results`, and the last 5 `writes`. With `--all`: every session with its target, mode, and last use.

### `profile add | list | remove`

Profiles are in `<home>/profiles.toml`. A profile has no password in the file.

- `profile add NAME URL`: the URL must not contain a password. Add `--ask-password` to type the password in a system dialog (PowerShell on Windows, `osascript` on macOS, `zenity` on Linux). The password goes to the OS keyring.
- `profile add NAME --env VAR [--env-file PATH]`: the URL stays in the variable. `--env-file` reads `VAR` from a dotenv file when the environment does not set it.
- `--note TEXT` saves a note, for example `production replica: keep queries narrow`. `profile list` and `connect` show it. To add a note to a saved profile, run `profile add` again with `--force` (the same host, port, and user keep the password), or add `note = '...'` to `profiles.toml`.
- `--read-write` saves the profile read-write. `--force` replaces a profile. When the new URL points to a different host, port, or user, `--force` deletes the old password.
- `profile list` (or `profile`, or `profiles`) shows each profile without the URL query options; `connect` shows the full target.
- `profile remove NAME` also deletes its password.

### `query <stmt>`

Runs one read, stores the full result, and returns a preview. The statement is SQL, a MongoDB JSON command, or a Redis JSON command. `--file PATH` reads it from a file; `--file -` reads stdin.

| Option | Default | Meaning |
|---|---|---|
| `--param V` | — | Bind a string. Repeat for more. |
| `--params JSON` | — | Bind typed values from a JSON array. Cannot be used with `--param`. |
| `--as NAME` | — | Name the result. A later result with the same name replaces it. The name must not look like a handle. |
| `--preview-rows N` | `20` | Rows in the preview. Max 200. |
| `--max-rows N` | `100000` | Row cap of the stored result. A result is also limited to 64 MiB. Over a cap: `RESULT_TOO_LARGE`. |

Placeholders are native: `$1` for PostgreSQL, `?` for SQLite and MySQL. `query` refuses a statement that is not a read with `READ_ONLY` (hint: `use exec`).

### `exec <stmt>`

Runs one guarded write. It needs a read-write connection. See [Writes](architecture.md#writes) for the full flow.

| Option | Meaning |
|---|---|
| `--allow-unbounded` | Allow `UPDATE` or `DELETE` without `WHERE`, `TRUNCATE`, or an empty MongoDB filter. |
| `--allow-destructive` | Allow `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, `REPLACE`, and upserts that update. |
| `--expect-rows N` | Roll back unless exactly `N` rows change. Not for Redis. |
| `--idempotency-key K` | The same key and statement return the first receipt. |
| `--replay` | Run again although an identical earlier write has an unknown outcome. |

SQL that `stql` cannot parse needs both `--allow-unbounded` and `--allow-destructive`. The gate table per backend is in [reference/writes.md](../skills/stateql/reference/writes.md).

### `receipt <write-handle>`

The record of a write: `stmt`, `statement_type`, `status` (`pending`, `committed`, `rolled_back`, `failed`, or `unknown`), `affected_rows`, `idempotency_key`, `error`, `created_at`, and `finished_at`. It does not show parameter values.

### `rows <handle>`

Pages a stored result. `--offset` (default 0), `--limit` (default 20, max 1000), `--columns A,B`. `--limit 0` returns only the metadata. The handle can be the result name.

### `filter <handle> <predicate>`

Makes a new result from the rows that match a SQLite predicate, with `?` placeholders. It does not query the database. `--param` binds strings; use `--params '[100]'` for numbers. Exact decimal strings compare as numbers. `--as NAME` names the new result. `--preview-rows N` (default 20, max 200) sets the preview size.

### `summarize <handle>`

Per column: type, null count, distinct count, min, and max. Response field: `summary`.

### `export <handle> --output FILE`

Writes all rows to a new file. It never overwrites a file. Without `--format`, the extension picks it: `.json` is JSON, `.jsonl` and `.ndjson` are JSON Lines, all else is CSV. Response: `path`, `format`, `exported`.

### `objects [KIND]`

Lists database objects. Kinds: `table`, `view`, `index`, `trigger`, `function`, `enum`; MongoDB `collection`, `view`; Redis `key`. `--search S` filters by name. `--limit` (default 100, max 1000). `--cursor` continues from `next_cursor`.

### `describe <name>`

Columns, keys, indexes, and DDL of one object. A PostgreSQL index shows its definition from the access method on (`btree (created_at)`), and `unique: true` when it is unique. `describe` has no `--max-bytes`. MongoDB: options, indexes, and the fields of a 100-document sample. Redis: type, TTL in milliseconds, and a bounded value. Response field: `object`.

### `history [--limit N]`

Recent commands of this session, newest first (default 20, max 1000): command, target, result, error code, duration, and the first 4 KiB of the statement. Parameter values are never stored.

### `purge`

Deletes stored results. Default: this session. `--session NAME`: one session. `--expired`: expired results of all sessions. `--all`: all results. Response: `purged`.

### `doctor`

Checks the home directory, the store, the OS keyring, and old TypeScript state. Response: `checks`, each with `name`, `ok`, `detail`, and an optional `hint`.

### `pipe`

Reads one JSON command per line on stdin and prints one response per line. All lines share one connection.

```json
{"command":"query","stmt":"SELECT id FROM users WHERE status = $1","params":["active"],"as":"active"}
```

- Fields are the long option names in snake_case. The statement is `stmt`, the result name is `as`.
- `profile` and `pipe` cannot be pipe lines. `--file -` cannot read stdin inside a pipe.
- By default the pipe stops at the first error. `--continue-on-error` runs all lines. The exit code is the code of the first error.
- `--atomic` runs all lines in one transaction and commits after the last line. Writes report `status:"pending"` until then. An error on any line rolls back all lines. A last line `{"ok":true,"status":"committed","operations":[...]}` confirms the commit. `connect` cannot run inside it. Not for MongoDB and Redis.
- `--isolation` sets the level for PostgreSQL and MySQL. SQLite always uses `BEGIN IMMEDIATE`.

### Removed commands

These names from the TypeScript version give `INVALID_COMMAND` with a hint: `--cache`, `inspect`, `object` (use `describe`), `transaction` (use `pipe --atomic`), `plan`, `apply` (use `exec --expect-rows N`).

## MongoDB and Redis

These backends take JSON commands in `query` and `exec`, not SQL. Values go in the JSON, not in `--param`. The operations, allowed commands, and row shapes are in [reference/native.md](../skills/stateql/reference/native.md).

## Errors and exit codes

```json
{"ok":false,"error":{"code":"ROW_COUNT_MISMATCH","message":"...","retryable":false,"executed":false,"hint":"...","actual_rows":3}}
```

- `retryable` is `true` only for `CONNECTION_FAILED` and `DEADLINE_EXCEEDED`.
- `executed` is `false` (nothing ran), `true`, or `"unknown"`.
- Optional fields: `hint`, `previous_operation` (the write handle of an earlier identical write), `actual_rows`, `notice`.
- When `query` names a table that does not exist, `hint` names the closest table or view when one is near: `did you mean users? (stql objects --search TEXT)`. Column names get no suggestion.

| Exit | Codes |
|---|---|
| 0 | Success. |
| 1 | `RESULT_TOO_LARGE`, `INTERNAL_ERROR` |
| 2 | `INVALID_COMMAND`, `INVALID_SQL` |
| 3 | `CONNECTION_FAILED`, `NOT_CONNECTED`, `CREDENTIAL_MISSING`, `CREDENTIAL_MISMATCH` |
| 4 | `QUERY_FAILED`, `DEADLINE_EXCEEDED` |
| 5 | `READ_ONLY`, `UNBOUNDED_MUTATION`, `DESTRUCTIVE_OPERATION` |
| 6 | `OUTCOME_UNKNOWN`, `IDEMPOTENCY_CONFLICT` |
| 7 | `ROW_COUNT_MISMATCH`, `RESULT_NOT_FOUND` |
| 8 | `PERMISSION_DENIED` |
| 9 | `UNSUPPORTED` |

What to do for each code is in [SKILL.md](../skills/stateql/SKILL.md#errors).
