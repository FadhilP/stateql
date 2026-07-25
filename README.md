# StateQL

StateQL is a stateful database CLI for AI agents and automation. It provides a
safe interface for querying, changing, and inspecting databases while keeping
results reusable and operations traceable across commands.

Requires Node.js 22.5 or newer.

## Quick start

Connect to an existing SQLite database, then run a filtered, parameterized
query. Parameters keep values separate from SQL; `ORDER BY` makes paging
stable, while `LIMIT` bounds work at the database.

```bash
npm install --global stateql
export STQL_SESSION=audit
stql profile add local ./app.sqlite
stql connect local

stql query \
  "SELECT id, name, email FROM users WHERE status = ? AND created_at >= ? ORDER BY id LIMIT 50" \
  --param active \
  --param 2026-01-01
```

Example output:

```json
{"ok":true,"handle":"q_1","rows":[{"id":7,"name":"Ada","email":"ada@example.com"},{"id":12,"name":"Grace","email":"grace@example.com"},{"id":18,"name":"Linus","email":"linus@kernel.org"}],"truncated":false,"cached":false,"total":3,"next_offset":null}
```

`q_1` is a durable result handle. Filter its stored snapshot without querying
the original database; the result becomes another durable handle:

```bash
stql filter q_1 "email LIKE ?" --param "%@example.com"
```

```json
{"ok":true,"handle":"q_2","rows":[{"id":7,"name":"Ada","email":"ada@example.com"},{"id":12,"name":"Grace","email":"grace@example.com"}],"truncated":false,"cached":false,"total":2,"next_offset":null}
```

Give the derived result a readable alias, page through it, inspect its count,
or export it—all without rerunning SQL:

```bash
stql alias set example-users q_2
stql rows example-users --offset 0 --limit 1
stql rows example-users --offset 1 --limit 1
stql count example-users
stql export example-users --output example-users.csv --format csv
```

Example first page:

```json
{"ok":true,"handle":"q_2","rows":[{"id":7,"name":"Ada","email":"ada@example.com"}],"total":2,"truncated":true,"next_offset":1}
```

Running the same normalized query with the same parameters reuses `q_1` while
its cache is valid. Use `--cache bypass` when a fresh read is required.

PostgreSQL credentials should come from an environment variable:

```bash
export APP_DATABASE_URL='postgres://user:password@host/app'
stql connect --env APP_DATABASE_URL --name app --read-only
```

## Commands

```text
stql connect <sqlite-path|postgres-url> [--name NAME] [--env ENV] [--read-write]
stql connect --profile NAME
stql status
stql profile add|list|show|remove
stql session start|list|show|summary|close
stql query <sql> [--params JSON | --param VALUE...] [--cache auto|bypass|require]
stql filter <result-handle> <predicate> [--params JSON | --param VALUE...]
stql exec <sql> [--params JSON | --param VALUE...] [--idempotency-key KEY] [--replay]
              [--allow-unbounded] [--allow-destructive]
stql show|count|columns <result-handle>
stql rows <result-handle> [--offset N] [--limit N]
stql export <result-handle> --output FILE [--format json|jsonl|csv]
stql inspect schema|table|columns|indexes|constraints [table]
stql transaction begin|status|commit|rollback [--isolation LEVEL]
stql plan <sql> [--allow-unbounded] [--allow-destructive]
stql apply <plan-handle>
stql history [--limit N]
stql receipt <operation-handle>
stql capabilities
stql batch [commands.json|commands.jsonl|-] [--continue-on-error]
stql pipe [--continue-on-error]
```

## Output modes

CLI output defaults to compact, one-line `agent` JSON. Successes flatten useful
data and expose the primary durable ID as `handle`; errors retain their complete
error object. Empty warnings and tracing metadata are omitted.

```json
{"ok":false,"error":{"code":"UNBOUNDED_MUTATION","message":"Mutation has no WHERE clause.","retryable":false,"executed":false,"override_flag":"--allow-unbounded"}}
```

Use `--output json` for the original pretty, verbose envelope, or
`--output jsonl` for that envelope on one line. `--output text` prints a short
human status; `--output silent` prints only a successful handle. Set
`STQL_OUTPUT` to choose a mode globally. For `export`, `--output` names the
file, so use `STQL_OUTPUT` to choose its response mode. Library responses keep
the full envelope regardless of CLI mode.

For shell-safe positional parameters, repeat `--param`. JSON scalars become
their native types; other values remain strings.

```powershell
stql exec "INSERT INTO users (name, status) VALUES (?, ?)" `
  --param Ada --param trial
```

Use `--params-file FILE` for arrays or named parameters that are awkward to
quote. `--params-file -` reads JSON from standard input.

## Local profiles

Profiles persist under `STQL_HOME` with other StateQL metadata.

```bash
stql profile add local ./app.sqlite --read-write
stql profile add production --env PROD_DATABASE_URL --read-only
stql profile list
stql connect local
stql connect --profile production
```

A bare connection target matching a profile name resolves to that profile;
otherwise it remains a path or database URL. Profiles store targets, read-only
policy, and environment-variable names. Credential values are never stored.

## Batch and pipes

`batch` reads a JSON array from a `.json` file or JSONL from a `.jsonl` file.
`pipe` reads JSONL from standard input. Commands run sequentially and stop on
the first error unless `--continue-on-error` is set. Output defaults to one
compact `agent` JSON object per line.

```bash
printf '%s\n' \
  '{"command":"query","sql":"SELECT id, email FROM users ORDER BY id","as":"users"}' \
  '{"command":"filter","handle":"users","where":"email LIKE ?","params":["%@example.com"],"as":"example_users"}' \
  '{"command":"rows","handle":"example_users","limit":10}' |
  stql pipe
```

```json
[
  {
    "command": "exec",
    "sql": "UPDATE jobs SET claimed = 1 WHERE id = ?",
    "params": [42],
    "idempotency_key": "claim-job-42"
  },
  {
    "command": "query",
    "sql": "SELECT * FROM jobs WHERE id = ?",
    "params": [42]
  }
]
```

Run the array with `stql batch commands.json`. Batch fields use snake case;
supported command names match CLI paths, such as `filter`,
`transaction.begin`, `session.summary`, `alias.set`, `plan`, and `apply`.
Batch filters use `where` for the predicate and may assign the derived result
with `as`.

State metadata lives under `STQL_HOME`, or the platform data directory when
unset. Set `STQL_SESSION` to select a named session.

Read cache entries expire after five minutes; materialized handles expire after
24 hours. Queries exceeding 10,000 rows fail before materialization; add a
narrower `WHERE` clause or `LIMIT`. Command history keeps the latest 10,000
entries per session. SQLite cache reuse also checks
the database file signature; PostgreSQL reuse is labeled `ttl_based`, never
authoritative. Transactions are staged in local state so they survive CLI
invocations, then executed atomically on commit. Connections cannot be changed
or disconnected while a transaction is active. SQLite supports `serializable`;
PostgreSQL also supports `repeatable read`, `read committed`, and
`read uncommitted`. PostgreSQL reads run inside database-enforced read-only
transactions.

StateQL stores no PostgreSQL password. Credential-bearing URLs must be supplied
through `--env`. SQLite result rows are materialized locally for durable access.
`filter` evaluates one scalar SQLite predicate against those stored rows, keeps
source order, state metadata, and expiry, and never accesses the original
database. Use parameters for values. Subqueries, query-shaping clauses, and
non-allowlisted functions are rejected; common deterministic functions such as
`lower`, `upper`, `length`, and `coalesce` are supported.

Destructive and unbounded operations require their respective flags
independently. Plans persist only flags explicitly supplied when the plan is
created; `apply` never adds authorization. If a database write starts but its
final outcome cannot be recorded safely, StateQL returns `OUTCOME_UNKNOWN` and
blocks automatic replay. Inspect database state before using `--replay`.
Interrupted commits remain fail-closed; stale `committing` records become
`outcome_unknown` after five minutes.

## Library

```ts
import { StateQL } from "stateql";

const stateql = new StateQL({ home: "./.stql" });
const response = await stateql.query("SELECT * FROM users");
if (response.ok) {
  const handle = (response.data as { result_id: string }).result_id;
  await stateql.filter(handle, "email LIKE ?", {
    params: ["%@example.com"],
  });
}
```
