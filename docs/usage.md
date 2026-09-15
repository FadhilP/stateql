# Usage

[Home](../README.md) · [Usage](usage.md) · [Database support](databases.md) · [TypeScript library](library.md)

- [Connections and profiles](#connections-and-profiles)
- [CLI reference](#cli-reference)
- [Durable state and result reuse](#durable-state-and-result-reuse)
- [Write safety](#write-safety)
- [Batch and pipes](#batch-and-pipes)

## Connections and profiles

A connection accepts exactly one source: a direct target, `--env`,
`--credential-ref`, or `--profile`. Library and batch callers may additionally
attach a [password reference](library.md#password-references) to a literal
password-free remote target; it is not an additional connection source.

```bash
stql connect <target> [--name NAME] [--read-only|--read-write]
stql connect --env ENV [--name NAME] [--read-only|--read-write]
stql connect --credential-ref REF [--name NAME] [--read-only|--read-write]
stql connect --profile NAME
stql disconnect
stql status
```

Direct targets accept SQLite paths or password-free PostgreSQL, MySQL, MongoDB,
and Redis/Rediss URLs. Connections default to read-only unless a profile or an
explicit flag selects read-write access.

### Environment-backed credentials

PostgreSQL, MySQL, MongoDB, and Redis credentials should come from environment
variables. The variable must contain the complete connection URL, not only its
password. Environment-backed SQLite paths require an explicit `sqlite:` prefix.
The URLs below are placeholders; supply real values through your secret-management
workflow rather than recording them in shell history.

```bash
export APP_DATABASE_URL='postgres://user:password@host/app'
stql connect --env APP_DATABASE_URL --name app --read-only

export MYSQL_DATABASE_URL='mysql://user:password@host/app'
stql connect --env MYSQL_DATABASE_URL --name mysql-app --read-only

export MONGODB_URL='mongodb://user:password@host/app'
stql connect --env MONGODB_URL --name mongo-app --read-only

export REDIS_URL='rediss://user:password@host/0'
stql connect --env REDIS_URL --name redis-app --read-only

export SQLITE_DATABASE='sqlite:./app.sqlite'
stql connect --env SQLITE_DATABASE --name local --read-only
```

StateQL stores no PostgreSQL, MySQL, MongoDB, or Redis password.
Credential-bearing URLs must be supplied through `--env` or an opaque full-URL
credential reference. SQLite paths remain persisted as connection metadata.

### Local profiles

Profiles store exactly one connection target, environment-variable name, or
opaque credential reference together with read-only policy. A remote literal
target may additionally store a `password_ref`; SQLite, environment-backed, and
full-URL `credential_ref` profiles cannot. Credential values are never stored.
Profiles persist under `STQL_HOME` with other StateQL metadata, and list/show
responses include nullable `credential_ref` and `password_ref` fields.

```bash
stql profile add local ./app.sqlite --read-only
stql profile add production --env PROD_DATABASE_URL --read-only
stql profile add hosted --credential-ref 'vault://team/app' --read-only
stql profile list
stql profile show production
stql profile update production --read-only
stql connect local
stql connect --profile production
```

Credential references are bounded nonempty opaque strings; StateQL does not
apply environment-variable syntax or normalization to them. They can only be
resolved by a trusted host `CredentialResolver`, so the standalone CLI may
store them in profiles but cannot connect with them.

A bare connection target matching a profile name resolves to that profile;
otherwise it remains a path or database URL.

`profile update` changes subsequent connections, not an already-open connection.
Omitting the source keeps it; supplying a new source replaces the old one.
Password references are configured through the library or batch API, not a CLI
`--password-ref` flag. See [safe profile updates](library.md#safe-profile-updates)
for source-replacement and downgrade restrictions.

## CLI reference

```bash
stql connect <target> [--name NAME] [--read-only|--read-write]
stql connect --env ENV [--name NAME] [--read-only|--read-write]
stql connect --credential-ref REF [--name NAME] [--read-only|--read-write]
stql connect --profile NAME
stql disconnect
stql status
stql profile add|update NAME [TARGET | --env ENV | --credential-ref REF]
                           [--read-only|--read-write]
stql profile list|show|remove
stql session start|list|show|summary|close
stql query <sql> [--params JSON | --param VALUE...] [--cache auto|bypass|require]
                 [--preview-rows 0..200]
stql filter <result-handle> <predicate> [--params JSON | --param VALUE...]
stql exec <sql> [--params JSON | --param VALUE...] [--idempotency-key KEY] [--replay]
              [--allow-unbounded] [--allow-destructive]
stql mongo query|exec|plan '<EJSON command>' [--cache MODE] [--idempotency-key KEY]
                                  [--replay] [--allow-unbounded] [--allow-destructive]
stql redis query '<JSON command>' [--cache auto|bypass|require]
stql redis exec '<JSON command>' [--idempotency-key KEY] [--replay]
stql redis plan '<JSON command>'
stql show|count|columns <result-handle>
stql rows <result-handle> [--offset N] [--limit N]
stql alias set <name> <result-handle>
stql export <result-handle> --output FILE [--format json|jsonl|csv]
stql inspect schema|table|collection|collections|columns|indexes|constraints [name]
stql objects [KIND] [--schema NAME] [--search TEXT] [--offset N|--cursor CURSOR] [--limit N]
stql object KIND NAME [IDENTITY] [--schema NAME]
stql transaction begin|status|commit|rollback [--isolation LEVEL]
stql plan <sql> [--params JSON | --param VALUE...] [--allow-unbounded] [--allow-destructive]
stql apply <plan-handle>
stql history [--limit N] [--offset N] [--category statement|introspection|management]
             [--internal|--external]
stql receipt <operation-handle>
stql doctor
stql purge [expired|results|history|all]
stql capabilities
stql batch [commands.json|commands.jsonl|-] [--continue-on-error]
stql pipe [--continue-on-error]
```

### SQL parameters

For shell-safe positional parameters, repeat `--param`. JSON scalars become
their native types; other values remain strings.

```bash
stql exec "INSERT INTO users (name, status) VALUES (?, ?)" `
  --param Ada --param trial
```

Use `--params JSON` for a JSON array or named parameters. Use
`--params-file FILE` when JSON is awkward to quote; `--params-file -` reads
JSON from standard input.

### Catalog inspection

Use `objects` to list a bounded page and `object` to describe an entry:

```bash
stql objects table --search users --limit 50
stql object table users
```

SQL/MongoDB offsets are non-negative numbers; limits default to 50 and are at
most 200. Redis uses `--cursor` with the opaque numeric SCAN cursor string
returned as `next_offset`. Its limit is a SCAN `COUNT` hint with a hard 200-item
response bound. Redis pages are not snapshots and can be empty or contain
duplicates while keys change.

Search is a case-insensitive name substring for SQL/MongoDB and escaped glob
substring matching for Redis. No exact counts are forced. Supported kinds are
returned on every page: SQLite `table,view,trigger`; PostgreSQL
`table,view,function,trigger,enum`; MySQL `table,view,function,trigger`; MongoDB
`collection,view`; Redis `key`. PostgreSQL function identities include identity
arguments; pass the returned identity to `object` to distinguish overloads.

Library equivalents are `listObjects(filter, options?)` and
`describeObject(object, options?)`, where `object` is a returned structured
catalog entry. The older `inspect` commands remain available for SQL and
MongoDB, but not Redis. See [database support](databases.md) for native commands.

### Output modes

CLI output defaults to compact, one-line `agent` JSON. Successful responses
flatten useful data and expose the primary durable ID as `handle`. Errors retain
their complete error object. Empty warnings and tracing metadata are omitted.

```json
{"ok":false,"error":{"code":"UNBOUNDED_MUTATION","message":"Mutation has no WHERE clause.","retryable":false,"executed":false,"override_flag":"--allow-unbounded"}}
```

Other modes are:

- `--output json`: original pretty, verbose envelope.
- `--output jsonl`: verbose envelope on one line.
- `--output text`: short human-readable status.
- `--output silent`: only a successful handle.

Set `STQL_OUTPUT` to choose a mode globally. For `export`, `--output` names the
file, so use `STQL_OUTPUT` to choose the command's response mode. Library
responses always keep the full envelope.

### Deadlines and cancellation

Database commands accept `--timeout-ms N`; the default is 30,000 ms. `Ctrl+C`
cancels active work.

- SQLite runs in a killable child process so long synchronous statements cannot
  block StateQL's event loop.
- PostgreSQL combines server-side `statement_timeout` with client deadlines.
- MySQL deadlines destroy the active connection.
- MongoDB uses driver deadlines and closes stopped operations.

A timed-out or cancelled write may return `OUTCOME_UNKNOWN` when its commit
status cannot be proven. Cancellation stops that command's driver work; it does
not close the `StateQL` actor, and later commands remain usable.

## Durable state and result reuse

State metadata lives under `STQL_HOME`, or the platform data directory when
unset. StateQL keeps connections, sessions, handles, aliases, cache entries,
plans, transactions, history, and receipts available across CLI invocations.

### Sessions and actors

Set `STQL_SESSION` to select a named session and `STQL_ACTOR` to select an
attached actor. A session is a shared workspace: attached actors reuse its
connection, handles, aliases, cache, and state version. Plans and staged
transactions remain owned by the actor that created them.

Callers that omit `actor` keep the legacy behavior where the actor ID is the
session name.

### Result lifetime and limits

Result rows are materialized locally for durable access. By default, read cache
entries expire after five minutes, and materialized handles expire after 24
hours. Expired results and plans are deleted the next time StateQL opens.

By default, queries exceeding 10,000 rows or 16 MiB of serialized row data fail
before persistence. Narrow the `WHERE` clause, add `LIMIT`, or select fewer
columns. These caps bound persisted materialization; the independent deadline
bounds execution time. Native commands may impose stricter
[database-specific limits](databases.md).

Query responses preview five rows by default. Use `--preview-rows N` to return
between 0 and 200 preview rows without rerunning or changing the stored result.
Additional rows remain available through `stql rows <result-handle>`.

Command history keeps the latest 10,000 entries per session. SQLite cache reuse
also checks the database file signature. PostgreSQL, MySQL, and MongoDB cache
reuse is labeled `ttl_based` and is never authoritative.

StateQL limits persisted result payloads to 256 MiB by default. When that quota
is reached it removes the oldest unaliased results; aliases remain protected. A
single result that cannot fit fails with `STATE_QUOTA_EXCEEDED`. Configure the
limit with `maxStateBytes` in the library or `--max-state-bytes` in the CLI.
Cache and result retention can be configured with `cacheTtlSeconds` and
`resultTtlSeconds`, or their `--cache-ttl-seconds` and
`--result-ttl-seconds` CLI equivalents.

`stql doctor` checks SQLite integrity and stored payload shapes without printing
SQL, parameters, or result values. `stql purge` removes expired data by default;
use `results`, `history`, or `all` for explicit session cleanup. On POSIX
systems, StateQL removes group and world access from its state directory,
database, and SQLite sidecar files.

### Local filtering

`filter` evaluates one scalar SQLite predicate against a stored result. It
preserves source order, state metadata, and expiry, and never accesses the
original database.

Use parameters for values. Subqueries, query-shaping clauses, and
non-allowlisted functions are rejected. Common deterministic functions such as
`lower`, `upper`, `length`, and `coalesce` are supported.

## Write safety

Writes require a read-write connection. Destructive and unbounded SQL/MongoDB
operations require `--allow-destructive` and `--allow-unbounded`, respectively.
The flags are independent.

`plan` validates and stores a write for later application. A plan persists only
the flags explicitly supplied when it is created; `apply` never adds
authorization.

Use an idempotency key to protect retryable writes from duplicate execution:

```bash
stql exec "UPDATE jobs SET claimed = 1 WHERE id = ?" \
  --param 42 \
  --idempotency-key claim-job-42
```

If a write starts but StateQL cannot safely record its final outcome, it returns
`OUTCOME_UNKNOWN` and blocks automatic replay. Inspect database state before
using `--replay`. Interrupted commits remain fail-closed; stale `committing`
records become `outcome_unknown` after five minutes.

### Transactions

Transactions are staged in local state so they survive CLI invocations, then
executed atomically on commit. While a transaction is active, StateQL rejects
database reads, plans, connection changes, and disconnects. Commit or roll back
first.

SQLite supports `serializable`. PostgreSQL and MySQL also support
`repeatable read`, `read committed`, and `read uncommitted`. Server reads run
inside database-enforced read-only transactions. MySQL staged transactions
reject DDL because MySQL implicitly commits those statements.
MongoDB transactions use `snapshot` isolation and require a replica set or
sharded deployment; standalone servers do not support them.
Redis does not support StateQL staged transactions; see its
[native write guarantees](databases.md#native-redis).

## Batch and pipes

`batch` reads a JSON array from a `.json` file or JSONL from a `.jsonl` file.
`pipe` reads JSONL from standard input. Commands run sequentially and stop on
the first error unless `--continue-on-error` is set. Output defaults to one
compact `agent` JSON object per line.

Pipe commands directly:

```bash
printf '%s\n' \
  '{"command":"query","sql":"SELECT id, email FROM users ORDER BY id","as":"users"}' \
  '{"command":"filter","handle":"users","where":"email LIKE ?","params":["%@example.com"],"as":"example_users"}' \
  '{"command":"rows","handle":"example_users","limit":10}' |
  stql pipe
```

Or save a JSON array as `commands.json`:

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

```bash
stql batch commands.json
```

Batch fields use snake case. Supported command names match CLI paths, such as
`filter`, `transaction.begin`, `session.summary`, `alias.set`, `plan`, and
`apply`. Query commands may set `preview_rows` from 0 to 200. Native MongoDB
batches use `mongo.query`, `mongo.exec`, or `mongo.plan` with the command object
in `mongo`; the same cache, replay, idempotency, safety, and timeout fields
apply. Database commands may set `timeout_ms`; otherwise they use the 30-second
default.
