# CLI reference

[StateQL](../README.md) · [Agent skill](../skills/stateql/SKILL.md) · [Connections](connections.md) · [CLI](cli.md) · [Results](results.md) · [Writes](writes.md) · [Batch](batch.md) · [Databases](databases.md) · [Library](library.md)

```bash
stql connect <target> [--name NAME] [--read-only|--read-write]
stql connect --env ENV [--name NAME] [--read-only|--read-write]
stql connect --credential-ref REF [--name NAME] [--read-only|--read-write]
stql connect --profile NAME
stql disconnect
stql status
stql actor show
stql actor set-name <display-name>
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
stql help
```

Global options: `--output MODE`, `--timeout-ms N`, `--max-cell-characters N`,
`--max-state-bytes N`, `--cache-ttl-seconds N`, `--result-ttl-seconds N`.

## SQL parameters

For shell-safe positional parameters, repeat `--param`. JSON scalars become
their native types; other values remain strings.

```bash
stql exec "INSERT INTO users (name, status) VALUES (?, ?)" `
  --param Ada --param trial
```

Use `--params JSON` for a JSON array or named parameters. Use
`--params-file FILE` when JSON is awkward to quote; `--params-file -` reads
JSON from standard input.

## Catalog inspection

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

## Output modes

CLI output defaults to compact, one-line `agent` JSON. Successful responses
flatten useful data and expose the primary durable ID as `handle`. Errors retain
their complete error object. Empty warnings and tracing metadata are omitted.

Agent mode also removes data that costs tokens but adds no information:

- `query`, `filter`, `show`, `mongo.query`, and `rows` return `columns` once and
  each row as an array in that order. When rows have different keys (for
  example, MongoDB documents), rows stay objects and `columns` is omitted.
- `next_offset` is present only when more rows exist.
- Null fields and `false` values of `cached`, `duplicate`, and
  `duplicate_override` are omitted.
- `NON_DETERMINISTIC_PAGINATION` is reported only when more rows exist.
- `history` lists `session_id`, `actor_id`, `actor_name`, and `origin` once
  when every entry shares them.
- `capabilities` lists enabled `features` as names.
- `objects` returns `columns` once and each entry as an array. `identity` is
  omitted when it equals `schema.name` (or `name`) for every entry.
- `rows`, `count`, and `columns` do not repeat the handle you passed.
- Result `handle` is the generated alias, which cannot be reassigned. Full
  `q_...` IDs work too and appear in `--output json`, `silent`, and history.
  `alias` is present only when you chose a different name (batch `as`).
- `exec`, `mongo.exec`, and `apply` omit `actor_id`, `plan_id`,
  `state_version_before`, and `committed` (use `status`). `receipt` keeps them.

```json
{"ok":true,"handle":"katcitg6un","total":333,"next_offset":2,"columns":["id","name"],"rows":[[2,"Ada"],[3,"Grace"]]}
```

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

## Deadlines and cancellation

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
