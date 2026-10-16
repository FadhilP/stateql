# Agent guide

[StateQL](../README.md) · [Agents](agents.md) · [Connections](connections.md) · [CLI](cli.md) · [Results](results.md) · [Writes](writes.md) · [Batch](batch.md) · [Databases](databases.md) · [Library](library.md)

The minimum an agent needs to query a database with `stql`. Read the other pages
only for the topic at hand.

## Setup

```bash
export STQL_SESSION=task-42            # keeps handles and aliases across calls
stql connect ./app.sqlite              # read-only by default
stql connect --env DATABASE_URL        # remote URL from an environment variable
```

## Read

```bash
stql objects table --search user       # find tables
stql object table users                # table DDL
stql query "SELECT id, name FROM users WHERE status = ? ORDER BY id" --param active
```

```json
{"ok":true,"handle":"katcitg6un","total":333,"next_offset":5,"columns":["id","name"],"rows":[[2,"Ada"],[3,"Grace"],[5,"Linus"],[6,"Alan"],[8,"Edsger"]]}
```

- Rows are arrays in `columns` order. If rows have different keys, they stay
  objects and `columns` is omitted.
- `next_offset` is present only when more rows exist.
- Null fields and false `cached`/`duplicate` flags are omitted.
- Long cells come back as `{"type":"text","length":N,"preview":"..."}`.
- `objects` uses the same `columns` + arrays shape.
- `rows`, `count`, and `columns` do not repeat the handle you passed.
- Result handles are short, stable aliases. Full `q_...` IDs also work.
- Writes return `handle`, `statement_type`, `affected_rows`, `status`, and
  `state_version_after`.

## Reuse a result without querying again

```bash
stql rows katcitg6un --offset 5 --limit 20
stql count katcitg6un
stql filter katcitg6un "name LIKE ?" --param "A%"
stql export katcitg6un --output users.csv
```

## Save tokens

- Select only the columns you need, and add `ORDER BY` and `LIMIT`.
- Use `--preview-rows 0` when you need only the handle and `total`.
- Use `stql pipe` to run several commands in one call:

```bash
printf '%s\n' \
  '{"command":"query","sql":"SELECT id, email FROM users ORDER BY id","as":"users"}' \
  '{"command":"count","handle":"users"}' | stql pipe
```

## Errors

Errors are `{"ok":false,"error":{"code":...,"message":...}}`. Follow
`suggested_action` or `override_flag` when present. Retry only when
`retryable` is true (lost connection, deadlock, lock timeout). SQL syntax errors
give the line, column, and nearby text. Do not replay a write that
returned `OUTCOME_UNKNOWN` until you have checked the database.

## Writes

Connect with `--read-write`. Use `stql exec` with `--idempotency-key KEY` for
retryable writes. Writes without `WHERE` need `--allow-unbounded`, and
destructive statements need `--allow-destructive`. See [write safety](writes.md).
