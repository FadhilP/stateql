# Batch and pipes

[StateQL](../README.md) · [Agent skill](../skills/stateql/SKILL.md) · [Connections](connections.md) · [CLI](cli.md) · [Results](results.md) · [Writes](writes.md) · [Batch](batch.md) · [Databases](databases.md) · [Library](library.md)

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
`actor.show`, `actor.set-name`, `filter`, `transaction.begin`, `session.summary`,
`alias.set`, `plan`, and `apply`. `actor.set-name` takes the display name in the
`name` field. Query commands may set `preview_rows` from 0 to 200. `rows` may
set `columns` to an array of column names; `summarize` takes `handle`. Native MongoDB
batches use `mongo.query`, `mongo.exec`, or `mongo.plan` with the command object
in `mongo`; the same cache, replay, idempotency, safety, and timeout fields
apply. Database commands may set `timeout_ms`; otherwise they use the 30-second
default.
