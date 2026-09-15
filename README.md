# StateQL

StateQL is a stateful database CLI and TypeScript library for AI agents and
automation. Query, change, and inspect databases while keeping results reusable
and operations traceable across commands.

- **SQL:** SQLite, PostgreSQL, and MySQL.
- **Native commands:** MongoDB (EJSON) and Redis (JSON).
- **Durable handles:** reuse, filter, page, count, alias, or export stored results
  without rerunning the original query.
- **Tracked writes:** inspect operations, review plans, and stage transactions
  where supported by the database.

Requires **Node.js 22.16 or newer** for the required `node:sqlite` APIs.

## Quick start

Install the CLI:

```bash
npm install -g @fadhilp/stateql
```

Connect to an existing SQLite database containing a `users` table. These shell
examples use Bash; see the [usage guide](docs/usage.md) for connection options and
SQL parameter handling.

```bash
export STQL_SESSION=audit
stql profile add local ./app.sqlite --read-only
stql connect local

stql query \
  "SELECT id, name, email FROM users WHERE status = ? AND created_at >= ? ORDER BY id LIMIT 50" \
  --param active \
  --param 2026-01-01
```

Parameters keep values separate from SQL. `ORDER BY` makes paging stable, and
`LIMIT` bounds work at the database. The default `agent` output is compact,
one-line JSON. An example response:

```json
{"ok":true,"handle":"q_k7m2v5x9c3d6f8h4j2n7p5r9tw","rows":[{"id":7,"name":"Ada","email":"ada@example.com"}],"truncated":false,"cached":false,"total":1,"next_offset":null}
```

Use the handle returned by your query in place of the example handle below.
Give the snapshot a readable alias, then work with the stored data:

```bash
stql alias set active-users q_k7m2v5x9c3d6f8h4j2n7p5r9tw
stql rows active-users --offset 0 --limit 10
stql count active-users
stql export active-users --output active-users.csv --format csv
stql filter active-users "email LIKE ?" --param "%@example.com"
```

These commands do not access the original database. `filter` creates a new
snapshot with its own handle. Repeating a query can reuse a valid cached result;
use `--cache bypass` when a fresh database read is required.

## Safety and state

- Connections are **read-only by default**. Writes require a read-write connection.
- Destructive and unbounded SQL/MongoDB operations require independent
  `--allow-destructive` and `--allow-unbounded` approvals, respectively.
- `plan` stores a write for review; `apply` never adds authorization. Use
  idempotency keys to protect retryable writes from duplicate execution.
- A write reported as `OUTCOME_UNKNOWN` must be inspected at the database before
  replaying it. Transactions are staged locally, not live interactive SQL
  transactions.
- Supply credential-bearing URLs through environment variables or a trusted
  library credential resolver, not literal command arguments or profiles.
- Results and history persist under `STQL_HOME` (or the platform data directory).
  Treat that state as database data; retention and size limits are configurable.

See [write safety](docs/usage.md#write-safety),
[credentials](docs/usage.md#environment-backed-credentials), and
[database restrictions](docs/databases.md) before using production data.

## Documentation

| Guide | Contents |
| --- | --- |
| [Usage](docs/usage.md) | Connections, CLI reference, results, sessions, limits, writes, transactions, batch and pipes |
| [Database support](docs/databases.md) | SQL dialects, diagnostics, maintenance, native MongoDB and Redis commands |
| [TypeScript library](docs/library.md) | Connection setup, response handling, cleanup, actors, workspaces, and credential resolution |

Run `stql --help` for command syntax or `stql capabilities` for capability details.

## TypeScript installation

```bash
npm install @fadhilp/stateql
```

The package exports `StateQL` and public TypeScript types. Start with the
[connected, cleanup-safe example](docs/library.md#getting-started).
