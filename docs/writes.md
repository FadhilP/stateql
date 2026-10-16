# Write safety

[StateQL](../README.md) · [Agent skill](../skills/stateql/SKILL.md) · [Connections](connections.md) · [CLI](cli.md) · [Results](results.md) · [Writes](writes.md) · [Batch](batch.md) · [Databases](databases.md) · [Library](library.md)

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

## Transactions

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
