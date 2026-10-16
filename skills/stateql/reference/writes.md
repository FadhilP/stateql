# Writes, plans, and transactions

All writes need a read-write connection (`stql connect ... --read-write`) and a
change the user asked for.

## Safety gates

| Statement | Needs |
| --- | --- |
| `UPDATE`/`DELETE` without `WHERE`, `TRUNCATE` | `--allow-unbounded` |
| `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, `REPLACE`, SQLite `INSERT/UPDATE OR REPLACE` | `--allow-destructive` |
| MongoDB empty update/replace/delete filter | `--allow-unbounded` |
| MongoDB `deleteOne`, `deleteMany`, `replaceOne` | `--allow-destructive` |

The two flags are independent: a `DELETE` without `WHERE` needs both. Add a
flag only after the user approved that exact operation. When a gate blocks a
command, the error has `override_flag`; show the user the statement and ask.

## Direct write

```bash
stql exec 'UPDATE jobs SET claimed = 1 WHERE id = $1' --param 42 --idempotency-key claim-job-42
```

```json
{"ok":true,"handle":"op_...","statement_type":"update","affected_rows":1,"status":"committed","state_version_after":"sv_1"}
```

- Check `affected_rows` against what you expected.
- Use `--idempotency-key` for any write you might retry. The same key never
  runs twice.
- Running the same statement with the same parameters again returns
  `POTENTIAL_DUPLICATE_WRITE` with `previous_operation_id`. Add `--replay` only
  if the user wants it to run again.
- `OUTCOME_UNKNOWN` (timeout or lost connection during the write) blocks
  automatic replay. Query the data to find out whether the write happened, tell
  the user, and only then decide about `--replay`.
- `stql receipt <op-handle>` shows the full record of a write.

## Plan, then apply

Use a plan when the user should review a write before it runs.

```bash
stql plan 'DELETE FROM sessions WHERE expires_at < $1' --param 2026-01-01 --allow-destructive
# show the user statement_type, required_overrides, and expires_at
stql apply <plan-handle>
```

- A plan keeps only the approval flags given to `plan`; `apply` never adds any.
- If data changed after `plan`, `apply` returns `STALE_PLAN`. Create a new plan.
- Plans expire after 10 minutes.

## Staged transactions

```bash
stql transaction begin                       # optional: --isolation 'repeatable read'
stql exec 'UPDATE accounts SET balance = balance - $1 WHERE id = $2' --params '[50, 1]'
stql exec 'UPDATE accounts SET balance = balance + $1 WHERE id = $2' --params '[50, 2]'
stql transaction status
stql transaction commit                      # or: stql transaction rollback
```

- Writes are stored locally (`status: "pending"`) and run atomically at commit.
- While a transaction is open, reads, plans, and connection changes return
  `TRANSACTION_FAILED`. Query what you need before `begin`.
- Isolation: SQLite `serializable`; PostgreSQL and MySQL also `repeatable read`,
  `read committed`, `read uncommitted`.
- MySQL transactions reject DDL. MongoDB transactions need a replica set and use
  `snapshot` isolation. Redis has no staged transactions.
