# Writes

All writes need a read-write connection (`stql connect ... --read-write`) and a
change that the user asked for. `connect <profile> --read-only` refuses writes,
also when the profile was saved with `--read-write`.

## Safety gates

| Statement | Needs |
| --- | --- |
| `UPDATE` or `DELETE` without `WHERE`, `TRUNCATE` | `--allow-unbounded` |
| `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, `REPLACE`, `INSERT/UPDATE OR REPLACE`, upsert that updates (`ON CONFLICT DO UPDATE`, `ON DUPLICATE KEY UPDATE`) | `--allow-destructive` |
| SQL that `stql` cannot parse | both flags |
| MongoDB update, replace, or delete with an empty filter | `--allow-unbounded` |
| MongoDB `deleteOne`, `deleteMany`, `replaceOne`, `dropIndex` | `--allow-destructive` |
| Redis `DEL`, `UNLINK`, `HDEL`, `SREM`, `ZREM` | `--allow-destructive` |

The flags are independent. A `DELETE` without `WHERE` needs both. When a gate
stops a command, nothing runs (`executed:false`), and `hint` names the flag.
Show the statement to the user. Add the flag only after the user approves
that operation.

## Write

```bash
stql exec 'UPDATE jobs SET claimed = 1 WHERE id = $1' --param 42 --expect-rows 1 --idempotency-key claim-job-42
```

```json
{"ok":true,"handle":"wck68sz","status":"committed","statement_type":"update","affected_rows":1}
```

- `exec` runs exactly one statement. `query` does not run writes (`READ_ONLY`),
  and `exec` does not run reads.
- Check `affected_rows` against what you expected.
- `stql receipt <write-handle>` shows the full record: statement, status, and
  times. It does not show parameter values.

## Check the row count: `--expect-rows N`

Use `--expect-rows` when you know how many rows must change.

- **SQL:** the write runs in a transaction. If the count is not `N`, `stql`
  rolls back and returns `ROW_COUNT_MISMATCH` with `actual_rows`. Nothing is
  kept (`executed:false`).
- **MySQL:** a DDL statement or SQL that `stql` cannot parse returns
  `UNSUPPORTED` with `--expect-rows`, because MySQL commits DDL at once.
  MySQL counts rows that match, also when a value does not change.
- **MongoDB:** only for `updateOne`, `updateMany`, `replaceOne`, `deleteOne`,
  and `deleteMany`. `stql` runs `countDocuments(filter)` first, and writes only
  if the count is `N`. These are two steps, not one transaction. A change
  between them is not found. For `updateOne` and `deleteOne`, `N` is the count
  of matching documents, not the count that will change.
- **Redis:** `UNSUPPORTED`.

## Retries: `--idempotency-key`

Give a key to each write that you can retry.

- The same key with the same statement and parameters:
  - after a committed write: returns the first receipt. The write does not run
    again.
  - after a write that failed or was rolled back: runs the write again.
  - after a write with an unknown outcome, or one that has not finished:
    returns `OUTCOME_UNKNOWN` with `previous_operation`.
- The same key with a different statement returns `IDEMPOTENCY_CONFLICT`.

## Unknown outcome: `OUTCOME_UNKNOWN`

A timeout or a lost connection during a write gives `OUTCOME_UNKNOWN`
(`executed:"unknown"`). The write may or may not have run.

1. Query the data to find out if the write ran.
2. Tell the user what you found.
3. The same statement with the same parameters in this session now returns
   `OUTCOME_UNKNOWN` with `previous_operation`. It does not run. Add `--replay`
   only if the user wants it to run again.

A write that committed does not stop the same write from running again. Use an
idempotency key to prevent a second run.

## Many writes in one transaction: `pipe --atomic`

```bash
printf '%s\n' \
  '{"command":"exec","stmt":"UPDATE accounts SET balance = balance - $1 WHERE id = $2","params":[50,1],"expect_rows":1}' \
  '{"command":"exec","stmt":"UPDATE accounts SET balance = balance + $1 WHERE id = $2","params":[50,2],"expect_rows":1}' \
  | stql pipe --atomic --isolation repeatable-read
```

- All lines run in one transaction. `stql` commits after the last line. Until
  then, each write reports `status:"pending"`.
- An error on any line, also `ROW_COUNT_MISMATCH`, rolls back all lines and
  stops the pipe.
- Reads in the pipe see the writes of the pipe.
- `--isolation`: `read-committed`, `repeatable-read`, `serializable`
  (PostgreSQL and MySQL). SQLite always uses `BEGIN IMMEDIATE`.
- MySQL: DDL returns `UNSUPPORTED`, because it commits at once. A read that
  `stql` cannot parse is refused. Writes to MyISAM tables are not rolled back.
- MongoDB and Redis: `UNSUPPORTED`.
- If a line does not stop at its deadline, the process exits (code 4). The
  transaction does not commit, and the pipe writes read as `OUTCOME_UNKNOWN`.
