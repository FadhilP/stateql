# MongoDB and Redis

These connections take one JSON command, not SQL. Use the same commands as for
SQL: `query` for reads and `exec` for writes. Put the values in the JSON. Do not
use `--param`. `objects`, `describe`, `rows`, `filter`, and `export` work as for
SQL.

In Windows PowerShell 5.1, send the JSON on standard input:

```powershell
'{"operation":"find","collection":"users","filter":{"active":true}}' | stql query --file -
```

## MongoDB

The URL must name a database (`mongodb://host/app`). Commands use Extended JSON
(EJSON): an ObjectId is `{"$oid":"..."}`, and a date is
`{"$date":"2026-01-01T00:00:00Z"}`.

```bash
stql objects collection --search user
stql describe users                          # options, indexes, and fields seen in 100 documents
stql query '{"operation":"find","collection":"users","filter":{"active":true},"options":{"projection":{"email":1},"sort":{"_id":1},"limit":50}}'
stql query '{"operation":"aggregate","collection":"orders","pipeline":[{"$match":{"status":"paid"}},{"$group":{"_id":"$user_id","n":{"$sum":1}}}]}'
stql exec '{"operation":"updateOne","collection":"users","filter":{"_id":7},"update":{"$set":{"active":false}}}' --expect-rows 1
```

| Operation | Keys |
| --- | --- |
| `find` | `filter`, `options`: `projection`, `sort`, `skip`, `limit`, `hint`, `collation`; `explain` |
| `aggregate` | `pipeline`, `options`: `allowDiskUse`, `hint`, `collation`; `explain` |
| `countDocuments` | `filter`, `options`: `skip`, `limit`, `hint`, `collation` |
| `distinct` | `field`, `filter`, `options`: `collation` |
| `insertOne` / `insertMany` | `document` / `documents`, `options`: `ordered` (`insertMany`) |
| `updateOne` / `updateMany` | `filter`, `update`, `options`: `upsert`, `collation`, `hint` |
| `replaceOne` | `filter`, `replacement`, `options`: `upsert`, `collation`, `hint` |
| `deleteOne` / `deleteMany` | `filter`, `options`: `collation`, `hint` |
| `createIndex` | `keys`, `options`: `name`, `unique`, `sparse`, `expireAfterSeconds`, `partialFilterExpression`, `collation` |
| `dropIndex` | `index` (the index name) |

Each command also has `operation` and `collection`. Other keys are refused.

- `explain` is `"queryPlanner"`, `"executionStats"`, or `"allPlansExecution"`.
- `$out`, `$merge`, `$changeStream`, `$where`, `$function`, `$accumulator`, and
  JavaScript code are refused (`PERMISSION_DENIED` or `INVALID_COMMAND`).
  Collections named `system.*` are refused.
- Results are rows. The columns are the top-level fields in the order that
  they first occur, with `_id` first. A missing field is `null`. Nested
  documents and arrays are JSON values. A field with different types has the
  type `mixed`.
- ObjectIds, dates, and `Decimal128` values are text. Use them in `filter` as
  text.
- `--expect-rows` counts the matching documents before the write. This is not
  atomic. See [writes.md](writes.md).
- `pipe --atomic` is not supported.

## Redis

A command is `{"command":"NAME","args":["..."]}`. All arguments are strings.

```bash
stql objects key --search session: --limit 50          # SCAN; continue with --cursor <next_cursor>
stql describe session:42                               # type, TTL in ms, and a bounded value
stql query '{"command":"HGET","args":["user:42","email"]}'
stql exec '{"command":"SET","args":["app:status","ready"]}' --idempotency-key status-ready
```

| Kind | Commands |
| --- | --- |
| Read (`query`) | `GET`, `MGET`, `TYPE`, `EXISTS`, `TTL`, `PTTL`, `HGET`, `HMGET`, `HGETALL`, `HLEN`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `ZCARD`, `ZSCORE`, `LLEN`, `STRLEN`, `DBSIZE`, `INFO [section]`, `LRANGE`, `ZRANGE key start stop [WITHSCORES]`, `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` |
| Write (`exec`) | `SET key value [EX s\|PX ms] [NX\|XX]`, `DEL`, `UNLINK`, `HSET`, `HDEL`, `HINCRBY`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `LPUSH`, `RPUSH`, `SADD`, `SREM`, `ZADD`, `ZREM` |

- Row shapes: `HGETALL` and `HSCAN` give `field, value`. `ZRANGE ... WITHSCORES`
  and `ZSCAN` give `value, score`. `MGET` and other lists give `index, value`.
  `INFO` gives `section, field, value`. One value gives `value`.
- `LRANGE` and `ZRANGE` return at most 200 values. Use indexes that are not
  negative. `HGETALL` and `SMEMBERS` fail with `RESULT_TOO_LARGE` on keys with
  more than 200 values. Use `HSCAN` or `SSCAN`.
- For `SCAN`, `HSCAN`, `SSCAN`, and `ZSCAN`, give `next_cursor` as the cursor of
  the next call. Pages can be empty or repeat keys while data changes. Continue
  until `next_cursor` is absent.
- A response is limited to 1 MiB.
- `KEYS`, scripts, pub/sub, blocking commands, and admin commands are refused.
- `affected_rows`: the count that Redis returns (for example, keys deleted).
  `SET` and the counter commands give 1.
- `--expect-rows` and `pipe --atomic` are not supported.
