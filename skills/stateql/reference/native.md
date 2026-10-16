# MongoDB and Redis

SQL commands (`query`, `exec`, `plan`) are rejected on these connections. Pass
the native command as one JSON argument. In Windows PowerShell 5.1, send it
through `stql pipe` instead (field `mongo` or `redis`):

```powershell
'{"command":"mongo.query","mongo":{"operation":"find","collection":"users","filter":{"active":true},"options":{"limit":20}}}' | stql pipe
```

## MongoDB

Commands use Extended JSON (EJSON): ObjectIds are `{"$oid":"..."}`, dates are
`{"$date":"2026-01-01T00:00:00Z"}`.

```bash
stql objects collection                      # list collections
stql inspect columns users                   # fields seen in sampled documents
stql mongo query '{"operation":"find","collection":"users","filter":{"active":true},"options":{"projection":{"email":1},"sort":{"_id":1},"limit":50}}'
stql mongo query '{"operation":"aggregate","collection":"orders","pipeline":[{"$match":{"status":"paid"}},{"$group":{"_id":"$user_id","n":{"$sum":1}}}]}'
```

| Operation | Keys |
| --- | --- |
| `find` | `collection`, `filter`, `options`: `projection`, `sort`, `skip`, `limit`, `hint`, `collation` |
| `aggregate` | `collection`, `pipeline`, `options`: `allowDiskUse`, `hint`, `collation` |
| `insertOne` / `insertMany` | `document` / `documents` |
| `updateOne` / `updateMany` | `filter`, `update`, `options`: `upsert`, `collation`, `hint` |
| `replaceOne` | `filter`, `replacement` |
| `deleteOne` / `deleteMany` | `filter` |

- Writes: `stql mongo exec '<command>'`; review first with `stql mongo plan`.
  Empty filters need `--allow-unbounded`; deletes and replacements need
  `--allow-destructive`. Both need user approval.
- `$out`, `$merge`, `$where`, `$function`, `$accumulator`, and server-side
  JavaScript are rejected.
- Documents with different fields come back as objects, not row arrays.
- The cache cannot see changes made outside this session. Use
  `--cache bypass` for a fresh read.

## Redis

Commands are `{"command":"NAME","args":["..."]}`. All arguments are strings.

```bash
stql objects key --search session: --limit 50        # SCAN-based; continue with --cursor <next_offset>
stql object key session:42                           # type, TTL, and a bounded value
stql redis query '{"command":"HGET","args":["user:42","email"]}'
stql redis exec '{"command":"SET","args":["app:status","ready"]}' --idempotency-key status-ready
```

| Kind | Allowed commands |
| --- | --- |
| Read (`redis query`) | `GET`, `MGET`, `TYPE`, `EXISTS`, `TTL`, `PTTL`, `HGET`, `HMGET`, bounded `LRANGE`, bounded `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` |
| Write (`redis exec`, `redis plan`) | one key: `SET`, `DEL`, `HSET`, `HDEL`, `LPUSH`, `RPUSH`, `SADD`, `SREM`, `ZADD`, `ZREM` |

- `KEYS`, scripts, pub/sub, blocking commands, and admin commands are rejected.
- Key pages can be empty or repeat keys while data changes. Keep following
  `next_offset` until it is absent.
- `redis plan` then `stql apply` fails with `ROW_CONFLICT` if the key changed
  in between. There are no staged transactions.
