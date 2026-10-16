# Durable state and result reuse

[StateQL](../README.md) · [Agent skill](../skills/stateql/SKILL.md) · [Connections](connections.md) · [CLI](cli.md) · [Results](results.md) · [Writes](writes.md) · [Batch](batch.md) · [Databases](databases.md) · [Library](library.md)

State metadata lives under `STQL_HOME`, or the platform data directory when
unset. StateQL keeps connections, sessions, handles, aliases, cache entries,
plans, transactions, history, and receipts available across CLI invocations.
Stored results contain row data, so protect this directory like the database.

## Sessions and actors

Set `STQL_SESSION` to select a named session and `STQL_ACTOR` to select an
attached actor. A session is a shared workspace: attached actors reuse its
connection, handles, aliases, cache, and state version. Plans and staged
transactions remain owned by the actor that created them.

Callers that omit `actor` keep the legacy behavior where the actor ID is the
session name.

Actor IDs are immutable authorization and ownership identities. Each actor also
has a mutable display name, initially equal to its ID. The selected actor may
change only its own display name with `stql actor set-name <display-name>`;
names are trimmed, limited to 80 characters, and cannot contain control
characters. Display names are contextual metadata and must not be used for
authorization.

History records both the stable `actor_id` and the `actor_name` in effect when
the command began. Renaming therefore does not rewrite earlier attribution; the
rename is itself recorded as an `actor.set-name` management command.

## Result lifetime and limits

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

Cells longer than 200 characters are returned as
`{"type":"text","length":N,"preview":"..."}`. Change the limit with
`--max-cell-characters N` (library: `maxCellCharacters`), or use `export` for
full values.

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

## Local filtering

`filter` evaluates one scalar SQLite predicate against a stored result. It
preserves source order, state metadata, and expiry, and never accesses the
original database.

Use parameters for values. Subqueries, query-shaping clauses, and
non-allowlisted functions are rejected. Common deterministic functions such as
`lower`, `upper`, `length`, and `coalesce` are supported.
