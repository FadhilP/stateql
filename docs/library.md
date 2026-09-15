# TypeScript library

[Home](../README.md) · [Usage](usage.md) · [Database support](databases.md) · [TypeScript library](library.md)

- [Getting started](#getting-started)
- [Responses and command context](#responses-and-command-context)
- [Actor workspaces](#actor-workspaces)
- [Credential resolution](#credential-resolution)
- [Safe profile updates](#safe-profile-updates)

## Getting started

Install with `npm install @fadhilp/stateql`. This example connects to an existing
SQLite database containing a `users` table, queries it, and filters the stored
result. `forActor()` resolves or creates a session; it does not establish a
database connection for a new session.

```ts
import { StateQL } from "@fadhilp/stateql";

const stateql = StateQL.forActor({
  home: "./.stql",
  actor: "audit",
});

try {
  const connected = await stateql.connect("./app.sqlite", { readOnly: true });
  if (!connected.ok) throw new Error(connected.error.message);

  const result = await stateql.query(
    "SELECT id, email FROM users WHERE status = ? ORDER BY id LIMIT 50",
    { params: ["active"], timeoutMs: 5_000, previewRows: 20 },
  );
  if (!result.ok) throw new Error(result.error.message);

  const filtered = await stateql.filter(result.data.result_id, "email LIKE ?", {
    params: ["%@example.com"],
  });
  if (!filtered.ok) throw new Error(filtered.error.message);

  console.log(filtered.data.preview);
} finally {
  stateql.close();
}
```

`close()` releases the client's local store; it does not disconnect or delete
the durable session. Await active commands before closing. Use `disconnect()`
only when you intend to remove the session's shared active connection.

Options include `timeoutMs`, `credentialTimeoutMs`, `maxResultBytes`,
`maxStateBytes`, `cacheTtlSeconds`, and `resultTtlSeconds`. Database calls also
accept `signal` for cancellation. See [limits](usage.md#result-lifetime-and-limits)
and [deadlines](usage.md#deadlines-and-cancellation) for defaults and behavior.
The same [write safety](usage.md#write-safety) and
[database restrictions](databases.md) apply to library calls.

## Responses and command context

Library responses retain the full response envelope regardless of the CLI
output mode. Check `response.ok` before reading `response.data`; failures expose
`response.error`. Queries return typed `ResultData` with `result_id`, `preview`,
and result metadata. Reuse `result_id` with methods such as `filter`, `rows`,
`count`, and `exportResult`; the CLI instead presents its primary ID as `handle`.
Queries preview five rows by default. Set `previewRows` from 0 to 200 on an
individual query to control its response without changing or rerunning the
materialized result. The constructor-level `previewRows` option changes the
client default.

The following snippets assume an open client and an appropriate connection.

Hosts that dispatch batch-shaped commands can attach trusted metadata out of
band. `origin` is audit/source metadata only; it never changes actor membership,
workspace access, or write authorization.

```ts
const controller = new AbortController();
await stateql.executeCommand(
  { command: "query", sql: "SELECT id, email FROM users ORDER BY id LIMIT 50", cache: "bypass" },
  { origin: "user", signal: controller.signal },
);

const userHistory = await stateql.history(50, { origin: "user" });
await stateql.executeCommand(
  { command: "history", limit: 50, history_origin: "user" },
  { origin: "model" },
);
```

Supported origins are `legacy`, `user`, `model`, `system`, and `api`. Existing
direct calls and `executeCommand(command)` calls are recorded as `legacy`.
`history_origin` is only a retrieval filter; putting an `origin` field in a
`BatchCommand` cannot attribute the command. `batch` accepts the same trusted
context as `options.executionContext` for all commands in that batch.

## Actor workspaces

`StateQL.forWorkspace(...)` is a trusted-host primitive that atomically creates
or reopens a durable workspace, attaches the requested actor, and returns a
client bound to that actor:

```ts
const stateql = StateQL.forWorkspace({
  home: "./.stql",
  workspace: "team-audit",
  actor: "audit-worker-1",
  credentialResolver,
  signal,
});
```

Repeated opens of the same actor and workspace are idempotent. An actor already
attached elsewhere fails with a `StateQLError` whose code is
`PERMISSION_DENIED`; StateQL never moves or merges it. All actor options,
including limits, credential resolution, cancellation, `home`, and `now`, are
preserved. The workspace name also reserves a same-named actor identity for
legacy compatibility, so workspace and actor identifiers must be globally
collision-free. The returned client is still bound only to `actor`, preserving
plan, transaction, operation, and history ownership.

`StateQL.forActor(...)` retains its existing behavior: it resolves the actor's
attached session directly from StateQL storage and creates a legacy-compatible
session named after the actor on first use. Use `new StateQL({ session, actor })`
when the session and membership are already known.

Membership management and `forWorkspace` are library-only host capabilities,
not batch or CLI commands. Existing member-authorized management remains
available through `linkActor(session, actorId)`, `unlinkActor(session, actorId)`,
`listActors(session)`, and `resolveActor(actorId)`. Integrations should ask for
user confirmation before changing membership or the shared connection; a host
calling `forWorkspace` is responsible for authorizing that workspace access.

## Credential resolution

Library integrations can resolve environment-variable names, opaque full-URL
credential references, or password-only references through a trusted approval
or secret-storage layer instead of mutating `process.env`.

Integrations pinned to an older published package should gate setup before
sending `password_ref`:

```ts
if ((StateQL.passwordReferenceVersion ?? 0) < 1) {
  throw new Error("Installed StateQL does not support password references.");
}
```

`passwordReferenceVersion = 1` guarantees the password-only resolver request,
validation, persistence, reconnect, and redaction contract documented below.

In this adapter example, `credentialBroker` is supplied by the host application;
it is not a StateQL export.

```ts
import {
  CredentialResolutionError,
  StateQL,
  type CredentialRequest,
} from "@fadhilp/stateql";

async function resolveCredential(
  request: CredentialRequest,
): Promise<string | undefined> {
  const approved = await credentialBroker.request({
    reference: request.reference,
    source: request.source ?? "secret_env",
    actor: request.actorId,
    session: request.session.id,
    operation: request.operation,
    access: request.access,
    signal: request.signal,
  });

  if (approved.denied) throw new CredentialResolutionError("denied");
  return approved.value;
}

const stateql = StateQL.forActor({
  actor: "agent-session-id",
  credentialResolver: resolveCredential,
});
```

Credential resolution has its own two-minute default deadline
(`credentialTimeoutMs`) and remains cancellable through `request.signal`.
The database-operation timeout begins after a credential is resolved.

When no custom resolver is configured, StateQL reads only `secret_env`
references from `process.env`; `credential_ref` and `password_ref` never fall
back to the environment. A configured resolver is authoritative for all
sources: returning `undefined` produces `CREDENTIAL_UNAVAILABLE` and never falls
back to the process environment. Resolver requests retain `reference` and
include `source` (`secret_env`, `credential_ref`, or `password_ref`); source may
be omitted only on legacy secret-environment request objects. A `password_ref`
request additionally includes the exact password-free effective `target`.
Resolvers may throw `CredentialResolutionError` with `denied`, `cancelled`,
`timeout`, or `unavailable` to produce controlled, secret-free failures. Unknown
resolver errors are replaced with a generic `CREDENTIAL_RESOLUTION_FAILED`
response.

StateQL calls the resolver only immediately before database access, after SQL
safety and duplicate checks. Requests contain actor and session identity, the
operation's effective read/write access, an abort signal, and sanitized
connection metadata.

For `secret_env` and `credential_ref`, returned values must be complete
PostgreSQL, MySQL, MongoDB, or Redis URLs, or explicit `sqlite:` sources. For
`password_ref`, the resolver returns only the password; an explicit empty string
is a resolved password, while `undefined` fails closed. StateQL percent-encodes
and injects only that password into the original target for adapter use, leaving
all nonsecret URL/TLS/CA bytes unchanged. It persists only the original target
and reference. Resolved credentials never enter connection metadata, history,
snapshots, cache keys, responses, or stored errors.

Hosts remain responsible for approval policy, binding lifetime, revocation,
and keeping values out of their own logs and model-visible data.

For writes, credential resolution happens after StateQL atomically reserves the
operation for duplicate protection. A resolution failure keeps a non-executed
`failed` audit record, does not consume the idempotency key, and permits a safe
retry.

### Password references

Library callers can keep nonsecret endpoint, username, database, TLS, and CA
options in the literal URL while resolving only its password:

```ts
await stateql.connect(
  "postgres://app@db.example/app?sslmode=verify-full&sslrootcert=/etc/app-ca.pem",
  { passwordRef: "vault://database/app/password", readOnly: true },
);
```

The same field is accepted by `addProfile`, `updateProfile`, and batch
`connect`/`profile.add`/`profile.update` commands (snake case in batch input).
Targets with an embedded password or query parameters that override endpoint or
credential fields are rejected before credential resolution or driver access.

## Safe profile updates

`updateProfile(name, options)` accepts optional `target`, `secretEnv`,
`credentialRef`, `passwordRef`, and `readOnly` fields. The four string fields
also accept `null` for explicit clearing.

```ts
const updated = await stateql.updateProfile("production", { readOnly: true });
if (!updated.ok) throw new Error(updated.error.message);
```

Omitting all source and password-reference fields keeps the existing source and
adjunct reference. Supplying any source field replaces the source atomically:
exactly one non-null source is required and the other source columns are
cleared. An omitted `passwordRef` is preserved when the existing literal target
is unchanged; changing the target clears it unless the update explicitly supplies
a replacement. Replacing the source with `secretEnv` or `credentialRef` clears
it. An explicit non-null password reference combined with either
reference-backed source is rejected. Direct URLs
with embedded passwords or secret-like query parameters are rejected.
`profile.list/show/update` return only
`{profile,target,secret_env,credential_ref,password_ref,read_only}`. Profile
changes affect subsequent `connect` calls and do not silently mutate an
already-open connection.

The `password_refs_v1` migration adds nullable `password_ref` columns to both
profiles and connections and enforces that they accompany only literal target
configuration. It rejects incompatible profile schemas/rows instead of dropping
references. Downgrading a state home containing password references is
unsupported: older binaries do not resolve this source and may attempt the
password-free target using ambient/trust authentication; constraint-protected
source replacements may also fail. Use the same or newer StateQL binary, or
explicitly clear all password references before downgrade.
