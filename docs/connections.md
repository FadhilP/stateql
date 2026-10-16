# Connections and profiles

[StateQL](../README.md) · [Agent skill](../skills/stateql/SKILL.md) · [Connections](connections.md) · [CLI](cli.md) · [Results](results.md) · [Writes](writes.md) · [Batch](batch.md) · [Databases](databases.md) · [Library](library.md)

A connection accepts exactly one source: a direct target, `--env`,
`--credential-ref`, or `--profile`. Library and batch callers may additionally
attach a [password reference](library.md#password-references) to a literal
password-free remote target; it is not an additional connection source.

```bash
stql connect <target> [--name NAME] [--read-only|--read-write]
stql connect --env ENV [--name NAME] [--read-only|--read-write]
stql connect --credential-ref REF [--name NAME] [--read-only|--read-write]
stql connect --profile NAME
stql disconnect
stql status
```

Direct targets accept SQLite paths or password-free PostgreSQL, MySQL, MongoDB,
and Redis/Rediss URLs. Connections default to read-only unless a profile or an
explicit flag selects read-write access.

## Environment-backed credentials

PostgreSQL, MySQL, MongoDB, and Redis credentials should come from environment
variables. The variable must contain the complete connection URL, not only its
password. Environment-backed SQLite paths require an explicit `sqlite:` prefix.
The URLs below are placeholders; supply real values through your secret-management
workflow rather than recording them in shell history.

```bash
export APP_DATABASE_URL='postgres://user:password@host/app'
stql connect --env APP_DATABASE_URL --name app --read-only

export MYSQL_DATABASE_URL='mysql://user:password@host/app'
stql connect --env MYSQL_DATABASE_URL --name mysql-app --read-only

export MONGODB_URL='mongodb://user:password@host/app'
stql connect --env MONGODB_URL --name mongo-app --read-only

export REDIS_URL='rediss://user:password@host/0'
stql connect --env REDIS_URL --name redis-app --read-only

export SQLITE_DATABASE='sqlite:./app.sqlite'
stql connect --env SQLITE_DATABASE --name local --read-only
```

StateQL stores no PostgreSQL, MySQL, MongoDB, or Redis password in its state.
Credential-bearing URLs must be supplied through `--env` or an opaque full-URL
credential reference, or the password can be [saved in the OS credential
store](#saved-passwords). SQLite paths remain persisted as connection metadata.

## Local profiles

Profiles store exactly one connection target, environment-variable name, or
opaque credential reference together with read-only policy. A remote literal
target may additionally store a `password_ref`; SQLite, environment-backed, and
full-URL `credential_ref` profiles cannot. Credential values are never stored.
Profiles persist under `STQL_HOME` with other StateQL metadata, and list/show
responses include nullable `credential_ref` and `password_ref` fields.

```bash
stql profile add local ./app.sqlite --read-only
stql profile add production --env PROD_DATABASE_URL --read-only
stql profile add hosted --credential-ref 'vault://team/app' --read-only
stql profile list
stql profile show production
stql profile update production --read-only
stql connect local
stql connect --profile production
```

Credential references are bounded nonempty opaque strings; StateQL does not
apply environment-variable syntax or normalization to them. Apart from the
`os:` password references below, they can only be resolved by a trusted host
`CredentialResolver`, so the standalone CLI may store them in profiles but
cannot connect with them.

## Saved passwords

`--ask-password` opens a password dialog and saves the password in the OS
credential store: Windows Credential Manager, macOS Keychain, or Linux Secret
Service. The profile keeps the password-free target and an `os:` password
reference.

```bash
stql profile add charlie 'postgres://reader@db.example.com:5432/app?sslmode=verify-full' --ask-password --read-only
stql profile update charlie --ask-password    # replace the saved password
stql profile remove charlie                   # also deletes the saved password
```

The dialog uses Windows PowerShell on Windows, `osascript` on macOS, and `zenity`
on Linux, so it also works when an agent runs the command. Replacing the
password, changing the target or source, or removing the profile deletes the
old saved password; reconnect sessions that used it. Any program running as
your OS user can read the store, so give agent profiles a read-only database
user.

A bare connection target matching a profile name resolves to that profile;
otherwise it remains a path or database URL.

`profile update` changes subsequent connections, not an already-open connection.
Omitting the source keeps it; supplying a new source replaces the old one.
Other password references are configured through the library or batch API, not
a CLI `--password-ref` flag. See [safe profile updates](library.md#safe-profile-updates)
for source-replacement and downgrade restrictions.
