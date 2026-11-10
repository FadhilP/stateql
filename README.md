# StateQL

StateQL (`stql`) is a database CLI for AI agents and automation. It runs a
query one time and keeps the full result on this computer behind a short
handle. Later commands page, filter, summarize, or export the result. They do
not query the database again.

- **SQL:** SQLite, PostgreSQL, and MySQL.
- **JSON commands:** MongoDB (EJSON) and Redis.
- **Compact output:** one line of JSON for each command, with a preview of 20 rows.
- **Safe writes:** read-only by default. Unbounded and destructive statements
  need an approval flag. Row-count checks, idempotency keys, and atomic pipes.

## Install

```bash
npm install -g @fadhilp/stateql
```

The npm package holds the prebuilt binaries for Linux (x64, arm64; static, for
glibc and musl), macOS (x64, arm64), and Windows (x64, arm64). On a global
install, an install script points the `stql` command at the binary, so a call
does not start Node.js. If install scripts are off, `stql` still works through
a small Node.js launcher.

From source (Rust 1.89 or later):

```bash
cargo install --locked --git https://github.com/FadhilP/stateql
```

## Quick start

```bash
export STQL_SESSION=audit
stql connect ./app.sqlite
stql query 'SELECT id, name FROM users WHERE status = ? ORDER BY id' --param active
```

```json
{"ok":true,"handle":"rqj6d8m","total":333,"next_offset":20,"columns":["id","name"],"types":["integer","text"],"rows":[[2,"Ada"],[3,"Grace"]]}
```

Use the handle. These commands do not go to the database:

```bash
stql rows rqj6d8m --offset 20 --limit 20
stql filter rqj6d8m 'name LIKE ?' --param 'A%'
stql export rqj6d8m --output users.csv
```

For a remote database, put the URL in an environment variable
(`stql connect --env DATABASE_URL`), or save a profile
(`stql profile add NAME --env VAR [--env-file .env]`, or
`stql profile add NAME 'URL without password' --ask-password`). A URL in a
command must not contain a password. `stql --profile NAME <command>` connects
to a profile first, so one call does not need a separate `connect`.

`stql help` shows all commands. `stql doctor` checks the home directory, the
store, and the OS keyring.

## State

`stql` keeps its state in `%LOCALAPPDATA%\stql` on Windows and in
`$XDG_DATA_HOME/stql` (default `~/.local/share/stql`) on other systems.
`STQL_HOME` changes this. Results expire
after 24 hours. On the first run, `stql` copies the profiles of the TypeScript
version from `state.sqlite` to `profiles.toml`. It does not change the old
files or keyring entries. The response of that first command has a `notice`
field with the report. In normal operation, `stql` writes nothing to stderr.

## Agent skill

`skills/stateql` is an [Agent Skill](https://agentskills.io). It teaches coding
agents to use `stql` safely and with few tokens. Copy the folder into the
skills directory of your agent, for example for Claude Code:

```bash
cp -r "$(npm root -g)/@fadhilp/stateql/skills/stateql" ~/.claude/skills/
```

## License

[MIT](LICENSE)
