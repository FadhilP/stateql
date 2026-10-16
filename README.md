# StateQL

StateQL is a stateful database CLI and TypeScript library for AI agents and
automation. It keeps query results as durable handles, so later commands can
page, filter, count, or export them without querying the database again.

- **SQL:** SQLite, PostgreSQL, and MySQL.
- **Native commands:** MongoDB (EJSON) and Redis (JSON).
- **Compact output:** one-line JSON with a 5-row preview and column names sent once.
- **Safe writes:** read-only by default, explicit approval for unbounded or
  destructive statements, idempotency keys, plans, and staged transactions.

Requires **Node.js 22.16 or newer**.

## Install

```bash
npm install -g @fadhilp/stateql   # CLI (stql)
npm install @fadhilp/stateql      # TypeScript library
```

## Quick start

```bash
export STQL_SESSION=audit
stql connect ./app.sqlite

stql query "SELECT id, name FROM users WHERE status = ? ORDER BY id" --param active
```

```json
{"ok":true,"handle":"katcitg6un","total":333,"next_offset":5,"columns":["id","name"],"rows":[[2,"Ada"],[3,"Grace"],[5,"Linus"],[6,"Alan"],[8,"Edsger"]]}
```

Reuse the result by its handle. These commands do not access the database:

```bash
stql rows katcitg6un --offset 5 --limit 20
stql filter katcitg6un "name LIKE ?" --param "A%"
stql export katcitg6un --output users.csv
```

## Agent skill

`skills/stateql` is an [Agent Skill](https://agentskills.io) that teaches coding
agents to use `stql` safely and with few tokens. Copy the folder into your
agent's skills directory, for example for Claude Code:

```bash
cp -r "$(npm root -g)/@fadhilp/stateql/skills/stateql" ~/.claude/skills/
```

Use `.claude/skills/` inside a project to share it with your team.

## Documentation

| Page | Contents |
| --- | --- |
| [Agent skill](skills/stateql/SKILL.md) | Rules, workflow, response format, quoting, and errors for coding agents |
| [Connections](docs/connections.md) | Targets, environment credentials, profiles |
| [CLI reference](docs/cli.md) | Commands, parameters, catalog, output modes, deadlines |
| [Results and state](docs/results.md) | Sessions, actors, limits, caching, local filtering |
| [Write safety](docs/writes.md) | Approvals, plans, idempotency, transactions |
| [Batch and pipes](docs/batch.md) | Run many commands in one call |
| [Database support](docs/databases.md) | Dialects, diagnostics, MongoDB and Redis commands |
| [TypeScript library](docs/library.md) | API, responses, actors, credential resolution |

Run `stql help` for command syntax and `stql capabilities` for feature details.

## License

[MIT](LICENSE)
