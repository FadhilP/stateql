import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  chmodSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { delimiter, join, resolve } from "node:path";
import { test } from "node:test";
import { StateQL } from "../src/stateql.js";
import { createTemporaryDirectory, succeed } from "./helpers.js";

const windows = process.platform === "win32";
const availableCommands = [
  "--help",
  "alias.set",
  "apply",
  "batch",
  "capabilities",
  "columns",
  "connect",
  "count",
  "disconnect",
  "exec",
  "export",
  "filter",
  "history",
  "inspect.columns",
  "inspect.constraints",
  "inspect.indexes",
  "inspect.schema",
  "inspect.table",
  "pipe",
  "plan",
  "profile.add",
  "profile.list",
  "profile.remove",
  "profile.show",
  "query",
  "receipt",
  "rows",
  "session.close",
  "session.list",
  "session.show",
  "session.start",
  "session.summary",
  "show",
  "status",
  "transaction.begin",
  "transaction.commit",
  "transaction.rollback",
  "transaction.status",
] as const;

type CommandId = (typeof availableCommands)[number];
type TerminalState = Record<string, string>;
type TerminalStep = {
  label: string;
  command: string | ((state: TerminalState) => string);
  commandId?: CommandId;
  capture?: string;
  expect?: RegExp;
  machineOutput?: boolean;
  responses?: number;
  expectedStatus?: number;
};

function quote(value: string): string {
  if (windows) return `"${value.replaceAll('"', '""')}"`;
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function stql(...args: string[]): string {
  return ["stql", ...args.map(quote)].join(" ");
}

function createStqlShim(directory: string): void {
  const node = process.execPath;
  const cli = resolve("dist/src/cli.js");
  if (windows) {
    writeFileSync(
      join(directory, "stql.cmd"),
      `@echo off\r\n"${node}" "${cli}" %*\r\n`,
    );
    return;
  }

  const shim = join(directory, "stql");
  writeFileSync(shim, `#!/bin/sh\nexec ${quote(node)} ${quote(cli)} "$@"\n`);
  chmodSync(shim, 0o755);
}

function required(state: TerminalState, key: string): string {
  const value = state[key];
  assert.ok(value, `Missing captured terminal value: ${key}`);
  return value;
}

function scenario(): TerminalStep[] {
  const database = process.env.STQL_PTY_DATABASE!;
  const exportFile = process.env.STQL_PTY_EXPORT!;
  const batchFile = process.env.STQL_PTY_BATCH!;
  const pipeFile = process.env.STQL_PTY_PIPE!;
  const switchToOwner = windows
    ? 'set "STQL_ACTOR=pty-test" & ver >nul'
    : "export STQL_ACTOR=pty-test";
  const switchToActor = windows
    ? 'set "STQL_ACTOR=terminal-actor" & ver >nul'
    : "export STQL_ACTOR=terminal-actor";
  const switchSession = windows
    ? 'set "STQL_SESSION=terminal-next" & set "STQL_ACTOR=" & ver >nul'
    : "export STQL_SESSION=terminal-next; unset STQL_ACTOR";

  const steps: TerminalStep[] = [
    {
      label: "help",
      command: stql("--help"),
      commandId: "--help",
      machineOutput: false,
      expect: /Usage: stql <command>/,
    },
    {
      label: "profile add",
      command: stql("profile", "add", "local", database, "--read-write"),
      commandId: "profile.add",
    },
    {
      label: "profile list",
      command: stql("profile", "list"),
      commandId: "profile.list",
      expect: /"profile":"local"/,
    },
    {
      label: "profile show",
      command: stql("profile", "show", "local"),
      commandId: "profile.show",
      expect: /"profile":"local"/,
    },
    {
      label: "connect through profile",
      command: stql("connect", "--profile", "local"),
      commandId: "connect",
      expect: /"read_only":false/,
    },
    {
      label: "connected status",
      command: stql("status"),
      commandId: "status",
      expect: /"actor_id":"terminal-actor".*"driver":"sqlite"/,
    },
    {
      label: "create schema",
      command: stql(
        "exec",
        "CREATE TABLE terminal_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, status TEXT NOT NULL, parent_id INTEGER REFERENCES terminal_items(id))",
      ),
      commandId: "exec",
    },
    {
      label: "insert initial rows",
      command: stql(
        "exec",
        "INSERT INTO terminal_items (name, status) VALUES ('Ada', 'new'), ('Grace', 'new')",
      ),
    },
    {
      label: "inspect schema",
      command: stql("inspect", "schema"),
      commandId: "inspect.schema",
      expect: /terminal_items/,
    },
    {
      label: "inspect table",
      command: stql("inspect", "table", "terminal_items"),
      commandId: "inspect.table",
      expect: /terminal_items/,
    },
    {
      label: "inspect columns",
      command: stql("inspect", "columns", "terminal_items"),
      commandId: "inspect.columns",
      expect: /"name":"status"/,
    },
    {
      label: "inspect indexes",
      command: stql("inspect", "indexes", "terminal_items"),
      commandId: "inspect.indexes",
      expect: /"indexes":\[\{/,
    },
    {
      label: "inspect constraints",
      command: stql("inspect", "constraints", "terminal_items"),
      commandId: "inspect.constraints",
      expect: /"primary_key":\["id"\].*"foreign_keys":\[\{/,
    },
    {
      label: "query rows",
      command: stql(
        "query",
        "SELECT id, name, status FROM terminal_items ORDER BY id",
      ),
      commandId: "query",
      capture: "result",
      expect: /"name":"Ada"/,
    },
    {
      label: "show result",
      command: (state) => stql("show", required(state, "result")),
      commandId: "show",
      expect: /"name":"Grace"/,
    },
    {
      label: "page result rows",
      command: (state) =>
        stql("rows", required(state, "result"), "--offset", "0", "--limit", "1"),
      commandId: "rows",
      expect: /"next_offset":1/,
    },
    {
      label: "count result",
      command: (state) => stql("count", required(state, "result")),
      commandId: "count",
      expect: /"total":2/,
    },
    {
      label: "list result columns",
      command: (state) => stql("columns", required(state, "result")),
      commandId: "columns",
      expect: /"name":"status"/,
    },
    {
      label: "filter result",
      command: (state) =>
        stql(
          "filter",
          required(state, "result"),
          "name = ?",
          "--param",
          "Ada",
        ),
      commandId: "filter",
      capture: "filtered",
      expect: /"total":1/,
    },
    {
      label: "alias filtered result",
      command: (state) =>
        stql("alias", "set", "ada-items", required(state, "filtered")),
      commandId: "alias.set",
      expect: /"alias":"ada-items"/,
    },
    {
      label: "consume result alias",
      command: stql("count", "ada-items"),
      expect: /"total":1/,
    },
    {
      label: "switch to workspace owner",
      command: switchToOwner,
      machineOutput: false,
    },
    {
      label: "owner reuses shared alias",
      command: stql("count", "ada-items"),
      expect: /"total":1/,
    },
    {
      label: "switch back to attached actor",
      command: switchToActor,
      machineOutput: false,
    },
    {
      label: "export aliased result",
      command: stql(
        "export",
        "ada-items",
        "--output",
        exportFile,
        "--format",
        "json",
      ),
      commandId: "export",
      expect: /"format":"json"/,
    },
    {
      label: "plan bounded update",
      command: stql(
        "plan",
        "UPDATE terminal_items SET status = 'reviewed' WHERE name = 'Ada'",
      ),
      commandId: "plan",
      capture: "plan",
    },
    {
      label: "switch owner before applying actor plan",
      command: switchToOwner,
      machineOutput: false,
    },
    {
      label: "reject applying another actor plan",
      command: (state) => stql("apply", required(state, "plan")),
      expectedStatus: 8,
      expect: /"code":"PERMISSION_DENIED"/,
    },
    {
      label: "restore plan owner",
      command: switchToActor,
      machineOutput: false,
    },
    {
      label: "apply plan",
      command: (state) => stql("apply", required(state, "plan")),
      commandId: "apply",
      capture: "operation",
    },
    {
      label: "read operation receipt",
      command: (state) => stql("receipt", required(state, "operation")),
      commandId: "receipt",
      expect: /"status":"committed"/,
    },
    {
      label: "verify applied plan",
      command: stql(
        "query",
        "SELECT status FROM terminal_items WHERE name = 'Ada' ORDER BY id",
      ),
      expect: /"status":"reviewed"/,
    },
    {
      label: "begin committed transaction",
      command: stql("transaction", "begin"),
      commandId: "transaction.begin",
      capture: "commitTransaction",
      expect: /"state":"active"/,
    },
    {
      label: "switch away from transaction owner",
      command: switchToOwner,
      machineOutput: false,
    },
    {
      label: "inspect active transaction",
      command: (state) =>
        stql("transaction", "status", required(state, "commitTransaction")),
      commandId: "transaction.status",
      expect: /"state":"active".*"owner_actor_id":"terminal-actor"/,
    },
    {
      label: "reject staging for another actor transaction",
      command: stql(
        "exec",
        "INSERT INTO terminal_items (name, status) VALUES ('Blocked', 'pending')",
      ),
      expectedStatus: 8,
      expect: /"code":"PERMISSION_DENIED"/,
    },
    {
      label: "reject committing another actor transaction",
      command: (state) =>
        stql("transaction", "commit", required(state, "commitTransaction")),
      expectedStatus: 8,
      expect: /"code":"PERMISSION_DENIED"/,
    },
    {
      label: "restore transaction owner",
      command: switchToActor,
      machineOutput: false,
    },
    {
      label: "stage committed write",
      command: stql(
        "exec",
        "INSERT INTO terminal_items (name, status) VALUES ('Linus', 'committed')",
      ),
      expect: /"committed":false/,
    },
    {
      label: "reject query during staged transaction",
      command: stql(
        "query",
        "SELECT name FROM terminal_items WHERE name = 'Linus' ORDER BY id",
      ),
      expectedStatus: 1,
      expect: /"code":"TRANSACTION_FAILED"/,
    },
    {
      label: "commit transaction",
      command: (state) =>
        stql("transaction", "commit", required(state, "commitTransaction")),
      commandId: "transaction.commit",
      expect: /"state":"committed"/,
    },
    {
      label: "verify committed write",
      command: stql(
        "query",
        "SELECT name FROM terminal_items WHERE name = 'Linus' ORDER BY id",
      ),
      expect: /"name":"Linus"/,
    },
    {
      label: "begin rolled-back transaction",
      command: stql("transaction", "begin"),
      capture: "rollbackTransaction",
      expect: /"state":"active"/,
    },
    {
      label: "stage rolled-back write",
      command: stql(
        "exec",
        "INSERT INTO terminal_items (name, status) VALUES ('Temporary', 'pending')",
      ),
      expect: /"committed":false/,
    },
    {
      label: "roll back transaction",
      command: (state) =>
        stql("transaction", "rollback", required(state, "rollbackTransaction")),
      commandId: "transaction.rollback",
      expect: /"state":"rolled_back"/,
    },
    {
      label: "verify rolled-back write absent",
      command: stql(
        "query",
        "SELECT COUNT(*) AS rolled_back FROM terminal_items WHERE name = 'Temporary' ORDER BY rolled_back",
      ),
      expect: /"rolled_back":0/,
    },
    {
      label: "read history",
      command: stql("history", "--limit", "1"),
      commandId: "history",
      expect: /"history":\[\{.*"actor_id":"terminal-actor"/,
    },
    {
      label: "read capabilities",
      command: stql("capabilities"),
      commandId: "capabilities",
      expect: /"sqlite"/,
    },
    {
      label: "run batch sequence",
      command: stql("batch", batchFile),
      commandId: "batch",
      responses: 2,
      expect: /"total":3/,
    },
    {
      label: "run piped sequence",
      command: `${stql("pipe")} < ${quote(pipeFile)}`,
      commandId: "pipe",
      responses: 2,
      expect: /"total":3/,
    },
    {
      label: "start second session",
      command: stql("session", "start", "terminal-next"),
      commandId: "session.start",
      expect: /"name":"terminal-next"/,
    },
    {
      label: "list sessions",
      command: stql("session", "list"),
      commandId: "session.list",
      expect: /"name":"terminal-next"/,
    },
    {
      label: "show second session",
      command: stql("session", "show", "terminal-next"),
      commandId: "session.show",
      expect: /"name":"terminal-next"/,
    },
    {
      label: "summarize current session",
      command: stql("session", "summary"),
      commandId: "session.summary",
      expect: /"name":"pty-test"/,
    },
    {
      label: "disconnect",
      command: stql("disconnect"),
      commandId: "disconnect",
      expect: /"disconnected":true/,
    },
    {
      label: "verify disconnected status",
      command: stql("status"),
      expect: /"connection":null/,
    },
    {
      label: "close current session",
      command: stql("session", "close"),
      commandId: "session.close",
      expect: /"state":"closed"/,
    },
    {
      label: "switch shell session",
      command: switchSession,
      machineOutput: false,
    },
    {
      label: "verify second session selected",
      command: stql("status"),
      expect: /"session_name":"terminal-next"/,
    },
    {
      label: "remove profile from second session",
      command: stql("profile", "remove", "local"),
      commandId: "profile.remove",
      expect: /"removed":true/,
    },
    {
      label: "close second session",
      command: stql("session", "close"),
      expect: /"state":"closed"/,
    },
  ];

  assert.deepEqual(
    [...new Set(steps.flatMap((step) => step.commandId ?? []))].sort(),
    [...availableCommands].sort(),
    "Terminal scenario must cover every CLI command and subcommand",
  );
  return steps;
}

function machineResponses(value: string): Array<Record<string, unknown>> {
  const plain = value
    .replace(/\u001B\][^\u0007]*(?:\u0007|\u001B\\)/g, "")
    .replace(/\u001B\[[0-?]*[ -/]*[@-~]/g, "")
    .replaceAll("\r", "");
  const responses: Array<Record<string, unknown>> = [];
  for (const line of plain.split("\n")) {
    const start = line.indexOf("{");
    if (start < 0) continue;
    try {
      const parsed = JSON.parse(line.slice(start).trim()) as unknown;
      if (
        parsed &&
        typeof parsed === "object" &&
        typeof (parsed as Record<string, unknown>).ok === "boolean"
      ) {
        responses.push(parsed as Record<string, unknown>);
      }
    } catch {
      // Shell echoes and prompts are not machine responses.
    }
  }
  return responses;
}

async function runTerminal(
  steps: TerminalStep[],
  env: NodeJS.ProcessEnv,
): Promise<string> {
  const { spawn } = await import("node-pty");
  return new Promise((resolveOutput, reject) => {
    const marker = `__STQL_PTY_${randomUUID().replaceAll("-", "")}__`;
    const shell = windows ? process.env.ComSpec ?? "cmd.exe" : "/bin/sh";
    const args = windows ? ["/d", "/q", "/v:on"] : [];
    const terminal = spawn(shell, args, {
      cwd: process.cwd(),
      env,
      name: "xterm-256color",
      cols: 2_000,
      rows: 30,
    });
    const state: TerminalState = {};
    let output = "";
    let stepStart = 0;
    let current = 0;
    let settled = false;

    const finish = (error?: Error): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      dataListener.dispose();
      exitListener.dispose();
      if (error) reject(error);
      else resolveOutput(output);
    };

    const fail = (step: TerminalStep, message: string): void => {
      terminal.kill();
      finish(new Error(`${step.label}: ${message}\n${output.slice(stepStart)}`));
    };

    const send = (): void => {
      const step = steps[current]!;
      const command =
        typeof step.command === "function" ? step.command(state) : step.command;
      const line = windows
        ? `${command} & echo ${marker}${current}:!errorlevel!`
        : `${command}; printf '\\n${marker}${current}:%s\\n' "$?"`;
      stepStart = output.length;
      terminal.write(`${line}\r`);
    };

    const dataListener = terminal.onData((data) => {
      output += data;
      const step = steps[current]!;
      const match = output.match(new RegExp(`${marker}${current}:(\\d+)`));
      if (!match || match.index === undefined) return;
      const status = Number(match[1]);
      const expectedStatus = step.expectedStatus ?? 0;
      if (status !== expectedStatus) {
        fail(step, `command exited with status ${status}, expected ${expectedStatus}`);
        return;
      }

      const segment = output.slice(stepStart, match.index);
      const responses = machineResponses(segment);
      const expectedResponses = step.responses ?? 1;
      if (
        step.machineOutput !== false &&
        (responses.length !== expectedResponses ||
          responses.some((response) => response.ok !== (expectedStatus === 0)))
      ) {
        fail(
          step,
          `expected ${expectedResponses} response(s) with ok=${expectedStatus === 0}, got ${JSON.stringify(responses)}`,
        );
        return;
      }
      if (step.expect && !step.expect.test(segment)) {
        fail(step, `output did not match ${step.expect}`);
        return;
      }
      if (step.capture) {
        const handles = [...segment.matchAll(/"handle":"([^"]+)"/g)];
        const handle = handles.at(-1)?.[1];
        if (!handle) {
          fail(step, `response did not contain a handle for ${step.capture}`);
          return;
        }
        state[step.capture] = handle;
      }

      current += 1;
      if (current < steps.length) {
        try {
          send();
        } catch (error) {
          fail(step, error instanceof Error ? error.message : String(error));
        }
      } else {
        terminal.write("exit\r");
      }
    });

    const exitListener = terminal.onExit(({ exitCode }) => {
      if (current === steps.length && exitCode === 0) finish();
      else {
        finish(
          new Error(
            `PTY exited after ${current}/${steps.length} steps with status ${exitCode}:\n${output}`,
          ),
        );
      }
    });

    const timeout = setTimeout(() => {
      terminal.kill();
      finish(
        new Error(
          `PTY test timed out after ${current}/${steps.length} steps:\n${output}`,
        ),
      );
    }, 90_000);

    send();
  });
}

async function runDriver(): Promise<void> {
  try {
    writeFileSync(1, await runTerminal(scenario(), process.env));
    // node-pty 1.1.0 leaves a MessagePort open after ConPTY exits on Windows.
    process.exit(0);
  } catch (error) {
    writeFileSync(2, `${error instanceof Error ? error.stack : error}\n`);
    process.exit(1);
  }
}

if (process.env.STQL_PTY_DRIVER === "1") {
  await runDriver();
} else {
  test("all stql commands run in sequence in a real terminal", async () => {
    const root = createTemporaryDirectory("stateql-pty-test-");
    const bin = join(root, "bin");
    const files = join(root, "terminal files");
    const home = join(root, "state");
    const database = join(files, "terminal.sqlite");
    const exportFile = join(files, "ada-items.json");
    const batchFile = join(files, "commands.json");
    const pipeFile = join(files, "commands.jsonl");
    mkdirSync(bin);
    mkdirSync(files);
    createStqlShim(bin);
    const owner = new StateQL({ home, session: "pty-test" });
    await succeed(owner.linkActor("pty-test", "terminal-actor"));
    owner.close();
    writeFileSync(
      batchFile,
      JSON.stringify([
        {
          command: "query",
          sql: "SELECT name FROM terminal_items ORDER BY id",
          as: "batch-items",
        },
        { command: "count", handle: "batch-items" },
      ]),
    );
    writeFileSync(
      pipeFile,
      [
        JSON.stringify({
          command: "query",
          sql: "SELECT name FROM terminal_items ORDER BY id",
          as: "pipe-items",
        }),
        JSON.stringify({ command: "rows", handle: "pipe-items", limit: 1 }),
        "",
      ].join("\n"),
    );

    const driver = spawnSync(
      process.execPath,
      [resolve("dist/test/terminal.test.js")],
      {
        cwd: process.cwd(),
        env: {
          ...process.env,
          PATH: `${bin}${delimiter}${process.env.PATH ?? ""}`,
          STQL_ACTOR: "terminal-actor",
          STQL_HOME: home,
          STQL_SESSION: "pty-test",
          STQL_PTY_BATCH: batchFile,
          STQL_PTY_DATABASE: database,
          STQL_PTY_DRIVER: "1",
          STQL_PTY_EXPORT: exportFile,
          STQL_PTY_PIPE: pipeFile,
        },
        encoding: "utf8",
        maxBuffer: 2 * 1024 * 1024,
        timeout: 105_000,
      },
    );

    assert.equal(driver.status, 0, driver.stderr || driver.stdout);
    const exported = JSON.parse(readFileSync(exportFile, "utf8")) as Array<{
      name: string;
    }>;
    assert.deepEqual(exported.map((row) => row.name), ["Ada"]);
  });
}
