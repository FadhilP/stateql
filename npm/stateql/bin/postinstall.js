"use strict";

// On a global npm install, npm's `stql` command starts Node, which then starts the binary.
// This script points the command at the binary. If it does nothing or fails, bin/stql.js still works.
const fs = require("node:fs");
const path = require("node:path");
const { findBinary } = require("./binary.js");

function relinkUnix(prefix, binary) {
  // The binary path is a real path, so the link directory must be one too, or the relative link is wrong.
  const directory = fs.realpathSync(path.join(prefix, "bin"));
  const link = path.join(directory, "stql");
  if (!fs.lstatSync(link).isSymbolicLink()) return;
  const replacement = `${link}.${process.pid}.tmp`;
  fs.symlinkSync(path.relative(directory, fs.realpathSync(binary)), replacement);
  // A rename replaces the old link in one step, so the command never goes missing.
  fs.renameSync(replacement, link);
}

function rewriteWindowsShims(prefix, binary) {
  if (!fs.existsSync(path.join(prefix, "stql.cmd"))) return;
  const relative = path.relative(prefix, binary);
  const posixRelative = relative.replaceAll("\\", "/");
  // npm reads the target back from these files to check that they belong to this package
  // (read-cmd-shim), so each keeps the shape npm writes: "%~dp0\<path>" %*, "$basedir/<path>" $args,
  // and "$basedir/<path>" "$@".
  const shims = {
    "stql.cmd": `@ECHO off\r\n"%~dp0\\${relative}" %*\r\n`,
    // A script in a pipeline gets the piped text in $input; the binary needs it on stdin.
    "stql.ps1": [
      "#!/usr/bin/env pwsh",
      "$basedir = Split-Path $MyInvocation.MyCommand.Definition -Parent",
      `if ($MyInvocation.ExpectingInput) { $input | & "$basedir/${posixRelative}" $args } else { & "$basedir/${posixRelative}" $args }`,
      "exit $LASTEXITCODE",
      "",
    ].join("\r\n"),
    // Git Bash and MSYS run this file, not stql.cmd. ${0%/*} avoids a dirname process, which is slow on Windows.
    stql: [
      "#!/bin/sh",
      'case "$0" in */*) basedir="${0%/*}" ;; *) basedir=. ;; esac',
      `exec "$basedir/${posixRelative}" "$@"`,
      "",
    ].join("\n"),
  };
  for (const [name, content] of Object.entries(shims)) {
    fs.writeFileSync(path.join(prefix, name), content);
  }
}

const prefix = process.env.npm_config_global_prefix;
const binary = findBinary();
const globalNpmInstall = process.env.npm_config_global === "true" && Boolean(prefix);

if (globalNpmInstall && binary) {
  try {
    if (process.platform === "win32") rewriteWindowsShims(prefix, binary);
    else relinkUnix(prefix, binary);
  } catch (error) {
    console.error(`stql: the command will start through Node (${error.message}).`);
  }
}
