#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const { findBinary } = require("./binary.js");

const binary = findBinary();
if (!binary) {
  console.error(
    `stql: no prebuilt binary for ${process.platform}-${process.arch}; ` +
      "install from source with `cargo install --locked --git https://github.com/FadhilP/stateql`",
  );
  process.exit(1);
}

const result = spawnSync(binary, process.argv.slice(2), { stdio: "inherit" });
if (result.error) {
  console.error(`stql: ${result.error.message}`);
  process.exit(1);
}
if (result.signal) {
  process.kill(process.pid, result.signal);
}
process.exit(result.status ?? 1);
