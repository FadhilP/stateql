"use strict";

const fs = require("node:fs");
const path = require("node:path");

const { platform, arch } = process;

// Best first. A static musl binary also runs on glibc.
function candidates() {
  const base = `${platform}-${arch}`;
  if (platform === "win32") return [`${base}-msvc`];
  if (platform !== "linux") return [base];
  const glibc = process.report?.getReport?.().header?.glibcVersionRuntime;
  return glibc ? [`${base}-gnu`, `${base}-musl`] : [`${base}-musl`];
}

// The package holds one directory per platform next to this file.
function findBinary() {
  const exe = platform === "win32" ? "stql.exe" : "stql";
  return candidates()
    .map((directory) => path.join(__dirname, directory, exe))
    .find((binary) => fs.existsSync(binary));
}

module.exports = { findBinary };
