use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(
    name = "stql",
    version,
    about = "Run a database query once, keep the result, and work from a handle."
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        default_value_t = 30_000,
        help = "Deadline for each command in milliseconds"
    )]
    pub timeout_ms: u64,
    #[arg(
        long,
        global = true,
        help = "Print human-readable tables instead of JSON"
    )]
    pub text: bool,
    #[arg(
        long,
        global = true,
        value_name = "NAME",
        help = "Connect this session to a saved profile first, unless it already is"
    )]
    pub profile: Option<String>,
    #[command(subcommand)]
    pub command: Command,
}

const CELL_HELP: &str = "Characters of a text cell before it is cut in this output (default 200)";
const BYTES_HELP: &str = "Drop rows from the end of the page until the JSON line fits in N bytes";

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct JsonArray(pub Vec<Value>);

fn parse_json_array(text: &str) -> Result<JsonArray, String> {
    serde_json::from_str(text).map_err(|error| format!("expected a JSON array: {error}"))
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Csv,
    Json,
    Jsonl,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Isolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Subcommand)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    #[command(about = "Open a database once, ping it, and keep the target in the session")]
    Connect {
        #[arg(help = "Database URL, sqlite:PATH, a file path, or a profile name")]
        target: Option<String>,
        #[arg(
            long,
            value_name = "VAR",
            conflicts_with = "target",
            help = "Read the full URL from this environment variable"
        )]
        env: Option<String>,
        #[arg(long, help = "Allow writes (the default is read-only)")]
        #[serde(default)]
        read_write: bool,
        #[arg(
            long,
            conflicts_with = "read_write",
            help = "Refuse writes, also for a profile saved with --read-write"
        )]
        #[serde(default)]
        read_only: bool,
    },
    #[command(about = "Forget the connection of this session")]
    Disconnect {},
    #[command(about = "Show the connection, open results, and recent writes")]
    Status {
        #[arg(long, help = "List all sessions")]
        #[serde(default)]
        all: bool,
    },
    #[command(about = "Manage saved connection profiles")]
    #[serde(skip)]
    Profile {
        #[command(subcommand)]
        action: ProfileCommand,
    },
    #[command(about = "Run a read, store the full result, and return a preview and a handle")]
    Query {
        #[arg(
            required_unless_present = "file",
            help = "SQL, or a MongoDB or Redis JSON command"
        )]
        stmt: Option<String>,
        #[arg(
            long,
            conflicts_with = "stmt",
            help = "Read the statement from a file (- for stdin)"
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "VALUE",
            help = "Bind a string parameter (repeat for more)"
        )]
        #[serde(default)]
        param: Vec<String>,
        #[arg(long, value_name = "JSON", value_parser = parse_json_array, conflicts_with = "param", help = "Bind parameters from a JSON array")]
        params: Option<JsonArray>,
        #[arg(long = "as", value_name = "NAME", help = "Name the result")]
        #[serde(rename = "as")]
        name: Option<String>,
        #[arg(long, value_name = "N", help = "Rows in the preview (default 20)")]
        preview_rows: Option<u64>,
        #[arg(
            long,
            value_name = "N",
            help = "Row cap for the stored result (default 100000)"
        )]
        max_rows: Option<u64>,
        #[arg(long, value_name = "N", help = CELL_HELP)]
        max_cell_characters: Option<usize>,
        #[arg(long, value_name = "N", help = BYTES_HELP)]
        max_bytes: Option<usize>,
    },
    #[command(about = "Run a guarded write")]
    Exec {
        #[arg(
            required_unless_present = "file",
            help = "SQL, or a MongoDB or Redis JSON command"
        )]
        stmt: Option<String>,
        #[arg(
            long,
            conflicts_with = "stmt",
            help = "Read the statement from a file (- for stdin)"
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "VALUE",
            help = "Bind a string parameter (repeat for more)"
        )]
        #[serde(default)]
        param: Vec<String>,
        #[arg(long, value_name = "JSON", value_parser = parse_json_array, conflicts_with = "param", help = "Bind parameters from a JSON array")]
        params: Option<JsonArray>,
        #[arg(
            long,
            value_name = "KEY",
            help = "Return the earlier receipt when the same key and statement repeat"
        )]
        idempotency_key: Option<String>,
        #[arg(
            long,
            help = "Run again although an earlier identical write has an unknown outcome"
        )]
        #[serde(default)]
        replay: bool,
        #[arg(
            long,
            help = "Allow UPDATE or DELETE without WHERE, TRUNCATE, or an empty MongoDB filter"
        )]
        #[serde(default)]
        allow_unbounded: bool,
        #[arg(
            long,
            help = "Allow DELETE, DROP, ALTER, TRUNCATE, REPLACE, and upserts that update"
        )]
        #[serde(default)]
        allow_destructive: bool,
        #[arg(
            long,
            value_name = "N",
            help = "Roll back unless exactly N rows change"
        )]
        expect_rows: Option<u64>,
    },
    #[command(about = "Show the receipt of a write")]
    Receipt { handle: String },
    #[command(about = "Page a stored result (--limit 0 returns only metadata)")]
    Rows {
        handle: String,
        #[arg(long)]
        offset: Option<u64>,
        #[arg(long)]
        limit: Option<u64>,
        #[arg(long, value_name = "A,B", help = "Comma-separated column names")]
        columns: Option<String>,
        #[arg(long, value_name = "N", help = CELL_HELP)]
        max_cell_characters: Option<usize>,
        #[arg(long, value_name = "N", help = BYTES_HELP)]
        max_bytes: Option<usize>,
    },
    #[command(about = "Make a new result from the rows that match a SQLite predicate")]
    Filter {
        handle: String,
        predicate: String,
        #[arg(
            long,
            value_name = "VALUE",
            help = "Bind a string parameter (repeat for more)"
        )]
        #[serde(default)]
        param: Vec<String>,
        #[arg(long, value_name = "JSON", value_parser = parse_json_array, conflicts_with = "param", help = "Bind parameters from a JSON array")]
        params: Option<JsonArray>,
        #[arg(long = "as", value_name = "NAME", help = "Name the new result")]
        #[serde(rename = "as")]
        name: Option<String>,
        #[arg(long, value_name = "N", help = CELL_HELP)]
        max_cell_characters: Option<usize>,
        #[arg(long, value_name = "N", help = BYTES_HELP)]
        max_bytes: Option<usize>,
    },
    #[command(about = "Show type, nulls, distinct values, min, and max per column")]
    Summarize { handle: String },
    #[command(about = "Write a stored result to a new file")]
    Export {
        handle: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, value_enum)]
        format: Option<ExportFormat>,
        #[arg(long, value_name = "A,B", help = "Comma-separated column names")]
        columns: Option<String>,
    },
    #[command(about = "List database objects")]
    Objects {
        kind: Option<String>,
        #[arg(long)]
        search: Option<String>,
        #[arg(long)]
        limit: Option<u64>,
        #[arg(long)]
        cursor: Option<String>,
    },
    #[command(about = "Show columns, keys, indexes, and DDL of one object")]
    Describe { name: String },
    #[command(about = "Show the history of this session")]
    History {
        #[arg(long)]
        limit: Option<u64>,
    },
    #[command(about = "Delete stored results (default: this session)")]
    Purge {
        #[arg(long, conflicts_with_all = ["session", "all"], help = "Delete expired results of all sessions")]
        #[serde(default)]
        expired: bool,
        #[arg(
            long,
            value_name = "NAME",
            conflicts_with = "all",
            help = "Delete the results of one session"
        )]
        session: Option<String>,
        #[arg(long, help = "Delete all results")]
        #[serde(default)]
        all: bool,
    },
    #[command(about = "Check the home directory, the store, the keyring, and old TypeScript state")]
    Doctor {},
    #[command(about = "Run one JSON command per stdin line on one connection")]
    #[serde(skip)]
    Pipe {
        #[arg(long, conflicts_with = "atomic")]
        continue_on_error: bool,
        #[arg(long, help = "Run all lines in one transaction")]
        atomic: bool,
        #[arg(long, value_enum, requires = "atomic")]
        isolation: Option<Isolation>,
    },
}

#[derive(Clone, Debug, PartialEq, Subcommand)]
pub enum ProfileCommand {
    #[command(about = "Save a profile")]
    Add {
        name: String,
        #[arg(
            required_unless_present = "env",
            help = "Database URL without a password"
        )]
        url: Option<String>,
        #[arg(long, value_name = "VAR", conflicts_with_all = ["url", "ask_password"], help = "Read the full URL from this environment variable")]
        env: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            requires = "env",
            help = "Read VAR from this file when the environment does not set it"
        )]
        env_file: Option<PathBuf>,
        #[arg(
            long,
            help = "Ask for the password in a dialog and keep it in the OS keyring"
        )]
        ask_password: bool,
        #[arg(long, help = "Connect read-write by default")]
        read_write: bool,
        #[arg(long, help = "Replace an existing profile")]
        force: bool,
    },
    #[command(about = "List profiles")]
    List,
    #[command(about = "Remove a profile and its saved password")]
    Remove { name: String },
}
