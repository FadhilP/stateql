use std::io::Write;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use stateql::{Cli, Code, Command, Engine, Error, PipeMode, json_line, text};

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            let _ = error.print();
            return ExitCode::SUCCESS;
        }
        Err(error) => return finish(&Err(parse_error(&error)), false),
    };
    let engine = match Engine::open(&Engine::default_home()) {
        Ok(engine) => engine,
        Err(error) => return finish(&Err(error), cli.text),
    };
    let session_name = std::env::var("STQL_SESSION").unwrap_or_else(|_| "default".to_string());
    let mut session = match engine.session(&session_name) {
        Ok(session) => session,
        Err(error) => return finish(&Err(error), cli.text),
    };
    session.set_timeout(Duration::from_millis(cli.timeout_ms));
    stateql::spawn_watchdog();
    let notice = engine.migration_notice().map(str::to_string);
    let command = match &cli.profile {
        None => cli.command,
        Some(profile) => match session.with_profile(profile, cli.command) {
            Ok(command) => command,
            Err(mut error) => {
                error.notice = notice;
                return finish(&Err(error), cli.text);
            }
        },
    };
    session.set_notice(notice);
    let mode = match command {
        Command::Pipe {
            atomic: true,
            isolation,
            ..
        } => PipeMode::Atomic(isolation),
        Command::Pipe {
            continue_on_error: true,
            ..
        } => PipeMode::ContinueOnError,
        Command::Pipe { .. } => PipeMode::StopOnError,
        command => return finish(&session.run(command), cli.text),
    };
    // Not locked for the whole pipe: the watchdog thread must be able to print.
    let mut stdout = std::io::stdout();
    match session.pipe(std::io::stdin().lock(), &mut stdout, mode) {
        Ok(code) => ExitCode::from(code),
        Err(error) => finish(&Err(error), false),
    }
}

const REMOVED_NAMES: [(&str, &str); 6] = [
    ("--cache", "stql has no cache; remove --cache"),
    ("inspect", "use stql describe NAME"),
    ("object", "use stql describe NAME"),
    ("transaction", "use stql pipe --atomic"),
    ("plan", "use stql exec --expect-rows N"),
    ("apply", "use stql exec --expect-rows N"),
];

// Keeps the first paragraph of clap's error; its tips and usage block mislead agents.
fn parse_error(error: &clap::Error) -> Error {
    let rendered = error.render().to_string();
    let paragraph: Vec<&str> = rendered
        .lines()
        .take_while(|line| !line.trim().is_empty())
        .map(str::trim)
        .collect();
    let message = paragraph.join(" ");
    let message = message.strip_prefix("error: ").unwrap_or(&message);
    let rejected = [ContextKind::InvalidSubcommand, ContextKind::InvalidArg]
        .into_iter()
        .find_map(|kind| match error.get(kind) {
            Some(ContextValue::String(token)) => Some(token.split('=').next().unwrap_or_default()),
            _ => None,
        });
    let hint = REMOVED_NAMES
        .iter()
        .find(|(name, _)| Some(*name) == rejected)
        .map_or("stql help", |(_, hint)| hint);
    Error::new(Code::InvalidCommand, message).with_hint(hint)
}

fn finish(result: &Result<stateql::Response, Error>, as_text: bool) -> ExitCode {
    let output = if as_text {
        text(result)
    } else {
        format!("{}\n", json_line(result))
    };
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(output.as_bytes());
    let _ = stdout.flush();
    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(error) => ExitCode::from(error.code.exit_code()),
    }
}
