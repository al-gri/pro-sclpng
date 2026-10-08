//! Bounded offline diagnostics for the explicitly synthetic REC-001F-1 profile.
#![forbid(unsafe_code)]

#[path = "../replay/mod.rs"]
mod replay;

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

const HELP: &str = "Usage: radar-replay --wal PATH --profile synthetic-rec001f1-v1\n\nRead-only, offline, single-segment synthetic diagnostic replay.\nCanonical applicability remains BLOCKED_UNVERIFIED; usable_data=false.\n";

enum Command {
    Help,
    Replay(PathBuf),
}

fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Command, &'static str> {
    let mut wal = None;
    let mut profile = None;
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--help") if wal.is_none() && profile.is_none() => {
                return if args.next().is_none() {
                    Ok(Command::Help)
                } else {
                    Err("--help must be used alone")
                };
            }
            Some("--wal") if wal.is_none() => {
                let value = args.next().ok_or("missing --wal value")?;
                if value.is_empty() || value.to_string_lossy().starts_with('-') {
                    return Err("invalid --wal value");
                }
                wal = Some(PathBuf::from(value));
            }
            Some("--profile") if profile.is_none() => {
                let value = args.next().ok_or("missing --profile value")?;
                if value.to_str() != Some(replay::profile::NAME) {
                    return Err("unknown profile");
                }
                profile = Some(());
            }
            _ => return Err("unknown, repeated or malformed argument"),
        }
    }
    match (wal, profile) {
        (Some(path), Some(())) => Ok(Command::Replay(path)),
        _ => Err("--wal and --profile are required"),
    }
}

fn main() -> ExitCode {
    let command = match parse(std::env::args_os().skip(1)) {
        Ok(command) => command,
        Err(reason) => {
            let _ = writeln!(io::stderr().lock(), "radar-replay: {reason}\n{HELP}");
            return ExitCode::from(2);
        }
    };
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let result = match command {
        Command::Help => out.write_all(HELP.as_bytes()).map(|()| true),
        Command::Replay(path) => replay::run(&path, &mut out),
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            let _ = writeln!(
                io::stderr().lock(),
                "radar-replay: incomplete or unsupported diagnostic; see stdout report"
            );
            ExitCode::from(1)
        }
        Err(error) => {
            let _ = writeln!(
                io::stderr().lock(),
                "radar-replay: report output failure {:?}",
                error.kind()
            );
            ExitCode::from(1)
        }
    }
}
