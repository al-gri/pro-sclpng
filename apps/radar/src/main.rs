//! Offline-only bootstrap CLI. Shadow is a label, not a market connection.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::io::{self, Write};
use std::process::ExitCode;

const USAGE: &str = "usage: radar [--mode offline|shadow]";

#[derive(Clone, Copy)]
enum Mode {
    Offline,
    Shadow,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Offline => "offline",
            Self::Shadow => "shadow",
        }
    }
}

fn parse_mode(mut args: impl Iterator<Item = OsString>) -> Result<Mode, String> {
    let mut mode = None;
    while let Some(argument) = args.next() {
        if argument != "--mode" {
            return Err(format!("unknown argument: {argument:?}"));
        }
        if mode.is_some() {
            return Err("--mode may only be specified once".to_owned());
        }
        let value = args.next().ok_or("--mode requires a value")?;
        mode = Some(match value.to_str() {
            Some("offline") => Mode::Offline,
            Some("shadow") => Mode::Shadow,
            Some("live") => return Err("live mode is not supported".to_owned()),
            _ => return Err(format!("unsupported mode: {value:?}")),
        });
    }
    Ok(mode.unwrap_or(Mode::Offline))
}

fn report_error(message: &str) {
    // A closed stderr must not turn an input error into a panic.
    let _ = writeln!(io::stderr().lock(), "error: {message}\n{USAGE}");
}

fn main() -> ExitCode {
    // args_os also rejects non-Unicode arguments without env::args panicking.
    let mode = match parse_mode(std::env::args_os().skip(1)) {
        Ok(mode) => mode,
        Err(error) => {
            report_error(&error);
            return ExitCode::from(2);
        }
    };

    // Validate the entire command line before emitting any success output.
    // Neither supported mode creates connections, signals, or background work.
    match writeln!(io::stdout().lock(), "mode={} execution=disabled", mode.label()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            report_error(&format!("cannot write status: {error}"));
            ExitCode::FAILURE
        }
    }
}
