//! Reproducible synthetic fixture; requires the writer's supported Unix sync.
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

#[path = "../tests/support/replay_fixture/mod.rs"]
mod replay_fixture;

const HELP: &str = "Usage: build_replay_fixture --output PATH\nCreates a synthetic sealed WAL with create_new; an existing PATH is rejected.\n";

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let output = match (args.next(), args.next(), args.next()) {
        (Some(option), None, None) if option == "--help" => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        (Some(option), Some(path), None) if option == "--output" && !path.is_empty() => {
            PathBuf::from(path)
        }
        _ => {
            eprintln!("{HELP}");
            return ExitCode::from(2);
        }
    };
    match replay_fixture::write_fixture(&output) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fixture build failed: {error}");
            ExitCode::from(1)
        }
    }
}
