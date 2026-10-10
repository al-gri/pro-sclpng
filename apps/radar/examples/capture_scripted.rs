//! Bounded, synchronous, explicitly synthetic capture. This opens no network connection.
#![forbid(unsafe_code)]

#[path = "../src/capture_driver/mod.rs"]
mod capture_driver;

use capture_driver::{DriverError, Scenario, run_scenario};
use std::path::PathBuf;

fn arguments() -> Result<(Scenario, PathBuf), DriverError> {
    let mut args = std::env::args_os().skip(1);
    let mut scenario = None;
    let mut output = None;
    while let Some(arg) = args.next() {
        match arg
            .to_str()
            .ok_or_else(|| DriverError::new("invalid_arguments"))?
        {
            "--scenario" if scenario.is_none() => {
                let value = args
                    .next()
                    .ok_or_else(|| DriverError::new("missing_scenario"))?;
                scenario = Some(Scenario::parse(
                    value
                        .to_str()
                        .ok_or_else(|| DriverError::new("invalid_scenario"))?,
                )?);
            }
            "--output" if output.is_none() => {
                output = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| DriverError::new("missing_output"))?
                        .into_string()
                        .map_err(|_| DriverError::new("invalid_output"))?,
                ));
            }
            _ => return Err(DriverError::new("invalid_arguments")),
        }
    }
    Ok((
        scenario.ok_or_else(|| DriverError::new("missing_scenario"))?,
        output.ok_or_else(|| DriverError::new("missing_output"))?,
    ))
}

fn main() {
    let result = arguments().and_then(|(scenario, output)| run_scenario(output, scenario));
    match result {
        Ok(summary) => {
            println!("{}", summary.json());
            let exit = summary.scenario.exit_code();
            if exit != 0 {
                eprintln!("capture_scripted: {}", summary.outcome);
            }
            std::process::exit(exit);
        }
        Err(error) => {
            eprintln!("capture_scripted: {}", error.code);
            std::process::exit(3);
        }
    }
}
