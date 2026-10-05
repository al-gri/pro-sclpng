use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_radar"))
        .args(args)
        .output()
        .expect("radar binary must start")
}

fn assert_success(args: &[&str], mode: &str) {
    let output = run(args);
    assert_eq!(output.status.code(), Some(0), "{args:?}: {output:?}");
    assert_eq!(
        output.stdout,
        format!("mode={mode} execution=disabled\n").as_bytes(),
        "{args:?}"
    );
    assert!(output.stderr.is_empty(), "{args:?}: {output:?}");
}

fn assert_rejected(output: &Output, diagnostic: &str) {
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(output.stdout.is_empty(), "success output: {output:?}");
    let stderr = std::str::from_utf8(&output.stderr).expect("UTF-8 diagnostic");
    assert!(stderr.starts_with("error: "), "{stderr}");
    assert!(stderr.contains(diagnostic), "{stderr}");
    assert!(stderr.contains("usage: radar [--mode offline|shadow]"));
    assert!(!stderr.contains("panicked"), "{stderr}");
}

#[test]
fn defaults_to_offline() {
    assert_success(&[], "offline");
}

#[test]
fn accepts_explicit_offline() {
    assert_success(&["--mode", "offline"], "offline");
}

#[test]
fn accepts_shadow_without_execution() {
    assert_success(&["--mode", "shadow"], "shadow");
}

#[test]
fn rejects_live_mode() {
    assert_rejected(&run(&["--mode", "live"]), "live mode is not supported");
}

#[test]
fn rejects_unknown_modes_without_fallback() {
    for value in ["paper", "LIVE", "", "offline ", "--live"] {
        assert_rejected(&run(&["--mode", value]), "unsupported mode");
    }
}

#[test]
fn rejects_missing_mode_value() {
    assert_rejected(&run(&["--mode"]), "--mode requires a value");
}

#[test]
fn rejects_live_flag() {
    assert_rejected(&run(&["--live"]), "unknown argument");
}

#[test]
fn rejects_unknown_arguments() {
    for argument in ["--unknown", "offline", "--help", "--", ""] {
        assert_rejected(&run(&[argument]), "unknown argument");
    }
}

#[test]
fn rejects_equals_syntax() {
    for argument in ["--mode=offline", "--mode=shadow", "--mode=live"] {
        assert_rejected(&run(&[argument]), "unknown argument");
    }
}

#[test]
fn rejects_trailing_arguments_after_valid_mode() {
    for mode in ["offline", "shadow"] {
        for argument in ["--live", "--unknown", "extra"] {
            assert_rejected(&run(&["--mode", mode, argument]), "unknown argument");
        }
    }
}

#[test]
fn rejects_duplicate_mode_even_when_values_match() {
    for first in ["offline", "shadow"] {
        for second in ["offline", "shadow", "live", "paper"] {
            assert_rejected(
                &run(&["--mode", first, "--mode", second]),
                "--mode may only be specified once",
            );
        }
    }
}

#[test]
fn rejects_duplicate_mode_without_second_value() {
    assert_rejected(
        &run(&["--mode", "offline", "--mode"]),
        "--mode may only be specified once",
    );
}

#[test]
fn rejects_invalid_mode_before_later_valid_mode() {
    assert_rejected(
        &run(&["--mode", "live", "--mode", "offline"]),
        "live mode is not supported",
    );
}

#[cfg(unix)]
#[test]
fn rejects_non_unicode_argument_without_panic() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let output = Command::new(env!("CARGO_BIN_EXE_radar"))
        .arg(OsString::from_vec(vec![0xff]))
        .output()
        .expect("radar binary must start");
    assert_rejected(&output, "unknown argument");
}

#[cfg(unix)]
#[test]
fn rejects_non_unicode_mode_without_panic() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let output = Command::new(env!("CARGO_BIN_EXE_radar"))
        .arg("--mode")
        .arg(OsString::from_vec(vec![0xff]))
        .output()
        .expect("radar binary must start");
    assert_rejected(&output, "unsupported mode");
}
