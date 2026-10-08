const MANIFEST: &str = include_str!("../Cargo.toml");
const CORE_SOURCES: &[(&str, &str)] = &[
    ("decoder.rs", include_str!("../src/decoder.rs")),
    ("continuity.rs", include_str!("../src/continuity.rs")),
    ("data_health.rs", include_str!("../src/data_health.rs")),
    ("json.rs", include_str!("../src/json.rs")),
];
const ALL_SOURCES: &[(&str, &str)] = &[
    ("lib.rs", include_str!("../src/lib.rs")),
    ("decoder.rs", include_str!("../src/decoder.rs")),
    ("continuity.rs", include_str!("../src/continuity.rs")),
    ("data_health.rs", include_str!("../src/data_health.rs")),
    ("json.rs", include_str!("../src/json.rs")),
    ("ws_supervisor.rs", include_str!("../src/ws_supervisor.rs")),
];

fn dependency_boundary_ok(manifest: &str) -> bool {
    let mut section = "";
    let mut runtime_seen = false;
    let mut dev_seen = false;
    let mut runtime = Vec::new();
    let mut dev = Vec::new();

    for raw in manifest.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            if line == "[dependencies]" {
                if runtime_seen {
                    return false;
                }
                runtime_seen = true;
            } else if line == "[dev-dependencies]" {
                if dev_seen {
                    return false;
                }
                dev_seen = true;
            } else if line.starts_with("[dependencies.")
                || line.starts_with("[dev-dependencies.")
                || line == "[build-dependencies]"
                || line.starts_with("[build-dependencies.")
                || (line.starts_with("[target.")
                    && (line.contains(".dependencies") || line.contains(".build-dependencies")))
            {
                return false;
            }
            section = line;
            continue;
        }
        if section == "[dependencies]" {
            runtime.push(line);
        } else if section == "[dev-dependencies]" {
            dev.push(line);
        }
    }

    runtime_seen
        && dev_seen
        && runtime.as_slice() == ["domain = { path = \"../domain\" }"]
        && dev.as_slice() == ["recording = { path = \"../recording\" }"]
}

fn grouped_std_runtime_namespace(compact: &str) -> bool {
    let mut cursor = 0;
    while let Some(relative) = compact[cursor..].find("std::{") {
        let start = cursor + relative + "std::{".len();
        let Some(relative_end) = compact[start..].find('}') else {
            return true;
        };
        let end = start + relative_end;
        for item in compact[start..end].split(',') {
            let root = item.split("::").next().unwrap_or(item);
            let name = root.split("as").next().unwrap_or(root);
            if matches!(name, "fs" | "net" | "process") || (name == "self" && item.contains("as")) {
                return true;
            }
        }
        cursor = end + 1;
    }
    false
}

fn forbidden_concrete_runtime(source: &str) -> Option<&'static str> {
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();
    for (needle, label) in [
        ("std::fs", "filesystem ownership"),
        ("std::net", "socket ownership"),
        ("std::process", "process ownership"),
        ("usestdas", "std namespace alias"),
        ("use::stdas", "absolute std namespace alias"),
        ("externcratestdas", "std crate alias"),
    ] {
        if compact.contains(needle) {
            return Some(label);
        }
    }
    if grouped_std_runtime_namespace(&compact) {
        return Some("grouped forbidden std namespace");
    }

    for (needle, label) in [
        ("TcpStream", "TCP socket ownership"),
        ("UdpSocket", "UDP socket ownership"),
        ("reqwest", "HTTP client"),
        ("hyper::", "HTTP runtime"),
        ("tokio::", "async runtime"),
        ("tungstenite", "WebSocket runtime crate"),
        ("OpenOptions", "filesystem open options"),
        ("File::open", "filesystem file open"),
        ("Instant::now", "live monotonic clock"),
        ("SystemTime::now", "live wall clock"),
        ("http://", "HTTP endpoint"),
        ("https://", "HTTPS endpoint"),
        ("/api/", "REST endpoint"),
    ] {
        if source.contains(needle) {
            return Some(label);
        }
    }
    None
}

#[test]
fn crate_has_exact_runtime_and_test_dependency_allow_lists() {
    assert!(
        dependency_boundary_ok(MANIFEST),
        "REC-001D runtime deps must remain domain-only; recording is test-only"
    );
}

#[test]
fn manifest_gate_rejects_arbitrary_and_hidden_production_dependencies() {
    let arbitrary = MANIFEST.replacen(
        "domain = { path = \"../domain\" }",
        "domain = { path = \"../domain\" }\nanything = \"1\"",
        1,
    );
    assert!(!dependency_boundary_ok(&arbitrary));

    let build = format!("{MANIFEST}\n[build-dependencies]\nanything = \"1\"\n");
    assert!(!dependency_boundary_ok(&build));

    let target = format!("{MANIFEST}\n[target.'cfg(unix)'.dependencies]\nanything = \"1\"\n");
    assert!(!dependency_boundary_ok(&target));

    let dependency_table = format!("{MANIFEST}\n[dependencies.anything]\npath = \"../anything\"\n");
    assert!(!dependency_boundary_ok(&dependency_table));
}

#[test]
fn source_gate_rejects_namespace_import_and_alias_bypasses() {
    for synthetic in [
        "use std::fs; fn x() { let _ = fs::read(\"x\"); }",
        "use std::net; fn x() { let _ = net::TcpStream::connect(\"x\"); }",
        "use std::{io, fs}; fn x() { let _ = fs::read(\"x\"); }",
        "use std::{net, io}; fn x() { let _ = net::TcpStream::connect(\"x\"); }",
        "use std as system; use system::fs;",
        "use ::std as system; use system::net;",
        "use std::{self as system}; use system::process;",
        "extern crate std as system; use system::net;",
    ] {
        assert!(
            forbidden_concrete_runtime(synthetic).is_some(),
            "synthetic forbidden source bypassed regression guard: {synthetic}"
        );
    }
}

#[test]
fn canonical_order_book_mutation_is_absent_from_market_data_source() {
    for (name, source) in ALL_SOURCES {
        for forbidden in [
            "MarketPayload",
            "LevelChange",
            "BookUpdate",
            "SetLevel",
            "DeleteLevel",
            "QuantitySteps",
            "parse_quantity",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} contains forbidden canonical mutation token {forbidden}"
            );
        }
    }
}

#[test]
fn rec001c_core_remains_free_of_runtime_io_and_live_clocks() {
    for (name, source) in CORE_SOURCES {
        assert!(
            forbidden_concrete_runtime(source).is_none(),
            "{name} unexpectedly owns runtime I/O: {:?}",
            forbidden_concrete_runtime(source)
        );
    }
}

#[test]
fn supervisor_is_protocol_only_without_rest_or_private_transport_stack() {
    let source = include_str!("../src/ws_supervisor.rs");
    assert_eq!(forbidden_concrete_runtime(source), None);
    assert!(source.contains("wss://ws.bitget.com/v3/ws/public"));
    assert!(!source.contains("\"op\":\"login\""));
    assert!(!source.contains("publicTrade"));
}

#[test]
fn private_api_execution_and_strategy_tokens_are_absent() {
    for (name, source) in ALL_SOURCES {
        for forbidden in [
            "api_key",
            "secret_key",
            "place_order",
            "cancel_order",
            "TradePlan",
            "Telegram",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} contains out-of-scope token {forbidden}"
            );
        }
    }
}
