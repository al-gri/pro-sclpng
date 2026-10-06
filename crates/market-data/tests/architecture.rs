const MANIFEST: &str = include_str!("../Cargo.toml");
const SOURCES: &[(&str, &str)] = &[
    ("lib.rs", include_str!("../src/lib.rs")),
    ("decoder.rs", include_str!("../src/decoder.rs")),
    ("continuity.rs", include_str!("../src/continuity.rs")),
    ("data_health.rs", include_str!("../src/data_health.rs")),
    ("json.rs", include_str!("../src/json.rs")),
];

fn runtime_dependency_boundary_ok(manifest: &str) -> bool {
    let mut section = "";
    let mut dependencies_seen = false;
    let mut runtime_dependencies = Vec::new();

    for raw in manifest.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            if line == "[dependencies]" {
                if dependencies_seen {
                    return false;
                }
                dependencies_seen = true;
            } else if line.starts_with("[dependencies.")
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
            runtime_dependencies.push(line);
        }
    }

    dependencies_seen && runtime_dependencies.as_slice() == ["domain = { path = \"../domain\" }"]
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
            if matches!(name, "fs" | "net" | "process")
                || (name == "self" && item.contains("as"))
            {
                return true;
            }
        }
        cursor = end + 1;
    }
    false
}

fn source_boundary_violation(source: &str) -> Option<&'static str> {
    let compact: String = source.chars().filter(|ch| !ch.is_whitespace()).collect();
    for (needle, label) in [
        ("std::fs", "std filesystem namespace"),
        ("std::net", "std network namespace"),
        ("std::process", "std process namespace"),
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
        ("tungstenite", "WebSocket runtime"),
        ("OpenOptions", "filesystem open options"),
        ("File::open", "filesystem file open"),
        ("Instant::now", "live monotonic clock"),
        ("SystemTime::now", "live wall clock"),
        ("http://", "HTTP endpoint"),
        ("https://", "HTTPS endpoint"),
        ("REST", "REST path"),
        ("WAL", "WAL file path"),
    ] {
        if source.contains(needle) {
            return Some(label);
        }
    }
    None
}

#[test]
fn crate_has_exact_accepted_runtime_dependency_allow_list() {
    assert!(
        runtime_dependency_boundary_ok(MANIFEST),
        "REC-001C runtime dependencies must be exactly domain = {{ path = \"../domain\" }} with no build/target production dependency sections"
    );
}

#[test]
fn manifest_gate_rejects_arbitrary_and_hidden_production_dependencies() {
    let arbitrary = MANIFEST.replacen(
        "domain = { path = \"../domain\" }",
        "domain = { path = \"../domain\" }\nanything = \"1\"",
        1,
    );
    assert!(!runtime_dependency_boundary_ok(&arbitrary));

    let build = format!("{MANIFEST}\n[build-dependencies]\n");
    assert!(!runtime_dependency_boundary_ok(&build));

    let target = format!("{MANIFEST}\n[target.'cfg(unix)'.dependencies]\nanything = \"1\"\n");
    assert!(!runtime_dependency_boundary_ok(&target));

    let dependency_table = format!("{MANIFEST}\n[dependencies.anything]\npath = \"../anything\"\n");
    assert!(!runtime_dependency_boundary_ok(&dependency_table));
}

#[test]
fn canonical_order_book_mutation_is_absent_from_market_data_source() {
    for (name, source) in SOURCES {
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
fn runtime_network_rest_wal_file_io_and_live_clocks_are_absent() {
    for (name, source) in SOURCES {
        assert!(
            source_boundary_violation(source).is_none(),
            "{name} contains forbidden runtime/I/O surface: {:?}",
            source_boundary_violation(source)
        );
    }
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
    ] {
        assert!(
            source_boundary_violation(synthetic).is_some(),
            "synthetic forbidden source bypassed regression guard: {synthetic}"
        );
    }
}

#[test]
fn private_api_execution_and_strategy_tokens_are_absent() {
    for (name, source) in SOURCES {
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
