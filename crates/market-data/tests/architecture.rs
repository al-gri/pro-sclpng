const MANIFEST: &str = include_str!("../Cargo.toml");
const SOURCES: &[(&str, &str)] = &[
    ("lib.rs", include_str!("../src/lib.rs")),
    ("decoder.rs", include_str!("../src/decoder.rs")),
    ("continuity.rs", include_str!("../src/continuity.rs")),
    ("json.rs", include_str!("../src/json.rs")),
];

#[test]
fn crate_has_no_external_runtime_dependencies() {
    assert!(
        !MANIFEST.contains("[dependencies]"),
        "REC-001A must remain std-only"
    );
}

#[test]
fn canonical_effect_types_are_absent_from_market_data_source() {
    for (name, source) in SOURCES {
        for forbidden in [
            "domain::",
            "MarketPayload",
            "LevelChange",
            "BookUpdate",
            "DeleteLevel",
            "Aggressor",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} contains forbidden canonical token {forbidden}"
            );
        }
    }
}

#[test]
fn network_rest_wal_and_file_io_are_absent_from_market_data_source() {
    for (name, source) in SOURCES {
        for forbidden in [
            "std::net::",
            "TcpStream",
            "UdpSocket",
            "reqwest",
            "hyper::",
            "tokio::",
            "std::fs::",
            "OpenOptions",
            "File::open",
            "http://",
            "https://",
            "REST",
            "WAL",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} contains forbidden I/O token {forbidden}"
            );
        }
    }
}
