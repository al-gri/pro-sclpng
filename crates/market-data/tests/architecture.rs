const MANIFEST: &str = include_str!("../Cargo.toml");
const SOURCES: &[(&str, &str)] = &[
    ("lib.rs", include_str!("../src/lib.rs")),
    ("decoder.rs", include_str!("../src/decoder.rs")),
    ("continuity.rs", include_str!("../src/continuity.rs")),
    ("data_health.rs", include_str!("../src/data_health.rs")),
    ("json.rs", include_str!("../src/json.rs")),
];

#[test]
fn crate_has_only_the_accepted_domain_runtime_dependency() {
    assert!(
        MANIFEST.contains("[dependencies]\ndomain = { path = \"../domain\" }"),
        "REC-001C must reuse the accepted domain contracts through the local path dependency"
    );
    for forbidden in [
        "reqwest",
        "hyper",
        "tokio",
        "tungstenite",
        "serde",
        "serde_json",
        "rusqlite",
        "parquet",
    ] {
        assert!(
            !MANIFEST.contains(forbidden),
            "market-data gained forbidden runtime dependency {forbidden}"
        );
    }
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
        for forbidden in [
            "std::net::",
            "TcpStream",
            "UdpSocket",
            "reqwest",
            "hyper::",
            "tokio::",
            "tungstenite",
            "std::fs::",
            "OpenOptions",
            "File::open",
            "Instant::now",
            "SystemTime::now",
            "http://",
            "https://",
            "REST",
            "WAL",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} contains forbidden runtime/I/O token {forbidden}"
            );
        }
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
