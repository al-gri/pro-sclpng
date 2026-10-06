use std::panic;

use market_data::{
    BOOKS50_MAX_LEVELS, DecodeError, DecodeLimits, JsonErrorKind, decode_message,
    decode_message_with_limits,
};

fn book_message(
    topic: &str,
    action: &str,
    asks: &str,
    pseq: &str,
    seq: &str,
    inner_ts: &str,
    outer_ts: &str,
) -> String {
    format!(
        r#"{{"arg":{{"instType":"usdt-futures","symbol":"BTCUSDT","topic":"{topic}"}},"action":"{action}","data":[{{"a":{asks},"b":[],"pseq":{pseq},"seq":{seq},"ts":"{inner_ts}"}}],"ts":{outer_ts}}}"#
    )
}

#[test]
fn malformed_json_is_typed_error() {
    assert!(matches!(
        decode_message(b"{"),
        Err(DecodeError::MalformedJson(_))
    ));
}

#[test]
fn oversized_message_is_rejected_before_json_parsing() {
    let limits = DecodeLimits::default();
    let bytes = vec![b' '; limits.max_message_bytes + 1];
    assert_eq!(
        decode_message(&bytes),
        Err(DecodeError::MessageTooLarge {
            actual: bytes.len(),
            max: limits.max_message_bytes,
        })
    );
}

#[test]
fn too_many_books50_levels_is_rejected() {
    let levels = (0..=BOOKS50_MAX_LEVELS)
        .map(|index| format!(r#"["{}","1"]"#, 100 + index))
        .collect::<Vec<_>>()
        .join(",");
    let asks = format!("[{levels}]");
    let message = book_message(
        "books50",
        "update",
        &asks,
        "1000",
        "1001",
        "1770000000000",
        "1770000000001",
    );

    assert!(matches!(
        decode_message(message.as_bytes()),
        Err(DecodeError::TooManyLevels {
            side: "ask",
            count,
            max: BOOKS50_MAX_LEVELS,
        }) if count == BOOKS50_MAX_LEVELS + 1
    ));
}

#[test]
fn wrong_topic_is_rejected() {
    let message = book_message(
        "ticker",
        "update",
        "[]",
        "1000",
        "1001",
        "1770000000000",
        "1770000000001",
    );
    assert_eq!(
        decode_message(message.as_bytes()),
        Err(DecodeError::UnsupportedTopic {
            topic: "ticker".to_owned()
        })
    );
}

#[test]
fn invalid_action_is_rejected() {
    let message = book_message(
        "books50",
        "partial",
        "[]",
        "1000",
        "1001",
        "1770000000000",
        "1770000000001",
    );
    assert!(matches!(
        decode_message(message.as_bytes()),
        Err(DecodeError::InvalidAction { .. })
    ));
}

#[test]
fn invalid_sequence_is_rejected() {
    let message = book_message(
        "books50",
        "update",
        "[]",
        "1000",
        "-1",
        "1770000000000",
        "1770000000001",
    );
    assert_eq!(
        decode_message(message.as_bytes()),
        Err(DecodeError::InvalidSequence { field: "seq" })
    );
}

#[test]
fn sequence_and_previous_sequence_overflow_are_rejected() {
    let seq_overflow = book_message(
        "books50",
        "update",
        "[]",
        "1000",
        "18446744073709551616",
        "1770000000000",
        "1770000000001",
    );
    assert_eq!(
        decode_message(seq_overflow.as_bytes()),
        Err(DecodeError::SequenceOverflow { field: "seq" })
    );

    let pseq_overflow = book_message(
        "books50",
        "update",
        "[]",
        "18446744073709551616",
        "1001",
        "1770000000000",
        "1770000000001",
    );
    assert_eq!(
        decode_message(pseq_overflow.as_bytes()),
        Err(DecodeError::SequenceOverflow { field: "pseq" })
    );
}

#[test]
fn invalid_and_overflowing_timestamps_are_rejected() {
    let invalid = book_message(
        "books50",
        "update",
        "[]",
        "1000",
        "1001",
        "not-a-time",
        "1770000000001",
    );
    assert_eq!(
        decode_message(invalid.as_bytes()),
        Err(DecodeError::InvalidTimestamp {
            field: "data[0].ts"
        })
    );

    let overflow = book_message(
        "books50",
        "update",
        "[]",
        "1000",
        "1001",
        "1770000000000",
        "9223372036854775808",
    );
    assert_eq!(
        decode_message(overflow.as_bytes()),
        Err(DecodeError::TimestampOverflow { field: "ts" })
    );
}

#[test]
fn invalid_field_type_is_rejected() {
    let message = book_message(
        "books50",
        "update",
        "[]",
        "1000",
        r#""1001""#,
        "1770000000000",
        "1770000000001",
    );
    assert_eq!(
        decode_message(message.as_bytes()),
        Err(DecodeError::InvalidFieldType {
            field: "seq",
            expected: "number",
        })
    );
}

#[test]
fn excessive_nesting_is_rejected_by_parser_guard() {
    let nested_value = format!("{}0{}", "[".repeat(6), "]".repeat(6));
    let message = format!(r#"{{"x":{nested_value}}}"#);
    let limits = DecodeLimits {
        max_nesting_depth: 4,
        ..DecodeLimits::default()
    };
    let error = decode_message_with_limits(message.as_bytes(), limits)
        .expect_err("nesting guard must reject input");
    assert!(matches!(
        error,
        DecodeError::MalformedJson(ref json) if json.kind == JsonErrorKind::NestingTooDeep
    ));
}

#[test]
fn excessive_array_size_is_rejected_by_parser_guard() {
    let message = br#"{
        "arg":{"instType":"usdt-futures","symbol":"BTCUSDT","topic":"books50"},
        "action":"update",
        "data":[],
        "ts":1,
        "x":[0,0,0,0,0,0]
    }"#;
    let limits = DecodeLimits {
        max_container_items: 5,
        ..DecodeLimits::default()
    };
    let error = decode_message_with_limits(message, limits)
        .expect_err("container guard must reject input");
    assert!(matches!(
        error,
        DecodeError::MalformedJson(ref json) if json.kind == JsonErrorKind::ContainerTooLarge
    ));
}

#[test]
fn bounded_trade_count_is_enforced() {
    let bytes = include_bytes!("../../../tests/fixtures/bitget/public-trades.json");
    let limits = DecodeLimits {
        max_trades_per_message: 1,
        ..DecodeLimits::default()
    };
    assert_eq!(
        decode_message_with_limits(bytes, limits),
        Err(DecodeError::TooManyTrades { count: 2, max: 1 })
    );
}

#[test]
fn malformed_external_bytes_never_panic() {
    let cases: [&[u8]; 8] = [
        b"",
        b"{",
        b"[]",
        b"{\"",
        &[0xff, 0xfe, 0xfd],
        b"{\"arg\":null}",
        b"{\"arg\":{\"instType\":true}}",
        b"{\"x\":\"\\uD800\"}",
    ];

    for input in cases {
        let result = panic::catch_unwind(|| decode_message(input));
        assert!(result.is_ok(), "decoder panicked for input: {input:?}");
    }
}
