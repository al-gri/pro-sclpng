use std::error::Error;
use std::fmt;

use crate::json::{JsonError, JsonValue, ParserLimits, parse_json};

pub const BOOKS50_MAX_LEVELS: usize = 50;

const HARD_MAX_MESSAGE_BYTES: usize = 1_048_576;
const HARD_MAX_TRADES_PER_MESSAGE: usize = 4_096;
const HARD_MAX_CONTAINER_ITEMS: usize = 8_192;
const HARD_MAX_NESTING_DEPTH: usize = 64;
const HARD_MAX_STRING_BYTES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeLimits {
    pub max_message_bytes: usize,
    pub max_trades_per_message: usize,
    pub max_container_items: usize,
    pub max_nesting_depth: usize,
    pub max_string_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024,
            max_trades_per_message: 1024,
            max_container_items: 2048,
            max_nesting_depth: 16,
            max_string_bytes: 4096,
        }
    }
}

impl DecodeLimits {
    fn validate(self) -> Result<Self, DecodeError> {
        for (field, value, hard_max) in [
            (
                "max_message_bytes",
                self.max_message_bytes,
                HARD_MAX_MESSAGE_BYTES,
            ),
            (
                "max_trades_per_message",
                self.max_trades_per_message,
                HARD_MAX_TRADES_PER_MESSAGE,
            ),
            (
                "max_container_items",
                self.max_container_items,
                HARD_MAX_CONTAINER_ITEMS,
            ),
            (
                "max_nesting_depth",
                self.max_nesting_depth,
                HARD_MAX_NESTING_DEPTH,
            ),
            (
                "max_string_bytes",
                self.max_string_bytes,
                HARD_MAX_STRING_BYTES,
            ),
        ] {
            if value > hard_max {
                return Err(DecodeError::InvalidLimit {
                    field,
                    value,
                    hard_max,
                });
            }
        }
        Ok(self)
    }

    fn parser_limits(self) -> ParserLimits {
        ParserLimits {
            max_nesting_depth: self.max_nesting_depth,
            max_container_items: self.max_container_items,
            max_string_bytes: self.max_string_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
    Spot,
    UsdtFutures,
    CoinFutures,
    UsdcFutures,
}

impl Category {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::UsdtFutures => "usdt-futures",
            Self::CoinFutures => "coin-futures",
            Self::UsdcFutures => "usdc-futures",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Topic {
    Books50,
    PublicTrade,
}

impl Topic {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Books50 => "books50",
            Self::PublicTrade => "publicTrade",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Snapshot,
    Update,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FillSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpiFlag {
    Yes,
    No,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LexicalValue(String);

impl LexicalValue {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireTimestampMs {
    pub value: i64,
    pub lexical: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireLevel {
    pub price: LexicalValue,
    pub quantity: LexicalValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Books50Frame {
    pub category: Category,
    pub symbol: String,
    pub topic: Topic,
    pub action: Action,
    pub envelope_timestamp: WireTimestampMs,
    pub source_timestamp: WireTimestampMs,
    pub seq: u64,
    pub pseq: u64,
    pub asks: Vec<WireLevel>,
    pub bids: Vec<WireLevel>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireTrade {
    pub price: LexicalValue,
    pub size: LexicalValue,
    pub execution_id: String,
    pub correlation_id: String,
    pub fill_side: FillSide,
    pub timestamp: WireTimestampMs,
    pub is_rpi: RpiFlag,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicTradeFrame {
    pub category: Category,
    pub symbol: String,
    pub topic: Topic,
    pub action: Action,
    pub envelope_timestamp: WireTimestampMs,
    pub trades: Vec<WireTrade>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitgetMessage {
    Books50(Books50Frame),
    PublicTrade(PublicTradeFrame),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    InvalidLimit {
        field: &'static str,
        value: usize,
        hard_max: usize,
    },
    MessageTooLarge {
        actual: usize,
        max: usize,
    },
    MalformedJson(JsonError),
    MissingField {
        field: &'static str,
    },
    InvalidFieldType {
        field: &'static str,
        expected: &'static str,
    },
    InvalidText {
        field: &'static str,
    },
    UnsupportedCategory {
        category: String,
    },
    UnsupportedTopic {
        topic: String,
    },
    UnsupportedProfile {
        topic: String,
    },
    InvalidAction {
        topic: Topic,
        action: String,
    },
    UnexpectedDataCount {
        topic: Topic,
        count: usize,
    },
    TooManyLevels {
        side: &'static str,
        count: usize,
        max: usize,
    },
    InvalidLevelShape {
        side: &'static str,
        index: usize,
        count: usize,
    },
    TooManyTrades {
        count: usize,
        max: usize,
    },
    InvalidSequence {
        field: &'static str,
    },
    SequenceOverflow {
        field: &'static str,
    },
    InvalidTimestamp {
        field: &'static str,
    },
    TimestampOverflow {
        field: &'static str,
    },
    InvalidFillSide {
        value: String,
    },
    InvalidRpiFlag {
        value: String,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for DecodeError {}

impl From<JsonError> for DecodeError {
    fn from(error: JsonError) -> Self {
        Self::MalformedJson(error)
    }
}

struct Subscription {
    category: Category,
    symbol: String,
    raw_topic: String,
}

pub fn decode_message(bytes: &[u8]) -> Result<BitgetMessage, DecodeError> {
    decode_message_with_limits(bytes, DecodeLimits::default())
}

pub fn decode_message_with_limits(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<BitgetMessage, DecodeError> {
    let limits = limits.validate()?;
    if bytes.len() > limits.max_message_bytes {
        return Err(DecodeError::MessageTooLarge {
            actual: bytes.len(),
            max: limits.max_message_bytes,
        });
    }

    let value = parse_json(bytes, limits.parser_limits())?;
    let root = expect_object(&value, "root")?;
    let subscription = parse_subscription(root)?;

    match subscription.raw_topic.as_str() {
        "books50" => decode_books50(root, subscription),
        "publicTrade" => decode_public_trade(root, subscription, limits),
        "rpi-books50" => Err(DecodeError::UnsupportedProfile {
            topic: subscription.raw_topic,
        }),
        _ => Err(DecodeError::UnsupportedTopic {
            topic: subscription.raw_topic,
        }),
    }
}

fn decode_books50(
    root: &[(String, JsonValue)],
    subscription: Subscription,
) -> Result<BitgetMessage, DecodeError> {
    let action = parse_action(root, Topic::Books50, true)?;
    let envelope_timestamp = parse_timestamp_number(field(root, "ts")?, "ts")?;
    let data = expect_array(field(root, "data")?, "data")?;
    if data.len() != 1 {
        return Err(DecodeError::UnexpectedDataCount {
            topic: Topic::Books50,
            count: data.len(),
        });
    }

    let payload = expect_object(&data[0], "data[0]")?;
    let asks = parse_levels(field(payload, "a")?, "ask")?;
    let bids = parse_levels(field(payload, "b")?, "bid")?;
    let pseq = parse_sequence(field(payload, "pseq")?, "pseq")?;
    let seq = parse_sequence(field(payload, "seq")?, "seq")?;
    let source_timestamp =
        parse_timestamp_string(field(payload, "ts")?, "data[0].ts")?;

    Ok(BitgetMessage::Books50(Books50Frame {
        category: subscription.category,
        symbol: subscription.symbol,
        topic: Topic::Books50,
        action,
        envelope_timestamp,
        source_timestamp,
        seq,
        pseq,
        asks,
        bids,
    }))
}

fn decode_public_trade(
    root: &[(String, JsonValue)],
    subscription: Subscription,
    limits: DecodeLimits,
) -> Result<BitgetMessage, DecodeError> {
    let action = parse_action(root, Topic::PublicTrade, false)?;
    let envelope_timestamp = parse_timestamp_number(field(root, "ts")?, "ts")?;
    let data = expect_array(field(root, "data")?, "data")?;
    if data.len() > limits.max_trades_per_message {
        return Err(DecodeError::TooManyTrades {
            count: data.len(),
            max: limits.max_trades_per_message,
        });
    }

    let mut trades = Vec::with_capacity(data.len());
    for item in data {
        let trade = expect_object(item, "trade")?;
        let price = parse_lexical(field(trade, "p")?, "trade.p")?;
        let size = parse_lexical(field(trade, "v")?, "trade.v")?;
        let execution_id = parse_nonempty_text(field(trade, "i")?, "trade.i")?;
        let correlation_id = parse_nonempty_text(field(trade, "L")?, "trade.L")?;
        let fill_side = match expect_string(field(trade, "S")?, "trade.S")? {
            "buy" => FillSide::Buy,
            "sell" => FillSide::Sell,
            value => {
                return Err(DecodeError::InvalidFillSide {
                    value: value.to_owned(),
                });
            }
        };
        let timestamp = parse_timestamp_string(field(trade, "T")?, "trade.T")?;
        let is_rpi = match expect_string(field(trade, "isRPI")?, "trade.isRPI")? {
            "yes" => RpiFlag::Yes,
            "no" => RpiFlag::No,
            value => {
                return Err(DecodeError::InvalidRpiFlag {
                    value: value.to_owned(),
                });
            }
        };

        trades.push(WireTrade {
            price,
            size,
            execution_id,
            correlation_id,
            fill_side,
            timestamp,
            is_rpi,
        });
    }

    Ok(BitgetMessage::PublicTrade(PublicTradeFrame {
        category: subscription.category,
        symbol: subscription.symbol,
        topic: Topic::PublicTrade,
        action,
        envelope_timestamp,
        trades,
    }))
}

fn parse_subscription(root: &[(String, JsonValue)]) -> Result<Subscription, DecodeError> {
    let arg = expect_object(field(root, "arg")?, "arg")?;
    let category_text = expect_string(field(arg, "instType")?, "arg.instType")?;
    let category = match category_text {
        "spot" => Category::Spot,
        "usdt-futures" => Category::UsdtFutures,
        "coin-futures" => Category::CoinFutures,
        "usdc-futures" => Category::UsdcFutures,
        value => {
            return Err(DecodeError::UnsupportedCategory {
                category: value.to_owned(),
            });
        }
    };
    let symbol = parse_nonempty_text(field(arg, "symbol")?, "arg.symbol")?;
    let raw_topic = parse_nonempty_text(field(arg, "topic")?, "arg.topic")?;

    Ok(Subscription {
        category,
        symbol,
        raw_topic,
    })
}

fn parse_action(
    root: &[(String, JsonValue)],
    topic: Topic,
    snapshot_allowed: bool,
) -> Result<Action, DecodeError> {
    let raw = expect_string(field(root, "action")?, "action")?;
    match raw {
        "snapshot" if snapshot_allowed => Ok(Action::Snapshot),
        "update" => Ok(Action::Update),
        _ => Err(DecodeError::InvalidAction {
            topic,
            action: raw.to_owned(),
        }),
    }
}

fn parse_levels(value: &JsonValue, side: &'static str) -> Result<Vec<WireLevel>, DecodeError> {
    let levels = expect_array(value, "book levels")?;
    if levels.len() > BOOKS50_MAX_LEVELS {
        return Err(DecodeError::TooManyLevels {
            side,
            count: levels.len(),
            max: BOOKS50_MAX_LEVELS,
        });
    }

    let mut decoded = Vec::with_capacity(levels.len());
    for (index, value) in levels.iter().enumerate() {
        let pair = expect_array(value, "book level")?;
        if pair.len() != 2 {
            return Err(DecodeError::InvalidLevelShape {
                side,
                index,
                count: pair.len(),
            });
        }
        decoded.push(WireLevel {
            price: parse_lexical(&pair[0], "book level price")?,
            quantity: parse_lexical(&pair[1], "book level quantity")?,
        });
    }
    Ok(decoded)
}

fn parse_lexical(value: &JsonValue, field_name: &'static str) -> Result<LexicalValue, DecodeError> {
    let text = expect_string(value, field_name)?;
    if text.is_empty() {
        return Err(DecodeError::InvalidText { field: field_name });
    }
    Ok(LexicalValue(text.to_owned()))
}

fn parse_nonempty_text(
    value: &JsonValue,
    field_name: &'static str,
) -> Result<String, DecodeError> {
    let text = expect_string(value, field_name)?;
    if text.is_empty() {
        return Err(DecodeError::InvalidText { field: field_name });
    }
    Ok(text.to_owned())
}

fn parse_sequence(value: &JsonValue, field_name: &'static str) -> Result<u64, DecodeError> {
    let text = expect_number(value, field_name)?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DecodeError::InvalidSequence { field: field_name });
    }
    text.parse::<u64>()
        .map_err(|_| DecodeError::SequenceOverflow { field: field_name })
}

fn parse_timestamp_number(
    value: &JsonValue,
    field_name: &'static str,
) -> Result<WireTimestampMs, DecodeError> {
    let text = expect_number(value, field_name)?;
    parse_timestamp_text(text, field_name)
}

fn parse_timestamp_string(
    value: &JsonValue,
    field_name: &'static str,
) -> Result<WireTimestampMs, DecodeError> {
    let text = expect_string(value, field_name)?;
    parse_timestamp_text(text, field_name)
}

fn parse_timestamp_text(
    text: &str,
    field_name: &'static str,
) -> Result<WireTimestampMs, DecodeError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DecodeError::InvalidTimestamp { field: field_name });
    }
    let value = text
        .parse::<i64>()
        .map_err(|_| DecodeError::TimestampOverflow { field: field_name })?;
    Ok(WireTimestampMs {
        value,
        lexical: text.to_owned(),
    })
}

fn field<'a>(
    object: &'a [(String, JsonValue)],
    name: &'static str,
) -> Result<&'a JsonValue, DecodeError> {
    object
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value))
        .ok_or(DecodeError::MissingField { field: name })
}

fn expect_object<'a>(
    value: &'a JsonValue,
    field_name: &'static str,
) -> Result<&'a [(String, JsonValue)], DecodeError> {
    match value {
        JsonValue::Object(entries) => Ok(entries),
        _ => Err(DecodeError::InvalidFieldType {
            field: field_name,
            expected: "object",
        }),
    }
}

fn expect_array<'a>(
    value: &'a JsonValue,
    field_name: &'static str,
) -> Result<&'a [JsonValue], DecodeError> {
    match value {
        JsonValue::Array(values) => Ok(values),
        _ => Err(DecodeError::InvalidFieldType {
            field: field_name,
            expected: "array",
        }),
    }
}

fn expect_string<'a>(
    value: &'a JsonValue,
    field_name: &'static str,
) -> Result<&'a str, DecodeError> {
    match value {
        JsonValue::String(text) => Ok(text),
        _ => Err(DecodeError::InvalidFieldType {
            field: field_name,
            expected: "string",
        }),
    }
}

fn expect_number<'a>(
    value: &'a JsonValue,
    field_name: &'static str,
) -> Result<&'a str, DecodeError> {
    match value {
        JsonValue::Number(text) => Ok(text),
        _ => Err(DecodeError::InvalidFieldType {
            field: field_name,
            expected: "number",
        }),
    }
}
