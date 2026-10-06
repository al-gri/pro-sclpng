use std::error::Error;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonErrorKind {
    UnexpectedEof,
    UnexpectedToken,
    TrailingData,
    InvalidNumber,
    InvalidStringEscape,
    InvalidUnicodeEscape,
    InvalidUtf8,
    ControlCharacterInString,
    DuplicateObjectKey,
    NestingTooDeep,
    ContainerTooLarge,
    StringTooLong,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonError {
    pub kind: JsonErrorKind,
    pub offset: usize,
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} at byte {}", self.kind, self.offset)
    }
}

impl Error for JsonError {}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ParserLimits {
    pub max_nesting_depth: usize,
    pub max_container_items: usize,
    pub max_string_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JsonValue {
    Null,
    Bool,
    Number(String),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

pub(crate) fn parse_json(input: &[u8], limits: ParserLimits) -> Result<JsonValue, JsonError> {
    let mut parser = Parser {
        input,
        pos: 0,
        limits,
    };
    parser.skip_whitespace();
    let value = parser.parse_value(0)?;
    parser.skip_whitespace();
    if parser.pos != input.len() {
        return Err(parser.error(JsonErrorKind::TrailingData));
    }
    Ok(value)
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
    limits: ParserLimits,
}

impl Parser<'_> {
    fn parse_value(&mut self, depth: usize) -> Result<JsonValue, JsonError> {
        self.skip_whitespace();
        match self.peek() {
            None => Err(self.error(JsonErrorKind::UnexpectedEof)),
            Some(b'n') => {
                self.consume_literal(b"null")?;
                Ok(JsonValue::Null)
            }
            Some(b't') => {
                self.consume_literal(b"true")?;
                Ok(JsonValue::Bool)
            }
            Some(b'f') => {
                self.consume_literal(b"false")?;
                Ok(JsonValue::Bool)
            }
            Some(b'"') => self.parse_string().map(JsonValue::String),
            Some(b'[') => {
                self.ensure_can_nest(depth)?;
                let next_depth = depth
                    .checked_add(1)
                    .ok_or_else(|| self.error(JsonErrorKind::NestingTooDeep))?;
                self.parse_array(next_depth)
            }
            Some(b'{') => {
                self.ensure_can_nest(depth)?;
                let next_depth = depth
                    .checked_add(1)
                    .ok_or_else(|| self.error(JsonErrorKind::NestingTooDeep))?;
                self.parse_object(next_depth)
            }
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            Some(_) => Err(self.error(JsonErrorKind::UnexpectedToken)),
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<JsonValue, JsonError> {
        let _ = self.bump();
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.peek() == Some(b']') {
            let _ = self.bump();
            return Ok(JsonValue::Array(values));
        }

        loop {
            if values.len() >= self.limits.max_container_items {
                return Err(self.error(JsonErrorKind::ContainerTooLarge));
            }
            values.push(self.parse_value(depth)?);
            self.skip_whitespace();
            match self.bump() {
                Some(b',') => self.skip_whitespace(),
                Some(b']') => break,
                None => return Err(self.error(JsonErrorKind::UnexpectedEof)),
                Some(_) => return Err(self.error(JsonErrorKind::UnexpectedToken)),
            }
        }
        Ok(JsonValue::Array(values))
    }

    fn parse_object(&mut self, depth: usize) -> Result<JsonValue, JsonError> {
        let _ = self.bump();
        self.skip_whitespace();
        let mut entries = Vec::new();
        if self.peek() == Some(b'}') {
            let _ = self.bump();
            return Ok(JsonValue::Object(entries));
        }

        loop {
            if entries.len() >= self.limits.max_container_items {
                return Err(self.error(JsonErrorKind::ContainerTooLarge));
            }
            if self.peek() != Some(b'"') {
                return Err(self.error(JsonErrorKind::UnexpectedToken));
            }
            let key = self.parse_string()?;
            if entries.iter().any(|(existing, _)| existing == &key) {
                return Err(self.error(JsonErrorKind::DuplicateObjectKey));
            }
            self.skip_whitespace();
            if self.bump() != Some(b':') {
                return Err(self.error(JsonErrorKind::UnexpectedToken));
            }
            self.skip_whitespace();
            let value = self.parse_value(depth)?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.bump() {
                Some(b',') => self.skip_whitespace(),
                Some(b'}') => break,
                None => return Err(self.error(JsonErrorKind::UnexpectedEof)),
                Some(_) => return Err(self.error(JsonErrorKind::UnexpectedToken)),
            }
        }
        Ok(JsonValue::Object(entries))
    }

    fn parse_string(&mut self) -> Result<String, JsonError> {
        let start = self.pos;
        let _ = self.bump();
        let mut out = Vec::new();

        loop {
            let byte = self
                .bump()
                .ok_or_else(|| self.error(JsonErrorKind::UnexpectedEof))?;
            match byte {
                b'"' => {
                    return String::from_utf8(out)
                        .map_err(|_| self.error_at(JsonErrorKind::InvalidUtf8, start));
                }
                b'\' => self.parse_escape(&mut out)?,
                0x00..=0x1f => {
                    return Err(self.error(JsonErrorKind::ControlCharacterInString));
                }
                _ => {
                    out.push(byte);
                    self.ensure_string_len(out.len())?;
                }
            }
        }
    }

    fn parse_escape(&mut self, out: &mut Vec<u8>) -> Result<(), JsonError> {
        let escaped = self
            .bump()
            .ok_or_else(|| self.error(JsonErrorKind::UnexpectedEof))?;
        match escaped {
            b'"' => out.push(b'"'),
            b'\' => out.push(b'\'),
            b'/' => out.push(b'/'),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'u' => {
                let high = self.parse_hex_quad()?;
                let scalar = if (0xd800..=0xdbff).contains(&high) {
                    if self.bump() != Some(b'\') || self.bump() != Some(b'u') {
                        return Err(self.error(JsonErrorKind::InvalidUnicodeEscape));
                    }
                    let low = self.parse_hex_quad()?;
                    if !(0xdc00..=0xdfff).contains(&low) {
                        return Err(self.error(JsonErrorKind::InvalidUnicodeEscape));
                    }
                    0x1_0000
                        + ((u32::from(high) - 0xd800) << 10)
                        + (u32::from(low) - 0xdc00)
                } else if (0xdc00..=0xdfff).contains(&high) {
                    return Err(self.error(JsonErrorKind::InvalidUnicodeEscape));
                } else {
                    u32::from(high)
                };
                let character = char::from_u32(scalar)
                    .ok_or_else(|| self.error(JsonErrorKind::InvalidUnicodeEscape))?;
                let mut buffer = [0_u8; 4];
                out.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            }
            _ => return Err(self.error(JsonErrorKind::InvalidStringEscape)),
        }
        self.ensure_string_len(out.len())
    }

    fn parse_hex_quad(&mut self) -> Result<u16, JsonError> {
        let mut value = 0_u16;
        for _ in 0..4 {
            let byte = self
                .bump()
                .ok_or_else(|| self.error(JsonErrorKind::UnexpectedEof))?;
            let digit = match byte {
                b'0'..=b'9' => u16::from(byte - b'0'),
                b'a'..=b'f' => u16::from(byte - b'a' + 10),
                b'A'..=b'F' => u16::from(byte - b'A' + 10),
                _ => return Err(self.error(JsonErrorKind::InvalidUnicodeEscape)),
            };
            value = value
                .checked_mul(16)
                .and_then(|current| current.checked_add(digit))
                .ok_or_else(|| self.error(JsonErrorKind::InvalidUnicodeEscape))?;
        }
        Ok(value)
    }

    fn parse_number(&mut self) -> Result<JsonValue, JsonError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            let _ = self.bump();
        }

        match self.peek() {
            Some(b'0') => {
                let _ = self.bump();
                if self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                    return Err(self.error(JsonErrorKind::InvalidNumber));
                }
            }
            Some(b'1'..=b'9') => {
                let _ = self.bump();
                while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                    let _ = self.bump();
                }
            }
            _ => return Err(self.error(JsonErrorKind::InvalidNumber)),
        }

        if self.peek() == Some(b'.') {
            let _ = self.bump();
            if !self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                return Err(self.error(JsonErrorKind::InvalidNumber));
            }
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                let _ = self.bump();
            }
        }

        if matches!(self.peek(), Some(b'e' | b'E')) {
            let _ = self.bump();
            if matches!(self.peek(), Some(b'+' | b'-')) {
                let _ = self.bump();
            }
            if !self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                return Err(self.error(JsonErrorKind::InvalidNumber));
            }
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                let _ = self.bump();
            }
        }

        let number = String::from_utf8(self.input[start..self.pos].to_vec())
            .map_err(|_| self.error_at(JsonErrorKind::InvalidUtf8, start))?;
        Ok(JsonValue::Number(number))
    }

    fn consume_literal(&mut self, literal: &[u8]) -> Result<(), JsonError> {
        let end = self
            .pos
            .checked_add(literal.len())
            .ok_or_else(|| self.error(JsonErrorKind::UnexpectedEof))?;
        if self.input.get(self.pos..end) != Some(literal) {
            return Err(self.error(JsonErrorKind::UnexpectedToken));
        }
        self.pos = end;
        Ok(())
    }

    fn ensure_can_nest(&self, depth: usize) -> Result<(), JsonError> {
        if depth >= self.limits.max_nesting_depth {
            return Err(self.error(JsonErrorKind::NestingTooDeep));
        }
        Ok(())
    }

    fn ensure_string_len(&self, len: usize) -> Result<(), JsonError> {
        if len > self.limits.max_string_bytes {
            return Err(self.error(JsonErrorKind::StringTooLong));
        }
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            let _ = self.bump();
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.pos += 1;
        Some(byte)
    }

    fn error(&self, kind: JsonErrorKind) -> JsonError {
        self.error_at(kind, self.pos)
    }

    fn error_at(&self, kind: JsonErrorKind, offset: usize) -> JsonError {
        JsonError { kind, offset }
    }
}
