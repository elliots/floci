//! A small JSON parser for what `smolvm-pods ctl` and `ctr events` print.
//! Numbers keep their text, so that nanosecond timestamps keep every digit.

use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

const NULL: Json = Json::Null;

impl Json {
    pub fn parse(text: &str) -> Result<Json, String> {
        let mut parser = Parser {
            text: text.as_bytes(),
            at: 0,
        };
        let value = parser.value()?;
        parser.space();
        if parser.at != parser.text.len() {
            return Err(parser.error("text after the JSON value"));
        }
        Ok(value)
    }

    /// The member `key` of an object, or null.
    pub fn get(&self, key: &str) -> &Json {
        match self {
            Json::Object(members) => members
                .iter()
                .find(|(k, _)| k == key)
                .map_or(&NULL, |(_, v)| v),
            _ => &NULL,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Json::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Number(n) => n.parse().ok(),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }
}

impl fmt::Display for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Json::Null => f.write_str("null"),
            Json::Bool(b) => write!(f, "{b}"),
            Json::Number(n) => f.write_str(n),
            Json::String(s) => write!(f, "{s:?}"),
            Json::Array(items) => {
                f.write_str("[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{item}")?;
                }
                f.write_str("]")
            }
            Json::Object(members) => {
                f.write_str("{")?;
                for (i, (key, value)) in members.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{key:?}:{value}")?;
                }
                f.write_str("}")
            }
        }
    }
}

struct Parser<'a> {
    text: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn error(&self, what: &str) -> String {
        format!("invalid JSON at byte {}: {what}", self.at)
    }

    fn space(&mut self) {
        while self
            .text
            .get(self.at)
            .is_some_and(|b| b" \t\r\n".contains(b))
        {
            self.at += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> bool {
        self.space();
        if self.text.get(self.at) == Some(&byte) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn literal(&mut self, word: &str, value: Json) -> Result<Json, String> {
        if self.text[self.at..].starts_with(word.as_bytes()) {
            self.at += word.len();
            Ok(value)
        } else {
            Err(self.error("unknown literal"))
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        self.space();
        match self.text.get(self.at) {
            None => Err(self.error("no value")),
            Some(b'n') => self.literal("null", Json::Null),
            Some(b't') => self.literal("true", Json::Bool(true)),
            Some(b'f') => self.literal("false", Json::Bool(false)),
            Some(b'"') => self.string().map(Json::String),
            Some(b'[') => {
                self.at += 1;
                let mut items = Vec::new();
                if self.eat(b']') {
                    return Ok(Json::Array(items));
                }
                loop {
                    items.push(self.value()?);
                    if self.eat(b']') {
                        return Ok(Json::Array(items));
                    }
                    if !self.eat(b',') {
                        return Err(self.error("expected , or ]"));
                    }
                }
            }
            Some(b'{') => {
                self.at += 1;
                let mut members = Vec::new();
                if self.eat(b'}') {
                    return Ok(Json::Object(members));
                }
                loop {
                    self.space();
                    let key = self.string()?;
                    if !self.eat(b':') {
                        return Err(self.error("expected :"));
                    }
                    members.push((key, self.value()?));
                    if self.eat(b'}') {
                        return Ok(Json::Object(members));
                    }
                    if !self.eat(b',') {
                        return Err(self.error("expected , or }"));
                    }
                }
            }
            Some(b'-' | b'0'..=b'9') => {
                let start = self.at;
                while self
                    .text
                    .get(self.at)
                    .is_some_and(|b| b"+-.eE0123456789".contains(b))
                {
                    self.at += 1;
                }
                let number = std::str::from_utf8(&self.text[start..self.at]).unwrap();
                number
                    .parse::<f64>()
                    .map_err(|_| self.error("invalid number"))?;
                Ok(Json::Number(number.to_string()))
            }
            Some(_) => Err(self.error("unexpected character")),
        }
    }

    fn string(&mut self) -> Result<String, String> {
        if self.text.get(self.at) != Some(&b'"') {
            return Err(self.error("expected a string"));
        }
        self.at += 1;
        let mut bytes = Vec::new();
        loop {
            let Some(&byte) = self.text.get(self.at) else {
                return Err(self.error("unterminated string"));
            };
            self.at += 1;
            match byte {
                b'"' => break,
                b'\\' => {
                    let Some(&escape) = self.text.get(self.at) else {
                        return Err(self.error("unterminated escape"));
                    };
                    self.at += 1;
                    let ch = match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => self.unicode()?,
                        _ => return Err(self.error("unknown escape")),
                    };
                    bytes.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
                }
                _ => bytes.push(byte),
            }
        }
        String::from_utf8(bytes).map_err(|_| self.error("string is not UTF-8"))
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let digits = self
            .text
            .get(self.at..self.at + 4)
            .and_then(|d| std::str::from_utf8(d).ok())
            .and_then(|d| u32::from_str_radix(d, 16).ok())
            .ok_or_else(|| self.error("invalid \\u escape"))?;
        self.at += 4;
        Ok(digits)
    }

    fn unicode(&mut self) -> Result<char, String> {
        let high = self.hex4()?;
        let code = if (0xd800..0xdc00).contains(&high) {
            if !self.text[self.at..].starts_with(b"\\u") {
                return Err(self.error("unpaired surrogate"));
            }
            self.at += 2;
            let low = self.hex4()?;
            if !(0xdc00..0xe000).contains(&low) {
                return Err(self.error("invalid low surrogate"));
            }
            0x10000 + ((high - 0xd800) << 10) + (low - 0xdc00)
        } else {
            high
        };
        char::from_u32(code).ok_or_else(|| self.error("invalid code point"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_values() {
        let json = Json::parse(
            r#" {"a": [1, -2.5e3, true, null], "b": {"c": "d\"e\u00e9\ud83d\ude00"}, "e": {}} "#,
        )
        .unwrap();
        let a = json.get("a").as_array().unwrap();
        assert_eq!(a[0].as_i64(), Some(1));
        assert_eq!(a[1].as_f64(), Some(-2500.0));
        assert_eq!(a[2].as_bool(), Some(true));
        assert!(a[3].is_null());
        assert_eq!(json.get("b").get("c").as_str(), Some("d\"e\u{e9}\u{1f600}"));
        assert!(matches!(json.get("e"), Json::Object(members) if members.is_empty()));
        assert!(json.get("missing").get("deeper").is_null());
    }

    #[test]
    fn keeps_every_digit_of_large_numbers() {
        let json = Json::parse(r#"{"t": 1792382839104101918}"#).unwrap();
        assert_eq!(json.get("t").as_i64(), Some(1_792_382_839_104_101_918));
    }

    #[test]
    fn refuses_invalid_text() {
        for text in ["", "{", "[1,]", r#"{"a" 1}"#, "nul", "1 2", r#""\x""#, "-"] {
            assert!(Json::parse(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn prints_what_it_parsed() {
        let text = r#"{"a":[1,"x\n",null],"b":false}"#;
        assert_eq!(Json::parse(text).unwrap().to_string(), text);
    }
}
