pub mod default;

use std::{error::Error, marker::PhantomData};

pub use default::{DefaultRowParser, FieldValue};
use fast_float::FastFloat;

use crate::{DefaultSchema, SEMICOLON};

pub type RowSpan = [u8];
pub type FieldSpan = [u8];

pub enum ParseError {}

pub struct ParseContext {
    delimiter: u8,
}

impl Default for ParseContext {
    fn default() -> Self {
        Self {
            delimiter: SEMICOLON,
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parse error: TODO")
    }
}

/// The [FieldParser] parses a single value (column) in a CSV row.
pub trait FieldParser<T> {
    /// Parses the value in the CSV row, returning the parsed value if any.
    /// If the column is empty, returns None. If parsing fails, an error is returned.
    fn parse(span: &RowSpan) -> Result<T, Box<dyn Error>>;
}

pub trait RowParser<S> {
    fn parse(row: &RowSpan, context: &ParseContext) -> S;
}

pub struct StringParser {}

impl FieldParser<String> for StringParser {
    fn parse(span: &RowSpan) -> Result<String, Box<dyn Error>> {
        match String::from_utf8(span.into()) {
            Ok(s) => Ok(s.trim().to_string()),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct FloatParser<T: FastFloat> {
    marker: PhantomData<T>,
}

impl<T: FastFloat> FieldParser<T> for FloatParser<T> {
    fn parse(span: &RowSpan) -> Result<T, Box<dyn Error>> {
        let ss = String::from_utf8_lossy(span);
        let s = ss.trim();
        match fast_float::parse(&s) {
            Ok(v) => Ok(v),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct BoolParser {}

impl FieldParser<bool> for BoolParser {
    fn parse(span: &RowSpan) -> Result<bool, Box<dyn Error>> {
        let ss = String::from_utf8_lossy(span);
        let s = ss.trim();

        match s.parse() {
            Ok(b) => Ok(b),
            Err(e) => Err(e.into())
        }
    }
}

pub trait IntoRowParser<S> {
    type Parser: RowParser<S>;
}

impl IntoRowParser<DefaultSchema> for DefaultSchema {
    type Parser = DefaultRowParser;
}

pub trait IntoFieldParser<T> {
    type Parser: FieldParser<T>;
}

impl IntoFieldParser<bool> for bool {
    type Parser = BoolParser;
}

impl IntoFieldParser<f32> for f32 {
    type Parser = FloatParser<f32>;
}

impl IntoFieldParser<f64> for f64 {
    type Parser = FloatParser<f64>;
}

impl IntoFieldParser<String> for String {
    type Parser = StringParser;
}

pub fn try_parse<T: IntoFieldParser<T>>(span: &FieldSpan) -> Option<T> {
    let s = <T as IntoFieldParser<T>>::Parser::parse(span);
    match s {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

pub struct RowSpanIterator<'a> {
    context: &'a ParseContext,
    row: &'a RowSpan,
    offset: usize,
}

impl<'a> RowSpanIterator<'a> {
    pub fn new(context: &'a ParseContext, row: &'a RowSpan) -> Self {
        Self {
            context,
            row,
            offset: 0,
        }
    }
}

impl<'a> Iterator for RowSpanIterator<'a> {
    type Item = &'a FieldSpan;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(index) = memchr::memchr(self.context.delimiter, &self.row[self.offset..]) {
            let res = &self.row[self.offset..self.offset + index];
            self.offset += index + 1;
            return Some(res);
        }

        let remaining = &self.row[self.offset..];
        if !remaining.is_empty() {
            return Some(remaining);
        }

        None
    }
}

#[cfg(test)]
mod test {
    mod bool_parser {
        use crate::parser::{FieldParser, BoolParser};

        #[test]
        fn parse_true_value_returns_ok() {
            let result = BoolParser::parse(b" true  ");
            assert!(result.is_ok());
            assert_eq!(true, result.unwrap());
        }

        #[test]
        fn parse_false_value_returns_ok() {
            let result = BoolParser::parse(b"  false ");
            assert!(result.is_ok());
            assert_eq!(false, result.unwrap());
        }

        #[test]
        fn parse_invalid_value_returns_err() {
            let result = BoolParser::parse(b"nope");
            assert!(result.is_err());
        }
    }

    mod float_parser {
        use crate::parser::{FieldParser, FloatParser};

        #[test]
        fn parse_valid_value_returns_ok() {
            let result = FloatParser::<f32>::parse(b"0.32");
            assert!(result.is_ok());
            assert_eq!(0.32, result.unwrap());
        }

        #[test]
        fn parse_invalid_value_returns_err() {
            let result = FloatParser::<f32>::parse(b"nope");
            assert!(result.is_err());
        }
    }

    mod default_row_parser {
        use crate::parser::{DefaultRowParser, FieldValue, ParseContext, RowParser};

        #[test]
        fn parse_returns_correct_values() {
            let row = b" Hello;  world! ; 30.2 ";

            let context: ParseContext = ParseContext::default();

            let result = DefaultRowParser::parse(row, &context).fields;

            assert_eq!(3, result.len());

            assert_eq!(Some(FieldValue::String("Hello".to_string())), result[0]);
            assert_eq!(Some(FieldValue::String("world!".to_string())), result[1]);
            assert_eq!(Some(FieldValue::Float(30.2f64)), result[2]);
        }

        #[test]
        fn parse_handle_empty_columns() {
            let row = b"Hello;world!;30.2";

            let context: ParseContext = ParseContext::default();

            let result = DefaultRowParser::parse(row, &context).fields;

            assert_eq!(3, result.len());

            assert_eq!(Some(FieldValue::String("Hello".to_string())), result[0]);
            assert_eq!(Some(FieldValue::String("world!".to_string())), result[1]);
            assert_eq!(Some(FieldValue::Float(30.2f64)), result[2]);
        }
    }

    mod string_parser {
        use crate::parser::{FieldParser, StringParser};

        #[test]
        fn parse_when_valid_string_returns_ok() {
            let result = StringParser::parse(b"Hello, world!");

            assert!(result.is_ok());

            let value = result.unwrap();

            assert_eq!(value, "Hello, world!");
        }

        #[test]
        fn parse_when_invalid_utf8_string_returns_err() {
            // https://stackoverflow.com/a/21070216/2704779
            let result = StringParser::parse(b"AB\xfc");

            assert!(result.is_err());
        }
    }
}
