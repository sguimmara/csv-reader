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
            Ok(s) => Ok(s),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct FloatParser<T: FastFloat> {
    marker: PhantomData<T>,
}

impl<T: FastFloat> FieldParser<T> for FloatParser<T> {
    fn parse(span: &RowSpan) -> Result<T, Box<dyn Error>> {
        match fast_float::parse(span) {
            Ok(v) => Ok(v),
            Err(e) => Err(e.into()),
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
