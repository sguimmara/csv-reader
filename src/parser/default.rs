use crate::DefaultSchema;

use super::{FieldParser, FloatParser, ParseContext, RowParser, RowSpan, StringParser};

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Float(f64),
    String(String),
}

pub struct DefaultRowParser {}

impl DefaultRowParser {
    fn try_parse_field(span: &RowSpan) -> Option<FieldValue> {
        if span.is_empty() {
            None
        } else if let Ok(float) = FloatParser::<f64>::parse(span) {
            Some(FieldValue::Float(float))
        } else if let Ok(v) = StringParser::parse(span) {
            Some(FieldValue::String(v))
        } else {
            None
        }
    }
}

impl RowParser<DefaultSchema> for DefaultRowParser {
    fn parse(row: &RowSpan, context: &ParseContext) -> DefaultSchema {
        let mut result: Vec<Option<FieldValue>> = Vec::new();

        let mut start = 0;

        while let Some(index) = memchr::memchr(context.delimiter, &row[start..]) {
            let span = &row[start..(start + index)];

            result.push(Self::try_parse_field(span));

            start += index + 1;
        }

        if start < row.len() - 1 {
            result.push(Self::try_parse_field(&row[start..]));
        }

        result
    }
}
