use std::{error::Error, fs::File, marker::PhantomData, path::Path, process::Output};

use fast_float::FastFloat;
use memmap::MmapOptions;

const SEMICOLON: u8 = 59u8;
const NEWLINE: u8 = 10u8;

type RowSpan = [u8];
type FieldSpan = [u8];

type DefaultSchema = Vec<Option<FieldValue>>;

struct CsvReader<Schema = DefaultSchema> {
    schema: PhantomData<Schema>,
}

impl<Schema: IntoRowParser<Schema>> Default for CsvReader<Schema> {
    fn default() -> Self {
        Self { schema: PhantomData }
    }
}

impl<Schema: IntoRowParser<Schema>> CsvReader<Schema> {
    pub fn read(span: &[u8]) -> Result<Vec<Schema>, Box<dyn Error>> {
        let mut result  : Vec<Schema> = Vec::new();

        let context = ParseContext::default();

        let mut offset = 0;

        while let Some(index) = memchr::memchr(NEWLINE, &span[offset..]) {
            let row = <Schema as IntoRowParser<Schema>>::Parser::parse(&span[offset..offset + index], &context);
            result.push(row);
            offset += index + 1;
        }

        Ok(result)
    }

    pub fn read_file(path: &Path) -> Result<Vec<Schema>, Box<dyn Error>> {
        let file = File::open(path)?;

        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

        Self::read(&mmap)
    }
}

enum ParseError {}

struct ParseContext {
    delimiter: u8,
}

impl Default for ParseContext {
    fn default() -> Self {
        Self { delimiter: SEMICOLON }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parse error: TODO")
    }
}

/// The [FieldParser] parses a single value (column) in a CSV row.
trait FieldParser<T> {
    /// Parses the value in the CSV row, returning the parsed value if any.
    /// If the column is empty, returns None. If parsing fails, an error is returned.
    fn parse(span: &RowSpan) -> Result<T, Box<dyn Error>>;
}

trait RowParser<S> {
    fn parse(row: &RowSpan, context: &ParseContext) -> S;
}

#[derive(Debug, Clone, PartialEq)]
enum FieldValue {
    Float(f64),
    String(String),
}

struct DefaultRowParser {}

impl RowParser<DefaultSchema> for DefaultRowParser {

    fn parse(row: &RowSpan, context: &ParseContext) -> DefaultSchema {
        let mut result: Vec<Option<FieldValue>> = Vec::new();

        let mut start = 0;

        while let Some(index) = memchr::memchr(context.delimiter, &row[start..]) {
            let span = &row[start..(start + index)];

            if span.is_empty() {
                result.push(None)
            } else {
                if let Ok(float) = FloatParser::<f64>::parse(span) {
                    result.push(Some(FieldValue::Float(float)));
                } else
                if let Ok(v) = StringParser::parse(span) {
                    result.push(Some(FieldValue::String(v)));
                }
            }

            start += index + 1;
        }

        result
    }
}

#[derive(Debug)]
struct StringParser {}

impl FieldParser<String> for StringParser {
    fn parse(span: &RowSpan) -> Result<String, Box<dyn Error>> {
        match String::from_utf8(span.into()) {
            Ok(s) => Ok(s),
            Err(e) => Err(e.into()),
        }
    }
}

struct FloatParser<T: FastFloat> {
    marker: PhantomData<T>
}

impl<T: FastFloat> FieldParser<T> for FloatParser<T> {
    fn parse(span: &RowSpan) -> Result<T, Box<dyn Error>> {
        match fast_float::parse(span) {
            Ok(v) => Ok(v),
            Err(e) => Err(e.into())
        }
    }
}

trait IntoRowParser<S> {
    type Parser: RowParser<S>;
}

impl IntoRowParser<DefaultSchema> for DefaultSchema {
    type Parser = DefaultRowParser;
}

trait IntoFieldParser<T> {
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

fn try_parse<T: IntoFieldParser<T>>(span: &FieldSpan) -> Option<T> {
    let s = <T as IntoFieldParser::<T>>::Parser::parse(span);
    match s {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

struct RowSpanIterator<'a> {
    context: &'a ParseContext,
    row: &'a RowSpan,
    offset: usize,
}

impl<'a> RowSpanIterator<'a> {
    pub fn new(context: &'a ParseContext, row: &'a RowSpan) -> Self {
        Self { context, row, offset: 0 }
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

        None
    }
}

#[macro_export]
macro_rules! schema {
    ($vis:vis $name:ident, $($field:ident:$type:ty),+) => {
        paste::paste! {
            // We generate the Row schema struct, with a field for each CSV column.
            #[derive(Debug, PartialEq, Default, Clone)]
            $vis struct $name {
                $(
                    [<$field>]: Option<[<$type>]>,
                )+
            }

            pub struct [<$name Parser>] {}

            impl crate::reader::RowParser<[<$name>]> for [<$name Parser>] {

                fn parse(row_span: &crate::reader::RowSpan, context: &crate::reader::ParseContext) -> $name {
                    let mut iterator = crate::reader::RowSpanIterator::new(context, row_span);
                    [<$name>] {
                        $(
                            [<$field>]: crate::reader::try_parse(iterator.next().unwrap()),
                        )+
                    }
                }
            }

            impl crate::reader::IntoRowParser<[<$name>]> for $name {
                type Parser = [<$name Parser>];
            }
        }
    };
}


#[cfg(test)]
mod test {
    mod float_parser {
        use crate::reader::{FloatParser, FieldParser};

        #[test]
        fn parse_valid_value_returns_ok() {
            let result = FloatParser::<f32>::parse(b"0.32");
            assert!(result.is_ok());
            assert_eq!(0.32, result.unwrap());
        }

        #[test]
        fn parse_valid_value_returns_err() {
            let result = FloatParser::<f32>::parse(b"nope");
            assert!(result.is_err());
        }
    }

    mod default_row_parser {
        use crate::reader::{DefaultRowParser, ParseContext, RowParser, FieldValue, SEMICOLON};

        #[test]
        fn parse_returns_correct_values() {
            let row = b"Hello;world!;30.2;";

            let context: ParseContext = ParseContext {
                delimiter: SEMICOLON,
            };

            let result = DefaultRowParser::parse(row, &context);

            assert_eq!(3, result.len());

            assert_eq!(Some(FieldValue::String("Hello".to_string())), result[0]);
            assert_eq!(Some(FieldValue::String("world!".to_string())), result[1]);
            assert_eq!(Some(FieldValue::Float(30.2f64)), result[2]);
        }

        #[test]
        fn parse_handle_empty_columns() {
            let row = b"Hello;world!;30.2;";

            let context: ParseContext = ParseContext {
                delimiter: SEMICOLON,
            };

            let result = DefaultRowParser::parse(row, &context);

            assert_eq!(3, result.len());

            assert_eq!(Some(FieldValue::String("Hello".to_string())), result[0]);
            assert_eq!(Some(FieldValue::String("world!".to_string())), result[1]);
            assert_eq!(Some(FieldValue::Float(30.2f64)), result[2]);
        }
    }

    mod schema {
        use crate::reader::{CsvReader, ParseContext, RowParser};

        schema!(pub MySchema, name:String, height:f64);

        #[test]
        fn parse_file() {
            let csv = b"foo1;0.32;\nfoo2;1;\n";

            let rows = CsvReader::<MySchema>::read(csv).unwrap();
            assert_eq!(rows.len(), 2);

            assert_eq!(rows[0].name, Some("foo1".to_string()));
            assert_eq!(rows[0].height, Some(0.32f64));

            assert_eq!(rows[1].name, Some("foo2".to_string()));
            assert_eq!(rows[1].height, Some(1f64));
        }

        #[test]
        fn schema() {
            let context = ParseContext::default();
            let p = MySchemaParser::parse(b"foo;0.2;", &context);

            assert_eq!(Some("foo".to_string()), p.name);
            assert_eq!(Some(0.2f64), p.height);
        }
    }

    mod string_parser {
        use crate::reader::{StringParser, FieldParser};

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