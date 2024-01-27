use std::{error::Error, fs::File, marker::PhantomData, path::Path};

use memmap::MmapOptions;
use parser::{FieldValue, IntoRowParser, ParseContext};

pub mod parser;

use parser::RowParser;

pub const SEMICOLON: u8 = 59u8;
pub const NEWLINE: u8 = 10u8;

type DefaultSchema = Vec<Option<FieldValue>>;

pub struct CsvReader<Schema = DefaultSchema> {
    schema: PhantomData<Schema>,
}

impl<Schema: IntoRowParser<Schema>> Default for CsvReader<Schema> {
    fn default() -> Self {
        Self {
            schema: PhantomData,
        }
    }
}

impl<Schema: IntoRowParser<Schema>> CsvReader<Schema> {
    pub fn read(span: &[u8]) -> Result<Vec<Schema>, Box<dyn Error>> {
        let mut result: Vec<Schema> = Vec::new();

        let context = ParseContext::default();

        let mut offset = 0;

        while let Some(index) = memchr::memchr(NEWLINE, &span[offset..]) {
            let row = <Schema as IntoRowParser<Schema>>::Parser::parse(
                &span[offset..offset + index],
                &context,
            );
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

            impl $crate::parser::RowParser<[<$name>]> for [<$name Parser>] {

                fn parse(row_span: &$crate::parser::RowSpan, context: &$crate::parser::ParseContext) -> $name {
                    let mut iterator = $crate::parser::RowSpanIterator::new(context, row_span);
                    [<$name>] {
                        $(
                            [<$field>]: $crate::parser::try_parse(iterator.next().unwrap()),
                        )+
                    }
                }
            }

            impl $crate::parser::IntoRowParser<[<$name>]> for $name {
                type Parser = [<$name Parser>];
            }
        }
    };
}

#[cfg(test)]
mod test {
    mod float_parser {
        use crate::parser::{FieldParser, FloatParser};

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
        use crate::parser::{DefaultRowParser, FieldValue, ParseContext, RowParser};

        #[test]
        fn parse_returns_correct_values() {
            let row = b"Hello;world!;30.2";

            let context: ParseContext = ParseContext::default();

            let result = DefaultRowParser::parse(row, &context);

            assert_eq!(3, result.len());

            assert_eq!(Some(FieldValue::String("Hello".to_string())), result[0]);
            assert_eq!(Some(FieldValue::String("world!".to_string())), result[1]);
            assert_eq!(Some(FieldValue::Float(30.2f64)), result[2]);
        }

        #[test]
        fn parse_handle_empty_columns() {
            let row = b"Hello;world!;30.2";

            let context: ParseContext = ParseContext::default();

            let result = DefaultRowParser::parse(row, &context);

            assert_eq!(3, result.len());

            assert_eq!(Some(FieldValue::String("Hello".to_string())), result[0]);
            assert_eq!(Some(FieldValue::String("world!".to_string())), result[1]);
            assert_eq!(Some(FieldValue::Float(30.2f64)), result[2]);
        }
    }

    mod schema {
        use crate::{
            parser::{ParseContext, RowParser},
            CsvReader,
        };

        schema!(pub MySchema, name:String, height:f64);

        #[test]
        fn parse_file() {
            let csv = b"foo1;0.32;\nfoo2;1\n";

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
            let p = MySchemaParser::parse(b"foo;0.2", &context);

            assert_eq!(Some("foo".to_string()), p.name);
            assert_eq!(Some(0.2f64), p.height);
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
