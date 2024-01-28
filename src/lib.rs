use std::{error::Error, fs::File, marker::PhantomData, path::Path};

use memmap::MmapOptions;
use parser::{FieldValue, IntoRowParser, ParseContext, RowSpan};

pub mod parser;

use parser::RowParser;

pub const NEWLINE: u8 = 0x0A;
pub const COMMA: u8 = 0x2C;

pub struct DefaultSchema {
    fields: Vec<Option<FieldValue>>,
}

impl DefaultSchema {
    pub fn new(fields: Vec<Option<FieldValue>>) -> Self {
        Self { fields }
    }
}

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

struct RowIterator<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> RowIterator<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = &'a RowSpan;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }
        if let Some(index) = memchr::memchr(NEWLINE, &self.data[self.offset..]) {
            let result = Some(&self.data[self.offset..self.offset + index]);
            self.offset += index + 1;
            return result;
        }

        None
    }
}

impl<Schema: IntoRowParser<Schema>> CsvReader<Schema> {
    pub fn read(&self, span: &[u8]) -> Result<Vec<Schema>, Box<dyn Error>> {
        let mut result: Vec<Schema> = Vec::new();

        let context = ParseContext::default();

        let iterator = RowIterator::new(span);

        // Skip header
        for line in iterator.skip(1) {
            let row = <Schema as IntoRowParser<Schema>>::Parser::parse(line, &context);
            result.push(row);
        }

        Ok(result)
    }

    pub fn read_file(&self, path: &Path) -> Result<Vec<Schema>, Box<dyn Error>> {
        let file = File::open(path)?;

        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

        self.read(&mmap)
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
    mod row_iterator {
        use crate::RowIterator;

        #[test]
        fn feature() {
            let data = b"header1,header-2\nvalue-1,value2\n";
            let iterator = RowIterator::new(data);

            let lines: Vec<_> = iterator.collect();

            assert_eq!(lines[0], b"header1,header-2");
            assert_eq!(lines[1], b"value-1,value2");
        }
    }

    mod csv_parser {
        use std::path::Path;

        use crate::{parser::FieldValue, CsvReader, DefaultSchema};

        #[test]
        fn read_file_1_row() {
            let result =
                CsvReader::<DefaultSchema>::default().read_file(Path::new("data/1-row.csv"));

            assert!(result.is_ok());

            let rows = result.unwrap();

            assert_eq!(rows.len(), 1);

            assert_eq!(rows[0].fields[0], Some(FieldValue::String("hello".into())));
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
            let csv = b"header1,header2\nfoo1,0.32\nfoo2,1\n";

            let rows = CsvReader::<MySchema>::default().read(csv).unwrap();
            assert_eq!(rows.len(), 2);

            assert_eq!(rows[0].name, Some("foo1".to_string()));
            assert_eq!(rows[0].height, Some(0.32f64));

            assert_eq!(rows[1].name, Some("foo2".to_string()));
            assert_eq!(rows[1].height, Some(1f64));
        }

        #[test]
        fn schema() {
            let context = ParseContext::default();
            let p = MySchemaParser::parse(b"foo,0.2", &context);

            assert_eq!(Some("foo".to_string()), p.name);
            assert_eq!(Some(0.2f64), p.height);
        }
    }
}
