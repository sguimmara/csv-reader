use std::path::Path;

use clap::Parser;
use rust_csv_reader::{CsvReader, DefaultSchema};

/// CLI arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The file to read.
    input: String,

    /// Don't print anything.
    #[arg(short, long)]
    quiet: bool,
}

fn print_stats(path: &Path, rows: &Vec<DefaultSchema>) {
    println!("{}:", path.to_str().unwrap());
    if rows.is_empty() {
        println!("no rows");
    } else {
        println!("{} row(s), {} column(s)", rows.len(), rows[0].fields().len());
    }
}

fn main() {
    let args = Args::parse();

    let path = Path::new(&args.input);
    let reader = CsvReader::<DefaultSchema>::with_default_schema();

    let result = reader.read_file(path);

    match result {
        Ok(v) => print_stats(path, &v),
        Err(e) => eprintln!("{}", e)
    }
}