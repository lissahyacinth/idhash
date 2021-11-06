use clap::{App, Arg};

use idhash::calculate_idhash;
use idhash::config::IdHashConfigBuilder;
use idhash::utils::CSVReader;

fn main() {
    let matches = App::new("IdHash")
        .version("0.0.1")
        .arg(
            Arg::with_name("input_file")
                .short("i")
                .value_name("FILE")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("truncation")
                .short("t")
                .value_name("TRUNCATION")
                .default_value("128")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("digits")
                .short("d")
                .default_value("7")
                .value_name("DIGITS")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("characters")
                .short("c")
                .value_name("CHARACTERS")
                .default_value("128")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("inference_rows")
                .short("r")
                .value_name("INFERENCE_ROWS")
                .default_value("100")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch_size")
                .short("b")
                .value_name("BATCH")
                .default_value("1024")
                .takes_value(true),
        )
        .get_matches();
    let file_path = matches.value_of("input_file").unwrap();
    let truncation = matches.value_of("truncation").unwrap();
    let digits = matches.value_of("digits").unwrap();
    let characters = matches.value_of("characters").unwrap();
    let inference_rows: usize = matches.value_of("inference_rows").unwrap().parse().unwrap();
    let batch_size: usize = matches.value_of("batch_size").unwrap().parse().unwrap();
    let config = IdHashConfigBuilder::new()
        .truncation(truncation.parse().unwrap())
        .digits(digits.parse().unwrap())
        .characters(characters.parse().unwrap())
        .build();
    let csv = CSVReader::new(file_path.to_string(), inference_rows, batch_size);
    let csv_schema = csv.schema.clone();
    let res = calculate_idhash(csv, csv_schema, config);
    println!("File: {} | ShortHash: {}", file_path, res);
}
