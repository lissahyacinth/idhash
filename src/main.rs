use std::time::Instant;

use clap::{App, Arg};

use idhash::config::IdHashConfigBuilder;
use idhash::utils::CSVReader;
use idhash::{calculate_idhash, calculate_idhash_par};
use rayon::iter::ParallelBridge;

fn main() {
    let matches = App::new("IdHash")
        .version("0.0.3")
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
        .arg(
            Arg::with_name("n_cpus")
                .short("n")
                .value_name("N_CPUS")
                .default_value("1")
                .takes_value(true),
        )
        .get_matches();
    let start_time = Instant::now();
    let file_path = matches.value_of("input_file").unwrap();
    let truncation = matches.value_of("truncation").unwrap();
    let digits = matches.value_of("digits").unwrap();
    let characters = matches.value_of("characters").unwrap();
    let inference_rows: usize = matches.value_of("inference_rows").unwrap().parse().unwrap();
    let batch_size: usize = matches.value_of("batch_size").unwrap().parse().unwrap();
    let n_cpus: usize = matches.value_of("n_cpus").unwrap().parse().unwrap();
    let config = IdHashConfigBuilder::new()
        .truncation(truncation.parse().unwrap())
        .digits(digits.parse().unwrap())
        .characters(characters.parse().unwrap())
        .build();
    let csv = CSVReader::new(file_path.to_string(), inference_rows, batch_size);
    let csv_schema = csv.schema.clone();
    let res = if n_cpus > 1 {
        calculate_idhash_par(csv.par_bridge(), csv_schema, config)
    } else {
        calculate_idhash(csv, csv_schema, config)
    };
    let end_time = Instant::now();
    println!(
        "File: {} | ShortHash: {} | Time Taken: {:?}",
        file_path,
        res,
        end_time.duration_since(start_time)
    );
}
