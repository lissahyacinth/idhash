use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use config::{IdHashConfig, IdHashConfigBuilder};
use hash_builder::idhash_batch;
extern crate num_cpus;

pub mod config;
pub mod hash_builder;
mod unf_vector;
pub mod utils;

use rayon::prelude::*;

/// Calculate Identifiable Hash for a series of RecordBatches
pub fn calculate_idhash<I>(batch_input: I, schema: Arc<Schema>, config: IdHashConfig) -> u128
where
    I: Iterator<Item = RecordBatch>,
{
    batch_input
        .map(|batch| idhash_batch(batch, &schema, config))
        .reduce(|acc, x| acc.wrapping_add(x))
        .unwrap()
}

/// Calculate Identifiable Hash for a series of RecordBatches
pub fn calculate_idhash_par<I>(batch_input: I, schema: Arc<Schema>, config: IdHashConfig) -> u128
where
    I: ParallelIterator<Item = RecordBatch>,
{
    batch_input
        .into_par_iter()
        .map(|batch| idhash_batch(batch, &schema, config))
        .reduce_with(|acc, x| acc.wrapping_add(x))
        .unwrap()
}

mod tests {
    use crate::utils::read_csv_data;

    use super::*;

    fn read_return_hash(file_path: &str) -> u128 {
        let config = IdHashConfigBuilder::new().build();
        let csv = read_csv_data(file_path.to_string(), 100, 1024);
        let schema = csv.schema();
        calculate_idhash(csv.into_iter().map(|x| x.unwrap()), schema, config)
    }

    #[test]
    fn load_float_from_file() {
        let file_path = "data/ExampleData.csv";
        let file_path_2 = "data/ExampleDataSorted.csv";
        assert_eq!(read_return_hash(file_path), read_return_hash(file_path_2))
    }
}
