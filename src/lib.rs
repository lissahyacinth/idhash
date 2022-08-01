use std::sync::Arc;

use arrow::{chunk::Chunk, datatypes::Schema};

use config::{IdHashConfig, IdHashConfigBuilder};
use hash_builder::idhash_batch;

pub mod config;
pub mod hash_builder;
pub mod unf_vector;
pub mod utils;

use rayon::prelude::*;
pub use utils::ThreadArrayChunk;

/// Calculate Identifiable Hash for a series of RecordBatches
pub fn calculate_idhash<I>(batch_input: I, schema: Arc<Schema>, config: IdHashConfig) -> u128
where
    I: Iterator<Item = Chunk<ThreadArrayChunk>>,
{
    batch_input
        .map(|batch| idhash_batch(batch, &schema, config))
        .reduce(|acc: u128, x: u128| acc.wrapping_add(x))
        .unwrap()
}

/// Calculate Identifiable Hash for a series of RecordBatches
pub fn calculate_idhash_par<I>(batch_input: I, schema: Arc<Schema>, config: IdHashConfig) -> u128
where
    I: ParallelIterator<Item = Chunk<ThreadArrayChunk>>,
{
    batch_input
        .into_par_iter()
        .map(|batch| idhash_batch(batch, &schema, config))
        .reduce(|| 0, |acc: u128, x: u128| acc.wrapping_add(x))
}

mod tests {
    use crate::utils::CSVReader;

    use super::*;

    fn _read_return_hash(file_path: &str, batch_size: usize) -> u128 {
        let config = IdHashConfigBuilder::new().build();
        // FIXME: Combining multiple batches is causing an issue.
        let reader = CSVReader::new(file_path.to_string(), 100, batch_size);
        let csv_schema = reader.schema.clone();
        calculate_idhash(reader, csv_schema, config)
    }

    #[test]
    pub fn batch_size_invariant() {
        let file_path = "data/ExampleData.csv";
        assert_eq!(
            _read_return_hash(file_path, 1024),
            _read_return_hash(file_path, 5 * 1024)
        )
    }

    #[test]
    fn load_float_from_file() {
        let file_path = "data/ExampleData.csv";
        let file_path_2 = "data/ExampleDataSorted.csv";
        assert_eq!(
            _read_return_hash(file_path, 1024),
            _read_return_hash(file_path_2, 1024)
        )
    }

    #[test]
    fn batch_count() {
        let file_path = "data/ExampleData.csv";
        let reader = CSVReader::new(file_path.to_string(), 100, 2);
        assert_eq!(reader.into_iter().count(), 2501);
    }

    #[test]
    fn load_date_data_from_file() {
        let file_path = "data/ExampleDateData.csv";
        _read_return_hash(file_path, 1024);
    }
}
