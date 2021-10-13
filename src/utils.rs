use std::fs::File;

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::io::csv::read::{
    deserialize_batch, deserialize_column, infer, infer_schema, read_rows, ByteRecord,
};
use arrow::io::csv::read::{Reader, ReaderBuilder};
use arrow::record_batch::RecordBatch;

pub struct CSVReader {
    reader: Reader<File>,
    buffer: Vec<ByteRecord>,
    pub batch_size: usize,
    pub schema: Arc<Schema>,
    exhausted: bool,
    line_number: usize,
}

impl CSVReader {
    pub fn new(file_path: String, lines_for_type_inference: usize, batch_size: usize) -> Self {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_path(file_path)
            .unwrap();

        let schema = Arc::new(
            infer_schema(&mut reader, Some(lines_for_type_inference), true, &infer).unwrap(),
        );

        CSVReader {
            reader,
            buffer: vec![ByteRecord::default(); batch_size],
            batch_size,
            line_number: 0,
            schema,
            exhausted: false,
        }
    }
}

impl Iterator for CSVReader {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let mut row_count = 0;
        for slot in self.buffer.iter_mut() {
            match self.reader.read_byte_record(slot) {
                Ok(has_more) => {
                    if !has_more {
                        self.exhausted = true;
                        break;
                    }
                    row_count += 1;
                }
                Err(_) => break,
            }
        }
        self.line_number += row_count;
        match deserialize_batch(
            &mut self.buffer[..row_count],
            self.schema.fields(),
            None,
            self.line_number - row_count,
            deserialize_column,
        ) {
            Ok(batch) => Some(batch),
            Err(_) => panic!("Failed to Deserialize Batch"),
        }
    }
}
