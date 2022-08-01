use std::fs::File;

use std::ops::Deref;
use std::sync::Arc;

use arrow::array::Array;
use arrow::chunk::Chunk;
use arrow::datatypes::Schema;
use arrow::io::csv::read::{
    deserialize_batch, deserialize_column, infer, infer_schema, ByteRecord,
};
use arrow::io::csv::read::{Reader, ReaderBuilder};

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

        let schema = Arc::new(Schema::from(
            infer_schema(&mut reader, Some(lines_for_type_inference), true, &infer)
                .unwrap()
                .0,
        ));

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

pub struct ThreadArrayChunk {
    array: Arc<Box<dyn Array>>,
}

impl Deref for ThreadArrayChunk {
    type Target = Arc<Box<dyn Array>>;

    fn deref(&self) -> &Self::Target {
        &self.array
    }
}

impl<'a> AsRef<dyn Array + 'a> for ThreadArrayChunk {
    fn as_ref(&self) -> &(dyn Array + 'a) {
        &**self.array
    }
}

impl From<Arc<Box<dyn Array>>> for ThreadArrayChunk {
    fn from(item: Arc<Box<dyn Array>>) -> Self {
        ThreadArrayChunk { array: item }
    }
}

impl From<Arc<Arc<dyn Array>>> for ThreadArrayChunk {
    fn from(item: Arc<Arc<dyn Array>>) -> Self {
        let boxed_item: Box<dyn Array> = item.to_boxed();
        ThreadArrayChunk {
            array: Arc::new(boxed_item),
        }
    }
}

impl Iterator for CSVReader {
    type Item = Chunk<ThreadArrayChunk>;

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
            &self.buffer[..row_count],
            &self.schema.fields,
            None,
            self.line_number - row_count,
            deserialize_column,
        ) {
            Ok(chunk) => Some(Chunk::new(
                chunk
                    .into_arrays()
                    .into_iter()
                    .map(|x| ThreadArrayChunk::from(Arc::new(x)))
                    .collect::<Vec<ThreadArrayChunk>>(),
            )),
            Err(_) => panic!("Failed to Deserialize Batch"),
        }
    }
}
