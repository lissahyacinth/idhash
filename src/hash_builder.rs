use fasthash::murmur3::hash128;
use std::sync::Arc;

use crate::{config::IdHashConfig, unf_vector::UNFVector};
use arrow::{
    array::{
        Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::Schema,
    record_batch::RecordBatch,
};

struct HashIterator<T>(Vec<T>);

impl<T> Iterator for HashIterator<T>
where
    T: Iterator<Item = Vec<u8>>,
{
    type Item = Vec<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter_mut().map(Iterator::next).collect()
    }
}

fn convert_col_to_raw<'a>(
    col: &'a dyn std::any::Any,
    column_index: usize,
    schema: &Arc<Schema>,
    config: IdHashConfig,
) -> Box<dyn Iterator<Item = Vec<u8>> + 'a> {
    match schema.field(column_index).data_type() {
        arrow::datatypes::DataType::Null => todo!(),
        arrow::datatypes::DataType::Boolean => todo!(),
        arrow::datatypes::DataType::Int8 => col
            .downcast_ref::<Int32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Int16 => col
            .downcast_ref::<Int32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Int32 => col
            .downcast_ref::<Int32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Int64 => col
            .downcast_ref::<Int64Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt8 => col
            .downcast_ref::<UInt16Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt16 => col
            .downcast_ref::<UInt16Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt32 => col
            .downcast_ref::<UInt32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt64 => col
            .downcast_ref::<UInt64Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Float16 => col
            .downcast_ref::<Float32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Float32 => col
            .downcast_ref::<Float32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Float64 => col
            .downcast_ref::<Float64Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Timestamp(_, _) => todo!(),
        arrow::datatypes::DataType::Date32 => todo!(),
        arrow::datatypes::DataType::Date64 => todo!(),
        arrow::datatypes::DataType::Time32(_) => todo!(),
        arrow::datatypes::DataType::Time64(_) => todo!(),
        arrow::datatypes::DataType::Duration(_) => todo!(),
        arrow::datatypes::DataType::Interval(_) => todo!(),
        arrow::datatypes::DataType::Binary => todo!(),
        arrow::datatypes::DataType::FixedSizeBinary(_) => todo!(),
        arrow::datatypes::DataType::LargeBinary => todo!(),
        arrow::datatypes::DataType::Utf8 => col
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast Utf8 -> StringArray")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::LargeUtf8 => col
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast Utf8 -> StringArray")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::List(_) => todo!(),
        arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
        arrow::datatypes::DataType::LargeList(_) => todo!(),
        arrow::datatypes::DataType::Struct(_) => todo!(),
        arrow::datatypes::DataType::Union(_) => todo!(),
        arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
        arrow::datatypes::DataType::Decimal(_, _) => todo!(),
    }
}

/// Produce MurmurHash for a given RecordBatch
///
pub(crate) fn idhash_batch(input: RecordBatch, schema: &Arc<Schema>, config: IdHashConfig) -> u128 {
    HashIterator(
        input
            .columns()
            .iter()
            .enumerate()
            .map(|(col_index, col)| convert_col_to_raw(col.as_any(), col_index, schema, config))
            .collect(),
    )
    .map(|row| hash128(row.into_iter().flatten().collect::<Vec<u8>>()))
    .reduce(|acc, x| acc.wrapping_add(x))
    .unwrap()
}
