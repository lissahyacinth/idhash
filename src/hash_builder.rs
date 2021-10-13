use fasthash::murmur3::hash128;
use std::sync::Arc;

use crate::{config::IdHashConfig, unf_vector::UNFVector};
use arrow::{
    array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::Schema,
    record_batch::RecordBatch,
};

use arrow::datatypes::TimeUnit;

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
        arrow::datatypes::DataType::Boolean => col
            .downcast_ref::<BooleanArray>()
            .expect("Failed to downcast to Bool")
            .raw(config.characters, config.digits),
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
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, _)
        | arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, _)
        | arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _)
        | arrow::datatypes::DataType::Timestamp(TimeUnit::Second, _) => col
            .downcast_ref::<Int64Array>()
            .expect("Failed to Downcast to Int64Array")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Time32(_) => todo!(),
        arrow::datatypes::DataType::Time64(_) => todo!(),
        arrow::datatypes::DataType::Duration(_) => todo!(),
        arrow::datatypes::DataType::Interval(_) => todo!(),
        arrow::datatypes::DataType::Binary => todo!(),
        arrow::datatypes::DataType::FixedSizeBinary(_) => todo!(),
        arrow::datatypes::DataType::LargeBinary => todo!(),
        arrow::datatypes::DataType::List(_) => todo!(),
        arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
        arrow::datatypes::DataType::LargeList(_) => todo!(),
        arrow::datatypes::DataType::Struct(_) => todo!(),
        arrow::datatypes::DataType::Union(_, _, _) => todo!(),
        arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
        arrow::datatypes::DataType::Decimal(_, _) => todo!(),
        arrow::datatypes::DataType::Map(_, _) => todo!(),
        arrow::datatypes::DataType::Extension(_, _, _) => todo!(),
        arrow::datatypes::DataType::Date32 => col
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast Date to Int32")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Date64 => col
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast Date to Int64")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Utf8 => todo!(),
        arrow::datatypes::DataType::LargeUtf8 => todo!(),
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
