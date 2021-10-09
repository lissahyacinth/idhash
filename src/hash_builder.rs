use fasthash::murmur3::hash128;
use std::sync::Arc;

use crate::{config::IdHashConfig, unf_vector::UNFVector};
use arrow::{
    array::{
        BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
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
        arrow::datatypes::DataType::Timestamp(TimeUnit::Second, None) => col
            .downcast_ref::<TimestampSecondArray>()
            .expect("Failed to downcast S Timeunit")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, None) => col
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("Failed to downcast NS Timeunit")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None) => col
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Failed to downcast Millisecond Unit")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None) => col
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Failed to downcast Microsecond Unit")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Timestamp(_, _) => todo!(),
        arrow::datatypes::DataType::Date32 => col
            .downcast_ref::<Date32Array>()
            .expect("Failed to downcast to Date32")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Date64 => col
            .downcast_ref::<Date64Array>()
            .expect("Failed to downcast to Date64")
            .raw(config.characters, config.digits),
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
