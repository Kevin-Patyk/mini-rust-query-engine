use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Type as PhysicalType;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::types::Type;
use query_engine_datatypes::arrow_type::ArrowType;
use query_engine_datatypes::record_batch::RecordBatch;
use query_engine_datatypes::schema::{Field, Schema};

use crate::data_source::DataSource;

/// ParquetDataSource implements the DataSource trait for reading Parquet files.
/// Unlike CsvDataSource, we don't need an optional schema or a has_headers flag
/// because Parquet files are self-describing - the schema is always embedded
/// in the file footer and is read automatically by the parquet crate.
/// All we need is the filename to locate the file and the batch_size to control how many rows are yielded per RecordBatch.
pub struct ParquetDataSource {
    pub filename: String,
    pub batch_size: usize,
}

impl ParquetDataSource {
    /// This is just a constructor.
    /// It lets you create ParquetDataSource by passing in the fields rather than have to write it out every time.
    pub fn new(filename: String, batch_size: usize) -> Self {
        Self {
            filename,
            batch_size,
        }
    }
}

impl DataSource for ParquetDataSource {
    /// Since Parquet data already has schema the things we need to do are:
    /// 1. Open the file
    /// 2. Read the footer to get the Parquet schema
    /// 3. Convert it to our own Schema type
    /// The parquet crate handles 1 and 2 for us
    /// Step 3 is where we need to mapp Arrow's field types to our ArrowType enum
    fn schema(&self) -> Schema {
        // Open the file for reading
        let file = File::open(&self.filename).expect("Failed to open Parquet file");

        // SerializedFileReader reads the file footer immediately on construction,
        // giving us access to the schema and row group metadata without reading any actual data.
        let reader = SerializedFileReader::new(file).expect("Failed to create Parquet reader");

        // The footer contains the file metadata, which includes the Parquet schema.
        let parquet_schema = reader.metadata().file_metadata().schema();

        // We are using t.as_ref() to borrow the value inside the Arc; it goes from Arc<Type> to &Type.
        // Rust's match doesn't auto-deref, so without as_ref() we would be trying to match on Arc<Type> rather than Type.
        let fields = parquet_schema
            .get_fields()
            .iter()
            .map(|t| {
                let data_type = match t.as_ref() {
                    Type::PrimitiveType { physical_type, .. } => match physical_type {
                        PhysicalType::BOOLEAN => ArrowType::Boolean,
                        PhysicalType::INT32 => ArrowType::Int32,
                        PhysicalType::INT64 => ArrowType::Int64,
                        PhysicalType::FLOAT => ArrowType::Float32,
                        PhysicalType::DOUBLE => ArrowType::Float64,
                        PhysicalType::BYTE_ARRAY => ArrowType::String,
                        _ => unimplemented!("Unsupported Parquet physical type"),
                    },
                    Type::GroupType { .. } => unimplemented!("Nested types not supported"),
                };
                Field {
                    name: t.name().to_string(),
                    data_type,
                }
            })
            .collect();

        Schema { fields }
    }

    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>> {
        todo!()
    }
}
