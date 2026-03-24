use std::fs::File;
use std::sync::Arc;

use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::basic::Type as PhysicalType;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::types::Type;
use query_engine_datatypes::arrow_field_vector::ArrowFieldVector;
use query_engine_datatypes::arrow_type::ArrowType;
use query_engine_datatypes::column_vector::ColumnVector;
use query_engine_datatypes::record_batch::RecordBatch;
use query_engine_datatypes::schema::{Field, Schema};

use crate::data_source::DataSource;

/// Summary:
/// ParquetDataSource implements the DataSource trait for reading Parquet files.
/// It holds the filename and batch size. Unlike CsvDataSource, no schema or headers flags is needed
/// because Parquet files are self-describing - the schema is always embedded in the file footer.
/// schema() opens the file, reads the footer, and converts the Parquet physical types into our own Schema type.
/// scan() opens the file, maps projection column names to indices, builds ProjectionMask to push
/// the projection down to the reader, and returns a ParquetIterator.
///
/// ParquetIterator implements the Iterator trait, yielding one RecordBatch per call to next()
/// It holds the resolved schema and the ParquetRecordBatchReader which tracks state internally.
/// next() delegates to the underlying reader, uses the schema resolved in scan() rather than
/// rebuilding it each time, converts the batch columns to ArrowFieldVectors, and packages everything into a RecordBatch.
/// When the file is exhausted, next() returns None.
///
/// ParquetDataSource implements the DataSource trait for reading Parquet files.
/// Unlike CsvDataSource, we don't need an optional schema or a has_headers flag
/// because Parquet files are self-describing - the schema is always embedded
/// in the file footer and is read automatically by the parquet crate.
/// All we need is the filename to locate the file and the batch_size to control how many rows are yielded per RecordBatch.
pub struct ParquetDataSource {
    /// Path to the Parquet file on disk
    pub filename: String,
    /// How many rows to yield per RecordBatch.
    /// Larger batches amortize overhead but use more memory.
    pub batch_size: usize,
}

impl ParquetDataSource {
    /// This is just a constructor.
    /// It lets you create ParquetDataSource by passing in the fields rather than having to write it out every time.
    pub fn new(filename: String, batch_size: usize) -> Self {
        Self {
            filename,
            batch_size,
        }
    }
}

/// ParquetIterator is a stateful iterator over a Parquet file.
/// It implements the Iterator trait, yielding on RecordBatch at a time.
/// State is held between calls to next() so the reader knows where it left off
/// and can continue reading from the file without starting over.
/// As a note, we do not need batch_size like we did for CSV because CSV doesn't have a built-in concept of batches.
/// We had to manually read rows and group them together into batches of batch_size.
/// Parquet is different - it has a built-in batch size that we can specify.
pub struct ParquetIterator {
    // The schema of the Parquet file, used to describe the structure of each RecordBatch we yield.
    pub schema: Schema,
    // The underlying Arrow Parquet reader that yields Arrow RecordBatches from the file.
    // Each call to next() on this reader returns the next batch of rows.
    pub parquet_reader: ParquetRecordBatchReader,
}

/// The above struct is the state machine - it holds everything the iterator needs to remember between calls to next().
/// In our case, that is schema, the Parquet reader, and the batch size.
/// The Iterator trait impl then just defines the behavior - what to do with that state each time next() is called.
impl ParquetIterator {
    /// This is just a constructor.
    /// It lets you create ParquetIterator by passing the fields rather than having to write it out every time.
    pub fn new(schema: Schema, parquet_reader: ParquetRecordBatchReader) -> Self {
        Self {
            schema,
            parquet_reader,
        }
    }
}

/// Implements the Iterator trait for the ParquetIterator.
/// Each call to next() delegates to the underlying ParquetRecordBatchReader,
/// which handles state tracking internally - we don't need to manually track
/// position or row offsets. We just convert whatever it returns to into our own RecordBatch
impl Iterator for ParquetIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Delegate to the underlying parquet reader.
        // The ? operator handles the Option - if the reader is exhausted and returns None, we return None immediately.
        // If there is a batch, we get Ok(batch) or Err and handle it in the match.
        match self.parquet_reader.next()? {
            Ok(batch) => {
                // Use the schema resolved during scan() rather than rebuilding it from the batch on every call.
                let schema = self.schema.clone();

                // batch.columns() gives &[Arc<dyn Array>]
                // Our ArrowFieldVector struct takes ArrayRef, which is an alias for Arc<dyn Array>,
                // so the conversion is straightforward: we iterate over the vector of Arc<dyn Array> and call
                // ArrowFieldVector::new for each one.
                // Each ArrowFieldVector is then wrapped in an Arc and cast to Arc<dyn ColumnVector> to match
                // the type our RecordBatch expects.
                let fields: Vec<Arc<dyn ColumnVector>> = batch
                    .columns()
                    .to_vec()
                    .into_iter()
                    .map(|array| Arc::new(ArrowFieldVector::new(array)) as Arc<dyn ColumnVector>)
                    .collect();

                Some(RecordBatch { schema, fields })
            },
            // If the reader returns an error, we return None to signal the end of the iterator.
            Err(_) => None,
        }
    }
}

/// Implements the DataSource trait for ParquetDataSource.
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
        // As a note, we are reading the Parquet footer directly, so we have limited physical types.
        // While next() works with Arrow's richer type system after conversion.
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

    /// Scans the Parquet file and returns a boxed iterator that yields RecordBatches.
    /// The batch_size controls how many rows are included per RecordBatch.
    /// If a projection is provided, only the requested columns are included in the schema.
    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>> {
        // Open the file for reading
        let file = File::open(&self.filename).expect("Failed to open Parquet file");

        // Create a builder from the file. We hold onto the builder rather than immediately building the reader
        // because we need to access the parquet schema to compute the projection indices before building
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        // Build the reader with or without a projection mask depending on whether columns were requested.
        // If no projection is specified, read all columns. Otherwise, map column names to indices
        // and build a ProjectionMask to push the projection down to the Parquet reader for efficiency.
        let reader = if projection.is_empty() {
            builder.with_batch_size(self.batch_size).build().unwrap()
        } else {
            // Map each projected column name to its index in the Parquet schema.
            // The ProjectionMask works with indices rather than names, so we need to find the position
            // of each requested column in the schema's column list.
            let indices: Vec<usize> = projection
                .iter()
                .map(|name| {
                    builder
                        .parquet_schema()
                        .columns()
                        .iter()
                        // position() is an iterator method that searches through the elements and returns Some(index)
                        // of the first element where the closure returns true, or None if no match is found
                        // It is essentially "find me the index of the element that matches this condition"
                        .position(|c| c.name() == name)
                        .unwrap()
                })
                .collect();

            // Build a ProjectionMask from the column indices.
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
            builder
                .with_batch_size(self.batch_size)
                .with_projection(mask)
                .build()
                .unwrap()
        };

        // If no projection is specified, use the full schema.
        // Otherwise, select only the requested columns from the schema.
        let final_schema = if projection.is_empty() {
            self.schema().clone()
        } else {
            let projection_refs: Vec<&str> = projection.iter().map(|s| s.as_str()).collect();
            self.schema()
                .select(&projection_refs)
                .expect("Selected columns do not exist in the schema")
        };

        // Wrap the reader and schema in a ParquetIterator and box it so it can be returned as a trait object
        Box::new(ParquetIterator::new(final_schema, reader))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch as ArrowRecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    #[test]
    fn test_parquet_data_source_scan() {
        // Define the Arrow schema for the test file
        let filename = "test.parquet";
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("age", DataType::Int32, false),
        ]));

        // Build a record batch with two rows of test data
        let batch = ArrowRecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int32Array::from(vec![30, 25])),
            ],
        )
        .unwrap();

        // Write the batch to a Parquet file using the ArrowWriter
        // close() flushes the footer to disk, making the file readable.
        let file = File::create(filename).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Create the data source and scan with no projection - return all columns
        let source = ParquetDataSource::new(filename.to_string(), 1024);
        let mut iter = source.scan(vec![]);

        // The first and only batch should contain both rows and all three columns
        let batch = iter.next().unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.column_count(), 3);

        // clean up the temporary file
        std::fs::remove_file(filename).unwrap();
    }
}
