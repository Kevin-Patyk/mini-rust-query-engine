use query_engine_datatypes::record_batch::RecordBatch;
use query_engine_datatypes::schema::Schema;

use crate::data_source::DataSource;

/// InMemoryDataSource is a data source that wraps existing RecordBatches already in memory.
/// Unlike CSV or Parquet, there is no file to open or data to convert - the batches are ready to use.
/// This makes it useful for testing and for queries that build on the results of other queries.
/// Like Parquet, no schema needs to be provided - it is derived directly from the RecordBatches
/// themselves since each RecordBatch already carries its own schema.
/// CSV is the exception - it has no type information embedded in the file, so the schema
/// must either be provided explicitly or inferred, typing all columns as String.
/// We also don't need batch_size since the data is already in RecordBatches of a fixed size;
/// there's nothing to chunk - you just return them as is.
pub struct InMemoryDataSource {
    // The actual data, already in RecordBatch form.
    // The schema is derived from the first batch rather than stored separately.
    pub record_batches: Vec<RecordBatch>,
}

impl InMemoryDataSource {
    /// This is just a constructor.
    /// It lets you create InMemoryDataSource by passing in the fields rather than having to write it out every time.
    pub fn new(record_batches: Vec<RecordBatch>) -> Self {
        Self { record_batches }
    }
}

/// InMemoryIterator is a stateful iterator over a Vec<RecordBatch>.
/// Unlike CSV and Parquet, there is no file to read or types to convert -
/// the data is already in RecordBatch form so we just need to step through it.
/// std::vec::IntoIter<RecordBatch> is the iterator type produced by calling into_iter() on a Vec<RecordBatch>.
/// It takes ownership of the Vec and tracks the current position internally, so next() just delegates to it - the
/// same pattern as relying on the Parquet and CSV readers to track their own state.
pub struct InMemoryIterator {
    pub batch: std::vec::IntoIter<RecordBatch>,
}

impl InMemoryIterator {
    // This is just a constructor.
    /// It lets you create InMemoryIterator by passing the fields rather than having to write it out every time.
    pub fn new(batch: std::vec::IntoIter<RecordBatch>) -> Self {
        Self { batch }
    }
}

/// Implements the Iterator trait for InMemoryIterator.
/// This is the simplest iterator of the three data sources - there is no file to read,
/// no raw bytes to parse, and no type conversion to do. The data is already in RecordBatch
/// form so next() just delegates to the underlying IntoIter, which tracks positions and returns
/// None automatically when all batches have been yielded.
impl Iterator for InMemoryIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Delegate directly to IntoIter's next() - no conversion needed.
        // IntoIter already knows where it is in the Vec and when it's exhausted.
        self.batch.next()
    }
}

/// Implements the DataSource trait for InMemoryDataSource.
/// schema() and scan() are trivial compared to CSV and Parquet since the data
/// is already in RecordBatch form - no file to open, no types to convert.
impl DataSource for InMemoryDataSource {
    /// Returns the schema by reading it directly from the first RecordBatch.
    /// No file or footer needs to be opened since the schema is already embedded in the data.
    /// Note: panics if record_batches is empty.
    fn schema(&self) -> Schema {
        self.record_batches[0].schema.clone()
    }

    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>> {
        let batches = if projection.is_empty() {
            // No projection — return all batches as-is since all columns are needed
            // We clone the vec so the iterator can take ownership of it
            self.record_batches.clone()
        } else {
            // Map projection column names to their indices in the schema
            // We need indices rather than names because RecordBatch stores columns
            // as a Vec, so we have to pick them out by position.
            let projection_indices: Vec<usize> = projection
                .iter()
                .map(|name| {
                    self.schema()
                        .fields
                        .iter()
                        // Find the position of the column with this name in the schema
                        .position(|f| f.name == *name)
                        .unwrap()
                })
                .collect();

            // Build the projected schema with only the requested columns
            // This narrows the schema to match the columns we are about to select from each batch
            let projected_schema = self
                .schema()
                .select(&projection.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .unwrap();

            // For each batch, pick only the columns at the projection indices.
            // We pair the narrowed schema with the selected columns to produce a new RecordBatch.
            self.record_batches
                .iter()
                .map(|batch| RecordBatch {
                    schema: projected_schema.clone(),
                    fields: projection_indices
                        .iter()
                        .map(|i| batch.fields[*i].clone())
                        .collect(),
                })
                .collect()
        };

        Box::new(InMemoryIterator {
            batch: batches.into_iter(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use query_engine_datatypes::arrow_field_vector::ArrowFieldVector;
    use query_engine_datatypes::arrow_type::ArrowType;
    use query_engine_datatypes::column_vector::ColumnVector;
    use query_engine_datatypes::record_batch::RecordBatch;
    use query_engine_datatypes::schema::{Field, Schema};

    use super::*;

    #[test]
    fn test_in_memory_data_source_scan() {
        // Build a schema with 2 columns
        let schema = Schema {
            fields: vec![
                Field {
                    name: "id".to_string(),
                    data_type: ArrowType::Int32,
                },
                Field {
                    name: "age".to_string(),
                    data_type: ArrowType::Int32,
                },
            ],
        };

        // Build a RecordBatch with 2 rows of test data
        let batch = RecordBatch {
            schema: schema.clone(),
            fields: vec![
                Arc::new(ArrowFieldVector::new(Arc::new(Int32Array::from(vec![
                    1, 2,
                ])))) as Arc<dyn ColumnVector>,
                Arc::new(ArrowFieldVector::new(Arc::new(Int32Array::from(vec![
                    30, 25,
                ])))) as Arc<dyn ColumnVector>,
            ],
        };

        // Create the data source with one batch
        let source = InMemoryDataSource::new(vec![batch]);

        // Scan with no projection - return all columns
        let mut iter = source.scan(vec![]);
        let result = iter.next().unwrap();
        assert_eq!(result.row_count(), 2);
        assert_eq!(result.column_count(), 2)
    }
}
