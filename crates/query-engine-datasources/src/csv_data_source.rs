use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use csv::ReaderBuilder;
use query_engine_datatypes::arrow_field_vector::ArrowFieldVector;
use query_engine_datatypes::arrow_type::ArrowType;
use query_engine_datatypes::column_vector::ColumnVector;
use query_engine_datatypes::record_batch::RecordBatch;
use query_engine_datatypes::schema::{Field, Schema};

use crate::data_source::DataSource;

/// Summary:
/// CsvDataSource implements the DataSource trait for reading CSV files.
/// It holds the filename, an optional schema, a flag for whether the file has headers, and a batch size.
/// schema() opens the file, resolves the schema or infers one from the header row, typing all columns as String.
/// scan() opens the file, resolves the schema, applies any projection, and returns a CsvIterator.
/// infer_schema() opens the file, reads the header row, and builds a Schema with all columns typed as String.
///
/// CsvIterator implements the Iterator trait, yielding one RecordBatch per call to next().
/// It holds the resolved schema, the csv Reader, and the batch size.
/// next() reads up to batch_size rows from the csv Reader, pivots them from row to columns,
/// builds an ArrowFieldVector for each column by parsing the string values into the correct Arrow type,
/// and packages everything into a RecordBatch.
/// When the file is exhausted, next() returns None.
///
/// CsvDataSource reads a CSV file and implements the DataSourceTrait,
/// allowing the query engine to treat a CSV Ffile like any other data source.
/// It owns all the configuration needed to open and parse the file.
pub struct CsvDataSource {
    /// Path to the CSV file on disk.
    pub filename: String,
    /// The schema describing column names and types.
    /// If None, the schema will be inferred from the header row,
    /// treating all columns as Strings.
    pub schema: Option<Schema>,
    /// Whether the first row of the CSV contains column names.
    pub has_headers: bool,
    /// How many rows to yield per RecordBatch.
    /// Larger batches amortize overhead but use more memory.
    pub batch_size: usize,
}

impl CsvDataSource {
    /// This is just a constructor.
    /// It lets you create CsvDataSource by passing in the fields rather than have to write it out every time.
    pub fn new(
        filename: String,
        schema: Option<Schema>,
        has_headers: bool,
        batch_size: usize,
    ) -> Self {
        Self {
            filename,
            schema,
            has_headers,
            batch_size,
        }
    }

    /// Infers a schema by opening the file and reading the header row.
    /// Every column is typed as a String since CSV has no type information.
    /// If has_headers is false, columns are named field_1, field_2, etc.
    /// If you infer the schema, every column will come back as ArrowType::String because
    /// CSV has no type information embedded in the file.
    pub fn infer_schema(&self) -> Schema {
        // File::open takes a path and returns a Result<File, Error>.
        // It asks the OS to open the file at the path for reading.
        // We pass a reference to the filename string since open() doesn't need ownership.
        // expect() unwraps the Result, panicking with a message if the file doesn't exist.
        let file = File::open(&self.filename).expect("Failed to open CSV file");

        // ReaderBuilder is the csv crate's way of configuring a CSV parser before creating it.
        // has_headers tells the parser whether to treat the first row as column names.
        // from_reader takes anything that implements the Read trait - File implements read
        // So the parser reads directly from the file handle without loading it all into memory
        // The reader is mut because parsing is stateful - every call to headers() or read_record() advanced an internal cursor through the file
        // This is essentially a configuration step before any actual reading happens - setting rules.
        let mut reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(file);

        // headers() returns the first row if has_headers is true.
        // If has_headers is false, the csv crate auto-generates positional headers like "0", "1", "2", which we
        // remapped to field_1, field_2, etc.
        let fields = reader
            .headers()
            .expect("Failed to read CSV headers")
            .iter()
            .enumerate()
            // We are converting the headers into Fields with this map
            // If we have an existing header, we use that for the name, otherwise we create simple field names using the index
            .map(|(i, name)| {
                let col_name = if self.has_headers {
                    name.to_string()
                } else {
                    // We are using i + 1 since we want to start at 1, not 0
                    // Rust is 0-indexed but we do not want to start with field_0
                    format!("field_{}", i + 1)
                };
                Field {
                    name: col_name,
                    data_type: ArrowType::String,
                }
            })
            .collect();

        Schema { fields }
    }
}

/// CsvIterator is a stateful iterator over a CSV file.
/// A stateful iterator is one that remembers where it is between calls to next()
/// Every time you call next() it picks up exactly where it left off, reading the next batch of rows rather than starting from the beginning.
/// Each call to next() reads up to batch_size rows from the file,
/// and packages them into RecordBatch.
/// It holds the csv Reader rather than a raw file because the Reader
/// owns the parsing state - knowing where in the file we are,
/// what the headers are, and how to decode each row.
pub struct CsvIterator {
    pub schema: Schema,
    pub csv_reader: csv::Reader<File>,
    pub batch_size: usize,
}

/// The above struct is the state machine - it holds everything the iterator needs to remember between calls to next().
/// In our case, that is schema, the csv reader, and the batch size.
/// The Iterator trait impl then just defines the behavior - what to do with that state each time next() is called.
impl CsvIterator {
    /// This is just a constructor.
    /// It lets you create CsvIterator by passing the fields rather than having to write it out every time.
    pub fn new(schema: Schema, csv_reader: csv::Reader<File>, batch_size: usize) -> Self {
        Self {
            schema,
            csv_reader,
            batch_size,
        }
    }
}

/// Implementing the Iterator trait for CsvIterator.
/// Each iterator implementation needs a state tracker and the trait implementation.
/// The struct holds the state and next() is what advances that state forward. Every call to next()
/// moves the csv reader's cursor forward by up to batch_size rows, so the state naturally tracks progress through the file.
/// When the reader is exhausted, next() returns None and the iterator is done.
impl Iterator for CsvIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<RecordBatch> {
        // Collect up to batch_size rows from the csv reader.
        // Each StringRecord is one row of raw string values.
        let records: Vec<csv::StringRecord> = self
            .csv_reader
            .records()
            .take(self.batch_size)
            .map(|r| r.unwrap())
            .collect();

        // If no rows were read, the file is exhausted.
        if records.is_empty() {
            return None;
        }

        // Pivot rows into columns.
        // columns[0] is all the value for the first column, columns[1] for the second, etc.
        let mut columns: Vec<Vec<String>> = Vec::new();
        for col_index in 0..self.schema.fields.len() {
            let mut column: Vec<String> = Vec::new();
            for row in &records {
                column.push(row[col_index].to_string())
            }
            columns.push(column);
        }

        // Convert each Vec<String> column into an ArrowFieldVector
        // by matching on the field's data type and building the correct Arrow array.
        let arrow_columns: Vec<ArrowFieldVector> = self
            .schema
            .fields
            .iter()
            .enumerate()
            // Our mapping will be taking the fields in our Schema and converting them to actual columns of data
            // based on the values we have in the columns we made in the step before.
            // So for each field in our schema, we make a corresponding actual column of data which will go into our RecordBatch
            .map(|(col_index, field)| {
                let col_data = &columns[col_index];
                let array: ArrayRef = match field.data_type {
                    // For each possible type, we need to create an ArrayRef, which is a concrete column of data.
                    // We match on our ArrowTypes since our each Field in our Schema has a data_type.
                    // When we encounter a specific data type, we iterate over the column data for this one column
                    // and try to parse it into that actual value.
                    ArrowType::String => Arc::new(StringArray::from(
                        col_data.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    )),
                    // Each numeric and boolean column is mapped to Vec<Option<T>> rather than Vec<T>.
                    // Arrow array constructors accept Vec<Option<T>> directly - None values are recorded
                    // in the validity bitmap as 0 (null) and Some(v) values are packed into data buffers.
                    // This means we never need to unwrap the Options ourselves; Arrow handles it for us.
                    ArrowType::Boolean => Arc::new(BooleanArray::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<bool>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::Int8 => Arc::new(Int8Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<i8>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::Int16 => Arc::new(Int16Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<i16>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::Int32 => Arc::new(Int32Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<i32>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::Int64 => Arc::new(Int64Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<i64>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::UInt8 => Arc::new(UInt8Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<u8>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::UInt16 => Arc::new(UInt16Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<u16>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::UInt32 => Arc::new(UInt32Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<u32>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::UInt64 => Arc::new(UInt64Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<u64>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::Float32 => Arc::new(Float32Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<f32>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    ArrowType::Float64 => Arc::new(Float64Array::from(
                        col_data
                            .iter()
                            .map(|s| {
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s.parse::<f64>().unwrap())
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                };
                ArrowFieldVector::new(array)
            })
            .collect();

        Some(RecordBatch {
            schema: self.schema.clone(),
            fields: arrow_columns
                .into_iter()
                .map(|v| Arc::new(v) as Arc<dyn ColumnVector>)
                .collect(),
        })
    }
}

impl DataSource for CsvDataSource {
    /// Returns the schema for this data source.
    /// If a schema was provided at construction time, it is returned directly.
    /// Otherwise, the schema is inferred from the CSV header row.
    fn schema(&self) -> Schema {
        match &self.schema {
            Some(schema) => schema.clone(),
            None => self.infer_schema(),
        }
    }

    /// Scans the CSV file and returns an iterator of RecordBatches.
    /// Each call to next() on the iterator reads up to batch_size rows from the file
    /// and packages them into a RecordBatch.
    /// If a projection is provided, only the requested columns are included in each batch,
    /// meaning we narrow the schema and only read those columns rather than the entire row.
    /// If no projection is provided, all columns are returned.
    /// scan() doesn't do any reading itself, it just sets up the CsvIterator and returns it.
    /// The actual reading only happens when something starts pulling from the iterator.
    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>> {
        // Opening the file.
        let file = File::open(&self.filename).expect("Failed to open CSV file");

        // Configuration step before the reading happens.
        let reader = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .from_reader(file);

        // Resolve the final schema - either the full schema if no projection was provided or a narrowed schema with only requested columns
        let final_schema = if projection.is_empty() {
            self.schema()
        } else {
            let projection_refs: Vec<&str> = projection.iter().map(|s| s.as_str()).collect();
            self.schema()
                .select(&projection_refs)
                .expect("Selected columns do not exist in the schema")
        };

        // Wrap the reader and schema in a CsvIterator and return it as a
        // Box<dyn Iterator> so the caller doesn't need to know the concrete type.
        Box::new(CsvIterator::new(final_schema, reader, self.batch_size))
        // Because CsvIterator implements the Iterator trait, Rust already knows how to drive it.
        // By wrapping it in Box::new() and returning it as Box<dyn Iterator<Item = RecordBatch>>,
        // we are just saying "here is something that implements iterator, figure out the rest at runtime."
        // The caller can then call .next() on it, us it in a for loop, or chain it with .map(), .filter(), etc.
    }
}

// In Rust, returning a custom iterator from a function requires 3 things:
// 1. A struct that holds the iterator's state (CsvIterator).
// 2. An Iterator trait impl on that struct defining next(), which is the behavior.
// 3. The function returns that struct wrapped in Box<dyn Iterator> since the concrete type size is unknown at compile time and needs to be heap allocated.
// This is more boilerplate than languages like Python or Kotlin, but gives Rust the information it needs to drive the iterator with 0 runtime overhead.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_data_source_scan() {
        // Create a temporary CSV file with a header row and two data rows
        let filename = "test.csv";
        std::fs::write(filename, "id,name,age\n1,Alice,30\n2,Bob,25").unwrap();

        // Build a schema matching the CSV columns
        let schema = Schema {
            fields: vec![
                Field {
                    name: "id".to_string(),
                    data_type: ArrowType::Int32,
                },
                Field {
                    name: "name".to_string(),
                    data_type: ArrowType::String,
                },
                Field {
                    name: "age".to_string(),
                    data_type: ArrowType::Int32,
                },
            ],
        };

        // Create an instance of the CsvDataSource
        let source = CsvDataSource::new(filename.to_string(), Some(schema), true, 1024);

        // Scan with no projection — return all columns
        let mut iter = source.scan(vec![]);

        // The first and only batch should contain both rows
        // We can drive the state forward by calling next() manually, through a for loop, or other iterator adapter
        let batch = iter.next().unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.column_count(), 3);

        // Clean up the temporary file
        std::fs::remove_file(filename).unwrap();
    }
}
