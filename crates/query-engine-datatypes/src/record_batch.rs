use std::sync::Arc;

use crate::arrow_type::ArrowType;
use crate::column_vector::ColumnVector;
use crate::schema::Schema;

/// Summary:
/// RecordBatch is a collection of equal-length columns paired with schema, representing a table of data in memory.
/// new() creates a new RecordBatch from a Schema and a Vec of Arc wrapped ColumnVectors.
/// row_count() returns the number of rows by asking the first column for its size.
/// column_count() returns the number of columns in the batch.
/// field() returns a clone of the Arc wrapped ColumnVector at a given index.
/// to_csv() serializes the batch into a command separated string.
///
/// A RecordBatch is essentially a table of data in memory - a collection of columns that all have the same length,
/// paired with a schema that describes what those columns are.
/// This is the fundamental unit of data that flows through the query engine.
/// Schema is the blueprint (column names and types) and the RecordBatch is the actual chunk of data conforming to that blueprint.
#[derive(Clone)]
pub struct RecordBatch {
    // Anything receiving a batch knows what the columns mean.
    pub schema: Schema,
    // The actual column data, using the abstraction we already built.
    // We need to use Vec<Arc<dyn ColumnVector>> since the vector will be holding different types that
    // implement the column vector trait.
    // The concrete types will be determined at runtime.
    // Since these are not concrete types, we need to wrap it in a Arc because Rust needs a size at compile time.
    // Arc is also used since the column data will need to be reference from multiple places simultaneously.
    // Box means taking exclusive ownership and we would have to clone the data to share it.
    pub fields: Vec<Arc<dyn ColumnVector>>,
}

impl RecordBatch {
    /// This is just a constructor.
    /// It lets you create RecordBatch by passing the fields rather than having to write it out every time.
    pub fn new(schema: Schema, fields: Vec<Arc<dyn ColumnVector>>) -> Self {
        Self { schema, fields }
    }

    /// Returns the number of rows in the batch by asking the first column for its size.
    /// We only ask for the size of the first column size all columns in a record batch must be the same length by definition.
    /// So, any column will give us the same answer and the first is just the convenient choice.
    pub fn row_count(&self) -> usize {
        // Rust's auto-deref automatically goes through the Arc to call the method on the inner T.
        // So this works without us needing to dereference anything.
        self.fields[0].size()
    }

    /// Returns the number of columns in the batch.
    pub fn column_count(&self) -> usize {
        self.fields.len()
    }

    /// Returns a reference to a ColumnVector by index (in the fields vector).
    pub fn fields(&self, index: usize) -> Arc<dyn ColumnVector> {
        // Cloning an Arc<T> always gives us back another Arc<T>, hence the return type.
        // If we used &dyn ColumnVector as the return type, we would have to deal with lifetime issues.
        self.fields[index].clone()
    }

    /// Helper function for printing a human readable representation of what is in RecordBatch.
    /// The outer loop loops over the rows.
    /// The inner loop loops over the columns.
    /// For each cell, handle null or downcast based on the column's type.
    /// Add a new line at the end of each row.
    pub fn to_csv(&self) -> String {
        let mut output = String::new();
        // When a double for loop runs, the inner loop runs in full for each iteration of the outer loop.
        for row_index in 0..self.row_count() {
            for col_index in 0..self.column_count() {
                if col_index > 0 {
                    output.push(',');
                }
                let col = &self.fields[col_index];
                let value = col.get_value(row_index);
                // get_value() returns Option<Arc<dyn Any>>. The first downcast happened inside of get_value()
                // to extract the value from Arrow's internal storage into a Rust primitive.
                // That primitive was then wrapped in Arc<dyn Any>, erasing the type information.
                // So here we downcast a second time, from dyn Any back to a concrete Rust type,
                // so we can call to_string() on it. get_type() tells us what type to downcast to,
                // since get_type() and get_value() are always in sync on the same column.
                // When we get a value in a row, its type gets erased, then we rely on get_type() for what to downcast to.
                match value {
                    None => output.push_str("null"),
                    Some(v) => match col.get_type() {
                        ArrowType::Boolean => {
                            output.push_str(&v.downcast_ref::<bool>().unwrap().to_string())
                        },
                        ArrowType::Int8 => {
                            output.push_str(&v.downcast_ref::<i8>().unwrap().to_string())
                        },
                        ArrowType::Int16 => {
                            output.push_str(&v.downcast_ref::<i16>().unwrap().to_string())
                        },
                        ArrowType::Int32 => {
                            output.push_str(&v.downcast_ref::<i32>().unwrap().to_string())
                        },
                        ArrowType::Int64 => {
                            output.push_str(&v.downcast_ref::<i64>().unwrap().to_string())
                        },
                        ArrowType::UInt8 => {
                            output.push_str(&v.downcast_ref::<u8>().unwrap().to_string())
                        },
                        ArrowType::UInt16 => {
                            output.push_str(&v.downcast_ref::<u16>().unwrap().to_string())
                        },
                        ArrowType::UInt32 => {
                            output.push_str(&v.downcast_ref::<u32>().unwrap().to_string())
                        },
                        ArrowType::UInt64 => {
                            output.push_str(&v.downcast_ref::<u64>().unwrap().to_string())
                        },
                        ArrowType::Float32 => {
                            output.push_str(&v.downcast_ref::<f32>().unwrap().to_string())
                        },
                        ArrowType::Float64 => {
                            output.push_str(&v.downcast_ref::<f64>().unwrap().to_string())
                        },
                        ArrowType::String => output.push_str(v.downcast_ref::<String>().unwrap()),
                    },
                }
            }
            // push_str() expects a string slice &str, which has some overhead for a single character.
            // push() takes a char directly which is more efficient since there's no need to treat it as a string.
            // For a single character, use push() but for a String use push_str()
            output.push('\n');
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, Int32Array, StringArray};

    use super::*;
    use crate::arrow_field_vector::ArrowFieldVector;
    use crate::schema::Field;

    fn create_record_batch() -> RecordBatch {
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
                    name: "salary".to_string(),
                    data_type: ArrowType::Float64,
                },
            ],
        };

        // The vec![] macro infers its type from the first element, which would be Arc<ArrowFieldVector>.
        // But RecordBatch expects `Vec<Arc<dyn ColumnVector>>`.
        // We need to explicitly tell Rust to coerce each element to the trait object.
        // This is a common pattern in Rust.
        let fields: Vec<Arc<dyn ColumnVector>> = vec![
            Arc::new(ArrowFieldVector::new(Arc::new(Int32Array::from(vec![
                1, 2, 3,
            ])))),
            Arc::new(ArrowFieldVector::new(Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Carol",
            ])))),
            Arc::new(ArrowFieldVector::new(Arc::new(Float64Array::from(vec![
                95000.0, 87000.0, 102000.0,
            ])))),
        ];

        RecordBatch::new(schema, fields)
    }

    #[test]
    fn test_record_batch_row_count() {
        let out = create_record_batch().row_count();
        assert_eq!(out, 3)
    }

    #[test]
    fn test_record_batch_col_count() {
        let out = create_record_batch().column_count();
        assert_eq!(out, 3)
    }

    #[test]
    fn test_record_batch_fields() {
        let out = create_record_batch().fields(0);
        assert_eq!(out.size(), 3);
        assert_eq!(out.get_type(), ArrowType::Int32)
    }

    #[test]
    fn test_record_batch_to_csv() {
        let out = create_record_batch().to_csv();
        let expected = "1,Alice,95000\n2,Bob,87000\n3,Carol,102000\n";
        assert_eq!(out, expected);
    }
}
