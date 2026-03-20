use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;

use crate::arrow_type::ArrowType;
use crate::column_vector::ColumnVector;

/// Summary:
/// ArrowFieldVector wraps an ArrayRef (any type that implements the Array trait) - essentially a column of data.
/// new() creates a new ArrowFieldVector from an ArrayRef.
/// get_type() matches the underlying Arrow data type to our ArrowType enum.
/// get_value() downcasts the array to its concrete type and extracts the value at the given index.
/// size() returns the length of the underlying array.

/// ArrowFieldVector is a concrete implementation of the ColumnVector trait.
/// It wraps an actual Apache Arrow array, handling the type-specific branching
/// in one place so that the rest of the engine can work with column data generically.
pub struct ArrowFieldVector {
    // ArrRef is Arrow's type alias for Arc<dyn Array>
    // dyn Array means any type that implements the array trait
    // Arc wraps it so the array can be shared across multiple places without copying the data
    // ArrayRef is saying: I have a column of data, I don't care what it is right now.
    // When we actually need to read values out of it later, that is when we downcast it back to the concrete type.
    pub array: ArrayRef,
}

impl ArrowFieldVector {
    /// This is just a constructor.
    /// It lets you create ArrowFieldVector by passing in an ArrayRef rather than having to write it out every time.
    pub fn new(array: ArrayRef) -> Self {
        Self { array }
    }
}

/// We are implementing the ColumnVector trait for ArrowFieldVector.
impl ColumnVector for ArrowFieldVector {
    // This answers the question: What type of data does this column hold?
    // The engine will need to know this during query planning.
    // Since ArrowFieldVector just holds an ArrayRef, the type information is buried inside it.
    // This method surfaces that information by inspecting the array's data type and returning the corresponding ArrowType
    // that the rest of the engine understands.
    // This tells us what type the column is.
    fn get_type(&self) -> ArrowType {
        match self.array.data_type() {
            DataType::Boolean => ArrowType::Boolean,
            DataType::Int8 => ArrowType::Int8,
            DataType::Int16 => ArrowType::Int16,
            DataType::Int32 => ArrowType::Int32,
            DataType::Int64 => ArrowType::Int64,
            DataType::UInt8 => ArrowType::UInt8,
            DataType::UInt16 => ArrowType::UInt16,
            DataType::UInt32 => ArrowType::UInt32,
            DataType::UInt64 => ArrowType::UInt64,
            DataType::Float32 => ArrowType::Float32,
            DataType::Float64 => ArrowType::Float64,
            DataType::Utf8 => ArrowType::String,
            // This is a macro that panics with a message.
            // It signals that this part of the code should not be able to be reached.
            // Using it if an unsupported data type is encountered.
            _ => unreachable!("Unsupported data type"),
        }
    }

    /// This method reads a single value from the column at a given index.
    /// It returns Option<Arc<dyn Any>> - None if the value is null, otherwise
    /// the value wrapped in a Arc so it can be returned generically regardless of the type.
    /// This actually reads the data out from the column.
    /// This extracts the value from Arrow's internal storage and turns it into a Rust primitive.
    /// The chain for reading a value from a column is:
    /// Arrow array -> downcast to Rust primitive in get_value() -> wrapped in Arc<dyn Any> (type erased)
    /// -> downcast again at the call site using get_type() to recover the concrete type.
    /// The double downcast is the price of having a single generic get_value() interface.
    /// The actual underlying value remains in memory, but Rust no longer knows what it is at compile time.
    fn get_value(&self, index: usize) -> Option<Arc<dyn Any>> {
        // Check for null first using Arrow's validity bitmap.
        // If null, return None immediately without trying to read the value.
        // is_null checks if the specific value at that index is null.
        if self.array.is_null(index) {
            return None;
        }

        // Match on the data type to know which concrete Arrow array type to downcast to.
        // We then downcast self.array to that concrete type, read the value at the given index,
        // and wrap it in Some(Arc::new(...)) to match the return type.
        // unwrap is safe here because we already know the type from the match.
        // We need to downcast to an Arrow array to work with the underlying data.
        match self.array.data_type() {
            // as_any converts the ArrayRef in array to &dyn Any, which enables runtime inspection, which evaluates the type at runtime rather than compile time.
            // &dyn Any is Rust's general purpose type for: I don't know what this is yet, let me inspect it at runtime.
            // downcast_ref attempts to cast it to a concrete Arrow array.
            // It returns an Option - Some if the cast succeeds, None if the types don't match.
            // unwrap is safe here because we already confirmed the type in the match above.
            DataType::Boolean => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(index),
            )),
            DataType::Int8 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::Int16 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::Int32 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::Int64 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::UInt8 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::UInt16 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::UInt32 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::UInt64 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::Float32 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(index),
            )),
            DataType::Float64 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(index),
            )),
            // to_string converts &str to an owned String so the returned value doesn't borrow from self, avoiding lifetime issues.
            // In Rust, string literals and slices always &str - a reference to some underlying data.
            // In this case, StringArray()::value gives a &str, not a copy.
            // We don't need this for primitives since they have the Copy trait and are just copied when they are returned.
            // str doesn't implement Copy in Rust, so without to_string, we would have lifetime issues.
            DataType::Utf8 => Some(Arc::new(
                self.array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(index)
                    .to_string(),
            )),
            _ => unreachable!("Unsupported data type"),
        }
    }

    /// This just returns how many values are in the column.
    /// If you have a column of 1,000 employees salaries, it will return 1,000.
    fn size(&self) -> usize {
        // len works directly on ArrayRef (Arc<dyn Array>) since the Array trait has a len method already.
        // Thus, we can call directly without any downcasting.
        self.array.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;

    use super::*;

    fn create_array() -> ArrayRef {
        Arc::new(Int32Array::from(vec![1, 2, 3, 4]))
    }

    #[test]
    fn test_arrow_field_vector_new() {
        let out = ArrowFieldVector::new(create_array());
        assert_eq!(out.array.len(), 4)
    }

    #[test]
    fn test_arrow_field_vector_get_type() {
        let out = ArrowFieldVector::new(create_array()).get_type();
        assert_eq!(out, ArrowType::Int32)
    }

    #[test]
    fn test_arrow_field_vector_get_value() {
        let out = ArrowFieldVector::new(create_array()).get_value(0);
        // binding keeps the Arc<dyn Any> alive long enough for downcast_ref to borrow from it.
        // Otherwise, the Arc is dropped at the end of the line and the reference is invalid.
        let binding = out.unwrap();
        // downcast_ref takes a &dyn Any and attempts to convert it to a concrete type at runtime.
        // You tell it what type you expect with the turbofish syntax and it returns an Option,
        // The some variant if the underlying type is the type specified or None if it isn't
        let value = binding.downcast_ref::<i32>().unwrap();
        assert_eq!(*value, 1)
    }

    #[test]
    fn test_arrow_field_vector_size() {
        let out = ArrowFieldVector::new(create_array()).size();
        assert_eq!(out, 4)
    }
}
