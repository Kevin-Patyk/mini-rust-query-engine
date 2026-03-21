use std::any::Any;
use std::sync::Arc;

use crate::arrow_type::ArrowType;
use crate::column_vector::ColumnVector;

/// Summary:
/// LiteralValueVector is a virtual column that stores a single value and returns it for any valid index.
/// new() creates a new LiteralValueVector from an ArrowType, a value, and a usize.
/// get_type() returns the stored ArrowType directly - no matching or downcasting needed.
/// get_value() checks the index is in bounds, then returns a clone of the stored Arc value.
/// size() returns the number of rows in the current batch.
///
/// A virtual column that returns the same value for every index.
/// Rather than allocating memory for thousands of identical values,
/// it stores the value once and returns it for any valid index.
/// Used for literals in expressions like salary * 1.1, where 1.1 needs to act like a column.
/// This is a single value that pretends to be a full column.
/// Every time the engine calls get_value(i) on it, no matter what i is, it gets back the same value.
/// It's an illusion of a column.
pub struct LiteralValueVector {
    // The ArrowType of the literal .
    data_type: ArrowType,
    // The actual literal value stored once.
    // Option handles nullability - None means the value is null.
    // Arc<dyn Any> allows returning any type (bool, int, float, string, etc.).
    value: Option<Arc<dyn Any>>,
    // The number of rows in the current batch.
    // Not the size of the data, hust how many times this value logically repeats.
    size: usize,
}

impl LiteralValueVector {
    /// This is just a constructor.
    /// It lets you create LiteralValueVector by passing in the fields rather than have to write it out every time.
    pub fn new(data_type: ArrowType, value: Option<Arc<dyn Any>>, size: usize) -> Self {
        Self {
            data_type,
            value,
            size,
        }
    }
}

/// We are implementing the ColumnVector for our LiteralValueVector.
impl ColumnVector for LiteralValueVector {
    /// This method extracts the ArrowType from the struct.
    /// During query planning, the engine needs to know the type of every column vector it's
    /// working with, whether it's a real column or literal.
    fn get_type(&self) -> ArrowType {
        self.data_type.clone()
    }

    /// Returns the same stored value for any valid index.
    /// Panics if the index is out-of-bounds.
    /// Returns None if the value is null, otherwise clones the Arc and returns it.
    /// Cloning an Arc is cheap - it just increments the reference count, no data is copied.
    fn get_value(&self, index: usize) -> Option<Arc<dyn std::any::Any>> {
        // This needs to be >= not just >.
        // If size is 3, valid indices are 0,1,2.
        // An index of 3 is out of bounds, but 3 > 3 would not panic.
        if index >= self.size {
            panic!("{}", format!("index {index} is out-of-bounds"))
        }

        // Check if the value is null.
        // If the value is null, return None.
        // The ? operator on an Option means: if this is None, return None early, otherwise unwrap it and continue.
        // The ; at the end discards the unwrapped value.
        // If self.value is None, return None, otherwise continue.
        // It's the idiomatic Rust way to propagate None through a function.
        // as_ref is needed here to avoid moving self.value out of the struct.
        self.value.as_ref()?;

        self.value.clone()
    }

    /// Returns the number of rows in the current batch, not the size of the data itself.
    fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_lv_vector() -> LiteralValueVector {
        // For the value, we are explicitly annotating it as i64 so that it downcasts properly when we need it.
        // If we did not do this, it would downcast to i32 by default in Rust.
        LiteralValueVector::new(ArrowType::Int64, Some(Arc::new(1i64)), 1)
    }

    #[test]
    fn test_literal_value_vector_new() {
        let out = create_lv_vector();
        assert_eq!(out.data_type, ArrowType::Int64);
        assert!(out.value.is_some());
        assert_eq!(out.size, 1)
    }

    #[test]
    fn test_literal_value_vector_get_type() {
        let out = create_lv_vector().get_type();
        assert_eq!(out, ArrowType::Int64)
    }

    #[test]
    fn test_literal_value_vector_get_value_success() {
        let out = create_lv_vector().get_value(0);
        // binding keeps the Arc<dyn Any> alive long enough for downcast_ref to borrow from it.
        // Otherwise, the Arc is dropped at the end of the line and the reference is invalid.
        let binding = out.unwrap();
        // downcast_ref takes a &dyn Any and attempts to convert it to a concrete type at runtime.
        // You tell it what type you expect with the turbofish syntax and it returns an Option,
        // The some variant if the underlying type is the type specified or None if it isn't
        let value = binding.downcast_ref::<i64>().unwrap();
        assert_eq!(*value, 1)
    }

    #[test]
    fn test_literal_value_vector_get_value_null() {
        let out = LiteralValueVector::new(ArrowType::Int64, None, 1).get_value(0);
        assert!(out.is_none())
    }

    #[test]
    #[should_panic]
    fn test_literal_value_vector_oob() {
        LiteralValueVector::new(ArrowType::Int64, None, 1).get_value(99);
    }
}
