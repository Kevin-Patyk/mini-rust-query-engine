use std::any::Any;
use std::sync::Arc;

use crate::arrow_type::ArrowType;

/// Summary:
/// ColumnVector is a trait defining the interface for working with column data.
/// It abstracts over different column implementations so the rest of the engine
/// can read column data without caring about the underlying storage.

/// Abstraction over different implementations of a column vector.
/// This trait allows the rest of the engine to work with column data
/// without caring about the underlying Arrow vector type.
pub trait ColumnVector {
    fn get_type(&self) -> ArrowType;
    /// Returns the value at the given index.
    /// Option handles nullability - None means the value is null.
    /// Arc<dyn: std::any::Any> allows returning any type (bool, int, float, string, etc.)
    /// since each column can hold a different type of data.
    /// In a more complete engine, this would be replaced.
    /// dyn means dynamic dispatch - the concrete type isn't known at compile time, it's figured out at runtime.
    /// We use Arc since dyn Any is a trait object and the size isn't known at compile time since it could be any type.
    /// Arc puts it on the heap and gives the compiler a fixed size pointer to work with instead.
    /// Unlike Box, Arc can be cloned cheaply by incrementing a reference count, allowing the value to be shared without copying.
    fn get_value(&self, index: usize) -> Option<Arc<dyn Any>>;
    fn size(&self) -> usize;
}
