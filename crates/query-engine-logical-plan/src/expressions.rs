use std::fmt::{self, Formatter};

use query_engine_datatypes::arrow_type::ArrowType;
use query_engine_datatypes::schema::Field;

use crate::logical_expr::LogicalExpr;

/// Column is the most fundamental expression.
/// It's a leaf node that just holds a column name and looks it up in the input plan's schema when to_field is called.
pub struct Column {
    pub name: String,
}

/// Implementing the LogicalExpr trait for Column.
impl LogicalExpr for Column {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        input
            .schema() // get the schema from the input plan
            .fields // Access the Vec of fields
            .iter() // iterate over &Field references
            .find(|field| field.name == self.name) // Find the first field whose name matches
            .cloned() // Convert Option<&Field> to Option<Field> so we own the values
            .unwrap_or_else(|| panic!("No column named '{}' in input schema", self.name))
        // if None, panic with a helpful message sgowing which column was missing

        // unwrap_or_else unwraps an Option, but instead of panicking with a generic message like unwrap would, it lets you provide a closure that
        // runs only if the value is None
        // It is essentially unwrap with a fallback
        // The _else part means "do this instead if it's None"
        // The closure is lazy - it only executes if needed, which is why it's a closure rather than just a value
    }
}

/// Display is the trait that defines how a type should be represented as a human-readable string.
/// When you implement it, Rust knows what to do whenever we call to_string, use it with {} in println! or format!,
/// or use it anywhere else that expects a displayable value.
/// Without Display implemented, Rust has no idea how to turn your type into a string.
impl fmt::Display for Column {
    // fmt is called whenever the value is formatted as a string,
    // for example when you call to_string() or use it in a println!
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // write! pushes the formatted string into the formatter f
        // the # prefix is a convention to signal "this is a column reference, not a string literal"
        // so Column { name: "salary" } formats as "#salary"
        write!(f, "#{}", self.name)
        // write! is a macro that pushes formatted text into the formatter f
        // Think of f as a buffer - write! is just filling that buffer with the string that you want
    }
}

/// A leaf expression that references a column by its position in the schema.
/// Used internally by the engine when the exact index is already known,
/// rather than looking it up by name.
pub struct ColumnIndex {
    pub index: usize, // zero-based position in the schema's fields vec
}

impl LogicalExpr for ColumnIndex {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        input
            .schema() // Get the schema from the input plan
            .fields // Access the Vec of fields
            .get(self.index) // Get the field at the given index, returns Option<&Field>
            .cloned() // Convert Option<&Field> to Option<Field> so we own the value
            .unwrap_or_else(|| panic!("No column at index '{}' in input schema", self.index))
        // If None, panic with a helpful message showing which index was out of bounds
    }
}

impl fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.index)
        // write! takes 3 things:
        // f - the formatter to write into
        // A format string - the template with {} placeholders
        // The values to fill in the placeholders

        // The formatter f accumulates the output and when fmt returns, whatever
        // was written into f becomes the string representation.
        // So write!(f, "#{}", self.name) pushes "#salary" into f
        // and thats what you get back when you call .to_string() or use {} in a format string.
    }
}
