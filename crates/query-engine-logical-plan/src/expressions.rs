use std::fmt::{self, Formatter};

use query_engine_datatypes::arrow_type::ArrowType;
use query_engine_datatypes::schema::Field;

use crate::logical_expr::LogicalExpr;

/// This module defines all the concrete expression types that implement LogicalExpr.
///
/// Expressions form their own tree, separate from the logical plan tree.
/// Each expression node implements to_field, which answers the question:
/// "What would this expression produce if evaluated against rows from this plan?"
/// This is how the engine validates expressions at planning time - before any data
/// is touched, it can walk the expression tree calling to_field() on each node to
/// check that column references exist, types are compatible, and output schemas are correct.
///
/// We will have 2 separate trees that work together:
/// 1. The logical plan tree describes the data flow - Scan feeds into Filter feeds into Projection.
/// Each node is a transformation on a set of rows.
/// 2. The expression tree describes computations within a plan node - salary * 0.1, age > 30, SUM(salary).
/// Each node is an operation on values.
///
/// The logical plan tree is walked to build and optimise a query.
/// The expression trees are walked to validate types and eventually to evaluate values at execution time.
///
/// The order of operations is: the expression tree is built first (just constructing the structs, no to_field calls yet).
/// Then during the planning phase, the engine calls to_field on the root expression
/// That cascades down the tree recursively and as each call returns, the types bubble up until the root has its final type
/// The sequence is:
/// 1. parse the query - build the expression tree (just data, no evaluation)
/// 2. planning phase - call to_field on the root - recursion goes down, types come back up
/// 3. execution phase - actually compute values row-by-row
/// Right now we are at step 1 - just defining the structs
/// The actual to_field calls happen later when we build up the planner

// Columns --------------------

/// Column is the most fundamental expression.
/// It's a leaf node that just holds a column name and looks it up in the input plan's schema when to_field is called.
pub struct Column {
    pub name: String,
}

/// Implementing the LogicalExpr trait for Column.
impl LogicalExpr for Column {
    // This will look up the field in the input plan's schema - it doesn't know
    // the name or type ahead of time, they discover it at planning time by searching through the schema.
    // Literals always know their type, where Column wioll have to ask the schema for the type
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

// Literals --------------------

/// A leaf expressions representing a literal string value.
/// Literal expressions hold a fixed value known at planning time -
/// they don't reference any column or computation, they just produce
/// the same value for every row. The input plan is not needed since
/// there is nothing to look up.
pub struct LiteralString {
    pub value: String,
}

impl LogicalExpr for LiteralString {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        // The input plan is not used - a literal doesn't depend on any schema
        // The field name is the value itself
        // The type is always String since this is a String literal
        Field {
            name: self.value.clone(),
            data_type: ArrowType::String,
        }
    }
}

impl fmt::Display for LiteralString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // The single quotes around {} means a value like hello
        // formats as 'hello', which is the standard SQL convention for string literals
        // to distinguish from column references
        write!(f, "'{}'", self.value)
    }
}

pub struct LiteralLong {
    pub value: i64,
}

impl LogicalExpr for LiteralLong {
    // The input plan is not used - a literal doesn't depend on any schema
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: self.value.to_string(),
            data_type: ArrowType::Int64,
        }
    }
}

impl fmt::Display for LiteralLong {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

pub struct LiteralDouble {
    pub value: f64,
}

impl LogicalExpr for LiteralDouble {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        // The input plan is not used - a literal doesn't depend on any schema
        Field {
            name: self.value.to_string(),
            data_type: ArrowType::Float64,
        }
    }
}

impl fmt::Display for LiteralDouble {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

pub struct LiteralFloat {
    pub value: f32,
}

impl LogicalExpr for LiteralFloat {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        // The input plan is not used - a literal doesn't depend on any schema
        Field {
            name: self.value.to_string(),
            data_type: ArrowType::Float32,
        }
    }
}

impl fmt::Display for LiteralFloat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

pub struct LiteralBoolean {
    pub value: bool,
}

impl LogicalExpr for LiteralBoolean {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        // The input plan is not used - a literal doesn't depend on any schema
        Field {
            name: self.value.to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for LiteralBoolean {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

// Binary Expressions --------------------

/// Binary expressions have a left and a right child expression and compute a result from both.
/// They sit in the middle of the expression tree as internal nodes - never leaves.
///
/// The Kotlin implementation uses an abstract base class BinaryExpr to hold the shared fields:
/// - name: A label for the expression, such as "add"
/// - op: the operator symbol, such as "+"
/// - l: left child expression
/// - r: right child expression
///
/// From BinaryExpr two families branch off:
/// - MatchExpr for arithmetic operations, to_field returns the same type as the left child
/// - BooleanBinaryExpr for comparisons and logical operations, to_field always returns boolean regardless of children's types
///
/// In Rust we don't have abstract classes, so we have 2 options:
/// - A shared struct that both families embed
/// - A trait that captures the shared behavior.

// Boolean Expressions --------------------
pub struct Eq {
    // We need Box<dyn LogicalExpr> because LogicalExpr is a trait and traits
    // don't have a known size at compile time.
    // The compiler needs to know how many bytes to allocate for each field in a struct,
    // but LogicalExpr can be anything that implements LogicalExpr - different sizes
    // Box solves this by storing the value on the heap and keeping a fixed-size pointer in the struct
    // The pointer is always the same size regardless of what's behind it, so the compiler is happy
    // Any time you have a trait object, you need some form of indirection
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Eq {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "eq".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for Eq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.l, self.r)
    }
}

pub struct Neq {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Neq {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "neq".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for Neq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} != {}", self.l, self.r)
    }
}

pub struct Gt {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Gt {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "gt".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for Gt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} > {}", self.l, self.r)
    }
}

pub struct GtEq {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for GtEq {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "gt_eq".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for GtEq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} >= {}", self.l, self.r)
    }
}

pub struct Lt {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Lt {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "lt".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for Lt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} < {}", self.l, self.r)
    }
}

pub struct LtEq {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for LtEq {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "lt_eq".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for LtEq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} <= {}", self.l, self.r)
    }
}

pub struct And {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for And {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "and".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for And {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} AND {}", self.l, self.r)
    }
}

pub struct Or {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Or {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "or".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for Or {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} OR {}", self.l, self.r)
    }
}

// Arithmetic Expressions --------------------

pub struct Add {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Add {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "add".to_string(),
            // The return type of an arithmetic expression is the same as the left child's type
            // e.g. salary (Float64) + bonus (Float64) -> Float64
            // We call to_field on the left child to discover its type at planning time
            data_type: self.l.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Add {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} + {}", self.l, self.r)
    }
}

pub struct Subtract {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Subtract {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "subtract".to_string(),
            data_type: self.l.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Subtract {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {}", self.l, self.r)
    }
}

pub struct Multiply {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Multiply {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "multiply".to_string(),
            data_type: self.l.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Multiply {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} * {}", self.l, self.r)
    }
}

pub struct Divide {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Divide {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "divide".to_string(),
            data_type: self.l.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Divide {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} / {}", self.l, self.r)
    }
}

pub struct Modulus {
    pub l: Box<dyn LogicalExpr>,
    pub r: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Modulus {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "modulus".to_string(),
            data_type: self.l.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Modulus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} % {}", self.l, self.r)
    }
}

// Aggregate Expressions --------------------

/// Aggregate expressions collapse multiple rows into a single value.
/// Unlike math or boolean expressions which produce on value per row,
/// aggegrates operate over a group of rows and produce one result for the whole group
/// e.g SUM(salary) doesn't give you one salary per row - it gives you one total for all rows.
///
/// All aggregate expressions follow the same structure:
/// - expr: the child expression being aggregated e.g. salary in SUM(salary)
///
/// to_field returns the same type as the child expression for most aggregates,
/// except for count.
/// Count is the exception - it always produces Int32 regardless of the child's type because counting
/// rows produces a whole number.
///
/// During logical planning, we don't care about the actual aggregation yet.
/// We just need to know the name and type of the result - actual collapsing of rows happens later at execution time.
pub struct Sum {
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Sum {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "sum".to_string(),
            // to_field is recursive - it calls to_field on the child expression,
            // which may itself call to_field on its own children, and so on down
            // the expression tree until it hits a leaf node (Column or Literal)
            // that returns a type directly without recursing further.
            // This is how the engine infers the output type of a complex expression
            // like SUM(salary * 0.1) -it walks the tree bottom-up resolving types
            // at each node until it reaches the root.
            // The to_field call goes top-down but the types are resolved bottom up.
            data_type: self.expr.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Sum {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SUM({})", self.expr)
    }
}

pub struct Min {
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Min {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "min".to_string(),
            data_type: self.expr.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Min {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MIN({})", self.expr)
    }
}

pub struct Max {
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Max {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "max".to_string(),
            data_type: self.expr.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Max {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MAX({})", self.expr)
    }
}

pub struct Avg {
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Avg {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "avg".to_string(),
            data_type: self.expr.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Avg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AVG({})", self.expr)
    }
}

pub struct Count {
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Count {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "count".to_string(),
            data_type: ArrowType::Int32,
        }
    }
}

impl fmt::Display for Count {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "COUNT({})", self.expr)
    }
}

// Aggregate Expressions --------------------

/// Aias wraps another expression and gives it a new name - like salary * 1.1 AS new_salary
/// It has 2 fields: the wrapped expression and the alias string
/// to_field returns a field with the alias as the name and the wrapped expression's type
pub struct Alias {
    pub expr: Box<dyn LogicalExpr>,
    pub alias: String,
}

impl LogicalExpr for Alias {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: self.alias.clone(),
            data_type: self.expr.to_field(input).data_type,
        }
    }
}

impl fmt::Display for Alias {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} as {}", self.expr, self.alias)
    }
}

// Scalar Expression --------------------

/// A scalar function is a function that takes one or more expressions as arguments and returns a single value per row
/// Unlike aggregate functions which collapse multiple rows into one value.
///
/// Examples: UPPER(name), CONCAT(first_name, ' ', last_name), ABS(salary)
///
/// Unlike the other expression types where to_field can infer the return type from the child expressions,
/// scalar functions need the return type passed in explicitly - the engine can't know that UPPER() returns a String
/// or that ABS returns the same type as its input without being told.
///
/// args is a Vec of child expressions - scalar functions can take any number of arguments, unlike
/// binary expressions which always have exactly two.
pub struct Scalar {
    pub name: String,
    pub args: Vec<Box<dyn LogicalExpr>>,
    pub data_type: ArrowType,
}

impl LogicalExpr for Scalar {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
        }
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let args = self
            .args
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}({})", self.name, args)
    }
}

// Miscellaneous Expressions --------------------

pub struct Not {
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalExpr for Not {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "not".to_string(),
            data_type: ArrowType::Boolean,
        }
    }
}

impl fmt::Display for Not {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "NOT({})", self.expr)
    }
}

pub struct CastExpr {
    pub expr: Box<dyn LogicalExpr>,
    pub target_type: ArrowType,
}

impl LogicalExpr for CastExpr {
    fn to_field(&self, input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: self.expr.to_field(input).name,
            data_type: self.target_type.clone(),
        }
    }
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Cast({} AS {:?})", self.expr, self.target_type.clone())
    }
}

pub struct CountDistinct {
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalExpr for CountDistinct {
    fn to_field(&self, _input: &dyn crate::logical_plan::LogicalPlan) -> Field {
        Field {
            name: "count_distinct".to_string(),
            data_type: ArrowType::Int32,
        }
    }
}

impl fmt::Display for CountDistinct {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "COUNT(DISTINCT {})", self.expr)
    }
}
