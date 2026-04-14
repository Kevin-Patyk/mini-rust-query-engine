use crate::expressions::*;
use crate::logical_expr::LogicalExpr;

/// Crates a Column expression from a string slice.
/// Takes &str instead of String so callers can write col("state") directly
/// rather than col("state".to_string()) - the conversion to String happens 
/// inside the function so the caller doesn't have to think about it.
pub fn col(name: &str) -> Column {
    Column {
        name: name.to_string(),
    }
}

/// Creates a LiteralString expression from a string slice.
/// Takes &str so callers can write lit_str("CO") directly.
/// rather than lit_str("CO".to_string())
pub fn lit_str(value: &str) -> LiteralString {
    LiteralString { 
        value: value.to_string()
    }
}

/// Creates a LiteralBoolean expression from a bool.
pub fn lit_bool(value: bool) -> LiteralBoolean {
    LiteralBoolean { value }
}

/// Creates a LiteralLong expression from an i64.
pub fn lit_i64(value: i64) -> LiteralLong {
    LiteralLong { value }
}

/// Creates a LiteralDouble expression from an f64.
pub fn lit_f64(value: f64) -> LiteralDouble {
    LiteralDouble { value }
}

// Creates a LiteralFloat expression from an f32.
pub fn lit_f32(value: f32) -> LiteralFloat {
    LiteralFloat { value }
}

// Note: A single lit() function is possible in Rust using a trait with multiple implementations.
// You would define a literal trait with a to_expr() method, implement it for each type (&str, bool, i64, etc.),
// and then lit() would take impl Literal and call to_expr() on it.
// We use separate lit_str, lit_i64, etc. here for clarity since this is a learning project.


/// An extension trait adds methods to an existing trait without modifying it.
/// 
/// The pattern is:
/// 1. Define a new trait with the methods you want to add
/// 2. Implement it for any type that already implements the original trait
/// 
/// For example, we can't add methods directly to LogicalExpr because it's defined in another module. But
/// we can define LogicalExprExt and implement it for any T that implements LogicalExpr - then those methods
/// are available on every expression type automatically. 
/// 
/// This is how Rust handles what other languages do with extension methods. 
/// 
/// A blanket implementation  looks like:
/// impl<T: LogicalExpr> LogicalExprExt for T { ... }
/// 
/// This reads as: "for any type T that implements LogicalExpr,
/// also implement LogicalExprExt." So Column, Eq, LiteralString -
/// every expression type gets these methods for free without having to implement the trait individually.

/// LogicalExprExt is the name of our new extension trait
/// : LogicalExpr is a supertrait bound, meaning only types that already implement LogicalExpr can implement this trait
pub trait LogicalExprExt: LogicalExpr {
    fn eq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr>;
    fn neq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr>;
    fn gt(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr>;
    fn gt_eq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr>;
    fn lt(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr>;
    fn lt_eq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr>;
    fn mult(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr>;
    fn alias(self: Box<Self>, name: &str) -> Box<dyn LogicalExpr>;
}

/// Blanket implementation of LogicalExprExt for every type T that implements LogicalExpr.
/// This means Column, Eq, LiteralString, Sum - every expression type - automatically 
/// gets all 8 methods without needing a separate impl block for each one.
/// Box<dyn LogicalExpr> is also covered, which is what enables chaining -
/// the return value of every method already has these methods available on it.
/// Box<dyn LogicalExpr> is covered since it itself implements LogicalExpr.
/// 
/// The 'static bound is required because Box<dyn LogicalExpr> implicitly requires
/// 'static - meaning the type inside cannot hold any borrowed references.
/// Our expression types don't hold any references so this is fine.
impl<T: LogicalExpr + 'static> LogicalExprExt for T {
    fn eq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr> {
        Box::new(Eq { l: self, r: rhs })
    }
    fn neq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr> {
        Box::new(Neq { l: self, r: rhs })
    }
    fn gt(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr> {
        Box::new(Gt { l: self, r: rhs })
    }
    fn gt_eq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr> {
        Box::new(GtEq { l: self, r: rhs })
    }
    fn lt(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr> {
        Box::new(Lt { l: self, r: rhs })
    }
    fn lt_eq(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr> {
        Box::new(LtEq { l: self, r: rhs })
    }
    fn mult(self: Box<Self>, rhs: Box<dyn LogicalExpr>) -> Box<dyn LogicalExpr> {
        Box::new(Multiply { l: self, r: rhs })
    }
    fn alias(self: Box<Self>, name: &str) -> Box<dyn LogicalExpr> {
        Box::new(Alias { expr: self, alias: name.to_string() })
    }
}

// Each method takes a Box<dyn LogicalExpr> as the right hand side and returns a Box<dyn LogicalExpr>.
// This mirrors the expression tree structure — expressions feed into each other as inputs and outputs,
// building the tree node by node through chaining.
//
// For example:
// col("salary").mult(lit_f64(0.1)).alias("bonus")
//
// Builds this tree:
// Alias
//   └── Multiply
//         ├── Column("salary")
//         └── LiteralFloat(0.1)
//
// Each method call adds one more node to the tree, with the previous result
// becoming the child of the new node. The chain reads left to right instead
// of inside out like manual struct construction would.

// Expressions live inside logical plan nodes - the two trees work together.
//
// The logical plan tree describes the data flow:
// Scan -> Selection -> Projection
//
// The expression tree lives inside individual plan nodes and describes the computations at each step:
// Selection holds an Eq which holds a Column and a LiteralString
// Projection holds a list of expressions like Multiply and Alias
//
// The plan tree is the skeleton of the query.
// The expressions are the logic that runs at each bone.
