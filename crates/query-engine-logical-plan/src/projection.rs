use std::fmt::{self, Formatter};

use query_engine_datatypes::schema::Schema;

use crate::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;

/// Projection holds an input plan and a list of expressions that define the output columns.
/// Unlike Selection, which holds a single expression, Projection holds a Vec of expressions
/// because SELECT can produce multiple output columns - each one described by its own expression.
/// For example, SELECT name, age, salary * 1.1 produces three output columns,
/// each represented by a separate expression in the Vec.
pub struct Projection {
    // Since dyn LogicalPlan has no known size at compile time, we need Box
    // Box is always a fixed-size pointer regardless of what it points to
    // The actual plan node lives on the heap and the Box is just a pointer to it
    pub input: Box<dyn LogicalPlan>,
    pub expr: Vec<Box<dyn LogicalExpr>>,
}

impl LogicalPlan for Projection {
    /// schema() builds the output schema for this Projection by calling to_field() on each expression.
    /// This is the only place in the logical plan layer where expressions do real work -
    /// every other node either passes the schema through unchanged (Selection) or derives from the
    /// data source (Scan).
    ///
    /// to_field() is called with the input plan so each expression can resolve column types.
    /// For example, Column("name") looks up "name" in the input schema to find its type.
    /// BinaryExpr uses its operands' types to determine its own output type.
    /// The result of each to_field() call is a Field describing the output column's name and type.
    ///
    /// All the fields are collected into a Schema, which becomes the output schema of this Projection.
    /// This is why to_field() had to exist on every expression before we could build plan nodes -
    /// Projection cannot derive its schema without it.
    ///
    /// Each plan node's schema() method works by asking expressions "What will you produce?" via to_field(), then
    /// assembling those fields into a Schema to return.
    /// Each node will do this slightly differently.
    /// But schema() on a plan node is really just "ask my expressions what they produce and that's my output schema."
    /// The expressions do the type inference via to_field() and schema() assembles the result.
    fn schema(&self) -> Schema {
        let fields = self
            .expr
            .iter()
            .map(|expr| expr.to_field(self.input.as_ref()))
            .collect();
        Schema { fields }
    }

    /// children() returns the single input plan that feeds into this Projection.
    /// Projection has exactly only child - the plan whose output rows get transformed
    /// by the expressions. as_ref() converts Box<dyn LogicalPlan> into &dyn LogicalPlan
    /// since children() returns references, not owned values.
    /// The recursion for walking the tree does not live here.
    /// Each node just reports its immediate children.
    fn children(&self) -> Vec<&dyn LogicalPlan> {
        vec![self.input.as_ref()]
    }
}

impl fmt::Display for Projection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // collect() needs to know what type to collect into.
        // Normally Rust can infer this from context, but here we are immediately calling .join()
        // on the result and join() is defined on [String] not a generic iterator,
        // so the compiler cannot infer the type on its own.
        // The turbofish syntax ::<Vec<_>> tells the compiler explicitly what type to collect into.
        // It is called tubofish because ::<> looks like a fish swimming
        // The _ inside Vec<_> lets the compiler infer the element type (String) on its own
        // so we only need to specify the outer container.
        // We could write Vec<String> but Vec<_> lets the compiler infer the element type so you
        // don't have to spell it out. Both are correct
        let expr_str = self
            .expr
            .iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Projection: {}", expr_str)
    }
}
