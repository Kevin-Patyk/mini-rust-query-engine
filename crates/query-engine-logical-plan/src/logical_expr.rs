use std::fmt;

use query_engine_datatypes::schema::Field;

use crate::logical_plan::LogicalPlan;

/// LogicalExpr is the expression equivalent of LogicalPlan - it's the trait that every expression node must implement
/// It will return a Field, which is from our datatypes crate and contains a name and data type
/// This is answering the question: "if I evaluate this expression against rows from this plan, what will the result look like?"
/// For example:
/// a ColumnReference("salary") would look up salary in the input plan's schema and return its field - name "salary", type Float64
/// a Literal(0.1) would return a field with some generated name and type Float64
/// a Multiply expression would look at its 2 children's fields and return a field with type Float64
/// This is what makes validation possible during planning
/// Before any data is touched, the engine can call to_field on every expression to check that column references actually exist,
/// that types are compatible, and figure out what the output schema will look like.
/// Since the input takes LogicalPlan and now a row of data, this is purely a planning-time operation, not just an execution-time one.
/// The actual computation happens later.
/// Right now, we are just reasoning about what the expression would produce, not computing it.
pub trait LogicalExpr: fmt::Display {
    fn to_field(&self, input: &dyn LogicalPlan) -> Field;
}

// For LogicalExpr, we are also using Display as a super trait.
// A supertrait can be thought of as a requirement.
// When we write trait LogicalExpr: fmt::Display, we are saying
// "you can only implement LogicalExpr if you have already implemented Display"
// So when the compiler sees dyn LogicalExpr, it knows two things are guaranteed to be there:
// 1. to_field()
// 2. The ability to format with {}
// In short, you cannot implement LogicalExpr without also implementing Display
// The payoff is that anywhere you have dyn LogicalExpr, the compiler knows
// Display is guaranteed to be there too, so you can saely use {} formatting on it