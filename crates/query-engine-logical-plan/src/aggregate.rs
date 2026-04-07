use std::fmt::{self, Formatter};

use query_engine_datatypes::schema::Schema;

use crate::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;

/// Aggregate holds an input plan and two lists of expressions
/// group_expr defines how rows are grouped
/// aggregate_expr defines how each group is collapsed into a single value
///
/// Both are Vec<Box<dyn LogicalExpr>> because there is no separate AggregateExpr like in the Kotlin code
pub struct Aggregate {
    pub input: Box<dyn LogicalPlan>,
    pub group_expr: Vec<Box<dyn LogicalExpr>>,
    pub aggregate_expr: Vec<Box<dyn LogicalExpr>>,
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Schema {
        // chain() takes two iterators and joins them end-to-end into a single iterator without allocating a new collection
        // The first iterator produces an iterator of Fields from the group expressions
        // The second produces an iterator of Fields from the aggregate expressions
        // chain() stitches them together so when you call collect() at the end, you get one single Vec<Field> with the group fields
        // followed by the aggregate fields
        let fields = self
            .group_expr
            .iter()
            .map(|expr| expr.to_field(self.input.as_ref()))
            .chain(
                self.aggregate_expr
                    .iter()
                    .map(|expr| expr.to_field(self.input.as_ref())),
            )
            .collect();
        Schema { fields }
    }

    /// children() returns the single input plan that feeds into this Aggregate.
    /// Aggregate has exactly one child - the plan whose rows get grouped and collapsed into aggregate values.
    /// The recursion for walking the tree lives in format_inner() not here - each node just reports its immediate children.
    /// as_ref() converts Box<dyn LogicalPlan> into &dyn LogicalPlan since children() returns references not owned values.
    fn children(&self) -> Vec<&dyn LogicalPlan> {
        vec![self.input.as_ref()]
    }
}

impl fmt::Display for Aggregate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let group_str = self
            .group_expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let aggregate_str = self
            .aggregate_expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "Aggregate: groupExpr=[{}], aggregateExpr=[{}]",
            group_str, aggregate_str
        )
    }
}
