use query_engine_datatypes::schema::Schema;

use crate::aggregate::Aggregate;
use crate::join::{Join, JoinType};
use crate::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use crate::projection::Projection;
use crate::selection::Selection;

/// DataFrame is the user-facing API for building up a logical plan tree.
/// It wraps a single LogicalPlan and each method call wraps the current plan in
/// a new plan node and returns a new DataFrame with that as the inner plan.
///
/// This means DataFrame is just a builder for a logical plan tree.
/// Nothing is executed when you call filter, project, aggregate etc.
/// You are just constructing nested plan nodes until you are ready to execute.
///
/// In the Kotlin implementation, this is split into a DataFrame interface (trait)
/// and a DataFrameImpl that implements it. Since we only ever have on implementation, we
/// skip the trait and go straight to a struct.
pub struct DataFrame {
    pub plan: Box<dyn LogicalPlan>,
}

/// Each method on the DataFrame consumes self and returns Self (DataFrame).
/// The models the fact that each method call adds a new layer to the logical plan tree -
/// the old DataFrame is gone and a new one wraps the updated plan.
/// There is no reason to keep the old DataFrame around since it has been absorbed
/// into the new plan node as its input.
impl DataFrame {
    /// Creates a new DataFrame wrapping the given logical plan.
    /// This is the entry point for starting a new DataFrame chain -
    /// every chain begins with a plan node (typically Scan) passed in here,
    /// and each subsequent method call wraps it in a new plan node.
    pub fn new(plan: Box<dyn LogicalPlan>) -> Self {
        Self { plan }
    }

    /// Wraps the current plan in a Projection node and returns a new DataFrame.
    /// The current plan becomes the input to the Projection and the given expressions
    /// define the output columns - equivalent to the SELECT clause in SQL.
    pub fn project(self, expr: Vec<Box<dyn LogicalExpr>>) -> Self {
        Self {
            plan: Box::new(Projection {
                input: self.plan, // The current plan feeds into the new Projection
                expr,             // The expressions define the output columns
            }),
        }
    }

    /// Wraps the current plan in a Selection node and returns a new DataFrame.
    /// The current plan becomes the input to the Selection and the given expression is the
    /// filter predicate - equivalent to the WHERE clause in SQL.
    /// The output schema is unchanged since filtering rows does not affect columns.
    pub fn filter(self, expr: Box<dyn LogicalExpr>) -> Self {
        Self {
            plan: Box::new(Selection {
                input: self.plan, // The current plan feeds into the new Selection
                expr,             // The boolean expression used to filter rows
            }),
        }
    }

    /// Wraps the current plan in an aggregate node and returns a new DataFrame.
    /// The current plan becomes the input to the Aggregate - equivalent to GROUP BY in SQL.
    /// group_expr defines the columns to group by
    /// aggregate_expr defines the aggregations to compute over each group
    /// Both are LogicalExpr since in our implementation aggregate expressions
    /// like Sum, Min, Max are just LogicalExpr implementors, not a separate type.
    pub fn aggregate(
        self,
        group_expr: Vec<Box<dyn LogicalExpr>>,
        aggregate_expr: Vec<Box<dyn LogicalExpr>>,
    ) -> Self {
        Self {
            plan: Box::new(Aggregate {
                input: self.plan, // The current plan feeds into the new Aggregate
                group_expr,       // Expressions defining the grouping columns
                aggregate_expr,   // Expressions defining the aggregations to compute
            }),
        }
    }

    /// Wraps the current plan and a right DataFrame's plan in a Join node and returns a new DataFrame.
    /// The current plan becomes the left input and the right DataFrame's plan becomes the right input.
    /// join_type defines the kind of join - INNER, LEFT, RIGHT, etc.
    /// on defines the column pairs to join on
    pub fn join(
        self,
        right: Box<dyn LogicalPlan>,
        join_type: JoinType,
        on: Vec<(String, String)>,
    ) -> Self {
        Self {
            plan: Box::new(Join {
                left: self.plan, // The current plan is always the left input
                right,           // The right input is passed explicitly
                join_type,       // Inner, left, right, etc.
                on,              // The column pairs defining the join condition
            }),
        }
    }

    /// Returns the schema of the final output - the columns and types that this DataFrame will produce.
    /// Since self.plan is always the outermost node, calling schema() on it gives you the schema
    /// of the end result after all transformations have been applied.
    /// For example, if the plan is a Projection(Selection(Scan)), this returns the projected columns,
    /// not all of the columns from the Scan.
    /// The DataFrame does not compute schema itself - it just delegates to the inner plan.
    pub fn schema(&self) -> Schema {
        self.plan.schema()
    }

    /// Returns the inner LogicalPlan, consuming the DataFrame in the process.
    /// This is the final call in the cnain - once you are done building up the plan
    /// tree through method chaining, you call logical_plan to extract it and hand it off to an executor.
    /// We consume self here because the DataFrame has served its purpose at this point -
    /// it was just a builder for the logical plan tree and now that the plan is extracted
    /// there is no reason to keep the DataFrame around.
    pub fn logical_plan(self) -> Box<dyn LogicalPlan> {
        self.plan
    }
}
