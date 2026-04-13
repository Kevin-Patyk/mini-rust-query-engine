use std::fmt::{self, Formatter};

use query_engine_datatypes::schema::Schema;

use crate::logical_plan::LogicalPlan;

/// Logical plan node representing a LIMIT clause.
/// Wraps an input plan and caps the number of rows it produces.
/// The schema is unchanged - limiting rows does not affect columns.
pub struct Limit {
    pub input: Box<dyn LogicalPlan>,
    pub limit: usize,
}

impl LogicalPlan for Limit {
    /// Returns the input plan's schema unchanged.
    /// Limit only reduces the number of rows - it never adds, removes, or transforms.
    /// For nodes that don't change schema, we only rely on the input nodes schema.
    /// If a node doesn't transform columns - it just filters, limits, or reorders rows - 
    /// it has nothing to compute itself.
    /// It just asks its input "What columns do you produce?" and passes that straight through
    /// The nodes that actually need to compute their own schema are the ones that change what columns exist:
    /// Projection, Aggregate, Join, etc.
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    /// Returns the single input plan that feeds rows into this Limit node.
    fn children(&self) -> Vec<&dyn LogicalPlan> {
        vec![self.input.as_ref()]
    }
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Limit: {}", self.limit)
    }
}
