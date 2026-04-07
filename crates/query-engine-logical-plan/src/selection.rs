use std::fmt::{self, Formatter};

use query_engine_datatypes::schema::Schema;

use crate::logical_expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;

/// Selection holds its input plan and filter expression as Box<dyn T> rather than Arc<dyn T>.
/// Box means sole ownership - Selection owns its input and expression outright and nothing else
/// needs to share them. Arc is for shared ownership across multiple owners or threads, which
/// we don't need here.
/// Both fields use dyn because the concrete type is not known at compile time -
/// input can be a Scan, another Selection, a Projection, etc.
/// expr could be a BinaryExpr, a Column, or any other LogicalExpr.
/// dyn + Box gives us runtime flexibility without the overhead of reference counting.
pub struct Selection {
    // With Box there is zero overhead - it's just a pointer; when the owner is dropped, the value is dropped, done.
    // With Arc there is a reference count stored alongside the data; the incrementing has to be atomic (thread safe)
    // which adds CPU overhead compared to a plain pointer.
    pub input: Box<dyn LogicalPlan>,
    pub expr: Box<dyn LogicalExpr>,
}

impl LogicalPlan for Selection {
    /// schema() just delegates to the input plan's schema.
    /// Filtering rows never changes the columns - if the input has name, age, and salary,
    /// the output after filtering still has name, age, salary, just potentially fewer rows.
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    /// children() returns the single input plan that feeds into this Selection.
    /// Each node only reports its immediate children - no recursion happens here.
    /// The recursion lives in format_inner() in logical_plan.rs, which calls children
    /// on each node and then calls itself on each child to walk the entire tree.
    /// as_ref() converts Box<dyn LogicalPlan> into &dyn LogicalPlan since children()
    /// returns references, not owned values.
    fn children(&self) -> Vec<&dyn LogicalPlan> {
        vec![self.input.as_ref()]
    }
}

impl fmt::Display for Selection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Selection: {}", self.expr)
    }
}
