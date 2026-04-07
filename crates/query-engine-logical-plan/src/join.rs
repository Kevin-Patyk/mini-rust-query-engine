use std::collections::HashSet;
use std::fmt::{self, Formatter};

use query_engine_datatypes::schema::Schema;

use crate::logical_plan::LogicalPlan;

/// JoinType represents the type of join to perform.
/// Inner - only rows where the join condition matches on both sides are kept.
/// Left - all rows from the left side are kept, with nulls filled in for the right side where there is no match.
/// Right - all rows from the right side are kept, with nulls filled in for the left side where there is no match.
#[derive(Debug)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// Join is the only plan node with 2 child plans - left and right.
/// It combines rows from both sides based on the join condition.
///
/// left: the left input plan.
/// right: the right input plan.
/// join_type: inner, left, or right - determines how unmatched rows are handled and which side's columns take priority
/// on: the join conditions as pairs of column names
pub struct Join {
    pub left: Box<dyn LogicalPlan>,
    pub right: Box<dyn LogicalPlan>,
    pub join_type: JoinType,
    pub on: Vec<(String, String)>,
}

impl LogicalPlan for Join {
    fn schema(&self) -> Schema {
        // Step 1 - find duplicate keys where the left and right column names are the same.
        // These columns only need to appear once in the output schema.
        let duplicate_keys: HashSet<String> = self
            .on
            .iter()
            .filter(|(l, r)| l == r)
            .map(|(l, _)| l.clone())
            .collect();

        // Step 2 - build the output fields based on the join type.
        let fields = match self.join_type {
            // Inner and Left joins keep all left fields and filter duplicates from right fields.
            // We keep all left and remove duplicates from the right fields
            JoinType::Inner | JoinType::Left => {
                let left_fields = self.left.schema().fields;
                let right_fields = self
                    .right
                    .schema()
                    .fields
                    .into_iter()
                    .filter(|f| !duplicate_keys.contains(&f.name))
                    .collect::<Vec<_>>();
                [left_fields, right_fields].concat()
            },
            // Right joins filter duplicates from left fields and keep all right fields.
            // We keep all right and remove duplicates from the left fields
            JoinType::Right => {
                let left_fields = self
                    .left
                    .schema()
                    .fields
                    .into_iter()
                    .filter(|f| !duplicate_keys.contains(&f.name))
                    .collect::<Vec<_>>();
                let right_fields = self.right.schema().fields;
                [left_fields, right_fields].concat()
            },
        };
        Schema { fields }
    }

    /// children() returns both the left and right input plans.
    /// Join is the only plan node with 2 children - every other node has either zero
    /// children (Scan) or one child (Selection, Projection, Aggregate).
    /// The tree walker in format_inner() will recurse into both children, printing the left subtree
    /// and then the right subtree indented under the Join node.
    /// as_ref() converts Box<dyn LogicalPlan> into &dyn LogicalPlan since children()
    /// returns references not owned values.
    fn children(&self) -> Vec<&dyn LogicalPlan> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }
}

// Display is required because LogicalPlan has Display as a supertrait.
// Prints the join type and the on conditions as pairs of column names.
impl fmt::Display for Join {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Join: type={:?}, on={:?}", self.join_type, self.on)
    }
}
