use std::fmt;

use query_engine_datatypes::schema::Schema;

/// A logical plan represents a data transformation or action that returns a relation
/// (a set of tuples). Plans form a tree - each node holds a reference to its children,
/// which are the input plans that feed rows into it.
/// format_inner() uses to_string() and to_string() requires Display to be implemented.
/// We need to add Display as a supertrait so that anything implementing LogicalPlan is also required to implement Display.
/// This is saying: "You can only implement LogicalPlan if you also implement Display".
/// So when the compiler sees &dyn LogicalPlan, it knows it can safely call to_string() on it.
pub trait LogicalPlan: fmt::Display {
    // Returns the schema of the data that will be produced by the logical plan.
    fn schema(&self) -> Schema;

    /// Returns the children (inputs) of this logical plan.
    /// Used to walk the plan tree - a scan returns an empty vec, a filter returns
    /// one child, a join returns two.
    /// Internal nodes and leaf nodes will both be LogicalPlan - those terms just describe their position in the tree:
    /// a Scan is a LogicalPlan that returns an empty vec (vec![])
    /// A Filter is a LogicalPlan that happens to return one child (vec![&self.input])
    /// A Join is a LogicalPlan that happens to return two children (vec![&self.left, &self.right])
    /// For example, children() on a Join returns its two input plans: left and right.
    /// Those inputs could be anything: a Scan, a Filter, another Join, whatever sits below the Join in the tree.
    /// The Join doesn't know or care what they are - it just holds references to them and hands them back when asked.
    fn children(&self) -> Vec<&dyn LogicalPlan>;
}

/// Prints the logical plan in human-readable form.
pub fn format(plan: &dyn LogicalPlan) -> String {
    // Entry point - starts the recursion at indent level 0
    format_inner(plan, 0)
}

/// The actual recursive implementation.
/// If we had a logical tree containing Projection, Filter, and Scan, the final result would look like:
/// Projection
///     Filter
///         Scan
fn format_inner(plan: &dyn LogicalPlan, indent: usize) -> String {
    let mut b = String::new();

    // Push one tab per indent level so children appear nested under their parent
    // In the very first call, indent = 0 runs zero times and produces no tabs at all
    // Once we recurse into the child and pass indent + 1, it pushes that, and so on
    for _ in 0..indent {
        b.push('\t');
    }

    // Append this node's string representation followed by a newline
    b.push_str(&plan.to_string());
    b.push('\n');

    // Recurse into each child with indent + 1, appending their formatted subtrees
    // Leaf nodes have no children so the loop does nothing and recursion terminates
    for child in plan.children() {
        b.push_str(&format_inner(child, indent + 1));
    }

    // Return the final, human readable string
    b
}
