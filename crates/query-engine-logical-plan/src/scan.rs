use std::sync::Arc;
use std::fmt::{self, Formatter};

use query_engine_datasources::data_source::DataSource;
use query_engine_datatypes::schema::Schema;
use crate::logical_plan::LogicalPlan;

/// Scan is the leaf node of every logical plan tree. It represents reading data from a data source 
/// (CSV, Parquet, in-memory) and is the point where data enters the query engine.
/// Every query tree bottoms out at one or more Scan nodes.
/// 
/// The rest of the plan tree (Filter, Projection, Aggregate, Join) needs data to work with.
/// Scan is what supplies that data. It holds a reference to a DataSource and describes which columns
/// to read via the projection. No other plan node knows or cares about the underlying storage format - that is entirely Scan's responsbility.
/// 
/// schema() returns the output schema of this Scan - either the full schema from the data source if no projection was specified
/// or a narrowed schema containing only the requested columns. This is called at planning time so upstream nodes know what columns are available
/// to them. No data is read here.
/// 
/// children() returns an empty Vec because Scan has no inputs - it is the bottom of the tree.
/// Other nodes return their child plans here so the tree can be walked recursively.
/// Scan has nothing below it so there is nothing to return.-
/// 
/// The entire logical plan layer is just a description - it's a blueprint of what the query will do. No data is touched anywhere
/// in this layer. When you call schema() on Scan, it calls datasource.schema() which just returns the column names and types - that's metadata, not data.
/// The actual datasource.scan() that opens the file and reads rows happens during the physical plan layer.
/// The whole logical plan tree is just a data structure sitting in memory describing the same of the query
/// It answers: what columns will this produce? what transformations will happen? without doing any actual work yet.
/// 
/// In the logical plan, expressions are just carried around as data. A Filter node holds a BinaryExpr but doesn't call anything on it
/// it just stores and uses it to answer "what is my output schema?" A Projection node holds a list of expressions but only calls
/// to_field() on them to derive its schema.
/// The expressions only actually run in the physical plan over RecordBathes.
/// In the logical plan, expressions serve one purpose - describing the output schema via to_field. That's it.

pub struct Scan {
    pub path: String,
    pub datasource: Arc<dyn DataSource>,
    pub projection: Vec<String>,
}

/// Implementing the LogicalPlan trait for Scan
impl LogicalPlan for Scan {
    /// schema() derives the output schema for this Scan node.
    /// If no projection was specified, the full schema from the data source is returned
    /// If a projection was specified, we narrow the schema down to only the requested columns
    /// using select(). This is planning-time only - no data is read here.
    /// The schema tells the rest of the plan tree what columns this Scan will produce,
    /// so upstream nodes like Filter and Projection can validate their expressions against it
    fn schema(&self) -> Schema {
        if self.projection.is_empty() {
            self.datasource.schema()
        } else {
            let projection_refs: Vec<&str> = self.projection.iter().map(|name| name.as_str()).collect();
            // Vec<T> can coerce to &[T] automatically in Rust - deref coercion
            // So &Vec<&str> becomes &[&str] because Vec implements the Deref trait
            self.datasource.schema().select(&projection_refs).unwrap()
        }
    }

    /// children() returns an empty vec because Scan is a leaf node.
    /// It has no child plans - it is the bottom of the plan tree, the point where data enters.
    /// Filter, Projection, and other nodes return their child plans here so the tree can be walked.
    /// Scan has nothing below it, so there is nothing to return.
    fn children(&self) -> Vec<&dyn LogicalPlan> {
        vec![]
    }
}

/// Display is required because LogicalPlan has Display as a supertrait.
/// Anything implementing LogicalPlan must also implement Display so the plan tree can be printed.
impl fmt::Display for Scan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.projection.is_empty() {
            write!(f, "Scan: {}; projection=None", self.path)
        } else {
            write!(f, "Scan: {}; projection={:?}", self.path, self.projection)
        }
    }
}
