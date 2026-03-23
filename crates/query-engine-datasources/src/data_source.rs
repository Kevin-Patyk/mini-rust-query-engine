use query_engine_datatypes::record_batch::RecordBatch;
use query_engine_datatypes::schema::Schema;

// A trait defines a shared interface that different types can implement.
// It describes what a type can do without specifying how.
// It is a shared interface that different types can implement using dynamic dispatch, meaning the concrete type is
// resolved at runtime rather than compile time.

/// DataSource is a trait defining the interface for reading data into the query engine.
/// Any data source (CSV files, Parquet files, in-memory, etc.) implements this trait,
/// allowing the rest of the engine to read data without caring about the underlying storage format.
pub trait DataSource {
    /// Returns the schema of the underlying data source.
    /// Called during query planning so the engine knows what columns and types are available.
    fn schema(&self) -> Schema;

    /// Scans the data source, returning only the columns specified by name.
    /// Projection pushdown - only reading the columns we actually need - is a key
    /// optimization, especially for wider tables or column formats like Parquet.
    /// Returns an iterator of RecordBatches rather than all data at once, enabling
    /// streaming so the engine never has to hold the entire dataset in memory.
    /// Box<dyn Iterator<Item = RecordBatch>> means any type that implements the Iterator trait and yields RecordBatch values.
    /// We will have different types of iterators (CSV, Parquet, in-memory).
    /// Each one will return a different concrete iterator type, so the trait can't commit to any single one at compile time.
    /// Our return type is the only way to express "I don't know which iterator this will be at runtime, but I guarantee it yields RecordBatch."
    fn scan(&self, projection: Vec<String>) -> Box<dyn Iterator<Item = RecordBatch>>;
    // We are using Box because it gives the caller sole ownership of the iterator without needing to know its concrete type at compile time.
    // Since every data source (CSV, Parquet, in-memory) will return a different iterator struct, we need the indirection to return them all under a single trait object.
    // In other words, we use Box because the compiler needs a fixed-size pointer to put on the stack and dyn Iterator alone has no known size at compile time.
}
