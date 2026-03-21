# query-engine-datasources

Data source connectors for the query engine.

Provides implementations for reading data from different sources into `RecordBatch`es
that the engine can process. Supports in-memory data, CSV files, and Parquet files.

Depends on `query-engine-datatypes` for core data structures and Apache Arrow for
the underlying data representation.
