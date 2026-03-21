# query-engine-datatypes

The type system and core data structures for the query engine.

Defines the data types the engine can work with (`ArrowType`), the schema system (`Field`, `Schema`),
column abstractions (`ColumnVector`, `ArrowFieldVector`, `LiteralValueVector`), and the `RecordBatch`
which is the fundamental unit of data flowing through the engine at runtime.

Built on top of Apache Arrow as the underlying in-memory data representation.
