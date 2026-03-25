# query-engine-logical-plan

The logical plan layer of the query engine.

Defines the `LogicalPlan` trait and the plan nodes that represent a query as a tree of relations —
`Scan`, `Filter`, `Projection`, and `Join`. Each node produces rows with a known schema and exposes
its inputs through `children()`, enabling the engine to walk, validate, and optimise the plan tree
before any physical execution decisions are made.

Sits above the datasource layer and below the query optimiser.
