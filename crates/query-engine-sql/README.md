# query-engine-sql

SQL parsing layer of the query engine.

Converts raw SQL text into a logical plan through two steps: parsing and planning.
Parsing breaks SQL text into tokens and builds a syntax tree that represents the query structure.
Planning walks that syntax tree and produces the same logical plan that the DataFrame API builds by hand.
