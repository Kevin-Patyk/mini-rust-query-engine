/// This file defines the AST (Abstract Syntax Tree) node types for SQl.
/// When the parser reads a SQL string like "SELECT a + b FROM employees WHERE age > 30",
/// it builds a tree of these types to represent the meaning of a query.
/// This tree sits between the tokenizer (raw text -> tokens) and the logical planner
/// (AST -> logical plan nodes like Scan, Filter, Projection).
/// These expressions are the return types of the parser's functions.
///
/// These expressions are structured text.
/// The tokenizer broke the raw string into labeled chunks and the parser arranges those chunks
/// in a tree that reflects the structure of what the user wrote.
/// The full pipeline is:
/// raw SQL string -> tokenizer -> tokens -> parser -> SqlExpr tree -> planner -> logical plan
///
/// SqlExpr is not the same as the LogicalExpr types in the logical plan later.
/// SqlExpr represents what the user wrote - pure syntax, no semantics.
/// LogicalExpr represents what the query means - it knows about data types, columns, and schema inference via to_field().
/// The planner bridges the two, walking the SqlExpr tree and converting each node into the
/// corresponding LogicalExpr.
///
/// These SqlExpr will be used by the parser to parse text into something that can be converted into a LogicalExpr at
/// a later step.

// This enum represents every possible node in a SQL AST.
// The parser produces these variants as it reads through the token stream.
#[derive(Debug, Clone)]
pub enum SqlExpr {
    // A simple name - a column reference like salary or a table name like employees
    Identifier(String),

    // Two expressions joined by an operator - a + b, age > 30, x AND y
    // Boxed because SqlExpression contains itself, which would be infinite size without indirection
    // BinaryExpr holds two SqlExpr children. If there were stored directly (not boxed),
    // the compiler would try to compute the size of SqlExpr, which contains SqlExpr,
    // which contains SqlExpr - infinite recursion at the type level. Box<SqlExpr> breaks
    // the cycle by storing a fixed-size pointer (8 bytes) on the stack, with actual data living on the heap.
    BinaryExpr {
        l: Box<SqlExpr>,
        op: String,
        r: Box<SqlExpr>,
    },

    // A literal string value - 'hello'
    String(String),

    // A literal integer value - 42
    Long(i64),

    // A literal floating point value - 3.14
    Double(f64),

    // A literal interval value - INTERVAL 1 year
    Interval(String),

    // A function call with a name and argument list - COUNT(*), SUM(Salary)
    // Args are boxed in a Vec since each arg is itself a SqlExpr
    Function {
        name: String,
        args: Vec<SqlExpr>,
    },

    // An expression with an alias - salary * 1.1 AS adjusted
    Alias {
        expr: Box<SqlExpr>,
        alias: String,
    },

    // A CAST expression - CAST(salary AS DOUBLE)
    Cast {
        expr: Box<SqlExpr>,
        data_type: String,
    },

    // A sort expression with a direction - salary ASC, age DESC
    Sort {
        expr: Box<SqlExpr>,
        asc: bool,
    },

    // A full SELECT statement - holds all the parsed clauses
    // This is the root node the parser produces for a complete query
    Select {
        // The list of expressions in the SELECT clause - SELECT a, b, salary * 1.1
        projection: Vec<SqlExpr>,
        // The optional WHERE clause expression - WHERE age > 30
        selection: Option<Box<SqlExpr>>,
        // The optional GROUP BY expressions - GROUP BY department
        group_by: Vec<SqlExpr>,
        // The optional ORDER BY expressions - ORDER BY salary DESC
        order_by: Vec<SqlExpr>,
        // The optional HAVING clause - HAVING COUNT(*) > 5
        having: Option<Box<SqlExpr>>,
        // The optional LIMIT - LIMIT 100
        limit: Option<i64>,
        // The table name from the FROM clause - FROM employees
        table_name: String,
    },
}
