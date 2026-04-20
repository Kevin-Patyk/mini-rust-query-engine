use crate::sql_expr::SqlExpr;

/// This file defines the PrattParser trait - the interface that any Pratt parser must implement.
/// It does not contain any parsing logic itself. The actual parsing logic lives in SqlParser, which implements this trait.
///
/// A Pratt parser works by assigning a precedence value to each token, then using that precedence to decide
/// how tightly tokens bind to each other. This is what allows 1 + 2 * 3 to correctly parse as 1 + (2 * 3) because * has a
/// higher precedence than +.
///
/// The algorithm has two kinds of expressions:
/// 1. Prefix - a token that appears at the start of an expression - a number, identifier, parenthese (
/// 2. Infix - a token that appears in the middle of an expression - +, *, AND, OR
///
/// Parsing is the process by which a compiler turns a sequence of tokens into a tree representation
/// The parser turns a flat string into a tree that captures the structure and hierarchy of the query.

pub trait PrattParser {
    // Return precedence of the next token in the stream.
    // Higher numbers bind more tightly - * (60) binds tighter than + (50)
    // Returns 0 if there is no text token or the token has no precedence,
    // which signals for the parser to stop.
    fn next_precedence(&self) -> i32;

    // Parses a prefix expression - a token that appears at the start of an expression, such as 42, salary, (
    // Returns None if there are no more tokens to parse.
    fn parse_prefix(&mut self) -> Option<SqlExpr>;

    // Parses an infix expression - a token that appears between two expressions, such as + between a + b, > between age > 30, etc.
    // Takes the left-hand expression that was already parsed and the current precedence
    // level so it knows how far to keep parsing on the right-hand side.
    fn parse_infix(&mut self, left: SqlExpr, precedence: i32) -> SqlExpr;

    // The core Pratt algorithm. This is the only method with a default implementation -
    // SqlParser gets this for free by implementing the three methods above.
    //
    // It works like this:
    // 1. Parse a prefix expression to get the initial left-hand side
    // 2. Loop: peek at the next token's precedence
    // 3. If the next token binds tighter than our current precedence, consume it as infix
    // 4. Keep looping until the next token binds less tightly, then retuen what we have
    //
    // The precedence argument is the minimum binding power - we only consume tokens
    // that bind MORE tightly than this. This is how recursive calls control how far they parse:
    // a + parse will stop at another + but keep going through *.

    // A trait can provide a default method body that implementors get for free without having to write it themselves.
    // If they want a different behavior, they can override it, but if they don't, the default just works.
    // We don't need to define it when implementing the trait unless we want to override it.
    fn parse(&mut self, precedence: i32) -> Option<SqlExpr> {
        // Try to parse a prefix expression - if there's nothing there, return None
        // The ? operator on Option unwraps the option if it's Some or
        // returns early with None if it's None
        // It is shorthand for this:
        // let mut expr = match self.parse_prefix() {
        //     Some(val) => val,
        //     None => return None,
        // };
        let mut expr = self.parse_prefix()?;

        // Keep consuming infix tokens as long as they bind tight than our current precedence
        while precedence < self.next_precedence() {
            let next_precedence = self.next_precedence();
            // Pass the current left-hand side and precedence into parse_infix,
            // which will consume the operator and parse the right-hand side
            expr = self.parse_infix(expr, next_precedence);
        }

        Some(expr)
    }
}
