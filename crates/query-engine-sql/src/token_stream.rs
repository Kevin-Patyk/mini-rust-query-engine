use core::fmt;

use crate::tokens::*;

/// TokenStream wraps the list of tokens produced by the tokenizer and gives
/// the parser a clean interface to walk through them one at a time.
/// It works like a cursor - i tracks the current position in the token list,
/// and the parser moves it forward as it consumes tokens
pub struct TokenStream {
    /// The list of tokens produced by the tokenizer
    tokens: Vec<Token>,
    /// The current position in the token list. Starts at 0 and advances
    /// as the tokens are consumed. Same ideas as offset in the SqlTokenizer but
    /// for tokens instead of characters.
    i: usize,
}

impl TokenStream {
    /// Creates a new TokenStream from a list of tokens.
    /// Calls by SqlTokenizer::tokenize() once all tokens have been collected.
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            // Start at the beginning of the token list
            i: 0,
        }
    }

    /// Looks at the current token without consuming it or advancing the cursor.
    /// Returns None if we have reached the end of the token list.
    /// The parser uses this to make decisions about what you want to do next
    /// without committing to consuming the token yet.
    /// In this context, "consuming" means advancing self.i forward by 1.
    /// When self.i moves past a token, that token will never be seen again by peek() or next() - it's
    /// behind the cursor. "Consuming" doesn't mean deleting the token, it just means the cursor has moved past it
    /// and the parser will never look at it again.
    pub fn peek(&self) -> Option<&Token> {
        // If i is within bounds, return a reference to the current token
        // If i is past the end of the list, return None
        if self.i < self.tokens.len() {
            // If the cursor is less than the length of the token list,
            // We return that index in self.tokens
            Some(&self.tokens[self.i])
        } else {
            None
        }
    }

    /// Returns the current token and advances the cursor by 1.
    /// Returns None if we have reached the end of the token list.
    /// Unlike peek(), this consumes the token - the cursor moves forward
    /// so the next call to advance() or peek() will see the following token.
    pub fn advance(&mut self) -> Option<&Token> {
        // We are using &mut self here because we are changing self.i, so it needs to be mutable

        // If i is within bounds, return the current token and advance the cursor
        if self.i < self.tokens.len() {
            // If the cursor is less than the length of the token list,
            // we return that index and advance the cursor one position
            // Get a reference to the current token
            let token = &self.tokens[self.i];
            self.i += 1;
            Some(token)
        } else {
            None
        }
    }

    /// Checks if the current token is a specific keyword and, if so, consumes it.
    /// Returns true if the keyword matched and the cursor was advanced.
    /// Returns false if the current token is not that keyword, leaving the cursor unchanged.
    /// The parser uses this to check for expected keywords like SELECT and FROM.
    pub fn consume_keyword(&mut self, s: &str) -> bool {
        // Peek at the current token without consuming it yet
        let peek = self.peek();

        match peek {
            // Check if the current token is a keyword and if its text matches
            // the keyword we are looking for, ignoring case so "select" matches "SELECT"
            Some(token)
                if matches!(token.token_type, TokenType::Keyword(_))
                    && token.text.eq_ignore_ascii_case(s) =>
            {
                // eq_ignore_ascii_case is a string comparison method that ignores case
                // so "SELECT" matches "select" or "Select"
                // It only does case-insensitive comparison for ASCII characters, which is fine for SQL keywords since
                // they are all plain ASCII letters.
                // "If the token is a keyword and is the token text equal to our input text"

                // It matched - advance the cursor and return true
                self.i += 1;
                true
            },
            // Either there is no token (None) or it did not match, leave the cursor unchanged
            _ => false,
        }
    }

    /// Checks if the next tokens match a sequence of keywords and if so consumes them all.
    /// Used for multi-word keywords like GROUP BY or ORDER BY.
    /// If any keyword in the sequence does not match, the cursor is reset to where it was
    /// before we started, so we never partially consume a sequence.
    pub fn consume_keywords(&mut self, keywords: &[&str]) -> bool {
        // Save the current cursor position so we can roll back if needed
        let save = self.i;

        for keyword in keywords {
            // Try to consume each keyword in order
            // If any one fails, reset the cursor and return false
            if !self.consume_keyword(keyword) {
                // Roll back to where we started - we don't want to partially
                // consume GROUP without the BY for example
                self.i = save;
                return false;
            }
        }

        // All keywords matched and were consumed
        true
    }

    /// Checks if the current token matches a specific token type and if so consumes it.
    /// Similar to consume_keyword but matches on the token type rather than the text.
    /// For example, checking if the current token is a Literal(Identifier) or Symbol(Plus).
    pub fn consume_token_type(&mut self, t: &TokenType) -> bool {
        // Peek at the current token without consuming it yet
        let peek = self.peek();

        match peek {
            // If the current token's type matches what we are looking for, consume it
            Some(token) if &token.token_type == t => {
                self.i += 1;
                true
            },
            // No match or no token - leave the cursor unchanged
            _ => false,
        }
    }
}

/// Display implementation for TokenStream - prints the pull token list
/// with a * marking the current cursor position
/// Useful for debugging to see where the parser is up to.
/// For example Token("SELECT", Keyword(Select), 6) *Token("a", Literal(Identifier), 8) ...
impl fmt::Display for TokenStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Walk through every token, adding a * prefix to the one at the current cursor position
        let result = self
            .tokens
            .iter()
            .enumerate()
            .map(|(index, token)| {
                // If this token is at the current cursor position, mark it with *
                if index == self.i {
                    format!("*{}", token)
                } else {
                    format!("{}", token)
                }
            })
            .collect::<Vec<String>>()
            .join(" ");
        // .enumerate() wraps each item in the iterator with its index, giving us (index, token)
        // pairs so we can see if the current token is at the cursor position

        // .collect()::Vec<String>>() gathers all the mappred strings into a Vec<String>.
        // The ::<Vec<String>> part is called the turbofish syntax - it tells the compiler
        // exactly what type to collect into. Without it, the compiler might not know whether we want
        // a Vec, a HashSet, or something else entirely. It looks like a fish ::<> which is where the name comes from

        // .join(" ") then takes the Vec<String> and joins all the strings together into one string with a space
        // between each token.

        write!(f, "{}", result)
    }
}

// If we wanted to implement the iterator trait for our TokenStream struct, it would look like:

// impl Iterator for TokenStream {
//     type Item = Token;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.i < self.tokens.len() {
//             let token = self.tokens[self.i].clone();
//             self.i += 1;
//             Some(token)
//         } else {
//             None
//         }
//     }
// }

// This would give us all of Rust's iterator machinery for free - for loops, .map(),
// .filter(), .collect() etc. However for a parser, peek() is critical and the standard iterator trait
// does not include it. You would need to wrap the iterator in std::iter::Peekable to get peek() back, which adds
// unnecessary complexity.
// Our advance() method does the same job as Iterator::next() but keeps things simple and avoids the naming conflict
// Clippy warned us about earlier.

// A cursor is a common pattern in programming for tracking your current position when 
// reading through a sequence of items one at a time.
// You use a cursor when:
// - You need to read items in order but not all at once
// - You need to peek at the current item before deciding what to do with it
// - You need to be able to roll back to a previous position if something doesn't match
//
// In our case we use a cursor twice:
// - offset in SqlTokenizer tracks position in the raw SQL string (character level)
// - i in TokenStream tracks position in the token list (token level)
//
// The pattern is always the same - start at 0, advance forward as you consume items
// never go backwards except to roll back on a failed match. It's a simple but powerful way
// to process any sequential data without loading everything into memory at once. 

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a basic token stream for testing
    fn make_token_stream() -> TokenStream {
        TokenStream::new(vec![
            Token::new("SELECT".to_string(), TokenType::Keyword(Keyword::Select), 6),
            Token::new("a".to_string(), TokenType::Literal(Literal::Identifier), 8),
            Token::new("+".to_string(), TokenType::Symbol(Symbol::Plus), 10),
            Token::new("b".to_string(), TokenType::Literal(Literal::Identifier), 12),
            Token::new("FROM".to_string(), TokenType::Keyword(Keyword::From), 17),
            Token::new("c".to_string(), TokenType::Literal(Literal::Identifier), 19),
        ])
    }

    #[test]
    fn test_peek_does_not_advance_cursor() {
        // peek() should return the current token without moving the cursor
        let stream = make_token_stream();
        let first_peek = stream.peek().unwrap().text.clone();
        let second_peek = stream.peek().unwrap().text.clone();
        // Both peeks should return the same token since cursor did not move
        assert_eq!(first_peek, "SELECT");
        assert_eq!(second_peek, "SELECT");
    }

    #[test]
    fn test_next_advances_cursor() {
        // advance() should return the current token and advance the cursor
        let mut stream = make_token_stream();
        let first = stream.advance().unwrap().text.clone();
        let second = stream.advance().unwrap().text.clone();
        assert_eq!(first, "SELECT");
        // Cursor advanced so we should now be at "a"
        assert_eq!(second, "a");
    }

    #[test]
    fn test_peek_returns_none_at_end() {
        // peek() should return None when we have consumed all tokens
        let mut stream = make_token_stream();
        // Consume all tokens
        while stream.advance().is_some() {}
        assert!(stream.peek().is_none());
    }

    #[test]
    fn test_consume_keyword_matches() {
        // consume_keyword() should return true and advance cursor on a match
        let mut stream = make_token_stream();
        assert!(stream.consume_keyword("SELECT"));
        // Cursor should have advanced — next token should be "a"
        assert_eq!(stream.peek().unwrap().text, "a");
    }

    #[test]
    fn test_consume_keyword_no_match() {
        // consume_keyword() should return false and leave cursor unchanged on no match
        let mut stream = make_token_stream();
        assert!(!stream.consume_keyword("FROM"));
        // Cursor should not have moved — still at SELECT
        assert_eq!(stream.peek().unwrap().text, "SELECT");
    }

    #[test]
    fn test_consume_keywords_sequence() {
        // consume_keywords() should consume a sequence of keywords in order
        let mut stream = TokenStream::new(vec![
            Token::new("GROUP".to_string(), TokenType::Keyword(Keyword::Group), 5),
            Token::new("BY".to_string(), TokenType::Keyword(Keyword::By), 8),
            Token::new("a".to_string(), TokenType::Literal(Literal::Identifier), 10),
        ]);
        assert!(stream.consume_keywords(&["GROUP", "BY"]));
        // Both keywords consumed — cursor should be at "a"
        assert_eq!(stream.peek().unwrap().text, "a");
    }

    #[test]
    fn test_consume_keywords_rolls_back_on_partial_match() {
        // If the sequence does not fully match, cursor should roll back to start
        let mut stream = TokenStream::new(vec![
            Token::new("GROUP".to_string(), TokenType::Keyword(Keyword::Group), 5),
            Token::new("a".to_string(), TokenType::Literal(Literal::Identifier), 7),
        ]);
        // GROUP is there but BY is not — should roll back
        assert!(!stream.consume_keywords(&["GROUP", "BY"]));
        // Cursor should be back at GROUP
        assert_eq!(stream.peek().unwrap().text, "GROUP");
    }

    #[test]
    fn test_consume_token_type_matches() {
        // consume_token_type() should match on token type and advance cursor
        let mut stream = make_token_stream();
        assert!(stream.consume_token_type(&TokenType::Keyword(Keyword::Select)));
        // Cursor advanced — should now be at "a"
        assert_eq!(stream.peek().unwrap().text, "a");
    }

    #[test]
    fn test_consume_token_type_no_match() {
        // consume_token_type() should return false and leave cursor unchanged on no match
        let mut stream = make_token_stream();
        assert!(!stream.consume_token_type(&TokenType::Literal(Literal::Identifier)));
        // Cursor should not have moved — still at SELECT
        assert_eq!(stream.peek().unwrap().text, "SELECT");
    }
}
