use crate::tokens::*;

/// SqlTokenizer takes a raw SQL string and breaks it into a list of tokens.
/// It walks through the string character by character, keeping track of where it
/// currently is with offset.
/// The struct is just a container for the SQL string we are working through and a bookmark
/// for where we currently are in it. Every time we scan a token, we advance the offet forward so the next call to
/// next_token picks up where the last one left off.
pub struct SqlTokenizer {
    /// The raw SQL string being tokenized
    pub sql: String,
    /// The current position in the string. Starts at 0 and advances
    /// as the tokens are scanned. Think of it as a cursor moving left to right.
    pub offset: usize,
}

pub struct TokenStream {
    pub tokens: Vec<Token>,
}

impl SqlTokenizer {
    /// Creates a new SqlTokenizer for the given SQL string.
    /// The offset starts at 0 because we always begin scanning
    /// from the very first character of the string.
    pub fn new(sql: String) -> Self {
        Self { sql, offset: 0 }
    }

    /// Tokenizer the entire SQL string and returns a TokenStream.
    /// This is the main entry point - call this to convert a raw SQL string
    /// into a list of tokens that the parser can work with.
    pub fn tokenize(&mut self) -> TokenStream {
        // Our method is &mut self because next_token() will be advancing the offset field as it scans through the string

        // Collect all tokens into a list until next_token() returns None,
        // which signals we have reached the end of the SQL string
        let mut list = Vec::new();

        // Keep calling next_token() until we get None (end of string)
        // Each Some(token) gets pushed into our list
        // while self.next_token() destructures to Some(token), extract token from Some()
        // and push it into list.
        while let Some(token) = self.next_token() {
            list.push(token);
        }
        // while let Some(x) is a pattern matching loop
        // It combines a while loop with destructuring an Option in one line
        // It is essentially shorthand for using a loop {} with a match statement that breaks on None

        // Wrap the list in a TokenStream and return it
        // The parser will use TokenStream to walk through the tokens
        TokenStream::new(list)
    }

    /// This is the core scanning method. It is called repeatedly by tokenize() to produce one token at a time.
    /// Returns None when the end of the SQL string is reached, which signals tokenize() to stop.
    /// Each call to next_token picks up at offset, scans on token, advances offset to the end of that token, and returns it.
    pub fn next_token(&mut self) -> Option<Token> {
        // First skip any whitespace so we start at the next meaningful character
        self.offset = self.skip_whitespace(self.offset);

        // Peek at the current character and decide what kind of token to scan.
        // If we are at the end of the string, return None to signal we are done
        // chars() converts the string into an iterator of characters and nth(n) grabs the character at position n
        // from that iterator
        // With chars(), it becomes something like S, E, L, E, C, T, ' ', a, ' ', +, ' ', b, ' ', F, R, O, M, ' ', c
        // "Give me the character at position offset in the string"
        // It returns Option<char>, Some if there is a character at that position
        // None if the offset is past the end of the String
        // chars().nth(offset) just peeks at one character to make the decision - "what am I looking at?"
        // Then based on that decision it hands off to a scan_x method which takes over and consumes as many character as needed to complete the full token
        match self.sql.chars().nth(self.offset) {
            None => None,
            Some(ch) => match ch {
                // A letter or a backticket means we are looking at an identifier or keyword
                // This is a guard pattern - it matches the character AND checks the condition
                // If it passes, we call scan_identifier which will consume the word and we then advance the offset
                // to where the token ended.
                ch if Literal::is_identifier_start(ch) => {
                    let token = self.scan_idenitifer(self.offset);
                    self.offset = token.end_offset;
                    Some(token)
                },
                // A digit or dot means we are looking at a number
                // If the guard pattern passes, we call scan_number which will consume the number and we then advance the offset
                // to where the token ended.
                ch if Literal::is_number_start(ch) => {
                    let token = self.scan_number(self.offset);
                    self.offset = token.end_offset;
                    Some(token)
                },
                // An operator or punctuation means we are looking at a symbol
                // If the guard pattern passes, we call scan_symbol which will consume the number and we then advance the offset
                // to where the token ended.
                ch if Symbol::is_symbol_start(ch) => {
                    let token = self.scan_symbol(self.offset);
                    self.offset = token.end_offset;
                    Some(token)
                },
                // A quote means we are looking at a string literal
                ch if Literal::is_char_start(ch) => {
                    let token = self.scan_chars(self.offset, ch);
                    self.offset = token.end_offset;
                    Some(token)
                },
                // Anything else is an unexpected character - we don't know what to do with it
                // If the guard passes, we call scan_chars passing both the current offset AND the character itself
                // The character is passed because scan_chars needs to know which quote type it's looking for
                _ => panic!("Unexpected character '{}' at position {}", ch, self.offset),
            },
        }
    }

    /// Advances past any whitespace characters and returns the new offset.
    /// Called at the start of next_token() so we always begin scanning at
    /// a meaningful character, never a space or newline.
    /// It checks if the current position is whitespace.
    fn skip_whitespace(&self, start_offset: usize) -> usize {
        // Walk forward from start_offset until we find a non-whitespace character.
        // If the rest of the string is all whitespace, this returns the end of the string
        let mut offset = start_offset;
        // self.sql.chars().nth(offset) gets the character at offset, returns an Option
        // .map_or handles the Option - "if it's None, return false, if it's Some run ch.is_whitespace and return the result" - Option -> bool
        // .map_or is used to handle an Option and produce a plain value in one step - it takes a default value to return if None
        // and a closure to run if Some, which receives the inner value
        // ! flips the bool - "If the character at this offset is NOT whitespace"
        // If chars.nth(offset) returns None, map_or returns false, then !false = true, so the if condition is true and we break
        // If chars.nth(offset) returns Some, map_or checks ch.is_whitespace(), if false, we break, if true, we advance the offset +1
        while offset < self.sql.len() {
            if !self
                .sql
                .chars()
                .nth(offset)
                .map_or(false, |ch| ch.is_whitespace())
            {
                break;
            }
            offset += 1;
        }
        offset
    }
}

// Using next_token and tokenize, the flow, for SELECT, for example is:
// 1. tokenize() calls next_token()
// 2. next_token() peeks at S, sees it's an identifier start, calls scan_identifier
// 3. scan_identifier conesumes S, E, L, E, C, T and returns a Token("SELECT", Keyword(Select), 6)
// 4. next_token() returns Some(Token("SELECT", Keyword(Select), 6)) back to tokenize()
// 5. tokenize() receives it, pushes it into the Vec
// 6. tokenize() calls next_token() again, this time starting at offset 6
