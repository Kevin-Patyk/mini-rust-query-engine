use crate::token_stream::TokenStream;
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
        let mut tokens = Vec::new();

        // Keep calling next_token() until we get None (end of string)
        // Each Some(token) gets pushed into our list
        // while self.next_token() destructures to Some(token), extract token from Some()
        // and push it into list.
        while let Some(token) = self.next_token() {
            tokens.push(token);
        }
        // while let Some(x) is a pattern matching loop
        // It combines a while loop with destructuring an Option in one line
        // It is essentially shorthand for using a loop {} with a match statement that breaks on None

        // Wrap the list in a TokenStream and return it
        // The parser will use TokenStream to walk through the tokens
        TokenStream::new(tokens)
    }

    // Using next_token and tokenize, the flow, for SELECT, for example is:
    // 1. tokenize() calls next_token()
    // 2. next_token() peeks at S, sees it's an identifier start, calls scan_identifier
    // 3. scan_identifier conesumes S, E, L, E, C, T and returns a Token("SELECT", Keyword(Select), 6)
    // 4. next_token() returns Some(Token("SELECT", Keyword(Select), 6)) back to tokenize()
    // 5. tokenize() receives it, pushes it into the Vec
    // 6. tokenize() calls next_token() again, this time starting at offset 6

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
                    let token = self.scan_identifier(self.offset);
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
                .is_some_and(|ch| ch.is_whitespace())
            {
                break;
            }
            offset += 1;
        }
        offset
    }

    /// Scans a number token starting at start_offset.
    /// Handles both integers like 42 and decimals like 3.14.
    /// Returns a Long token for integers and a Double token for decimals.
    fn scan_number(&self, start_offset: usize) -> Token {
        // If the number starts with a minus sign, skip past it before scanning digits
        // e.g. -42 should be scanned as a negative number
        let mut end_offset = if self.sql.chars().nth(start_offset) == Some('-') {
            self.index_of_first(start_offset + 1, |ch| !ch.is_ascii_digit())
        } else {
            self.index_of_first(start_offset, |ch| !ch.is_ascii_digit())
        };

        // If we consumed the entire rest of the string, it must be a plain integer
        // e.g. a query ending in "42" with nothing after it
        if end_offset == self.sql.len() {
            return Token::new(
                self.sql[start_offset..end_offset].to_string(),
                TokenType::Literal(Literal::Long),
                end_offset,
            );
        }

        // Check if the next character is a dot, which means this is a decimal number
        // e.g. 3.14 - we have scanned "3" so far and now see "."
        let is_float = self.sql.chars().nth(end_offset) == Some('.');
        if is_float {
            // Skip past the dot and continue scanning the decimal digits
            end_offset = self.index_of_first(end_offset + 1, |ch| !ch.is_ascii_digit());
        }

        // Build the token - Long for integers, Double for decimals
        Token::new(
            self.sql[start_offset..end_offset].to_string(),
            if is_float {
                TokenType::Literal(Literal::Double)
            } else {
                TokenType::Literal(Literal::Long)
            },
            end_offset,
        )
    }

    /// Scans an identifier or keyword token starting at start_offset.
    /// An identifier is a name like a column or table name e.g. `employees`.
    /// A keyword is a reserved SQL word like SELECT or FROM.
    /// The difference is determined after scanning - we check if the text matches a keyword.
    fn scan_identifier(&self, start_offset: usize) -> Token {
        // Handle backtick escaped identifiers like `select` - where a reserved keyword
        // is being used as an identifier name. We skip the opening backtick, scan until
        // we find the closing backtick, and return everything in between as an identifier.
        if self.sql.chars().nth(start_offset) == Some('`') {
            let end_offset = self.get_offset_until_terminated_char('`', start_offset + 1);
            return Token::new(
                // Skip the opening backtick with start_offset + 1
                self.sql[start_offset + 1..end_offset].to_string(),
                TokenType::Literal(Literal::Identifier),
                // Skip past the closing backtick with end_offset + 1
                end_offset + 1,
            );
        }

        // Scan forward until we hit a character that cannot be part of an identifier
        // e.g. for "employees WHERE" we stop at the space before WHERE
        let end_offset = self.index_of_first(start_offset, |ch| !Literal::is_identifier_part(ch));
        let text = self.sql[start_offset..end_offset].to_string();

        // GROUP and ORDER are ambiguous - they could be identifiers or keywords
        // depending on whether they are followed by BY. e.g. "GROUP BY" is a keyword
        // but a column named "group" is an identifier
        if self.is_ambiguous_identifier(&text) {
            let token_type = self.process_ambiguous_identifier(end_offset, &text);
            return Token::new(text, token_type, end_offset);
        }

        // Check if the text matches a known keyword like SELECT or FROM.
        // If it does, use that keyword type. If not, it is a plain identifier.
        let token_type = Keyword::text_of(&text)
            .map(TokenType::Keyword)
            // .map on an Option transforms the value inside Some without unwrapping it
            // If Keyword::text_of(&test) returns Some(Keyword::Select), .map(|k| TokenType::Keyword(k)) transforms it into
            // Some(TokenType::Keyword(Keyword::Select))
            // and_then is similar to map but the closure you pass to it returns an Option itself, so it avoids double wrapping Some(Some(x))
            // it is used when a transformation can fail
            // Use .map when your closure returns a plain value (and wraps it back in some)
            // Use .and_then when your closure returns an option (to avoid double wrapping)
            // Both of these work on options
            .unwrap_or(TokenType::Literal(Literal::Identifier));

        Token::new(text, token_type, end_offset)
    }

    /// Returns true if the identifier could be either a keyword or a table/column name.
    /// GROUP and ORDER are ambiguous because they are only keywords when followed by BY.
    fn is_ambiguous_identifier(&self, text: &str) -> bool {
        text.eq_ignore_ascii_case("GROUP") || text.eq_ignore_ascii_case("ORDER")
    }

    /// Checks if GROUP or ORDER is followed by BY, making it a keyword.
    /// If not followed by BY, it is treated as a plain identifier.
    fn process_ambiguous_identifier(&self, start_offset: usize, text: &str) -> TokenType {
        let skip = self.skip_whitespace(start_offset);
        if skip + 2 <= self.sql.len() && self.sql[skip..skip + 2].eq_ignore_ascii_case("BY") {
            Keyword::text_of(text)
                .map(TokenType::Keyword)
                .unwrap_or(TokenType::Literal(Literal::Identifier))
        } else {
            TokenType::Literal(Literal::Identifier)
        }
    }

    /// Scans a symbol token like +, -, <=, <> etc.
    /// Tries to match the longest possible symbol first, then shrinks if no match found.
    fn scan_symbol(&self, start_offset: usize) -> Token {
        // Scan forward while we are still seeing symbol characters
        let mut end_offset = self.index_of_first(start_offset, |ch| !Symbol::is_symbol_start(ch));
        let mut text = self.sql[start_offset..end_offset].to_string();

        // Try to match the longest symbol first, then shrink by one character at a time
        // until we find a valid symbol. e.g. "<<=" would try "<<=" first, then "<<"
        while Symbol::text_of(&text).is_none() {
            end_offset -= 1;
            text = self.sql[start_offset..end_offset].to_string();
        }
        // A while loop keeps running as long as its condition is true.
        // Use it when you don't know how many iterations you need upfront -
        // unlike a for loop which iterates over a known collection.
        // Here we don't know how many characters to shrink by, so we keep
        // shrinking until Symbol::text_of finds a valid match
        // The loop stops the moment the condition becomes false - i.e.
        // when text_of returns Some instead of None, is_none() becomes false and we exit.

        Token::new(
            text.clone(),
            TokenType::Symbol(Symbol::text_of(&text).unwrap()),
            end_offset,
        )
    }

    /// Scans a string literal token like 'hello' or "world".
    /// Handles escaped quotes - two quotes in a row like '' or "" are treated as
    /// a single quote character inside the string, not the end of it.
    fn scan_chars(&self, start_offset: usize, terminated_char: char) -> Token {
        let mut result = String::new();
        let mut i = start_offset + 1;

        while i < self.sql.len() {
            let ch = self.sql.chars().nth(i).unwrap();

            if ch == terminated_char {
                // Check if this is an escaped quote - two quotes in a row
                if i + 1 < self.sql.len() && self.sql.chars().nth(i + 1) == Some(terminated_char) {
                    // It is an escaped quote, add one quote to the result and skip both
                    result.push(terminated_char);
                    i += 2;
                } else {
                    // It is the closing quote - we are done
                    return Token::new(result, TokenType::Literal(Literal::String), i + 1);
                }
            } else {
                result.push(ch);
                i += 1;
            }
        }

        panic!("Unterminated string starting at position {}", start_offset);
    }

    /// Walks forward from start_index until the predicate returns true,
    /// then returns that position. If the predicate never returns true,
    /// returns the end of the string.
    fn index_of_first<F>(&self, start_index: usize, predicate: F) -> usize
    where
        F: Fn(char) -> bool,
    {
        // F is a generic type parameter - it represents any type that implements Fn(char) -> bool
        // This means the caller can pass any function or closure that takes a char and returns a bool
        // For example: |ch| !ch.is_ascii_digit() or |ch| !Literal::is_identifier_part(ch)
        //
        // The where clause is how we add constraints to generic type parameters.
        // where F: Fn(char) -> bool means "F must be a callable that takes a char and returns a bool."
        // Fn(char) -> bool is a trait - it describes any function or closure with that signature
        // Without the where clause, the compiler would not know what F is capable of doing,
        // so we could not call predicate(ch) inside of the function body.
        let mut index = start_index;
        while index < self.sql.len() {
            if let Some(ch) = self.sql.chars().nth(index) {
                if predicate(ch) {
                    return index;
                }
            }
            index += 1;
        }
        self.sql.len()
    }

    /// Finds the next occurrence of terminated_char starting from start_offset.
    /// Panics if the character is never found - used for closing backticks and quotes.
    fn get_offset_until_terminated_char(
        &self,
        terminated_char: char,
        start_offset: usize,
    ) -> usize {
        self.sql[start_offset..]
            .find(terminated_char)
            .map(|i| i + start_offset)
            .unwrap_or_else(|| {
                panic!(
                    "Must contain '{}' in remaining sql from position {}",
                    terminated_char, start_offset
                )
            })
        // .find() is a method on &str that searches for a character and returns Option<usize> - Some(index) if found, None if not.
        // .map() takes the Option, doesn't unwrap it, and does the found value + start offset
        // If it is None rather than Some, .map() does nothing and it is passed through to .unwrap_or_else
        // .unwrap_or_else either unwraps the corrected position or panics if find() is None
    }
}

/// A simple error type for unexpected characters or unterminated strings.
/// In a production tokenizer this would carry more context for better error messages.
#[derive(Debug)]
pub struct TokenizeException {
    pub msg: String,
}

impl TokenizeException {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }
}
