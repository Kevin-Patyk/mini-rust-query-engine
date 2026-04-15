use std::fmt::{self, Formatter};
use std::str::FromStr;

use strum_macros::EnumString;

/// This file defines the building blocks of the SQL tokenizer.
/// It contains the three token type enums - Keyword, Literal, and Symbol - which represent
/// every possible category of token in a SQL query, as well as the TokenType enum that
/// wraps them all together, and the Token struct itself which is the final assembled piece
/// the tokenizer produces. Every other part of the SQL parsing pipeline depends on these types.
///
/// Represents the different kinds of literal values that can appear in a SQL query.
/// For example, the number 42 would be a Long, 3.14 would be a Double,
/// 'hello' would be a String, and a column/table name like "employees" would be an identifier.
#[derive(Debug)]
pub enum Literal {
    Long,
    Double,
    String,
    Identifier,
}

/// Note on associated functions in this file:
/// Many of the methods on these enums like is_symbol_start(), is_number_start() etc.
/// are associated functions, meaning they do not take &self and do not operate on
/// an instance of the enum. They are just namedspaced under the enum for organization.
/// For example Symbol::is_symbol_start(ch) does not need a Symbol instance to call -
/// it is just helper logic that lives on Symbol because it is conceptually related to symbols.
/// You call them like Symbol::is_symbol_start(ch) or Literal::is_number_start(ch).

/// Associated functions are functions that belong to a type but do not take &self,
/// meaning they do not operate on an instance of the type. They are called on the type itself
/// rather than on a value, like Symbol::is_symbol_start(ch) rather than
/// my_symbol.is_symbol_start(ch). In Rust, new() is the most common example -
/// you call String::new() not my_string.new().
impl Literal {
    /// A char in Rust is a single Unicode character, e.g. 'a', '1', '.'
    /// You can make a char with single quotes: 'a' is a char, "a" is a &str
    /// ASCII is the first 128 characters of Unicode, covering basic Latin letters,
    /// digits, and symbols like '!', '+', '=', etc.
    /// Essentially, it covers the basics you would find on an English keyboard -
    /// anything beyond that - accented letters, Chinese, emojis - is outside ASCII
    /// and in the broader unicode range.
    /// is_ascii_digit checks for 0-9
    pub fn is_number_start(ch: char) -> bool {
        // A number can start with a digit (0-9) or a dot (.) for decimals like .5
        ch.is_ascii_digit() || ch == '.'
    }

    /// An identifier is a name for a table, column, alias, etc. e.g. `employees` or `employee_name`
    pub fn is_identifier_start(ch: char) -> bool {
        // An identifier can start with a letter (a-z, A-Z) or a backtick (`)
        // Backticks are used to escape reserved keywords as identifiers, e.g. `select`
        ch.is_ascii_alphabetic() || ch == '`'
    }

    /// Returns true if the character can appear inside an identifier (but not necessarily start one).
    /// For example, `employee_name2` is valid but `2employee_name` is not because digits
    /// and underscores are allowed inside an identifier but not at the start.
    pub fn is_identifier_part(ch: char) -> bool {
        // Letters, digits, and underscores are all valid inside an identifier
        ch.is_ascii_alphabetic() || ch.is_ascii_digit() || ch == '_'
    }

    /// Returns true if the character is the start of a string literal.
    /// In SQL, strings can be wrapped in single quotes 'hello' or double quotes "hello"
    pub fn is_char_start(ch: char) -> bool {
        // A string literal starts with either a single quote (') or a double quote (")
        // '\'' is an escape single quote char - the backslash is needed because
        // single quotes are used to delimit chars in Rust, so without it Rust
        // would get confused thinking the char literal ended prematurely.
        ch == '\'' || ch == '"'
    }
}

/// Represents every SQL keyword the tokenizer recognizes.
/// When the tokenizer finds an identifier, it checks if it matches on of these keywords -
/// if so, it gets classified as a Keywork token instead of an identifier
/// Keywords are reserved words that are part of the SQL language itself.
/// They fall into a few categories: Commands, clauses, conditionals, types, and functions.
/// Keywords are reserved by the SQL language - you can't name your table SELECT or your column WHERE
/// without escaping them with backticks because the tokenizer will always classify them as keywords first.
#[derive(EnumString)]
#[strum(ascii_case_insensitive)]
#[derive(Debug)]
pub enum Keyword {
    // Common
    Schema,
    Database,
    Table,
    Column,
    View,
    Index,
    Trigger,
    Procedure,
    Tablespace,
    Function,
    Sequence,
    Cursor,
    From,
    To,
    Of,
    If,
    On,
    For,
    While,
    Do,
    No,
    By,
    With,
    Without,
    True,
    False,
    Temporary,
    Temp,
    Comment,

    // Create
    Create,
    Replace,
    Before,
    After,
    Instead,
    Each,
    Row,
    Statement,
    Execute,
    Bitmap,
    Nosort,
    Reverse,
    Compile,

    // Alter
    Alter,
    Add,
    Modify,
    Rename,
    Enable,
    Disable,
    Validate,
    User,
    Identified,

    // Truncate
    Truncate,

    // Drop
    Drop,
    Cascade,

    // Insert
    Insert,
    Into,
    Values,

    // Update
    Update,
    Set,

    // Delete
    Delete,

    // Select
    Select,
    Distinct,
    Limit,
    As,
    Case,
    When,
    Else,
    Then,
    End,
    Left,
    Right,
    Full,
    Inner,
    Outer,
    Cross,
    Join,
    Use,
    Using,
    Natural,
    Where,
    Order,
    Asc,
    Desc,
    Group,
    Having,
    Union,

    // Others
    Declare,
    Grant,
    Fetch,
    Revoke,
    Close,
    Cast,
    New,
    Escape,
    Lock,
    Some,
    Leave,
    Iterate,
    Repeat,
    Until,
    Open,
    Out,
    Inout,
    Over,
    Advise,
    Siblings,
    Loop,
    Explain,
    Default,
    Except,
    Intersect,
    Minus,
    Password,
    Local,
    Global,
    Storage,
    Data,
    Coalesce,

    // Types
    Char,
    Character,
    Varying,
    Varchar,
    Varchar2,
    Integer,
    Int,
    Smallint,
    Decimal,
    Dec,
    Numeric,
    Float,
    Real,
    Double,
    Precision,
    Date,
    Time,
    Interval,
    Boolean,
    Blob,

    // Conditionals
    And,
    Or,
    Xor,
    Is,
    Not,
    Null,
    In,
    Between,
    Like,
    Any,
    All,
    Exists,

    // Functions
    Avg,
    Max,
    Min,
    Sum,
    Count,
    Greatest,
    Least,
    Round,
    Trunc,
    Position,
    Extract,
    Length,
    CharLength,
    Substring,
    Substr,
    Instr,
    Initcap,
    Upper,
    Lower,
    Trim,
    Ltrim,
    Rtrim,
    Both,
    Leading,
    Trailing,
    Translate,
    Convert,
    Lpad,
    Rpad,
    Decode,
    Nvl,

    // Constraints
    Constraint,
    Unique,
    Primary,
    Foreign,
    Key,
    Check,
    References,
}

impl Keyword {
    /// Takes a string from the SQL query and checks if it is a known SQL keyword.
    /// For example "SELECT" or "select" or "Select" all returns Some(Keyword::Select),
    /// while "employees" return None because it is not a keyword - it is an Identifier instead.
    /// This is our lookup for checking if the input text is a reserved SQL keyword.
    pub fn text_of(text: &str) -> Option<Self> {
        // Try to parse the string as a Keyword variant, returning None if it doesn't match
        // from_str returns a Result and .ok() converts it to Option -
        // So Some(Keyword) if it matched, None if not.
        // This is much cleaner than 150 matches arms.
        Keyword::from_str(text).ok()
    }
}

/// Represents every SQL operator and punctuation mark the tokenizer recognizes.
/// Unlike keywords, symbols are not words but special characters like +, -, *, =, etc.
/// This is just the definition of what variants exist.
#[derive(Debug)]
pub enum Symbol {
    // Parentheses and brackets
    LeftParen,
    RightParen,
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,

    // Punctuation
    Semi,
    Comma,
    Dot,
    DoubleDot,
    Colon,
    DoubleColon,
    ColonEq,

    // Arithmetic operators
    Plus,
    Sub,
    Star,
    Slash,
    Percent,

    // Comparison operators
    Eq,
    Gt,
    Lt,
    LtEq,
    GtEq,
    LtEqGt,
    LtGt,

    // Bang operators
    Bang,
    BangEq,
    BangGt,
    BangLt,

    // Bitwise operators
    Amp,
    Bar,
    DoubleAmp,
    DoubleBar,
    DoubleLt,
    DoubleGt,
    Tilde,
    Caret,

    // Misc
    Question,
    At,
    Pound,
}

impl Symbol {
    /// Takes a string from the SQL query and checks if it is a known SQL symbol.
    /// This is our lookup for checking if the input symbol is something our SQL supports.
    /// For example "+" returns Some(Symbol::Plus), "<=" returns Some(Symbol::LtEq),
    /// and "hello" returns None because it is not a symbol.
    pub fn text_of(text: &str) -> Option<Symbol> {
        match text {
            // Parentheses and brackets
            "(" => Some(Symbol::LeftParen),
            ")" => Some(Symbol::RightParen),
            "{" => Some(Symbol::LeftBrace),
            "}" => Some(Symbol::RightBrace),
            "[" => Some(Symbol::LeftBracket),
            "]" => Some(Symbol::RightBracket),

            // Punctuation
            ";" => Some(Symbol::Semi),
            "," => Some(Symbol::Comma),
            "." => Some(Symbol::Dot),
            ".." => Some(Symbol::DoubleDot),
            ":" => Some(Symbol::Colon),
            "::" => Some(Symbol::DoubleColon),
            ":=" => Some(Symbol::ColonEq),

            // Arithmetic operators
            "+" => Some(Symbol::Plus),
            "-" => Some(Symbol::Sub),
            "*" => Some(Symbol::Star),
            "/" => Some(Symbol::Slash),
            "%" => Some(Symbol::Percent),

            // Comparison operators
            "=" => Some(Symbol::Eq),
            ">" => Some(Symbol::Gt),
            "<" => Some(Symbol::Lt),
            "<=" => Some(Symbol::LtEq),
            ">=" => Some(Symbol::GtEq),
            "<=>" => Some(Symbol::LtEqGt),
            "<>" => Some(Symbol::LtGt),

            // Bang operators
            "!" => Some(Symbol::Bang),
            "!=" => Some(Symbol::BangEq),
            "!>" => Some(Symbol::BangGt),
            "!<" => Some(Symbol::BangLt),

            // Bitwise operators
            "&" => Some(Symbol::Amp),
            "|" => Some(Symbol::Bar),
            "&&" => Some(Symbol::DoubleAmp),
            "||" => Some(Symbol::DoubleBar),
            "<<" => Some(Symbol::DoubleLt),
            ">>" => Some(Symbol::DoubleGt),
            "~" => Some(Symbol::Tilde),
            "^" => Some(Symbol::Caret),

            // Misc
            "?" => Some(Symbol::Question),
            "@" => Some(Symbol::At),
            "#" => Some(Symbol::Pound),

            _ => None,
        }
    }

    /// Returns true if the character could be the start of a SQL symbol.
    /// The tokenizer uses this to decide whether to start scanning a symbol token.
    /// Note that some symbols are multiple characters like <= or <>, so this just
    /// checks if the character COULD be the start of one, not that it is one on its own
    pub fn is_symbol_start(ch: char) -> bool {
        // matches! is a macro that checks if a value matches any of a list of patterns.
        // It is shorthand for a match statement that returns true/false, so instead of:
        // match ch { '(' => true, ')' => true, ... _ => false }
        // we can write it all in one line with the | (or) separator
        matches!(
            ch,
            '(' | ')'
                | '{'
                | '}'
                | '['
                | ']'
                | ';'
                | ','
                | '.'
                | ':'
                | '+'
                | '-'
                | '*'
                | '/'
                | '%'
                | '='
                | '>'
                | '<'
                | '!'
                | '&'
                | '|'
                | '~'
                | '^'
                | '?'
                | '@'
                | '#'
        )
    }
}

/// TokenType represents the category of a token and which specific variant it is.
/// It wraps the three possible categories of tokens - keywords, literals, and symbols.
/// The inner enum tells you the specific variant within that category, for example:
/// TokenType::Keyword(Keyword::Select) tells you it is a keyword AND that it is SELECT
/// TokenType::Literal(Literal::Long) tells you it is a literal AND it is a Long
/// TokenType::Symbol(Symbol::Plus) tells you it is a symbol and it is a +
/// This is just an enum and each variant in the enum takes another enum.
#[derive(Debug)]
pub enum TokenType {
    // A reserved SQL word like SELECT, FROM, WHERE, etc.
    Keyword(Keyword),
    // A literal value like a number, string, or identifer.
    Literal(Literal),
    // A SQL operator or punctuation mark like +, -, <=, (, etc.
    Symbol(Symbol),
}

/// Token is the final assembled piece the tokenizer produces.
/// After scanning through the raw SQL string character by character,
/// the tokenizer packages each recognized chunk into a Token.
/// A full SQL query becomes a list of these tokens that the parser then reads.
pub struct Token {
    /// The actual characters from the query, like "SELECT", "+", or "42"
    pub text: String,
    /// The category and specific type of this token, like TokenType::Keyword(Keyword::Select)
    pub token_type: TokenType,
    /// Where in the original SQL string this token ended.
    /// Uses usize because it is an index into a string and Rust uses usize for indexing.
    /// Useful for error messages like "syntax error at position 12".
    pub end_offset: usize,
}

impl Token {
    /// Crates a new Token with the given text, type, and end offset.
    /// Called by the tokenizer each time it finishes scanning a chunk of the SQL string.
    pub fn new(text: String, token_type: TokenType, end_offset: usize) -> Self {
        // Self refers to the type we are implementing, in this case Token.
        // It is just a shorthand so we don't have to write Token { ... } explicitly,
        // which is useful if the type is ever renamed.
        Self {
            // When a field name and the variable being assigned have the same name,
            // Rust allows you to write just `text` instead of `text: text` -
            // This is called field init shorthand.
            text,
            token_type,
            end_offset,
        }
    }
}

/// Display allows us to print a Token in a human readable format using println! or format!
/// Without this, Rust would not know how to convert Token to a string.
/// This is the equivalent of toString() in Kotlin.
impl fmt::Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Match on the token type bo build a string like "Keyword(Select)" or "Symbol(Plus)"
        // We use {:?} because we derived debug on the inner enums, which gives us the variant name
        let token_type_str = match &self.token_type {
            TokenType::Keyword(k) => format!("Keyword({:?})", k),
            TokenType::Literal(l) => format!("Literal({:?})", l),
            TokenType::Symbol(s) => format!("Symbol({:?})", s),
        };

        // Produces output like: Token("SELECT", Keyword(Select), 6)
        write!(
            f,
            "Token(\"{}\", {}, {})",
            self.text, token_type_str, self.end_offset
        )
    }
}

// If we were to implement Display for an enum, we would need to match on every variant
// and return the string representation for each one. This is because Rust has no built-in
// way to convert an enum variant to a string - you have to explicitly tell it what each
// variant should look like as text.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_number_start() {
        assert!(Literal::is_number_start('4'))
    }

    #[test]
    fn test_is_identifier_start() {
        assert!(Literal::is_identifier_start('`'));
        assert!(Literal::is_identifier_start('h'))
    }

    #[test]
    fn test_is_identifier_part() {
        assert!(Literal::is_identifier_part('_'));
        assert!(Literal::is_identifier_part('b'))
    }

    #[test]
    fn test_is_char_start() {
        assert!(Literal::is_char_start('\''));
        assert!(Literal::is_char_start('"'))
    }

    #[test]
    fn test_text_of_keyword() {
        assert!(Keyword::text_of("SELECT").is_some());
        assert!(Keyword::text_of("hello").is_none())
    }

    #[test]
    fn test_text_of_symbol() {
        assert!(Symbol::text_of("+").is_some());
        assert!(Symbol::text_of("hello").is_none())
    }

    #[test]
    fn test_is_symbol_start() {
        assert!(Symbol::is_symbol_start('('))
    }
}
