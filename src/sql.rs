//! SQL utilities for PostgreSQL identifier handling.

/// Quote a PostgreSQL identifier unconditionally.
///
/// Always wraps the identifier in double quotes and escapes any embedded
/// double quotes by doubling them. This is the safest approach as it:
/// - Avoids incomplete reserved word lists
/// - Handles all special characters
/// - Preserves case sensitivity
/// - Works with any valid PostgreSQL identifier
///
/// PostgreSQL accepts double-quoted identifiers universally, so there's
/// no downside to always quoting except slightly longer SQL output.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident_simple() {
        // All identifiers are now quoted
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("my_table"), "\"my_table\"");
        assert_eq!(quote_ident("column1"), "\"column1\"");
    }

    #[test]
    fn test_quote_ident_uppercase() {
        assert_eq!(quote_ident("Users"), "\"Users\"");
        assert_eq!(quote_ident("myTable"), "\"myTable\"");
    }

    #[test]
    fn test_quote_ident_reserved_words() {
        // Reserved words are handled automatically
        assert_eq!(quote_ident("user"), "\"user\"");
        assert_eq!(quote_ident("table"), "\"table\"");
        assert_eq!(quote_ident("select"), "\"select\"");
        assert_eq!(quote_ident("order"), "\"order\"");
    }

    #[test]
    fn test_quote_ident_special_chars() {
        assert_eq!(quote_ident("my-table"), "\"my-table\"");
        assert_eq!(quote_ident("my table"), "\"my table\"");
    }

    #[test]
    fn test_quote_ident_starts_with_digit() {
        assert_eq!(quote_ident("1table"), "\"1table\"");
    }

    #[test]
    fn test_quote_ident_embedded_quotes() {
        // Embedded quotes are doubled
        assert_eq!(quote_ident("user\"name"), "\"user\"\"name\"");
        assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }
}
