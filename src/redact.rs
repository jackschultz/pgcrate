//! Redaction utilities for safe output.
//!
//! ## Credential Lifecycle Rules
//!
//! 1. **Credentials arrive** via env var, config file, or CLI (`--database-url`)
//! 2. **Credentials exist only in memory** during execution
//! 3. **Credentials never appear in stdout/stderr** - always redacted by default
//! 4. **Redaction is default-on** for all environments; `--no-redact` is explicit opt-out
//!
//! ## What Gets Redacted
//!
//! - **DSNs**: Password and query parameters removed (may contain secrets like `sslpassword`)
//! - **SQL queries**: String literals replaced with `'...'` (may contain PII)
//! - **Long queries**: Truncated to 200 characters
//!
//! ## Usage
//!
//! Redaction is applied automatically before output. To disable:
//! ```bash
//! pgcrate locks --no-redact  # Warns: output may contain sensitive data
//! ```

use url::Url;

/// Maximum query length before truncation (characters).
const MAX_QUERY_LENGTH: usize = 200;

/// Redact a database URL (connection string).
///
/// Keeps: scheme, host, port, database name, user
/// Removes: password, query parameters that may contain secrets
#[allow(dead_code)] // Useful utility, may be used in future for context command
pub fn redact_dsn(dsn: &str) -> String {
    match Url::parse(dsn) {
        Ok(mut url) => {
            // Remove password
            if url.password().is_some() {
                let _ = url.set_password(Some("***"));
            }
            // Remove query params (may contain secrets like sslpassword)
            url.set_query(None);
            url.to_string()
        }
        Err(_) => {
            // If we can't parse it, aggressively redact credentials.
            // Use rfind('@') to handle @ characters in passwords.
            if let Some(at_pos) = dsn.rfind('@') {
                if let Some(scheme_end) = dsn.find("://") {
                    let scheme = &dsn[..scheme_end + 3];
                    // Take everything after @ but strip query params
                    let after_at = &dsn[at_pos + 1..];
                    let host_part = after_at.split('?').next().unwrap_or(after_at);
                    format!("{}***@{}", scheme, host_part)
                } else {
                    "***REDACTED***".to_string()
                }
            } else {
                // No @ means no credentials - but still strip query params
                if let Some(q_pos) = dsn.find('?') {
                    dsn[..q_pos].to_string()
                } else {
                    dsn.to_string()
                }
            }
        }
    }
}

/// Redact SQL query text.
///
/// - Truncates long queries
/// - Removes string literals and replaces with '...'
/// - Keeps structure visible for debugging
pub fn redact_query(query: &str) -> String {
    // First, remove string literals
    let redacted = redact_string_literals(query);

    // Then truncate if too long (using char count, not byte count, to avoid UTF-8 panic)
    truncate_str(&redacted, MAX_QUERY_LENGTH)
}

/// Truncate a string to at most `max_chars` characters, appending "..." if truncated.
/// Safe for UTF-8: uses char boundaries, not byte slicing.
fn truncate_str(s: &str, max_chars: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_chars {
        return s.to_string();
    }

    // Find byte position of the max_chars-th character
    let byte_pos = s
        .char_indices()
        .nth(max_chars)
        .map(|(i, _)| i)
        .unwrap_or(s.len());

    format!("{}...", &s[..byte_pos])
}

/// Replace SQL string literals with '...' placeholder.
///
/// Only single-quoted strings are redacted (SQL string literals).
/// Double-quoted strings are identifiers in SQL (e.g., "column_name") and are preserved.
fn redact_string_literals(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let mut in_string = false;
    let mut chars = query.chars().peekable();

    while let Some(c) = chars.next() {
        if in_string {
            // Check for escaped quote ('') or end of string
            if c == '\'' {
                if chars.peek() == Some(&'\'') {
                    // Escaped quote - skip both
                    chars.next();
                } else {
                    // End of string
                    result.push_str("'...'");
                    in_string = false;
                }
            }
            // Otherwise skip (we're in a string literal)
        } else if c == '\'' {
            // Start of string literal (single quotes only)
            in_string = true;
        } else {
            result.push(c);
        }
    }

    // If we ended inside a string, close it
    if in_string {
        result.push_str("'...'");
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_dsn_with_password() {
        let dsn = "postgres://user:secret123@localhost:5432/mydb";
        let redacted = redact_dsn(dsn);
        assert!(redacted.contains("user"));
        assert!(redacted.contains("localhost"));
        assert!(redacted.contains("mydb"));
        assert!(!redacted.contains("secret123"));
        assert!(redacted.contains("***"));
    }

    #[test]
    fn test_redact_dsn_no_password() {
        let dsn = "postgres://user@localhost:5432/mydb";
        let redacted = redact_dsn(dsn);
        assert_eq!(redacted, "postgres://user@localhost:5432/mydb");
    }

    #[test]
    fn test_redact_dsn_removes_query_params() {
        let dsn = "postgres://user:pass@localhost/db?sslmode=require&sslpassword=secret";
        let redacted = redact_dsn(dsn);
        assert!(!redacted.contains("sslpassword"));
        assert!(!redacted.contains("secret"));
    }

    #[test]
    fn test_redact_dsn_with_at_in_password() {
        // @ in password - the url crate may parse this incorrectly, but we should
        // at least not leak the password portion.
        let dsn = "postgres://user:p@ss@localhost:5432/mydb";
        let redacted = redact_dsn(dsn);
        // The password (or part of it) should be replaced with ***
        assert!(
            redacted.contains("***"),
            "password not redacted: {}",
            redacted
        );
        // Should not contain the literal password characters "p@ss"
        assert!(!redacted.contains("p@ss"), "password leaked: {}", redacted);
    }

    #[test]
    fn test_redact_query_removes_strings() {
        let query = "SELECT * FROM users WHERE email = 'secret@example.com'";
        let redacted = redact_query(query);
        assert!(!redacted.contains("secret@example.com"));
        assert!(redacted.contains("'...'"));
    }

    #[test]
    fn test_redact_query_preserves_structure() {
        let query = "INSERT INTO logs (msg) VALUES ('sensitive data')";
        let redacted = redact_query(query);
        assert!(redacted.contains("INSERT INTO logs"));
        assert!(redacted.contains("VALUES"));
        assert!(!redacted.contains("sensitive data"));
    }

    #[test]
    fn test_redact_query_truncates_long() {
        let query = "SELECT ".to_string() + &"a".repeat(500);
        let redacted = redact_query(&query);
        assert!(redacted.len() <= MAX_QUERY_LENGTH + 3); // +3 for "..."
        assert!(redacted.ends_with("..."));
    }

    #[test]
    fn test_redact_query_handles_escaped_quotes() {
        let query = "SELECT * FROM t WHERE name = 'O''Brien'";
        let redacted = redact_query(query);
        assert!(!redacted.contains("O''Brien"));
        assert!(redacted.contains("'...'"));
    }

    #[test]
    fn test_redact_query_preserves_double_quoted_identifiers() {
        // Double quotes are SQL identifiers, not strings - should be preserved
        let query = r#"SELECT "user_id", "email" FROM "Users" WHERE name = 'secret'"#;
        let redacted = redact_query(query);
        assert!(
            redacted.contains(r#""user_id""#),
            "identifier lost: {}",
            redacted
        );
        assert!(
            redacted.contains(r#""email""#),
            "identifier lost: {}",
            redacted
        );
        assert!(
            redacted.contains(r#""Users""#),
            "identifier lost: {}",
            redacted
        );
        assert!(
            !redacted.contains("secret"),
            "string not redacted: {}",
            redacted
        );
    }

    #[test]
    fn test_truncate_str_utf8_safe() {
        // Multi-byte UTF-8: each emoji is 4 bytes
        let query = "SELECT '🔥🔥🔥🔥🔥' FROM t"; // 5 fire emojis
                                                  // This should not panic when truncating
        let _ = truncate_str(query, 10);
        let _ = truncate_str(query, 15);
        let _ = truncate_str(query, 20);
    }

    #[test]
    fn test_truncate_str_boundary() {
        // "café" = 5 bytes (é is 2 bytes), 4 chars
        let s = "café";
        assert_eq!(truncate_str(s, 10), "café"); // no truncation needed
        assert_eq!(truncate_str(s, 4), "café"); // exactly 4 chars, no truncation
        assert_eq!(truncate_str(s, 3), "caf..."); // truncate to 3 chars
        assert_eq!(truncate_str(s, 2), "ca..."); // truncate to 2 chars
    }

    #[test]
    fn test_redact_query_utf8_truncation() {
        // Create a query with UTF-8 that exceeds MAX_QUERY_LENGTH
        let base = "SELECT * FROM t WHERE x = ";
        let padding = "あ".repeat(300); // Japanese hiragana, 3 bytes each
        let query = format!("{}{}", base, padding);

        // Should not panic
        let redacted = redact_query(&query);
        assert!(redacted.ends_with("..."));
        // Character count should be at most MAX_QUERY_LENGTH + 3
        assert!(redacted.chars().count() <= MAX_QUERY_LENGTH + 3);
    }
}
