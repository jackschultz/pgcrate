//! Anonymization support for pgcrate.
//!
//! Provides server-side PII anonymization for extracting safe test data from production.
//! Uses PostgreSQL functions to transform data deterministically based on a seed.

use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};

/// Known anonymization strategies
pub const STRATEGIES: &[&str] = &[
    "fake_email",
    "fake_name",
    "fake_first_name",
    "fake_last_name",
    "redact",
    "null",
    "zero",
    "fake_uuid",
    "skip",
    "preserve",
];

/// A single anonymization rule
#[derive(Debug, Clone)]
pub struct AnonymizeRule {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: Option<String>, // None = table-level rule (skip)
    pub strategy: String,
}

impl AnonymizeRule {
    /// Create a new column-level rule
    #[allow(dead_code)]
    pub fn column(schema: &str, table: &str, column: &str, strategy: &str) -> Self {
        Self {
            table_schema: schema.to_string(),
            table_name: table.to_string(),
            column_name: Some(column.to_string()),
            strategy: strategy.to_string(),
        }
    }

    /// Create a new table-level skip rule
    #[allow(dead_code)]
    pub fn skip_table(schema: &str, table: &str) -> Self {
        Self {
            table_schema: schema.to_string(),
            table_name: table.to_string(),
            column_name: None,
            strategy: "skip".to_string(),
        }
    }

    /// Check if this is a table-level skip rule
    pub fn is_skip(&self) -> bool {
        self.column_name.is_none() && self.strategy == "skip"
    }

    /// Get the fully qualified table name
    pub fn qualified_table(&self) -> String {
        format!("{}.{}", self.table_schema, self.table_name)
    }
}

/// Validate a strategy name
pub fn validate_strategy(strategy: &str) -> Result<()> {
    if !STRATEGIES.contains(&strategy) {
        bail!(
            "Unknown strategy \"{}\"\nAvailable strategies: {}",
            strategy,
            STRATEGIES.join(", ")
        );
    }
    Ok(())
}

/// Parse a table name, extracting schema if present
/// Returns (schema, table_name)
pub fn parse_table_name(name: &str) -> (String, String) {
    if let Some(pos) = name.find('.') {
        let schema = &name[..pos];
        let table = &name[pos + 1..];
        (schema.to_string(), table.to_string())
    } else {
        ("public".to_string(), name.to_string())
    }
}

/// Build the SQL expression for a column transformation
pub fn build_column_expression(column: &str, strategy: &str, seed: &str) -> String {
    let quoted_seed = format!("'{}'", seed.replace('\'', "''"));
    match strategy {
        "null" => format!("NULL AS {}", crate::sql::quote_ident(column)),
        "zero" => format!("0 AS {}", crate::sql::quote_ident(column)),
        "preserve" => crate::sql::quote_ident(column),
        "fake_email" => format!(
            "pgcrate.anon_fake_email({}, {}) AS {}",
            crate::sql::quote_ident(column),
            quoted_seed,
            crate::sql::quote_ident(column)
        ),
        "fake_name" => format!(
            "pgcrate.anon_fake_name({}, {}) AS {}",
            crate::sql::quote_ident(column),
            quoted_seed,
            crate::sql::quote_ident(column)
        ),
        "fake_first_name" => format!(
            "pgcrate.anon_fake_first_name({}, {}) AS {}",
            crate::sql::quote_ident(column),
            quoted_seed,
            crate::sql::quote_ident(column)
        ),
        "fake_last_name" => format!(
            "pgcrate.anon_fake_last_name({}, {}) AS {}",
            crate::sql::quote_ident(column),
            quoted_seed,
            crate::sql::quote_ident(column)
        ),
        "redact" => format!(
            "pgcrate.anon_redact({}) AS {}",
            crate::sql::quote_ident(column),
            crate::sql::quote_ident(column)
        ),
        "fake_uuid" => format!(
            "pgcrate.anon_fake_uuid({}::text, {})::uuid AS {}",
            crate::sql::quote_ident(column),
            quoted_seed,
            crate::sql::quote_ident(column)
        ),
        // skip and unknown strategies preserve the column
        _ => crate::sql::quote_ident(column),
    }
}

/// Build a SELECT query for anonymized data from a table
pub fn build_anonymized_select(
    schema: &str,
    table: &str,
    columns: &[String],
    rules: &[AnonymizeRule],
    seed: &str,
) -> String {
    // Build a map of column -> strategy for this table
    let rule_map: HashMap<&str, &str> = rules
        .iter()
        .filter(|r| r.table_schema == schema && r.table_name == table && r.column_name.is_some())
        .map(|r| {
            (
                r.column_name.as_ref().unwrap().as_str(),
                r.strategy.as_str(),
            )
        })
        .collect();

    // Build column expressions
    let col_exprs: Vec<String> = columns
        .iter()
        .map(|col| {
            let strategy = rule_map.get(col.as_str()).copied().unwrap_or("preserve");
            build_column_expression(col, strategy, seed)
        })
        .collect();

    format!(
        "SELECT {} FROM {}.{}",
        col_exprs.join(", "),
        crate::sql::quote_ident(schema),
        crate::sql::quote_ident(table)
    )
}

/// Schemas to exclude from anonymization dumps
pub const EXCLUDED_SCHEMAS: &[&str] = &["pgcrate", "pg_catalog", "pg_toast", "information_schema"];

/// Check if a schema should be excluded from dumps
pub fn is_excluded_schema(schema: &str) -> bool {
    EXCLUDED_SCHEMAS.contains(&schema) || schema.starts_with("pg_")
}

/// Tables that should be skipped (based on rules)
pub fn get_skipped_tables(rules: &[AnonymizeRule]) -> HashSet<String> {
    rules
        .iter()
        .filter(|r| r.is_skip())
        .map(|r| r.qualified_table())
        .collect()
}

// =============================================================================
// SQL Definitions for Setup
// =============================================================================

/// SQL for the anon_fake_email function
pub const CREATE_ANON_FAKE_EMAIL: &str = r#"
CREATE OR REPLACE FUNCTION pgcrate.anon_fake_email(val TEXT, seed TEXT) RETURNS TEXT AS $$
DECLARE
    hash_val TEXT;
    first_names TEXT[] := ARRAY['alice', 'bob', 'carol', 'david', 'emma', 'frank', 'grace', 'henry', 'iris', 'jack'];
    last_names TEXT[] := ARRAY['smith', 'jones', 'wilson', 'taylor', 'brown', 'davies', 'evans', 'thomas', 'johnson', 'roberts'];
    domains TEXT[] := ARRAY['example.com', 'test.org', 'sample.net'];
    hash_int BIGINT;
    first_name TEXT;
    last_name TEXT;
    domain TEXT;
    suffix TEXT;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    hash_val := encode(sha256((val || seed)::bytea), 'hex');
    hash_int := ('x' || substring(hash_val, 1, 8))::bit(32)::bigint;
    first_name := first_names[1 + abs(hash_int) % array_length(first_names, 1)];
    hash_int := ('x' || substring(hash_val, 9, 8))::bit(32)::bigint;
    last_name := last_names[1 + abs(hash_int) % array_length(last_names, 1)];
    hash_int := ('x' || substring(hash_val, 17, 8))::bit(32)::bigint;
    domain := domains[1 + abs(hash_int) % array_length(domains, 1)];
    suffix := substring(hash_val, 25, 4);
    RETURN first_name || '.' || last_name || '.' || suffix || '@' || domain;
END;
$$ LANGUAGE plpgsql IMMUTABLE
"#;

/// SQL for the anon_fake_name function
pub const CREATE_ANON_FAKE_NAME: &str = r#"
CREATE OR REPLACE FUNCTION pgcrate.anon_fake_name(val TEXT, seed TEXT) RETURNS TEXT AS $$
DECLARE
    hash_val TEXT;
    first_names TEXT[] := ARRAY['Alice', 'Bob', 'Carol', 'David', 'Emma', 'Frank', 'Grace', 'Henry', 'Iris', 'Jack', 'Karen', 'Leo', 'Mia', 'Noah', 'Olivia'];
    last_names TEXT[] := ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Wilson', 'Anderson', 'Taylor', 'Thomas', 'Moore'];
    hash_int BIGINT;
    first_name TEXT;
    last_name TEXT;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    hash_val := encode(sha256((val || seed)::bytea), 'hex');
    hash_int := ('x' || substring(hash_val, 1, 8))::bit(32)::bigint;
    first_name := first_names[1 + abs(hash_int) % array_length(first_names, 1)];
    hash_int := ('x' || substring(hash_val, 9, 8))::bit(32)::bigint;
    last_name := last_names[1 + abs(hash_int) % array_length(last_names, 1)];
    RETURN first_name || ' ' || last_name;
END;
$$ LANGUAGE plpgsql IMMUTABLE
"#;

/// SQL for the anon_fake_first_name function
pub const CREATE_ANON_FAKE_FIRST_NAME: &str = r#"
CREATE OR REPLACE FUNCTION pgcrate.anon_fake_first_name(val TEXT, seed TEXT) RETURNS TEXT AS $$
DECLARE
    hash_val TEXT;
    first_names TEXT[] := ARRAY['Alice', 'Bob', 'Carol', 'David', 'Emma', 'Frank', 'Grace', 'Henry', 'Iris', 'Jack', 'Karen', 'Leo', 'Mia', 'Noah', 'Olivia'];
    hash_int BIGINT;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    hash_val := encode(sha256((val || seed)::bytea), 'hex');
    hash_int := ('x' || substring(hash_val, 1, 8))::bit(32)::bigint;
    RETURN first_names[1 + abs(hash_int) % array_length(first_names, 1)];
END;
$$ LANGUAGE plpgsql IMMUTABLE
"#;

/// SQL for the anon_fake_last_name function
pub const CREATE_ANON_FAKE_LAST_NAME: &str = r#"
CREATE OR REPLACE FUNCTION pgcrate.anon_fake_last_name(val TEXT, seed TEXT) RETURNS TEXT AS $$
DECLARE
    hash_val TEXT;
    last_names TEXT[] := ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Wilson', 'Anderson', 'Taylor', 'Thomas', 'Moore'];
    hash_int BIGINT;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    hash_val := encode(sha256((val || seed)::bytea), 'hex');
    hash_int := ('x' || substring(hash_val, 1, 8))::bit(32)::bigint;
    RETURN last_names[1 + abs(hash_int) % array_length(last_names, 1)];
END;
$$ LANGUAGE plpgsql IMMUTABLE
"#;

/// SQL for the anon_redact function
pub const CREATE_ANON_REDACT: &str = r#"
CREATE OR REPLACE FUNCTION pgcrate.anon_redact(val TEXT) RETURNS TEXT AS $$
DECLARE
    result TEXT := '';
    c CHAR;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    FOR i IN 1..length(val) LOOP
        c := substring(val, i, 1);
        IF c ~ '[a-zA-Z]' THEN
            result := result || 'X';
        ELSIF c ~ '[0-9]' THEN
            result := result || '9';
        ELSE
            result := result || c;
        END IF;
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE
"#;

/// SQL for the anon_fake_uuid function
pub const CREATE_ANON_FAKE_UUID: &str = r#"
CREATE OR REPLACE FUNCTION pgcrate.anon_fake_uuid(val TEXT, seed TEXT) RETURNS TEXT AS $$
DECLARE
    hash_val TEXT;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    hash_val := encode(sha256((val || seed)::bytea), 'hex');
    -- Format as UUID: 8-4-4-4-12
    RETURN substring(hash_val, 1, 8) || '-' ||
           substring(hash_val, 9, 4) || '-' ||
           '4' || substring(hash_val, 14, 3) || '-' ||
           '8' || substring(hash_val, 18, 3) || '-' ||
           substring(hash_val, 21, 12);
END;
$$ LANGUAGE plpgsql IMMUTABLE
"#;

/// All function creation SQL statements
pub const ALL_FUNCTION_SQL: &[&str] = &[
    CREATE_ANON_FAKE_EMAIL,
    CREATE_ANON_FAKE_NAME,
    CREATE_ANON_FAKE_FIRST_NAME,
    CREATE_ANON_FAKE_LAST_NAME,
    CREATE_ANON_REDACT,
    CREATE_ANON_FAKE_UUID,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_strategy_valid() {
        assert!(validate_strategy("fake_email").is_ok());
        assert!(validate_strategy("fake_name").is_ok());
        assert!(validate_strategy("redact").is_ok());
        assert!(validate_strategy("null").is_ok());
        assert!(validate_strategy("zero").is_ok());
        assert!(validate_strategy("skip").is_ok());
        assert!(validate_strategy("preserve").is_ok());
    }

    #[test]
    fn test_validate_strategy_invalid() {
        let result = validate_strategy("unknown");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown strategy"));
    }

    #[test]
    fn test_parse_table_name_with_schema() {
        let (schema, table) = parse_table_name("app.users");
        assert_eq!(schema, "app");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        let (schema, table) = parse_table_name("users");
        assert_eq!(schema, "public");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_build_column_expression_null() {
        let expr = build_column_expression("email", "null", "test");
        assert_eq!(expr, "NULL AS \"email\"");
    }

    #[test]
    fn test_build_column_expression_zero() {
        let expr = build_column_expression("count", "zero", "test");
        assert_eq!(expr, "0 AS \"count\"");
    }

    #[test]
    fn test_build_column_expression_preserve() {
        let expr = build_column_expression("id", "preserve", "test");
        assert_eq!(expr, "\"id\"");
    }

    #[test]
    fn test_build_column_expression_fake_email() {
        let expr = build_column_expression("email", "fake_email", "my-seed");
        assert!(expr.contains("pgcrate.anon_fake_email"));
        assert!(expr.contains("'my-seed'"));
    }

    #[test]
    fn test_build_anonymized_select() {
        let rules = vec![
            AnonymizeRule::column("public", "users", "email", "fake_email"),
            AnonymizeRule::column("public", "users", "name", "fake_name"),
        ];
        let columns = vec!["id".to_string(), "email".to_string(), "name".to_string()];
        let sql = build_anonymized_select("public", "users", &columns, &rules, "my-seed");

        assert!(sql.starts_with("SELECT"));
        assert!(sql.contains("\"id\"")); // preserved
        assert!(sql.contains("pgcrate.anon_fake_email")); // transformed
        assert!(sql.contains("'my-seed'"));
        assert!(sql.contains("pgcrate.anon_fake_name")); // transformed
        assert!(sql.contains("FROM \"public\".\"users\""));
    }

    #[test]
    fn test_is_excluded_schema() {
        assert!(is_excluded_schema("pgcrate"));
        assert!(is_excluded_schema("pg_catalog"));
        assert!(is_excluded_schema("pg_toast"));
        assert!(is_excluded_schema("information_schema"));
        assert!(is_excluded_schema("pg_temp_1"));
        assert!(!is_excluded_schema("public"));
        assert!(!is_excluded_schema("app"));
    }

    #[test]
    fn test_get_skipped_tables() {
        let rules = vec![
            AnonymizeRule::skip_table("public", "audit_logs"),
            AnonymizeRule::column("public", "users", "email", "fake_email"),
            AnonymizeRule::skip_table("app", "secrets"),
        ];
        let skipped = get_skipped_tables(&rules);
        assert!(skipped.contains("public.audit_logs"));
        assert!(skipped.contains("app.secrets"));
        assert!(!skipped.contains("public.users"));
        assert_eq!(skipped.len(), 2);
    }

    #[test]
    fn test_anonymize_rule_is_skip() {
        let skip_rule = AnonymizeRule::skip_table("public", "audit_logs");
        assert!(skip_rule.is_skip());

        let column_rule = AnonymizeRule::column("public", "users", "email", "fake_email");
        assert!(!column_rule.is_skip());
    }
}
