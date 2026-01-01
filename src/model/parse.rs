use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::{Materialized, ModelHeader, Relation, Test};

/// Parse a model file into header and body SQL
pub fn parse_model_file(path: &Path) -> Result<(ModelHeader, String)> {
    let text =
        fs::read_to_string(path).with_context(|| format!("read model: {}", path.display()))?;

    let mut header_lines: Vec<&str> = Vec::new();
    let mut body_lines: Vec<&str> = Vec::new();
    let mut lines = text.lines();

    while let Some(line) = lines.next() {
        if line.trim_start().starts_with("--") {
            header_lines.push(line);
            continue;
        }
        if line.trim().is_empty() {
            header_lines.push(line);
            continue;
        }
        body_lines.push(line);
        body_lines.extend(lines);
        break;
    }

    let header = parse_header_block(&header_lines)
        .with_context(|| format!("parse model header: {}", path.display()))?;
    let body_sql = body_lines.join("\n").trim().to_string();
    if body_sql.is_empty() {
        bail!("model body is empty: {}", path.display());
    }

    Ok((header, body_sql))
}

/// Parse header lines into ModelHeader
pub fn parse_header_block(lines: &[&str]) -> Result<ModelHeader> {
    let mut kv: HashMap<String, String> = HashMap::new();
    for line in lines {
        let s = line.trim();
        if !s.starts_with("--") {
            continue;
        }
        let s = s.trim_start_matches("--").trim();
        let Some((k, v)) = s.split_once(':') else {
            continue;
        };
        kv.insert(k.trim().to_string(), v.trim().to_string());
    }

    let materialized = kv
        .get("materialized")
        .ok_or_else(|| anyhow!("missing required header key: materialized"))?;
    let materialized = Materialized::parse(materialized)?;
    let deps = kv
        .get("deps")
        .map(|s| parse_rel_list(s))
        .transpose()?
        .unwrap_or_default();

    let unique_key = kv
        .get("unique_key")
        .map(|s| parse_ident_list(s))
        .transpose()?
        .unwrap_or_default();

    let tests = kv
        .get("tests")
        .map(|s| parse_tests(s))
        .transpose()?
        .unwrap_or_default();

    let tags = kv
        .get("tags")
        .map(|s| parse_tags(s))
        .transpose()?
        .unwrap_or_default();

    if matches!(materialized, Materialized::Incremental) && unique_key.is_empty() {
        bail!("materialized: incremental requires unique_key");
    }

    Ok(ModelHeader {
        materialized,
        deps,
        unique_key,
        tests,
        tags,
    })
}

fn parse_rel_list(s: &str) -> Result<Vec<Relation>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    s.split(',')
        .map(Relation::parse)
        .collect::<Result<Vec<_>>>()
}

fn parse_ident_list(s: &str) -> Result<Vec<String>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    Ok(s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect())
}

/// Parse and validate tags (lowercase, alphanumeric with _ and -)
fn parse_tags(s: &str) -> Result<Vec<String>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut tags = Vec::new();
    for part in s.split(',') {
        let tag = part.trim().to_lowercase();
        if tag.is_empty() {
            continue;
        }

        // Validate tag: only a-z, 0-9, _, -
        if !tag
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
        {
            bail!(
                "invalid tag '{}': tags must contain only lowercase letters, numbers, underscores, and hyphens",
                part.trim()
            );
        }

        tags.push(tag);
    }

    Ok(tags)
}

/// Parse arguments with bracket-awareness (commas inside [] don't split)
fn parse_args_bracket_aware(s: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0;

    for ch in s.chars() {
        match ch {
            '[' => {
                depth += 1;
                current.push(ch);
            }
            ']' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    args.push(trimmed);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if depth != 0 {
        bail!("unbalanced brackets in test arguments: {}", s);
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        args.push(trimmed);
    }

    Ok(args)
}

/// Parse a list of quoted string values: ['val1', 'val2']
/// Handles embedded commas inside quoted strings correctly.
fn parse_string_list(s: &str) -> Result<Vec<String>> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        bail!("expected list syntax ['val1', 'val2'], got: {}", s);
    }
    let inner = &s[1..s.len() - 1]; // strip brackets
    if inner.trim().is_empty() {
        bail!("accepted_values list cannot be empty");
    }

    let mut values = Vec::new();
    let mut chars = inner.chars().peekable();

    while chars.peek().is_some() {
        // Skip whitespace and commas between values
        while let Some(&c) = chars.peek() {
            if c == ' ' || c == ',' || c == '\t' || c == '\n' {
                chars.next();
            } else {
                break;
            }
        }

        let Some(&quote_char) = chars.peek() else {
            break;
        };

        if quote_char != '\'' && quote_char != '"' {
            bail!(
                "values must be quoted with ' or \": got unexpected char '{}'",
                quote_char
            );
        }
        chars.next(); // consume opening quote

        let mut value = String::new();
        let mut found_closing = false;

        while let Some(c) = chars.next() {
            if c == quote_char {
                // Check for escaped quote (doubled quote)
                if chars.peek() == Some(&quote_char) {
                    chars.next(); // consume second quote
                    value.push(quote_char); // add single quote to value
                } else {
                    found_closing = true;
                    break;
                }
            } else {
                value.push(c);
            }
        }

        if !found_closing {
            bail!("unclosed quote in value list");
        }

        values.push(value);
    }

    if values.is_empty() {
        bail!("accepted_values list cannot be empty");
    }

    Ok(values)
}

/// Parse a column reference: schema.table.column
fn parse_column_ref(s: &str) -> Result<(super::Relation, String)> {
    let s = s.trim();
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        bail!(
            "relationships() second argument must be schema.table.column (e.g., 'app.users.id'), got: {}",
            s
        );
    }
    Ok((
        super::Relation {
            schema: parts[0].to_string(),
            name: parts[1].to_string(),
        },
        parts[2].to_string(),
    ))
}

fn parse_tests(s: &str) -> Result<Vec<Test>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut tests = Vec::new();
    let mut remaining = s;

    while !remaining.is_empty() {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }

        // Skip comma separator
        if remaining.starts_with(',') {
            remaining = &remaining[1..];
            continue;
        }

        // Find function name and args: name(args)
        let Some(paren_start) = remaining.find('(') else {
            bail!(
                "invalid test syntax (expected 'test_name(args)'): {}",
                remaining
            );
        };
        let name = remaining[..paren_start].trim().to_lowercase();
        remaining = &remaining[paren_start + 1..];

        // Find matching closing paren (bracket-aware)
        let mut depth = 1;
        let mut paren_end = None;
        for (i, ch) in remaining.char_indices() {
            match ch {
                '(' | '[' => depth += 1,
                ')' | ']' => {
                    depth -= 1;
                    if depth == 0 {
                        paren_end = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(paren_end) = paren_end else {
            bail!("invalid test syntax (missing closing paren): {}", s);
        };

        let args_str = &remaining[..paren_end];
        remaining = &remaining[paren_end + 1..];

        let args = parse_args_bracket_aware(args_str)?;

        if args.is_empty() {
            bail!("test '{}' requires at least one argument", name);
        }

        let test = match name.as_str() {
            "not_null" => {
                if args.len() != 1 {
                    bail!("not_null() takes exactly one column, got {}", args.len());
                }
                Test::NotNull {
                    column: args[0].clone(),
                }
            }
            "unique" => Test::Unique { columns: args },
            "accepted_values" => {
                if args.len() != 2 {
                    bail!(
                        "accepted_values() takes column and list, e.g., accepted_values(status, ['a', 'b'])"
                    );
                }
                let column = args[0].clone();
                let values = parse_string_list(&args[1])?;
                Test::AcceptedValues { column, values }
            }
            "relationships" => {
                if args.len() != 2 {
                    bail!(
                        "relationships() takes column and reference, e.g., relationships(user_id, app.users.id)"
                    );
                }
                let column = args[0].clone();
                let (target_table, target_column) = parse_column_ref(&args[1])?;
                Test::Relationships {
                    column,
                    target_table,
                    target_column,
                }
            }
            _ => bail!("unknown test type: {}. Valid types: not_null, unique, accepted_values, relationships", name),
        };
        tests.push(test);
    }

    Ok(tests)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tests_not_null() {
        let tests = parse_tests("not_null(id)").unwrap();
        assert_eq!(tests.len(), 1);
        assert!(matches!(&tests[0], Test::NotNull { column } if column == "id"));
    }

    #[test]
    fn test_parse_tests_unique() {
        let tests = parse_tests("unique(email)").unwrap();
        assert_eq!(tests.len(), 1);
        assert!(matches!(&tests[0], Test::Unique { columns } if columns == &["email"]));
    }

    #[test]
    fn test_parse_tests_unique_multi_column() {
        let tests = parse_tests("unique(a, b)").unwrap();
        assert_eq!(tests.len(), 1);
        assert!(matches!(&tests[0], Test::Unique { columns } if columns == &["a", "b"]));
    }

    #[test]
    fn test_parse_tests_multiple() {
        let tests = parse_tests("not_null(id), unique(email)").unwrap();
        assert_eq!(tests.len(), 2);
    }

    #[test]
    fn test_parse_header_block_minimal() {
        let lines = vec!["-- materialized: view", "-- deps:"];
        let header = parse_header_block(&lines).unwrap();
        assert_eq!(header.materialized, Materialized::View);
        assert!(header.deps.is_empty());
    }

    #[test]
    fn test_parse_header_block_with_deps() {
        let lines = vec![
            "-- materialized: table",
            "-- deps: staging.orders, staging.users",
        ];
        let header = parse_header_block(&lines).unwrap();
        assert_eq!(header.materialized, Materialized::Table);
        assert_eq!(header.deps.len(), 2);
        assert_eq!(header.deps[0].to_string(), "staging.orders");
    }

    #[test]
    fn test_parse_header_block_incremental_requires_unique_key() {
        let lines = vec!["-- materialized: incremental", "-- deps:"];
        let err = parse_header_block(&lines).unwrap_err();
        assert!(err.to_string().contains("unique_key"));
    }

    #[test]
    fn test_parse_header_block_incremental_with_unique_key() {
        let lines = vec![
            "-- materialized: incremental",
            "-- deps:",
            "-- unique_key: id",
        ];
        let header = parse_header_block(&lines).unwrap();
        assert_eq!(header.materialized, Materialized::Incremental);
        assert_eq!(header.unique_key, vec!["id"]);
    }

    #[test]
    fn test_parse_header_block_missing_materialized() {
        let lines = vec!["-- deps:"];
        let err = parse_header_block(&lines).unwrap_err();
        assert!(err.to_string().contains("materialized"));
    }

    #[test]
    fn test_parse_header_block_missing_deps_defaults_to_empty() {
        let lines = vec!["-- materialized: view"];
        let header = parse_header_block(&lines).unwrap();
        assert_eq!(header.materialized, Materialized::View);
        assert!(header.deps.is_empty());
    }

    #[test]
    fn test_parse_tests_accepted_values() {
        let tests =
            parse_tests("accepted_values(status, ['pending', 'active', 'closed'])").unwrap();
        assert_eq!(tests.len(), 1);
        match &tests[0] {
            Test::AcceptedValues { column, values } => {
                assert_eq!(column, "status");
                assert_eq!(values, &["pending", "active", "closed"]);
            }
            _ => panic!("expected AcceptedValues"),
        }
    }

    #[test]
    fn test_parse_tests_accepted_values_single() {
        let tests = parse_tests("accepted_values(type, ['single'])").unwrap();
        assert_eq!(tests.len(), 1);
        match &tests[0] {
            Test::AcceptedValues { column, values } => {
                assert_eq!(column, "type");
                assert_eq!(values, &["single"]);
            }
            _ => panic!("expected AcceptedValues"),
        }
    }

    #[test]
    fn test_parse_tests_accepted_values_double_quotes() {
        let tests = parse_tests(r#"accepted_values(status, ["a", "b"])"#).unwrap();
        assert_eq!(tests.len(), 1);
        match &tests[0] {
            Test::AcceptedValues { column, values } => {
                assert_eq!(column, "status");
                assert_eq!(values, &["a", "b"]);
            }
            _ => panic!("expected AcceptedValues"),
        }
    }

    #[test]
    fn test_parse_tests_accepted_values_empty_list_error() {
        let err = parse_tests("accepted_values(status, [])").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_parse_tests_accepted_values_unquoted_error() {
        let err = parse_tests("accepted_values(status, [pending, active])").unwrap_err();
        assert!(err.to_string().contains("quoted"));
    }

    #[test]
    fn test_parse_tests_relationships() {
        let tests = parse_tests("relationships(user_id, app.users.id)").unwrap();
        assert_eq!(tests.len(), 1);
        match &tests[0] {
            Test::Relationships {
                column,
                target_table,
                target_column,
            } => {
                assert_eq!(column, "user_id");
                assert_eq!(target_table.schema, "app");
                assert_eq!(target_table.name, "users");
                assert_eq!(target_column, "id");
            }
            _ => panic!("expected Relationships"),
        }
    }

    #[test]
    fn test_parse_tests_relationships_missing_schema_error() {
        let err = parse_tests("relationships(user_id, users.id)").unwrap_err();
        assert!(err.to_string().contains("schema.table.column"));
    }

    #[test]
    fn test_parse_tests_mixed_with_accepted_values() {
        let tests = parse_tests("not_null(id), accepted_values(status, ['a', 'b'])").unwrap();
        assert_eq!(tests.len(), 2);
        assert!(matches!(&tests[0], Test::NotNull { column } if column == "id"));
        assert!(matches!(&tests[1], Test::AcceptedValues { column, .. } if column == "status"));
    }

    #[test]
    fn test_parse_tests_unbalanced_brackets_error() {
        // This produces "missing closing paren" because ] is replaced with )
        // which doesn't match the outer paren's depth
        let err = parse_tests("accepted_values(status, ['a', 'b')").unwrap_err();
        assert!(err.to_string().contains("paren") || err.to_string().contains("bracket"));

        // Test actual unbalanced brackets inside the args
        let err2 = parse_tests("accepted_values(status, ['a', 'b')").unwrap_err();
        assert!(err2.to_string().contains("paren"));
    }

    #[test]
    fn test_parse_args_bracket_aware_unbalanced() {
        let err = parse_args_bracket_aware("status, ['a', 'b'");
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("unbalanced"));
    }

    #[test]
    fn test_parse_tests_case_insensitive() {
        let tests = parse_tests("NOT_NULL(id), ACCEPTED_VALUES(status, ['a'])").unwrap();
        assert_eq!(tests.len(), 2);
        assert!(matches!(&tests[0], Test::NotNull { .. }));
        assert!(matches!(&tests[1], Test::AcceptedValues { .. }));
    }

    #[test]
    fn test_parse_tests_unknown_type_error() {
        let err = parse_tests("invalid_test(col)").unwrap_err();
        assert!(err.to_string().contains("unknown test type"));
    }

    #[test]
    fn test_parse_string_list_embedded_comma() {
        let tests = parse_tests("accepted_values(desc, ['hello, world', 'foo, bar'])").unwrap();
        assert_eq!(tests.len(), 1);
        match &tests[0] {
            Test::AcceptedValues { column, values } => {
                assert_eq!(column, "desc");
                assert_eq!(values, &["hello, world", "foo, bar"]);
            }
            _ => panic!("expected AcceptedValues"),
        }
    }

    #[test]
    fn test_parse_string_list_escaped_quotes() {
        // Doubled quotes inside strings should be parsed as single quotes
        let tests = parse_tests("accepted_values(name, ['it''s', 'O''Brien'])").unwrap();
        assert_eq!(tests.len(), 1);
        match &tests[0] {
            Test::AcceptedValues { column, values } => {
                assert_eq!(column, "name");
                assert_eq!(values, &["it's", "O'Brien"]);
            }
            _ => panic!("expected AcceptedValues"),
        }
    }

    #[test]
    fn test_parse_string_list_unclosed_quote() {
        let err = parse_tests("accepted_values(status, ['unclosed)").unwrap_err();
        assert!(err.to_string().contains("unclosed") || err.to_string().contains("paren"));
    }
}
