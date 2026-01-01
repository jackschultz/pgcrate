//! Seed file discovery, parsing, and schema handling for pgcrate.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

/// Type of seed file
#[derive(Debug, Clone, PartialEq)]
pub enum SeedType {
    /// CSV file with optional schema sidecar
    Csv,
    /// SQL file executed directly
    Sql,
}

/// A discovered seed file
#[derive(Debug, Clone)]
pub struct SeedFile {
    /// Name of the seed (filename without extension)
    pub name: String,
    /// Full path to the seed file
    pub path: PathBuf,
    /// Type of seed (CSV or SQL)
    pub seed_type: SeedType,
    /// Path to schema file if it exists (for CSV seeds)
    pub schema_path: Option<PathBuf>,
}

/// A column definition (from inference or schema file)
#[derive(Debug, Clone)]
pub struct SeedColumn {
    /// Column name
    pub name: String,
    /// PostgreSQL type
    pub pg_type: String,
}

/// Schema definition from .schema.toml file
#[derive(Debug, Deserialize, Default)]
pub struct SeedSchema {
    /// Column type overrides
    #[serde(default)]
    pub columns: HashMap<String, String>,
    /// Primary key columns
    pub primary_key: Option<Vec<String>>,
}

/// Parsed CSV seed ready for loading
#[derive(Debug)]
pub struct ParsedCsvSeed {
    pub name: String,
    pub columns: Vec<SeedColumn>,
    pub rows: Vec<Vec<Option<String>>>,
    pub schema: Option<SeedSchema>,
    /// Raw CSV content for COPY loading
    pub csv_content: String,
}

/// Parsed SQL seed
#[derive(Debug)]
pub struct ParsedSqlSeed {
    pub name: String,
    pub sql: String,
}

/// Unified parsed seed
#[derive(Debug)]
pub enum ParsedSeed {
    Csv(ParsedCsvSeed),
    Sql(ParsedSqlSeed),
}

impl ParsedSeed {
    pub fn name(&self) -> &str {
        match self {
            ParsedSeed::Csv(s) => &s.name,
            ParsedSeed::Sql(s) => &s.name,
        }
    }

    pub fn row_count(&self) -> Option<usize> {
        match self {
            ParsedSeed::Csv(s) => Some(s.rows.len()),
            ParsedSeed::Sql(_) => None, // SQL seeds don't have a known row count
        }
    }
}

/// Discover all seed files in the seeds directory
pub fn discover_seeds(seeds_dir: &Path) -> Result<Vec<SeedFile>> {
    let mut seeds = Vec::new();

    if !seeds_dir.exists() {
        return Ok(seeds);
    }

    for entry in fs::read_dir(seeds_dir)
        .with_context(|| format!("read seeds directory: {}", seeds_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();

        // Skip directories and schema files
        if path.is_dir() {
            continue;
        }

        let ext = path.extension().and_then(|e| e.to_str());
        let stem = path.file_stem().and_then(|s| s.to_str());

        // Skip .schema.toml files (they're sidecars, not seeds)
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(".schema.toml") {
                continue;
            }
        }

        match (ext, stem) {
            (Some("csv"), Some(name)) => {
                // Check for schema sidecar
                let schema_path = seeds_dir.join(format!("{}.schema.toml", name));
                seeds.push(SeedFile {
                    name: name.to_string(),
                    path,
                    seed_type: SeedType::Csv,
                    schema_path: if schema_path.exists() {
                        Some(schema_path)
                    } else {
                        None
                    },
                });
            }
            (Some("sql"), Some(name)) => {
                seeds.push(SeedFile {
                    name: name.to_string(),
                    path,
                    seed_type: SeedType::Sql,
                    schema_path: None,
                });
            }
            _ => {
                // Skip unknown file types
            }
        }
    }

    // Sort by name for consistent ordering (will be reordered by dependencies later)
    seeds.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(seeds)
}

/// Parse a seed file (CSV or SQL)
pub fn parse_seed(seed_file: &SeedFile) -> Result<ParsedSeed> {
    match seed_file.seed_type {
        SeedType::Csv => {
            let parsed = parse_csv_seed(&seed_file.path, seed_file.schema_path.as_deref())?;
            Ok(ParsedSeed::Csv(parsed))
        }
        SeedType::Sql => {
            let parsed = parse_sql_seed(&seed_file.path)?;
            Ok(ParsedSeed::Sql(parsed))
        }
    }
}

/// Parse a CSV seed file with optional schema sidecar
pub fn parse_csv_seed(path: &Path, schema_path: Option<&Path>) -> Result<ParsedCsvSeed> {
    let name = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Load schema file if it exists
    let schema = if let Some(sp) = schema_path {
        let content = fs::read_to_string(sp)
            .with_context(|| format!("read schema file: {}", sp.display()))?;
        let parsed: SeedSchema = toml::from_str(&content)
            .with_context(|| format!("parse schema file: {}", sp.display()))?;
        Some(parsed)
    } else {
        None
    };

    // Read CSV content for COPY
    let csv_content =
        fs::read_to_string(path).with_context(|| format!("read CSV file: {}", path.display()))?;

    // Parse CSV
    let file = File::open(path).with_context(|| format!("open seed file: {}", path.display()))?;
    let mut reader = csv::Reader::from_reader(file);

    // Get headers
    let headers: Vec<String> = reader
        .headers()
        .with_context(|| format!("read CSV headers: {}", path.display()))?
        .iter()
        .map(|h| h.to_string())
        .collect();

    if headers.is_empty() {
        anyhow::bail!("CSV file has no headers: {}", path.display());
    }

    // Read all rows
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for result in reader.records() {
        let record = result.with_context(|| format!("read CSV row: {}", path.display()))?;
        let row: Vec<Option<String>> = record
            .iter()
            .map(|field| {
                let trimmed = field.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .collect();
        rows.push(row);
    }

    // Determine column types (schema overrides inference)
    let sample_size = std::cmp::min(100, rows.len());
    let columns: Vec<SeedColumn> = headers
        .iter()
        .enumerate()
        .map(|(i, name)| {
            // Check schema for explicit type
            let pg_type = schema
                .as_ref()
                .and_then(|s| s.columns.get(name).cloned())
                .unwrap_or_else(|| {
                    // Infer from data
                    let values: Vec<&str> = rows[..sample_size]
                        .iter()
                        .filter_map(|row| row.get(i).and_then(|v| v.as_deref()))
                        .collect();
                    infer_type(&values)
                });
            SeedColumn {
                name: name.clone(),
                pg_type,
            }
        })
        .collect();

    Ok(ParsedCsvSeed {
        name,
        columns,
        rows,
        schema,
        csv_content,
    })
}

/// Parse a SQL seed file
pub fn parse_sql_seed(path: &Path) -> Result<ParsedSqlSeed> {
    let name = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let sql =
        fs::read_to_string(path).with_context(|| format!("read SQL seed: {}", path.display()))?;

    Ok(ParsedSqlSeed { name, sql })
}

/// Infer PostgreSQL type from a sample of string values
pub fn infer_type(values: &[&str]) -> String {
    if values.is_empty() {
        return "text".to_string();
    }

    // Try each type in order of specificity
    if values.iter().all(|v| is_boolean(v)) {
        return "boolean".to_string();
    }

    if values.iter().all(|v| is_uuid(v)) {
        return "uuid".to_string();
    }

    if values.iter().all(|v| is_integer(v)) {
        return "bigint".to_string();
    }

    if values.iter().all(|v| is_numeric(v)) {
        return "numeric".to_string();
    }

    if values.iter().all(|v| is_date(v)) {
        return "date".to_string();
    }

    if values.iter().all(|v| is_timestamp(v)) {
        return "timestamptz".to_string();
    }

    if values.iter().all(|v| is_json(v)) {
        return "jsonb".to_string();
    }

    // Default to text
    "text".to_string()
}

fn is_boolean(s: &str) -> bool {
    let lower = s.to_lowercase();
    lower == "true" || lower == "false" || lower == "t" || lower == "f"
}

fn is_uuid(s: &str) -> bool {
    // UUID format: 8-4-4-4-12 hex chars
    let s = s.trim();
    if s.len() != 36 {
        return false;
    }
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 {
        return false;
    }
    parts[0].len() == 8
        && parts[1].len() == 4
        && parts[2].len() == 4
        && parts[3].len() == 4
        && parts[4].len() == 12
        && parts
            .iter()
            .all(|p| p.chars().all(|c| c.is_ascii_hexdigit()))
}

fn is_integer(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    let s = s.strip_prefix('-').unwrap_or(s);
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}

fn is_numeric(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    let s = s.strip_prefix('-').unwrap_or(s);
    if s.is_empty() {
        return false;
    }

    let mut seen_dot = false;
    for c in s.chars() {
        if c == '.' {
            if seen_dot {
                return false;
            }
            seen_dot = true;
        } else if !c.is_ascii_digit() {
            return false;
        }
    }
    true
}

fn is_date(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return false;
    }
    parts[0].len() == 4
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && parts[1].len() == 2
        && parts[1].chars().all(|c| c.is_ascii_digit())
        && parts[2].len() == 2
        && parts[2].chars().all(|c| c.is_ascii_digit())
}

fn is_timestamp(s: &str) -> bool {
    let s = s.replace('T', " ");
    // Remove timezone suffix if present
    let s = s.split('+').next().unwrap_or(&s);
    let s = s.split('Z').next().unwrap_or(s);

    let parts: Vec<&str> = s.split(' ').collect();
    if parts.len() != 2 {
        return false;
    }
    if !is_date(parts[0]) {
        return false;
    }
    let time = parts[1].split('.').next().unwrap_or("");
    let time_parts: Vec<&str> = time.split(':').collect();
    if time_parts.len() < 2 {
        return false;
    }
    time_parts
        .iter()
        .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_digit()))
}

fn is_json(s: &str) -> bool {
    let s = s.trim();
    (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_type_boolean() {
        assert_eq!(infer_type(&["true", "false", "true"]), "boolean");
        assert_eq!(infer_type(&["True", "FALSE", "t", "f"]), "boolean");
    }

    #[test]
    fn test_infer_type_uuid() {
        assert_eq!(
            infer_type(&[
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
            ]),
            "uuid"
        );
    }

    #[test]
    fn test_infer_type_integer() {
        assert_eq!(infer_type(&["1", "2", "100", "-5"]), "bigint");
        assert_eq!(infer_type(&["0", "999999999999"]), "bigint");
    }

    #[test]
    fn test_infer_type_numeric() {
        assert_eq!(infer_type(&["1.5", "2.0", "100.99"]), "numeric");
        assert_eq!(infer_type(&["-3.14", "0.001"]), "numeric");
    }

    #[test]
    fn test_infer_type_date() {
        assert_eq!(infer_type(&["2024-01-01", "2024-12-31"]), "date");
    }

    #[test]
    fn test_infer_type_timestamp() {
        assert_eq!(
            infer_type(&["2024-01-01T10:30:00", "2024-12-31 23:59:59"]),
            "timestamptz"
        );
        assert_eq!(
            infer_type(&["2024-01-01T10:30:00Z", "2024-12-31 23:59:59+00"]),
            "timestamptz"
        );
    }

    #[test]
    fn test_infer_type_json() {
        assert_eq!(
            infer_type(&[r#"{"key": "value"}"#, r#"{"nested": {"a": 1}}"#]),
            "jsonb"
        );
        assert_eq!(infer_type(&["[1, 2, 3]", r#"["a", "b"]"#]), "jsonb");
    }

    #[test]
    fn test_infer_type_text_fallback() {
        assert_eq!(infer_type(&["hello", "world"]), "text");
        assert_eq!(infer_type(&["1", "two", "3"]), "text");
    }

    #[test]
    fn test_infer_type_empty() {
        assert_eq!(infer_type(&[]), "text");
    }

    #[test]
    fn test_is_boolean() {
        assert!(is_boolean("true"));
        assert!(is_boolean("false"));
        assert!(is_boolean("True"));
        assert!(is_boolean("FALSE"));
        assert!(is_boolean("t"));
        assert!(is_boolean("f"));
        assert!(!is_boolean("yes"));
        assert!(!is_boolean("1"));
    }

    #[test]
    fn test_is_uuid() {
        assert!(is_uuid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(is_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8"));
        assert!(!is_uuid("not-a-uuid"));
        assert!(!is_uuid("550e8400e29b41d4a716446655440000")); // No dashes
    }

    #[test]
    fn test_is_integer() {
        assert!(is_integer("123"));
        assert!(is_integer("-456"));
        assert!(is_integer("0"));
        assert!(!is_integer("12.34"));
        assert!(!is_integer("abc"));
        assert!(!is_integer(""));
    }

    #[test]
    fn test_is_numeric() {
        assert!(is_numeric("123"));
        assert!(is_numeric("12.34"));
        assert!(is_numeric("-456.789"));
        assert!(is_numeric("0.001"));
        assert!(!is_numeric("12.34.56"));
        assert!(!is_numeric("abc"));
    }

    #[test]
    fn test_is_date() {
        assert!(is_date("2024-01-15"));
        assert!(is_date("1999-12-31"));
        assert!(!is_date("2024-1-15"));
        assert!(!is_date("24-01-15"));
        assert!(!is_date("2024/01/15"));
    }

    #[test]
    fn test_is_timestamp() {
        assert!(is_timestamp("2024-01-15T10:30:00"));
        assert!(is_timestamp("2024-01-15 10:30:00"));
        assert!(is_timestamp("2024-01-15T10:30:00.123"));
        assert!(is_timestamp("2024-01-15T10:30:00Z"));
        assert!(is_timestamp("2024-01-15T10:30:00+00"));
        assert!(!is_timestamp("2024-01-15"));
        assert!(!is_timestamp("10:30:00"));
    }

    #[test]
    fn test_is_json() {
        assert!(is_json(r#"{"key": "value"}"#));
        assert!(is_json("[1, 2, 3]"));
        assert!(!is_json("not json"));
        assert!(!is_json("{incomplete"));
    }

    #[test]
    fn test_parse_schema_toml() {
        let toml_str = r#"
            primary_key = ["code"]

            [columns]
            code = "char(2)"
            name = "text"
            population = "bigint"
        "#;
        let schema: SeedSchema = toml::from_str(toml_str).unwrap();
        assert_eq!(schema.columns.get("code"), Some(&"char(2)".to_string()));
        assert_eq!(schema.primary_key, Some(vec!["code".to_string()]));
    }
}
