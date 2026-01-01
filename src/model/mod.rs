mod compile;
mod dag;
mod execute;
pub mod lint;
mod parse;
pub mod select;

use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;

pub use compile::{compile_model, generate_run_sql};
pub use dag::{get_downstream_order, get_upstream_order, load_project, topo_sort};
pub use execute::execute_model;
pub use lint::{lint_deps, qualify_model_sql, rewrite_deps_line, rewrite_model_body_sql};
pub use parse::parse_model_file;
pub use select::apply_selectors;

/// A schema-qualified relation (schema.name)
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Relation {
    pub schema: String,
    pub name: String,
}

impl fmt::Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

impl Relation {
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        let mut parts = s.split('.');
        let schema = parts.next().unwrap_or("");
        let name = parts.next().unwrap_or("");
        if schema.is_empty() || name.is_empty() || parts.next().is_some() {
            bail!("invalid relation (expected schema.table): {s}");
        }
        Ok(Self {
            schema: schema.to_string(),
            name: name.to_string(),
        })
    }
}

/// How a model is materialized in the database
#[derive(Clone, Debug, PartialEq)]
pub enum Materialized {
    View,
    Table,
    Incremental,
}

impl Materialized {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim() {
            "view" => Ok(Self::View),
            "table" => Ok(Self::Table),
            "incremental" => Ok(Self::Incremental),
            other => bail!("invalid materialized value: {other}"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Materialized::View => "view",
            Materialized::Table => "table",
            Materialized::Incremental => "incremental",
        }
    }
}

/// A data test defined in model header
#[derive(Clone, Debug)]
pub enum Test {
    NotNull {
        column: String,
    },
    Unique {
        columns: Vec<String>,
    },
    AcceptedValues {
        column: String,
        values: Vec<String>,
    },
    Relationships {
        column: String,
        target_table: Relation,
        target_column: String,
    },
}

/// Escape a string value for SQL (double single quotes)
fn sql_escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Quote a SQL identifier (column/table name) with double quotes
fn sql_quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

impl Test {
    /// Generate SQL to run this test. Returns rows if test FAILS.
    pub fn to_sql(&self, model: &Relation) -> String {
        match self {
            Test::NotNull { column } => {
                format!(
                    "SELECT COUNT(*) as violations FROM {} WHERE {} IS NULL",
                    model,
                    sql_quote_ident(column)
                )
            }
            Test::Unique { columns } => {
                let cols: Vec<String> = columns.iter().map(|c| sql_quote_ident(c)).collect();
                let cols_str = cols.join(", ");
                format!(
                    "SELECT {cols_str}, COUNT(*) as cnt FROM {model} GROUP BY {cols_str} HAVING COUNT(*) > 1"
                )
            }
            Test::AcceptedValues { column, values } => {
                let escaped: Vec<String> = values
                    .iter()
                    .map(|v| format!("'{}'", sql_escape_string(v)))
                    .collect();
                format!(
                    "SELECT COUNT(*) as violations FROM {} WHERE {} NOT IN ({})",
                    model,
                    sql_quote_ident(column),
                    escaped.join(", ")
                )
            }
            Test::Relationships {
                column,
                target_table,
                target_column,
            } => {
                format!(
                    "SELECT COUNT(*) as violations FROM {} m \
                     WHERE m.{} IS NOT NULL \
                     AND NOT EXISTS (SELECT 1 FROM {} t WHERE t.{} = m.{})",
                    model,
                    sql_quote_ident(column),
                    target_table,
                    sql_quote_ident(target_column),
                    sql_quote_ident(column)
                )
            }
        }
    }

    pub fn description(&self) -> String {
        match self {
            Test::NotNull { column } => format!("not_null({})", column),
            Test::Unique { columns } => format!("unique({})", columns.join(", ")),
            Test::AcceptedValues { column, values } => {
                format!("accepted_values({}, [{}])", column, values.join(", "))
            }
            Test::Relationships {
                column,
                target_table,
                target_column,
            } => {
                format!(
                    "relationships({}, {}.{})",
                    column, target_table, target_column
                )
            }
        }
    }
}

/// Parsed model header from SQL comments
#[derive(Clone, Debug)]
pub struct ModelHeader {
    pub materialized: Materialized,
    pub deps: Vec<Relation>,
    pub unique_key: Vec<String>,
    pub tests: Vec<Test>,
    pub tags: Vec<String>,
}

/// A SQL model with its metadata
#[derive(Clone, Debug)]
pub struct Model {
    pub id: Relation,
    pub path: PathBuf,
    pub header: ModelHeader,
    pub body_sql: String,
}

/// A collection of models and their sources
#[derive(Clone, Debug)]
pub struct Project {
    pub root: PathBuf,
    pub models: HashMap<Relation, Model>,
    pub sources: HashSet<Relation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relation_parse_valid() {
        let r = Relation::parse("analytics.users").unwrap();
        assert_eq!(r.schema, "analytics");
        assert_eq!(r.name, "users");
    }

    #[test]
    fn test_relation_parse_no_dot() {
        let err = Relation::parse("nodot").unwrap_err();
        assert!(err.to_string().contains("expected schema"));
    }

    #[test]
    fn test_relation_parse_too_many_dots() {
        let err = Relation::parse("a.b.c").unwrap_err();
        assert!(err.to_string().contains("expected schema"));
    }

    #[test]
    fn test_relation_display() {
        let r = Relation {
            schema: "s".into(),
            name: "n".into(),
        };
        assert_eq!(format!("{}", r), "s.n");
    }

    #[test]
    fn test_materialized_parse_view() {
        assert_eq!(Materialized::parse("view").unwrap(), Materialized::View);
    }

    #[test]
    fn test_materialized_parse_table() {
        assert_eq!(Materialized::parse("table").unwrap(), Materialized::Table);
    }

    #[test]
    fn test_materialized_parse_incremental() {
        assert_eq!(
            Materialized::parse("incremental").unwrap(),
            Materialized::Incremental
        );
    }

    #[test]
    fn test_materialized_parse_invalid() {
        let err = Materialized::parse("unknown").unwrap_err();
        assert!(err.to_string().contains("invalid materialized"));
    }

    #[test]
    fn test_to_sql_not_null() {
        let test = Test::NotNull {
            column: "id".into(),
        };
        let model = Relation {
            schema: "analytics".into(),
            name: "users".into(),
        };
        let sql = test.to_sql(&model);
        assert!(sql.contains("analytics.users"));
        assert!(sql.contains("\"id\" IS NULL"));
    }

    #[test]
    fn test_to_sql_unique() {
        let test = Test::Unique {
            columns: vec!["email".into()],
        };
        let model = Relation {
            schema: "analytics".into(),
            name: "users".into(),
        };
        let sql = test.to_sql(&model);
        assert!(sql.contains("analytics.users"));
        assert!(sql.contains("\"email\""));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("HAVING COUNT(*) > 1"));
    }

    #[test]
    fn test_to_sql_accepted_values() {
        let test = Test::AcceptedValues {
            column: "status".into(),
            values: vec!["pending".into(), "active".into(), "closed".into()],
        };
        let model = Relation {
            schema: "analytics".into(),
            name: "orders".into(),
        };
        let sql = test.to_sql(&model);
        assert!(sql.contains("analytics.orders"));
        assert!(sql.contains("\"status\" NOT IN"));
        assert!(sql.contains("'pending'"));
        assert!(sql.contains("'active'"));
        assert!(sql.contains("'closed'"));
    }

    #[test]
    fn test_to_sql_accepted_values_escapes_quotes() {
        let test = Test::AcceptedValues {
            column: "name".into(),
            values: vec!["it's".into(), "O'Brien".into()],
        };
        let model = Relation {
            schema: "app".into(),
            name: "users".into(),
        };
        let sql = test.to_sql(&model);
        // Single quotes should be doubled for SQL escaping
        assert!(sql.contains("'it''s'"));
        assert!(sql.contains("'O''Brien'"));
    }

    #[test]
    fn test_to_sql_relationships() {
        let test = Test::Relationships {
            column: "user_id".into(),
            target_table: Relation {
                schema: "app".into(),
                name: "users".into(),
            },
            target_column: "id".into(),
        };
        let model = Relation {
            schema: "analytics".into(),
            name: "orders".into(),
        };
        let sql = test.to_sql(&model);
        assert!(sql.contains("analytics.orders"));
        assert!(sql.contains("m.\"user_id\" IS NOT NULL"));
        assert!(sql.contains("NOT EXISTS"));
        assert!(sql.contains("app.users"));
        assert!(sql.contains("t.\"id\" = m.\"user_id\""));
    }

    #[test]
    fn test_sql_quote_ident_special_chars() {
        // Verify identifier quoting handles edge cases
        assert_eq!(sql_quote_ident("simple"), "\"simple\"");
        assert_eq!(sql_quote_ident("has space"), "\"has space\"");
        assert_eq!(sql_quote_ident("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn test_description_accepted_values() {
        let test = Test::AcceptedValues {
            column: "status".into(),
            values: vec!["a".into(), "b".into()],
        };
        assert_eq!(test.description(), "accepted_values(status, [a, b])");
    }

    #[test]
    fn test_description_relationships() {
        let test = Test::Relationships {
            column: "user_id".into(),
            target_table: Relation {
                schema: "app".into(),
                name: "users".into(),
            },
            target_column: "id".into(),
        };
        assert_eq!(test.description(), "relationships(user_id, app.users.id)");
    }
}
