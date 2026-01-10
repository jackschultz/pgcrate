use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

use super::{Materialized, Model, Project};

/// Output from compiling a model
#[derive(Debug)]
pub struct CompileOutput {
    pub output_path: PathBuf,
}

/// Compile a model to target/compiled/<schema>/<name>.sql
pub fn compile_model(project: &Project, model: &Model) -> Result<CompileOutput> {
    let body = model.body_sql.trim().trim_end_matches(';').trim();
    let compiled_sql = match model.header.materialized {
        Materialized::View => format!("CREATE OR REPLACE VIEW {} AS\n{};\n", model.id, body),
        Materialized::Table => format!("CREATE TABLE {} AS\n{};\n", model.id, body),
        Materialized::Incremental => {
            let uk = model.header.unique_key.join(", ");
            format!(
                "-- incremental model: {} (unique_key: {})\n-- Note: MERGE is generated at runtime in execute.rs\n{};\n",
                model.id, uk, body
            )
        }
    };

    let out_dir = project.root.join("target/compiled").join(&model.id.schema);
    fs::create_dir_all(&out_dir)
        .with_context(|| format!("create compile dir: {}", out_dir.display()))?;
    let output_path = out_dir.join(format!("{}.sql", model.id.name));
    fs::write(&output_path, &compiled_sql)
        .with_context(|| format!("write compiled SQL: {}", output_path.display()))?;

    Ok(CompileOutput { output_path })
}

/// Generate the CREATE SQL only (without DROP statements)
/// Used by execute.rs which handles drops separately due to PostgreSQL transaction visibility.
pub fn generate_create_sql(model: &Model) -> String {
    let body = model.body_sql.trim().trim_end_matches(';').trim();
    match model.header.materialized {
        Materialized::View => {
            format!("CREATE OR REPLACE VIEW {} AS\n{}", model.id, body)
        }
        Materialized::Table | Materialized::Incremental => {
            format!("CREATE TABLE {} AS\n{}", model.id, body)
        }
    }
}

/// Generate full SQL to execute a model (drops + create, for compile output and display)
pub fn generate_run_sql(model: &Model) -> String {
    let create_sql = generate_create_sql(model);
    format!(
        "DROP VIEW IF EXISTS {} CASCADE;\nDROP TABLE IF EXISTS {} CASCADE;\n{}",
        model.id, model.id, create_sql
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelHeader, Relation};
    use std::path::PathBuf;

    fn make_model(mat: Materialized, body: &str) -> Model {
        Model {
            id: Relation {
                schema: "analytics".into(),
                name: "users".into(),
            },
            path: PathBuf::new(),
            header: ModelHeader {
                materialized: mat,
                deps: Vec::new(),
                unique_key: Vec::new(),
                tests: Vec::new(),
                tags: Vec::new(),
                watermark: None,
                lookback: None,
                incremental_filter: None,
            },
            body_sql: body.into(),
            base_sql: None,
            incremental_sql: None,
        }
    }

    #[test]
    fn test_run_sql_view() {
        let model = make_model(Materialized::View, "SELECT 1");
        let sql = generate_run_sql(&model);
        assert!(sql.contains("CREATE OR REPLACE VIEW analytics.users"));
        assert!(sql.contains("SELECT 1"));
        // Views now drop both to handle any existing object type
        assert!(sql.contains("DROP VIEW IF EXISTS analytics.users CASCADE"));
        assert!(sql.contains("DROP TABLE IF EXISTS analytics.users CASCADE"));
    }

    #[test]
    fn test_run_sql_table() {
        let model = make_model(Materialized::Table, "SELECT 1");
        let sql = generate_run_sql(&model);
        // Tables now drop both views and tables to handle materialization type changes
        assert!(sql.contains("DROP VIEW IF EXISTS analytics.users CASCADE"));
        assert!(sql.contains("DROP TABLE IF EXISTS analytics.users CASCADE"));
        assert!(sql.contains("CREATE TABLE analytics.users AS"));
    }

    #[test]
    fn test_run_sql_incremental_as_table() {
        let mut model = make_model(Materialized::Incremental, "SELECT 1");
        model.header.unique_key = vec!["id".into()];
        let sql = generate_run_sql(&model);
        // MVP: incremental treated as full refresh with view/table cleanup
        assert!(sql.contains("DROP VIEW IF EXISTS"));
        assert!(sql.contains("DROP TABLE IF EXISTS"));
        assert!(sql.contains("CREATE TABLE"));
    }

    #[test]
    fn test_run_sql_strips_semicolon() {
        let model = make_model(Materialized::View, "SELECT 1;");
        let sql = generate_run_sql(&model);
        assert!(!sql.contains(";;"));
        assert!(sql.ends_with("SELECT 1"));
    }
}
