use anyhow::{Context, Result};
use regex::Regex;
use serde::Serialize;
use std::fmt;
use tokio_postgres::{error::ErrorPosition, error::SqlState, Client, Error as PgError};

use super::{generate_create_sql, Materialized, Model};
use crate::sql::quote_ident;
use crate::suggest::{best_match, levenshtein};

/// Get PostgreSQL major version as an integer (e.g., 17 for "17.2")
async fn get_pg_major_version(client: &Client) -> Result<u32> {
    let row = client
        .query_one("SHOW server_version_num", &[])
        .await
        .context("get server version")?;
    let version_num: String = row.get(0);
    // server_version_num is XXYYZZ format (e.g., 170002 for 17.0.2)
    let version: u32 = version_num.parse().unwrap_or(0);
    Ok(version / 10000)
}

#[derive(Debug, Default, Clone)]
pub struct ExecuteResult {
    pub rows_affected: Option<u64>,
    pub incremental: Option<IncrementalSummary>,
}

#[derive(Debug, Clone)]
pub struct IncrementalSummary {
    pub action: IncrementalAction,
    pub inserted: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAction {
    CreatedTable,
    Merged,
    Upserted,
}

#[derive(Debug, Serialize)]
pub struct ModelExecutionError {
    pub model: ModelExecutionModel,
    pub error: ModelExecutionErrorDetails,
    pub sql: ModelExecutionSql,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ModelExecutionModel {
    pub id: String,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct ModelExecutionErrorDetails {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ModelExecutionSql {
    pub preview: String,
}

pub async fn ensure_schema(client: &Client, schema: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            &[&schema],
        )
        .await
        .context("check schema exists")?;
    let exists: bool = row.get(0);
    if exists {
        return Ok(false);
    }

    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(schema));
    client
        .execute(&create_schema, &[])
        .await
        .with_context(|| format!("create schema: {}", schema))?;
    Ok(true)
}

/// Execute a single model against the database.
pub async fn execute_model(
    client: &Client,
    model: &Model,
    full_refresh: bool,
) -> Result<ExecuteResult> {
    // Handle incremental models specially
    if matches!(model.header.materialized, Materialized::Incremental) {
        let summary = execute_incremental(client, model, full_refresh).await?;
        return Ok(ExecuteResult {
            rows_affected: None,
            incremental: Some(summary),
        });
    }

    // Drop any existing object with this name (could be view or table from previous runs)
    // Note: In PostgreSQL 18+, DROP VIEW IF EXISTS fails with 42809 if object is a TABLE,
    // and DROP TABLE IF EXISTS fails with 42809 if object is a VIEW.
    // So we must check the object type first and drop appropriately.
    let existing_view = view_exists(client, &model.id.schema, &model.id.name).await?;
    let existing_table = table_exists(client, &model.id.schema, &model.id.name).await?;

    if existing_view {
        let drop_sql = format!(
            "DROP VIEW {}.{} CASCADE",
            quote_ident(&model.id.schema),
            quote_ident(&model.id.name)
        );
        if let Err(e) = client.batch_execute(&drop_sql).await {
            return Err(build_model_execution_error(client, model, &drop_sql, &e)
                .await
                .into());
        }
    } else if existing_table {
        let drop_sql = format!(
            "DROP TABLE {}.{} CASCADE",
            quote_ident(&model.id.schema),
            quote_ident(&model.id.name)
        );
        if let Err(e) = client.batch_execute(&drop_sql).await {
            return Err(build_model_execution_error(client, model, &drop_sql, &e)
                .await
                .into());
        }
    }

    let create_sql = generate_create_sql(model);
    if let Err(e) = client.batch_execute(&create_sql).await {
        return Err(build_model_execution_error(client, model, &create_sql, &e)
            .await
            .into());
    }

    Ok(ExecuteResult {
        rows_affected: None,
        incremental: None,
    })
}

/// Check if a table exists in the database
async fn table_exists(client: &Client, schema: &str, name: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &name],
        )
        .await
        .context("check table exists")?;
    Ok(row.get(0))
}

/// Check if a view exists in the database
async fn view_exists(client: &Client, schema: &str, name: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.views
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &name],
        )
        .await
        .context("check view exists")?;
    Ok(row.get(0))
}

/// Get column names for an existing table
async fn get_table_columns(client: &Client, schema: &str, name: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT column_name FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema, &name],
        )
        .await
        .context("get table columns")?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Execute an incremental model using MERGE (PostgreSQL 17+)
async fn execute_incremental(
    client: &Client,
    model: &Model,
    full_refresh: bool,
) -> Result<IncrementalSummary> {
    let unique_key = &model.header.unique_key;

    let table_exists = table_exists(client, &model.id.schema, &model.id.name).await?;
    let view_exists = view_exists(client, &model.id.schema, &model.id.name).await?;

    if table_exists && !full_refresh {
        // Subsequent run: determine SQL to use based on model config
        // Priority: @incremental section > watermark filter > incremental_filter > body as-is
        let body = if model.incremental_sql.is_some() {
            // Has @incremental section - use it with ${this} substitution
            model.incremental_run_sql()
        } else if let Some(filter) = model.watermark_filter_sql() {
            // Has watermark - wrap body with filter
            // We wrap as: SELECT * FROM (body) AS __watermark_source WHERE filter
            let base = model.body_sql.trim().trim_end_matches(';');
            format!(
                "SELECT * FROM ({}) AS __watermark_source WHERE {}",
                base, filter
            )
        } else if let Some(ref filter) = model.header.incremental_filter {
            // Has custom incremental_filter - wrap body with filter
            let base = model.body_sql.trim().trim_end_matches(';');
            format!(
                "SELECT * FROM ({}) AS __filter_source WHERE {}",
                base, filter
            )
        } else {
            // No sections, no watermark, no filter - use body as-is (Level 1: full scan)
            model.body_sql.clone()
        };
        let body = body.trim().trim_end_matches(';').trim();

        let pg_version = get_pg_major_version(client).await?;
        let columns = get_table_columns(client, &model.id.schema, &model.id.name).await?;

        if pg_version >= 17 {
            // PostgreSQL 17+: Use MERGE with RETURNING merge_action()
            let merge_sql = generate_merge_sql(model, &columns, body, unique_key);
            let counts_sql = wrap_merge_for_counts(&merge_sql);
            let row = client
                .query_one(&counts_sql, &[])
                .await
                .with_context(|| format!("merge into {}", model.id))?;
            let inserted: i64 = row.get(0);
            Ok(IncrementalSummary {
                action: IncrementalAction::Merged,
                inserted: inserted.max(0) as u64,
            })
        } else {
            // PostgreSQL 9.5-16: Use INSERT ON CONFLICT (upsert)
            let upsert_sql = generate_upsert_sql(model, &columns, body, unique_key);
            let row = client
                .query_one(&upsert_sql, &[])
                .await
                .with_context(|| format!("upsert into {}", model.id))?;
            let affected: i64 = row.get(0);
            Ok(IncrementalSummary {
                action: IncrementalAction::Upserted,
                inserted: affected.max(0) as u64,
            })
        }
    } else {
        // First run OR full refresh: use @base section
        let body = model.first_run_sql();
        let body = body.trim().trim_end_matches(';').trim();

        // Drop view first if it exists (handles view->table materialization change)
        if view_exists {
            let drop_view_sql = format!(
                "DROP VIEW {}.{} CASCADE",
                quote_ident(&model.id.schema),
                quote_ident(&model.id.name)
            );
            if let Err(e) = client.batch_execute(&drop_view_sql).await {
                return Err(
                    build_model_execution_error(client, model, &drop_view_sql, &e)
                        .await
                        .into(),
                );
            }
        }
        if table_exists {
            let drop_sql = format!(
                "DROP TABLE {}.{} CASCADE",
                quote_ident(&model.id.schema),
                quote_ident(&model.id.name)
            );
            if let Err(e) = client.batch_execute(&drop_sql).await {
                return Err(build_model_execution_error(client, model, &drop_sql, &e)
                    .await
                    .into());
            }
        }
        let sql = generate_first_run_sql(model, body, unique_key);
        if let Err(e) = client.batch_execute(&sql).await {
            return Err(build_model_execution_error(client, model, &sql, &e)
                .await
                .into());
        }

        let count_sql = format!(
            "SELECT COUNT(*) FROM {}.{}",
            quote_ident(&model.id.schema),
            quote_ident(&model.id.name)
        );
        let row = client
            .query_one(&count_sql, &[])
            .await
            .with_context(|| format!("count rows for {}", model.id))?;
        let count: i64 = row.get(0);
        Ok(IncrementalSummary {
            action: IncrementalAction::CreatedTable,
            inserted: count.max(0) as u64,
        })
    }
}

/// Generate SQL for first run of incremental model
pub fn generate_first_run_sql(model: &Model, body: &str, unique_key: &[String]) -> String {
    let qualified_table = format!(
        "{}.{}",
        quote_ident(&model.id.schema),
        quote_ident(&model.id.name)
    );
    let pk_cols: Vec<String> = unique_key.iter().map(|k| quote_ident(k)).collect();
    let constraint_name = format!("{}_pkey", model.id.name);
    format!(
        "CREATE TABLE {} AS\n{};\n\
         ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({});",
        qualified_table,
        body,
        qualified_table,
        quote_ident(&constraint_name),
        pk_cols.join(", ")
    )
}

/// Generate MERGE SQL for incremental model (PostgreSQL 15+)
pub fn generate_merge_sql(
    model: &Model,
    columns: &[String],
    body: &str,
    unique_key: &[String],
) -> String {
    let qualified_table = format!(
        "{}.{}",
        quote_ident(&model.id.schema),
        quote_ident(&model.id.name)
    );

    // Build ON clause: t."key1" = s."key1" AND t."key2" = s."key2"
    let on_clause: Vec<String> = unique_key
        .iter()
        .map(|k| format!("t.{} = s.{}", quote_ident(k), quote_ident(k)))
        .collect();

    // Build UPDATE SET clause: "col1" = s."col1", "col2" = s."col2" (excluding keys)
    let update_cols: Vec<String> = columns
        .iter()
        .filter(|c| !unique_key.contains(c))
        .map(|c| format!("{} = s.{}", quote_ident(c), quote_ident(c)))
        .collect();

    // Build INSERT column list and values
    let insert_cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let insert_vals: Vec<String> = columns
        .iter()
        .map(|c| format!("s.{}", quote_ident(c)))
        .collect();

    let mut sql = format!(
        "MERGE INTO {} AS t\nUSING (\n{}\n) AS s\nON {}\n",
        qualified_table,
        body,
        on_clause.join(" AND ")
    );

    if !update_cols.is_empty() {
        sql.push_str(&format!(
            "WHEN MATCHED THEN UPDATE SET {}\n",
            update_cols.join(", ")
        ));
    }

    sql.push_str(&format!(
        "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
        insert_cols.join(", "),
        insert_vals.join(", ")
    ));

    // Capture inserted vs updated counts via RETURNING.
    // Note: `merge_action()` is available in PostgreSQL 15+ for MERGE RETURNING.
    sql.push_str("\nRETURNING merge_action() AS action;");

    sql
}

fn wrap_merge_for_counts(merge_sql: &str) -> String {
    let merge_sql = merge_sql.trim_end_matches(';');
    format!(
        "WITH m AS (\n{}\n)\nSELECT\n  COUNT(*) FILTER (WHERE action = 'INSERT')::bigint AS inserted\nFROM m;",
        merge_sql
    )
}

/// Generate INSERT ON CONFLICT SQL for incremental model (PostgreSQL 9.5+)
/// Uses a CTE to count affected rows since INSERT ON CONFLICT doesn't distinguish
/// inserts from updates in its row count.
pub fn generate_upsert_sql(
    model: &Model,
    columns: &[String],
    body: &str,
    unique_key: &[String],
) -> String {
    let qualified_table = format!(
        "{}.{}",
        quote_ident(&model.id.schema),
        quote_ident(&model.id.name)
    );

    // Build column list
    let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();

    // Build UPDATE SET clause for non-key columns: col = EXCLUDED.col
    let update_cols: Vec<String> = columns
        .iter()
        .filter(|c| !unique_key.contains(c))
        .map(|c| format!("{} = EXCLUDED.{}", quote_ident(c), quote_ident(c)))
        .collect();

    // Build conflict target (the unique key columns)
    let conflict_cols: Vec<String> = unique_key.iter().map(|k| quote_ident(k)).collect();

    // Build the upsert with a CTE to count rows
    format!(
        "WITH source AS (\n{}\n),\nupserted AS (\n  INSERT INTO {} ({})\n  SELECT {} FROM source\n  ON CONFLICT ({}) DO UPDATE SET {}\n  RETURNING 1\n)\nSELECT COUNT(*)::bigint FROM upserted;",
        body,
        qualified_table,
        col_list.join(", "),
        col_list.join(", "),
        conflict_cols.join(", "),
        update_cols.join(", ")
    )
}

async fn build_model_execution_error(
    client: &Client,
    model: &Model,
    sql: &str,
    err: &PgError,
) -> ModelExecutionError {
    let preview = sql_preview(sql, 600);
    let (message, sqlstate, position) = pg_error_details(err);

    let mut hints = vec![
        format!("Edit: {}", model.path.display()),
        format!("Rerun: pgcrate model run -s {}", model.id),
        "Tip: run with --json for machine-readable errors".to_string(),
        "Tip: run with --help-llm for agent-oriented docs".to_string(),
    ];

    let mut suggestions: Vec<String> = Vec::new();

    if let Some(state) = sqlstate.as_deref() {
        if state == SqlState::UNDEFINED_TABLE.code() {
            if let Some(missing) = extract_missing_relation(&message) {
                suggestions.extend(
                    suggest_relations(client, &missing)
                        .await
                        .unwrap_or_default(),
                );
            }
        } else if state == SqlState::INVALID_SCHEMA_NAME.code() {
            if let Some(missing) = extract_missing_schema(&message) {
                let (schemas, maybe) = suggest_schemas(client, &missing).await.unwrap_or_default();
                if let Some(best) = maybe {
                    suggestions.push(best);
                } else if !schemas.is_empty() {
                    hints.push(format!("Schemas found: {}", schemas.join(", ")));
                }
            }
        } else if state == SqlState::UNDEFINED_COLUMN.code() {
            suggestions.extend(suggest_columns(client, err).await.unwrap_or_default());
        }
    }

    suggestions.sort();
    suggestions.dedup();
    suggestions.truncate(3);

    hints.retain(|h| !h.trim().is_empty());

    ModelExecutionError {
        model: ModelExecutionModel {
            id: model.id.to_string(),
            path: model.path.display().to_string(),
        },
        error: ModelExecutionErrorDetails {
            message,
            sqlstate,
            position,
        },
        sql: ModelExecutionSql { preview },
        hints,
        suggestions,
    }
}

fn pg_error_details(err: &PgError) -> (String, Option<String>, Option<u32>) {
    if let Some(db) = err.as_db_error() {
        let sqlstate = Some(db.code().code().to_string());
        let pos = db.position().and_then(|p| match p {
            ErrorPosition::Original(pos) => Some(*pos),
            _ => None,
        });
        (db.message().to_string(), sqlstate, pos)
    } else {
        (err.to_string(), None, None)
    }
}

fn sql_preview(sql: &str, max_chars: usize) -> String {
    let s = sql.trim();
    if s.len() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push_str("...");
    out
}

fn extract_missing_relation(message: &str) -> Option<String> {
    // Common: relation "orders" does not exist
    let re = Regex::new(r#"relation "([^"]+)" does not exist"#).ok()?;
    re.captures(message)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
}

fn extract_missing_schema(message: &str) -> Option<String> {
    // Common: schema "analytics" does not exist
    let re = Regex::new(r#"schema "([^"]+)" does not exist"#).ok()?;
    re.captures(message)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
}

async fn suggest_schemas(client: &Client, missing: &str) -> Result<(Vec<String>, Option<String>)> {
    let rows = client
        .query(
            "SELECT schema_name
             FROM information_schema.schemata
             WHERE schema_name NOT LIKE 'pg_%' AND schema_name <> 'information_schema'
             ORDER BY (schema_name = 'public') DESC, schema_name ASC",
            &[],
        )
        .await
        .context("list schemas")?;
    let schemas: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let best = best_match(missing, &schemas, 2).map(|s| s.to_string());
    Ok((schemas, best))
}

async fn suggest_relations(client: &Client, missing: &str) -> Result<Vec<String>> {
    // First: same table name in other schemas.
    let rows = client
        .query(
            "SELECT table_schema
             FROM information_schema.tables
             WHERE table_name = $1
               AND table_schema NOT LIKE 'pg_%'
               AND table_schema <> 'information_schema'
             ORDER BY (table_schema = 'public') DESC, table_schema ASC",
            &[&missing],
        )
        .await
        .context("find tables by name")?;
    if !rows.is_empty() {
        let mut out: Vec<String> = rows
            .iter()
            .map(|r| {
                let schema: String = r.get(0);
                format!("{}.{}", schema, missing)
            })
            .collect();
        out.truncate(3);
        return Ok(out);
    }

    // Second: similar names.
    let pattern = format!("%{}%", missing);
    let rows = client
        .query(
            "SELECT table_schema, table_name
             FROM information_schema.tables
             WHERE table_schema NOT LIKE 'pg_%'
               AND table_schema <> 'information_schema'
               AND table_name ILIKE $1
             LIMIT 200",
            &[&pattern],
        )
        .await
        .context("find similar tables")?;

    let mut candidates: Vec<(String, usize)> = Vec::new();
    for row in rows {
        let schema: String = row.get(0);
        let name: String = row.get(1);
        candidates.push((format!("{}.{}", schema, name), levenshtein(missing, &name)));
    }
    candidates.sort_by_key(|(_, d)| *d);

    Ok(candidates
        .into_iter()
        .filter(|(_, d)| *d > 0 && *d <= 2)
        .take(3)
        .map(|(s, _)| s)
        .collect())
}

async fn suggest_columns(client: &Client, err: &PgError) -> Result<Vec<String>> {
    let Some(db) = err.as_db_error() else {
        return Ok(Vec::new());
    };
    let Some(column) = db.column() else {
        return Ok(Vec::new());
    };
    let Some(table) = db.table() else {
        return Ok(Vec::new());
    };
    let schema = db.schema().unwrap_or("public");
    let rows = client
        .query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await
        .context("list columns for table")?;
    let cols: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let mut out = Vec::new();
    if let Some(best) = best_match(column, &cols, 2) {
        out.push(format!(
            "Did you mean column '{}' on {}.{}?",
            best, schema, table
        ));
    }
    Ok(out)
}

impl fmt::Display for ModelExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Model {} failed: {}", self.model.id, self.error.message)?;
        writeln!(f, "Path: {}", self.model.path)?;
        if let Some(state) = &self.error.sqlstate {
            writeln!(f, "SQLSTATE: {}", state)?;
        }
        if let Some(pos) = self.error.position {
            writeln!(f, "Position: {}", pos)?;
        }
        if !self.suggestions.is_empty() {
            writeln!(f, "Suggestions:")?;
            for s in &self.suggestions {
                writeln!(f, "  - {}", s)?;
            }
        }
        writeln!(f, "SQL preview:\n{}", self.sql.preview)?;
        if !self.hints.is_empty() {
            writeln!(f, "Next steps:")?;
            for h in &self.hints {
                writeln!(f, "  - {}", h)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for ModelExecutionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelHeader, Relation};
    use std::path::PathBuf;

    fn make_incremental_model(body: &str, unique_key: Vec<String>) -> Model {
        Model {
            id: Relation {
                schema: "analytics".into(),
                name: "users".into(),
            },
            path: PathBuf::new(),
            header: ModelHeader {
                materialized: Materialized::Incremental,
                deps: Vec::new(),
                unique_key,
                tests: Vec::new(),
                tags: Vec::new(),
                watermark: None,
                lookback: None,
                incremental_filter: None,
            },
            body_sql: body.into(),
            base_sql: Some(body.into()),
            incremental_sql: None,
        }
    }

    #[test]
    fn test_first_run_sql() {
        let model = make_incremental_model("SELECT id, name FROM source", vec!["id".into()]);
        let sql = generate_first_run_sql(
            &model,
            "SELECT id, name FROM source",
            &model.header.unique_key,
        );
        assert!(sql.contains(r#"CREATE TABLE "analytics"."users" AS"#));
        assert!(sql.contains(r#"ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id")"#));
    }

    #[test]
    fn test_first_run_sql_composite_key() {
        let model = make_incremental_model(
            "SELECT user_id, date, amount FROM source",
            vec!["user_id".into(), "date".into()],
        );
        let sql = generate_first_run_sql(
            &model,
            "SELECT user_id, date, amount FROM source",
            &model.header.unique_key,
        );
        assert!(sql.contains(r#"PRIMARY KEY ("user_id", "date")"#));
    }

    #[test]
    fn test_merge_sql() {
        let model = make_incremental_model("SELECT id, name, email FROM source", vec!["id".into()]);
        let columns = vec!["id".into(), "name".into(), "email".into()];
        let sql = generate_merge_sql(
            &model,
            &columns,
            "SELECT id, name, email FROM source",
            &model.header.unique_key,
        );

        assert!(sql.contains(r#"MERGE INTO "analytics"."users" AS t"#));
        assert!(sql.contains(r#"ON t."id" = s."id""#));
        assert!(sql.contains(r#"UPDATE SET "name" = s."name", "email" = s."email""#));
        assert!(
            sql.contains(r#"INSERT ("id", "name", "email") VALUES (s."id", s."name", s."email")"#)
        );
        // Key column should NOT be in UPDATE SET
        assert!(!sql.contains(r#"UPDATE SET "id" = s."id""#));
    }

    #[test]
    fn test_merge_sql_composite_key() {
        let model = make_incremental_model(
            "SELECT user_id, date, amount FROM source",
            vec!["user_id".into(), "date".into()],
        );
        let columns = vec!["user_id".into(), "date".into(), "amount".into()];
        let sql = generate_merge_sql(
            &model,
            &columns,
            "SELECT user_id, date, amount FROM source",
            &model.header.unique_key,
        );

        assert!(sql.contains(r#"ON t."user_id" = s."user_id" AND t."date" = s."date""#));
        assert!(sql.contains(r#"UPDATE SET "amount" = s."amount""#));
        // Key columns should NOT be in UPDATE SET
        assert!(!sql.contains(r#"UPDATE SET "user_id""#));
        assert!(!sql.contains(r#"UPDATE SET "date""#));
    }

    #[test]
    fn test_merge_sql_reserved_words() {
        // Test that reserved words like "order" are properly quoted
        let model = make_incremental_model("SELECT id, order, user FROM source", vec!["id".into()]);
        let columns = vec!["id".into(), "order".into(), "user".into()];
        let sql = generate_merge_sql(
            &model,
            &columns,
            "SELECT id, order, user FROM source",
            &model.header.unique_key,
        );

        // Reserved words should be quoted
        assert!(sql.contains(r#""order" = s."order""#));
        assert!(sql.contains(r#""user" = s."user""#));
    }

    #[test]
    fn test_upsert_sql() {
        let model = make_incremental_model("SELECT id, name, email FROM source", vec!["id".into()]);
        let columns = vec!["id".into(), "name".into(), "email".into()];
        let sql = generate_upsert_sql(
            &model,
            &columns,
            "SELECT id, name, email FROM source",
            &model.header.unique_key,
        );

        assert!(sql.contains(r#"INSERT INTO "analytics"."users""#));
        assert!(sql.contains(r#"ON CONFLICT ("id") DO UPDATE SET"#));
        assert!(sql.contains(r#""name" = EXCLUDED."name""#));
        assert!(sql.contains(r#""email" = EXCLUDED."email""#));
        // Key column should NOT be in UPDATE SET
        assert!(!sql.contains(r#""id" = EXCLUDED."id""#));
        // Should use CTE for counting
        assert!(sql.contains("WITH source AS"));
        assert!(sql.contains("SELECT COUNT(*)::bigint FROM upserted"));
    }

    #[test]
    fn test_upsert_sql_composite_key() {
        let model = make_incremental_model(
            "SELECT user_id, date, amount FROM source",
            vec!["user_id".into(), "date".into()],
        );
        let columns = vec!["user_id".into(), "date".into(), "amount".into()];
        let sql = generate_upsert_sql(
            &model,
            &columns,
            "SELECT user_id, date, amount FROM source",
            &model.header.unique_key,
        );

        assert!(sql.contains(r#"ON CONFLICT ("user_id", "date") DO UPDATE SET"#));
        assert!(sql.contains(r#""amount" = EXCLUDED."amount""#));
        // Key columns should NOT be in UPDATE SET
        assert!(!sql.contains(r#""user_id" = EXCLUDED"#));
        assert!(!sql.contains(r#""date" = EXCLUDED"#));
    }

    #[test]
    fn test_upsert_sql_reserved_words() {
        let model = make_incremental_model("SELECT id, order, user FROM source", vec!["id".into()]);
        let columns = vec!["id".into(), "order".into(), "user".into()];
        let sql = generate_upsert_sql(
            &model,
            &columns,
            "SELECT id, order, user FROM source",
            &model.header.unique_key,
        );

        // Reserved words should be quoted
        assert!(sql.contains(r#""order" = EXCLUDED."order""#));
        assert!(sql.contains(r#""user" = EXCLUDED."user""#));
    }
}
