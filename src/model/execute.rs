use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::{generate_run_sql, Materialized, Model};
use crate::sql::quote_ident;

/// Execute a single model against the database.
pub async fn execute_model(client: &Client, model: &Model, full_refresh: bool) -> Result<()> {
    // Ensure schema exists
    let create_schema = format!(
        "CREATE SCHEMA IF NOT EXISTS {}",
        quote_ident(&model.id.schema)
    );
    client
        .execute(&create_schema, &[])
        .await
        .with_context(|| format!("create schema: {}", model.id.schema))?;

    // Handle incremental models specially
    if matches!(model.header.materialized, Materialized::Incremental) {
        execute_incremental(client, model, full_refresh).await?;
        return Ok(());
    }

    // Execute the model SQL (view or table)
    let sql = generate_run_sql(model);
    client.batch_execute(&sql).await.with_context(|| {
        let preview: String = sql.chars().take(200).collect();
        format!("execute model {}\nSQL: {}", model.id, preview)
    })?;

    Ok(())
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

/// Execute an incremental model using MERGE (PostgreSQL 15+)
async fn execute_incremental(client: &Client, model: &Model, full_refresh: bool) -> Result<()> {
    let body = model.body_sql.trim().trim_end_matches(';').trim();
    let unique_key = &model.header.unique_key;

    let table_exists = table_exists(client, &model.id.schema, &model.id.name).await?;

    if table_exists && !full_refresh {
        // Subsequent run: MERGE into existing table
        let columns = get_table_columns(client, &model.id.schema, &model.id.name).await?;
        let sql = generate_merge_sql(model, &columns, body, unique_key);
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("merge into {}", model.id))?;
    } else {
        // First run OR full refresh: DROP (if exists) + CREATE TABLE + PRIMARY KEY
        if table_exists {
            let drop_sql = format!(
                "DROP TABLE {}.{} CASCADE",
                quote_ident(&model.id.schema),
                quote_ident(&model.id.name)
            );
            client
                .batch_execute(&drop_sql)
                .await
                .with_context(|| format!("drop table for full refresh: {}", model.id))?;
        }
        let sql = generate_first_run_sql(model, body, unique_key);
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("create incremental table {}", model.id))?;
    }

    Ok(())
}

/// Generate SQL for first run of incremental model
fn generate_first_run_sql(model: &Model, body: &str, unique_key: &[String]) -> String {
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
fn generate_merge_sql(
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
        "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});",
        insert_cols.join(", "),
        insert_vals.join(", ")
    ));

    sql
}

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
            },
            body_sql: body.into(),
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
}
