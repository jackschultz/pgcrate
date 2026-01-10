use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::io::Read;
use tokio_postgres::SimpleQueryMessage;

use super::connect;

#[derive(Serialize)]
struct SqlResponse {
    ok: bool,
    results: Vec<SqlResult>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum SqlResult {
    #[serde(rename = "query")]
    Query {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
    #[serde(rename = "command")]
    CommandComplete { rows: u64 },
}

pub async fn sql(
    database_url: &str,
    command: Option<&str>,
    allow_write: bool,
    quiet: bool,
    json: bool,
) -> Result<()> {
    let sql = match command {
        Some(c) => c.to_string(),
        None => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("read SQL from stdin")?;
            buf
        }
    };

    let sql = sql.trim();
    if sql.is_empty() {
        bail!(
            "No SQL provided. Use: pgcrate sql -c \"SELECT 1\" or echo \"SELECT 1\" | pgcrate sql"
        );
    }

    if !allow_write && looks_like_write(sql)? {
        bail!("SQL appears to write. Re-run with --allow-write to proceed.");
    }

    let client = connect(database_url).await?;
    let messages = client.simple_query(sql).await.context("execute SQL")?;

    let mut results: Vec<SqlResult> = Vec::new();
    let mut current_columns: Option<Vec<String>> = None;
    let mut current_rows: Vec<Vec<Option<String>>> = Vec::new();

    for msg in messages {
        match msg {
            SimpleQueryMessage::RowDescription(cols) => {
                current_columns = Some(cols.iter().map(|c| c.name().to_string()).collect());
            }
            SimpleQueryMessage::Row(row) => {
                if current_columns.is_none() {
                    current_columns =
                        Some(row.columns().iter().map(|c| c.name().to_string()).collect());
                }
                let values: Vec<Option<String>> = (0..row.len())
                    .map(|i| row.get(i).map(|s| s.to_string()))
                    .collect();
                current_rows.push(values);
            }
            SimpleQueryMessage::CommandComplete(rows) => {
                if let Some(cols) = current_columns.take() {
                    results.push(SqlResult::Query {
                        columns: cols,
                        rows: std::mem::take(&mut current_rows),
                    });
                }
                results.push(SqlResult::CommandComplete { rows });
            }
            _ => {}
        }
    }

    if let Some(cols) = current_columns.take() {
        results.push(SqlResult::Query {
            columns: cols,
            rows: std::mem::take(&mut current_rows),
        });
    }

    if json {
        let payload = SqlResponse { ok: true, results };
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    if quiet {
        return Ok(());
    }

    for result in results {
        match result {
            SqlResult::Query { columns, rows } => {
                print_table(&columns, &rows);
            }
            SqlResult::CommandComplete { rows } => {
                println!("OK ({rows} rows)");
            }
        }
    }

    Ok(())
}

fn looks_like_write(sql: &str) -> Result<bool> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql).context("parse SQL")?;

    for stmt in statements {
        use sqlparser::ast::Statement;
        match stmt {
            Statement::Query(_) => {}
            Statement::Set(_) => {}
            Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Explain { .. } => {}
            _ => return Ok(true),
        }
    }

    Ok(false)
}

fn print_table(columns: &[String], rows: &[Vec<Option<String>>]) {
    if columns.is_empty() {
        return;
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i >= widths.len() {
                continue;
            }
            let s = cell.as_deref().unwrap_or("NULL");
            widths[i] = widths[i].max(s.len());
        }
    }

    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    for row in rows {
        let line: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let s = row.get(i).and_then(|v| v.as_deref()).unwrap_or("NULL");
                format!("{:width$}", s, width = widths[i])
            })
            .collect();
        println!("{}", line.join(" | "));
    }
}
