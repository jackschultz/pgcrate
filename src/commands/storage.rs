//! Storage command: Disk usage analysis.
//!
//! Analyzes database storage usage including tables, indexes, TOAST data,
//! and tablespaces. Helps identify space hogs and potential cleanup targets.

use anyhow::{Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

/// Storage status thresholds
const BLOAT_CRITICAL_PCT: f64 = 50.0;
const DEAD_TUPLE_WARNING_PCT: f64 = 10.0;

/// Storage status level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageStatus {
    Healthy,
    Warning,
    Critical,
}

impl StorageStatus {
    pub fn emoji(&self) -> &'static str {
        match self {
            StorageStatus::Healthy => "✓",
            StorageStatus::Warning => "⚠",
            StorageStatus::Critical => "✗",
        }
    }
}

/// Table storage information
#[derive(Debug, Clone, Serialize)]
pub struct TableStorage {
    pub schema: String,
    pub name: String,
    pub total_bytes: i64,
    pub total_size: String,
    pub table_bytes: i64,
    pub table_size: String,
    pub index_bytes: i64,
    pub index_size: String,
    pub toast_bytes: i64,
    pub toast_size: String,
    pub row_count: i64,
    pub dead_tuples: i64,
    pub dead_tuple_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_vacuum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_analyze: Option<String>,
    pub status: StorageStatus,
}

/// Index storage information
#[derive(Debug, Clone, Serialize)]
pub struct IndexStorage {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub size_bytes: i64,
    pub size: String,
    pub index_type: String,
    pub is_unique: bool,
    pub is_primary: bool,
    pub scans: i64,
}

/// Tablespace information
#[derive(Debug, Clone, Serialize)]
pub struct TablespaceInfo {
    pub name: String,
    pub size_bytes: i64,
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// Full storage results
#[derive(Debug, Serialize)]
pub struct StorageResult {
    pub database_size_bytes: i64,
    pub database_size: String,
    pub tables: Vec<TableStorage>,
    pub indexes: Vec<IndexStorage>,
    pub tablespaces: Vec<TablespaceInfo>,
    pub temp_bytes: i64,
    pub temp_size: String,
    pub overall_status: StorageStatus,
}

/// Get database total size
async fn get_database_size(client: &Client) -> Result<(i64, String)> {
    let query = r#"
        SELECT
            pg_database_size(current_database()) as size_bytes,
            pg_size_pretty(pg_database_size(current_database())) as size
    "#;

    let row = client
        .query_one(query, &[])
        .await
        .context("Failed to get database size")?;

    Ok((row.get("size_bytes"), row.get("size")))
}

/// Get top tables by size
async fn get_table_storage(client: &Client, limit: usize) -> Result<Vec<TableStorage>> {
    let query = r#"
        SELECT
            s.schemaname,
            s.relname,
            pg_total_relation_size(s.relid) as total_bytes,
            pg_size_pretty(pg_total_relation_size(s.relid)) as total_size,
            pg_relation_size(s.relid) as table_bytes,
            pg_size_pretty(pg_relation_size(s.relid)) as table_size,
            pg_indexes_size(s.relid) as index_bytes,
            pg_size_pretty(pg_indexes_size(s.relid)) as index_size,
            COALESCE(pg_relation_size(c.reltoastrelid), 0) as toast_bytes,
            pg_size_pretty(COALESCE(pg_relation_size(c.reltoastrelid), 0)) as toast_size,
            s.n_live_tup as row_count,
            s.n_dead_tup as dead_tuples,
            CASE
                WHEN s.n_live_tup + s.n_dead_tup > 0
                THEN round(100.0 * s.n_dead_tup / (s.n_live_tup + s.n_dead_tup), 2)::float8
                ELSE 0::float8
            END as dead_tuple_pct,
            s.last_vacuum::text,
            s.last_analyze::text
        FROM pg_stat_user_tables s
        JOIN pg_class c ON c.oid = s.relid
        ORDER BY pg_total_relation_size(s.relid) DESC
        LIMIT $1
    "#;

    let rows = client
        .query(query, &[&(limit as i64)])
        .await
        .context("Failed to get table storage")?;

    let mut tables = Vec::new();
    for row in rows {
        let dead_tuple_pct: f64 = row.get("dead_tuple_pct");

        let status = if dead_tuple_pct >= BLOAT_CRITICAL_PCT {
            StorageStatus::Critical
        } else if dead_tuple_pct >= DEAD_TUPLE_WARNING_PCT {
            StorageStatus::Warning
        } else {
            StorageStatus::Healthy
        };

        tables.push(TableStorage {
            schema: row.get("schemaname"),
            name: row.get("relname"),
            total_bytes: row.get("total_bytes"),
            total_size: row.get("total_size"),
            table_bytes: row.get("table_bytes"),
            table_size: row.get("table_size"),
            index_bytes: row.get("index_bytes"),
            index_size: row.get("index_size"),
            toast_bytes: row.get("toast_bytes"),
            toast_size: row.get("toast_size"),
            row_count: row.get("row_count"),
            dead_tuples: row.get("dead_tuples"),
            dead_tuple_pct,
            last_vacuum: row.get("last_vacuum"),
            last_analyze: row.get("last_analyze"),
            status,
        });
    }

    Ok(tables)
}

/// Get top indexes by size
async fn get_index_storage(client: &Client, limit: usize) -> Result<Vec<IndexStorage>> {
    let query = r#"
        SELECT
            sui.schemaname,
            sui.relname as table_name,
            sui.indexrelname as index_name,
            pg_relation_size(sui.indexrelid) as size_bytes,
            pg_size_pretty(pg_relation_size(sui.indexrelid)) as size,
            am.amname as index_type,
            i.indisunique as is_unique,
            i.indisprimary as is_primary,
            sui.idx_scan as scans
        FROM pg_stat_user_indexes sui
        JOIN pg_index i ON i.indexrelid = sui.indexrelid
        JOIN pg_am am ON am.oid = (
            SELECT relam FROM pg_class WHERE oid = sui.indexrelid
        )
        ORDER BY pg_relation_size(sui.indexrelid) DESC
        LIMIT $1
    "#;

    let rows = client
        .query(query, &[&(limit as i64)])
        .await
        .context("Failed to get index storage")?;

    let mut indexes = Vec::new();
    for row in rows {
        indexes.push(IndexStorage {
            schema: row.get("schemaname"),
            table: row.get("table_name"),
            name: row.get("index_name"),
            size_bytes: row.get("size_bytes"),
            size: row.get("size"),
            index_type: row.get("index_type"),
            is_unique: row.get("is_unique"),
            is_primary: row.get("is_primary"),
            scans: row.get("scans"),
        });
    }

    Ok(indexes)
}

/// Get tablespace information
async fn get_tablespaces(client: &Client) -> Result<Vec<TablespaceInfo>> {
    let query = r#"
        SELECT
            spcname as name,
            pg_tablespace_size(oid) as size_bytes,
            pg_size_pretty(pg_tablespace_size(oid)) as size,
            pg_tablespace_location(oid) as location
        FROM pg_tablespace
        ORDER BY pg_tablespace_size(oid) DESC
    "#;

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to get tablespaces")?;

    let mut tablespaces = Vec::new();
    for row in rows {
        let location: Option<String> = row.get("location");
        tablespaces.push(TablespaceInfo {
            name: row.get("name"),
            size_bytes: row.get("size_bytes"),
            size: row.get("size"),
            location: if location.as_ref().map(|l| l.is_empty()).unwrap_or(true) {
                None
            } else {
                location
            },
        });
    }

    Ok(tablespaces)
}

/// Get temp file usage
async fn get_temp_usage(client: &Client) -> Result<(i64, String)> {
    let query = r#"
        SELECT
            COALESCE(temp_bytes, 0) as temp_bytes,
            pg_size_pretty(COALESCE(temp_bytes, 0)) as temp_size
        FROM pg_stat_database
        WHERE datname = current_database()
    "#;

    let row = client
        .query_one(query, &[])
        .await
        .context("Failed to get temp usage")?;

    Ok((row.get("temp_bytes"), row.get("temp_size")))
}

/// Run full storage analysis
pub async fn run_storage(client: &Client, limit: usize) -> Result<StorageResult> {
    let (database_size_bytes, database_size) = get_database_size(client).await?;
    let tables = get_table_storage(client, limit).await?;
    let indexes = get_index_storage(client, limit).await?;
    let tablespaces = get_tablespaces(client).await?;
    let (temp_bytes, temp_size) = get_temp_usage(client).await?;

    // Determine overall status from table statuses
    let overall_status = tables
        .iter()
        .map(|t| match t.status {
            StorageStatus::Critical => 2,
            StorageStatus::Warning => 1,
            StorageStatus::Healthy => 0,
        })
        .max()
        .map(|s| match s {
            2 => StorageStatus::Critical,
            1 => StorageStatus::Warning,
            _ => StorageStatus::Healthy,
        })
        .unwrap_or(StorageStatus::Healthy);

    Ok(StorageResult {
        database_size_bytes,
        database_size,
        tables,
        indexes,
        tablespaces,
        temp_bytes,
        temp_size,
        overall_status,
    })
}

/// Print storage in human-readable format
pub fn print_human(result: &StorageResult, quiet: bool) {
    println!("STORAGE OVERVIEW");
    println!("{}", "=".repeat(60));
    println!();
    println!("Database Size: {}", result.database_size);
    println!();

    // Top tables
    if !result.tables.is_empty() {
        println!("TOP TABLES BY SIZE:");
        println!(
            "  {:3} {:40} {:>10} {:>8}  ROWS",
            "", "TABLE", "SIZE", "DEAD%"
        );
        println!("  {}", "-".repeat(70));

        for table in &result.tables {
            let qualified = format!("{}.{}", table.schema, table.name);
            let display_name = if qualified.len() > 40 {
                format!("{}...", &qualified[..37])
            } else {
                qualified
            };

            println!(
                "  {} {:40} {:>10} {:>7.1}%  {}",
                table.status.emoji(),
                display_name,
                table.total_size,
                table.dead_tuple_pct,
                format_number(table.row_count)
            );
        }
        println!();
    }

    // Top indexes
    if !result.indexes.is_empty() && !quiet {
        println!("TOP INDEXES BY SIZE:");
        println!("  {:40} {:>10} {:>10}  TYPE", "INDEX", "SIZE", "SCANS");
        println!("  {}", "-".repeat(70));

        for idx in result.indexes.iter().take(10) {
            let display_name = if idx.name.len() > 40 {
                format!("{}...", &idx.name[..37])
            } else {
                idx.name.clone()
            };

            let flags = if idx.is_primary {
                "PK"
            } else if idx.is_unique {
                "UQ"
            } else {
                ""
            };

            println!(
                "  {:40} {:>10} {:>10}  {} {}",
                display_name,
                idx.size,
                format_number(idx.scans),
                idx.index_type,
                flags
            );
        }
        println!();
    }

    // Tablespaces
    if result.tablespaces.len() > 1 && !quiet {
        println!("TABLESPACES:");
        for ts in &result.tablespaces {
            let loc = ts
                .location
                .as_ref()
                .map(|l| format!(" ({})", l))
                .unwrap_or_default();
            println!("  {:20} {:>10}{}", ts.name, ts.size, loc);
        }
        println!();
    }

    // Temp usage
    if result.temp_bytes > 0 && !quiet {
        println!("TEMP FILES: {}", result.temp_size);
        println!();
    }

    // Warnings
    let critical_tables: Vec<_> = result
        .tables
        .iter()
        .filter(|t| t.status == StorageStatus::Critical)
        .collect();

    let warning_tables: Vec<_> = result
        .tables
        .iter()
        .filter(|t| t.status == StorageStatus::Warning)
        .collect();

    if !critical_tables.is_empty() {
        println!(
            "✗ {} tables with >{}% dead tuples (need VACUUM)",
            critical_tables.len(),
            BLOAT_CRITICAL_PCT
        );
        for t in critical_tables.iter().take(3) {
            println!("    {}.{}: {:.1}% dead", t.schema, t.name, t.dead_tuple_pct);
        }
    }

    if !warning_tables.is_empty() && !quiet {
        println!(
            "⚠ {} tables with >{}% dead tuples",
            warning_tables.len(),
            DEAD_TUPLE_WARNING_PCT
        );
    }
}

/// Format large numbers for display
fn format_number(n: i64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

/// Print storage as JSON with schema versioning
pub fn print_json(
    result: &StorageResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        StorageStatus::Healthy => Severity::Healthy,
        StorageStatus::Warning => Severity::Warning,
        StorageStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::STORAGE, result, severity, t),
        None => DiagnosticOutput::new(schema::STORAGE, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.5K");
        assert_eq!(format_number(1_500_000), "1.5M");
        assert_eq!(format_number(1_500_000_000), "1.5B");
    }

    #[test]
    fn test_storage_status_emoji() {
        assert_eq!(StorageStatus::Healthy.emoji(), "✓");
        assert_eq!(StorageStatus::Warning.emoji(), "⚠");
        assert_eq!(StorageStatus::Critical.emoji(), "✗");
    }
}
