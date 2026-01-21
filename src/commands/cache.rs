//! Cache command: Buffer cache hit ratio analysis.
//!
//! Shows cache hit ratios at database and table level to identify
//! memory pressure and tables that would benefit from more RAM.

use anyhow::{Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

/// Status thresholds (percentage)
const CACHE_CRITICAL_PCT: f64 = 90.0;
const CACHE_WARNING_PCT: f64 = 95.0;

/// Cache hit status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheStatus {
    Healthy,
    Warning,
    Critical,
}

impl CacheStatus {
    pub fn from_hit_ratio(ratio_pct: f64) -> Self {
        if ratio_pct < CACHE_CRITICAL_PCT {
            CacheStatus::Critical
        } else if ratio_pct < CACHE_WARNING_PCT {
            CacheStatus::Warning
        } else {
            CacheStatus::Healthy
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            CacheStatus::Healthy => "✓",
            CacheStatus::Warning => "⚠",
            CacheStatus::Critical => "✗",
        }
    }
}

/// Database-level cache statistics
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseCacheStats {
    pub database: String,
    pub blks_hit: i64,
    pub blks_read: i64,
    pub hit_ratio_pct: f64,
    pub status: CacheStatus,
}

/// Table-level cache statistics
#[derive(Debug, Clone, Serialize)]
pub struct TableCacheStats {
    pub schema: String,
    pub table: String,
    pub heap_blks_hit: i64,
    pub heap_blks_read: i64,
    pub hit_ratio_pct: f64,
    pub idx_blks_hit: i64,
    pub idx_blks_read: i64,
    pub idx_hit_ratio_pct: Option<f64>,
    pub status: CacheStatus,
}

/// Full cache analysis results
#[derive(Debug, Serialize)]
pub struct CacheResult {
    pub database_stats: DatabaseCacheStats,
    pub tables: Vec<TableCacheStats>,
    pub overall_status: CacheStatus,
}

/// Get database-level cache statistics
async fn get_database_cache_stats(client: &Client) -> Result<DatabaseCacheStats> {
    let query = r#"
        SELECT
            datname as database,
            COALESCE(blks_hit, 0) as blks_hit,
            COALESCE(blks_read, 0) as blks_read,
            CASE WHEN COALESCE(blks_hit, 0) + COALESCE(blks_read, 0) > 0
                THEN (100.0 * blks_hit / (blks_hit + blks_read))::float8
                ELSE 100.0::float8
            END as hit_ratio_pct
        FROM pg_stat_database
        WHERE datname = current_database()
    "#;

    let row = client
        .query_one(query, &[])
        .await
        .context("Failed to query database cache stats")?;

    let hit_ratio_pct: f64 = row.get("hit_ratio_pct");

    Ok(DatabaseCacheStats {
        database: row.get("database"),
        blks_hit: row.get("blks_hit"),
        blks_read: row.get("blks_read"),
        hit_ratio_pct,
        status: CacheStatus::from_hit_ratio(hit_ratio_pct),
    })
}

/// Get per-table cache statistics
async fn get_table_cache_stats(client: &Client, limit: usize) -> Result<Vec<TableCacheStats>> {
    // Get tables with lowest cache hit ratios (most likely to benefit from more RAM)
    let query = r#"
        SELECT
            schemaname as schema,
            relname as table,
            COALESCE(heap_blks_hit, 0) as heap_blks_hit,
            COALESCE(heap_blks_read, 0) as heap_blks_read,
            CASE WHEN COALESCE(heap_blks_hit, 0) + COALESCE(heap_blks_read, 0) > 0
                THEN (100.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read))::float8
                ELSE NULL
            END as heap_hit_ratio_pct,
            COALESCE(idx_blks_hit, 0) as idx_blks_hit,
            COALESCE(idx_blks_read, 0) as idx_blks_read,
            CASE WHEN COALESCE(idx_blks_hit, 0) + COALESCE(idx_blks_read, 0) > 0
                THEN (100.0 * idx_blks_hit / (idx_blks_hit + idx_blks_read))::float8
                ELSE NULL
            END as idx_hit_ratio_pct
        FROM pg_statio_user_tables
        WHERE COALESCE(heap_blks_hit, 0) + COALESCE(heap_blks_read, 0) > 0
        ORDER BY
            CASE WHEN COALESCE(heap_blks_hit, 0) + COALESCE(heap_blks_read, 0) > 0
                THEN (100.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read))::float8
                ELSE 100.0::float8
            END ASC
        LIMIT $1
    "#;

    let rows = client
        .query(query, &[&(limit as i64)])
        .await
        .context("Failed to query table cache stats")?;

    let mut tables = Vec::new();
    for row in rows {
        let heap_hit_ratio: Option<f64> = row.get("heap_hit_ratio_pct");
        let hit_ratio_pct = heap_hit_ratio.unwrap_or(100.0);

        tables.push(TableCacheStats {
            schema: row.get("schema"),
            table: row.get("table"),
            heap_blks_hit: row.get("heap_blks_hit"),
            heap_blks_read: row.get("heap_blks_read"),
            hit_ratio_pct,
            idx_blks_hit: row.get("idx_blks_hit"),
            idx_blks_read: row.get("idx_blks_read"),
            idx_hit_ratio_pct: row.get("idx_hit_ratio_pct"),
            status: CacheStatus::from_hit_ratio(hit_ratio_pct),
        });
    }

    Ok(tables)
}

/// Run full cache analysis
pub async fn run_cache(client: &Client, limit: usize) -> Result<CacheResult> {
    let database_stats = get_database_cache_stats(client).await?;
    let tables = get_table_cache_stats(client, limit).await?;

    // Overall status is worst of database and table statuses
    let mut overall_status = database_stats.status;
    for table in &tables {
        if table.status == CacheStatus::Critical {
            overall_status = CacheStatus::Critical;
            break;
        } else if table.status == CacheStatus::Warning && overall_status != CacheStatus::Critical {
            overall_status = CacheStatus::Warning;
        }
    }

    Ok(CacheResult {
        database_stats,
        tables,
        overall_status,
    })
}

/// Print cache stats in human-readable format
pub fn print_human(result: &CacheResult, quiet: bool) {
    if quiet && result.overall_status == CacheStatus::Healthy {
        return;
    }

    println!("BUFFER CACHE ANALYSIS");
    println!("{}", "=".repeat(60));
    println!();

    // Database-level stats
    let db = &result.database_stats;
    println!(
        "Database: {} {} {:.1}% hit ratio",
        db.database,
        db.status.emoji(),
        db.hit_ratio_pct
    );
    println!(
        "  Blocks hit: {}  Blocks read: {}",
        format_number(db.blks_hit),
        format_number(db.blks_read)
    );
    println!();

    // Table stats
    if result.tables.is_empty() {
        println!("No table I/O statistics available.");
    } else {
        println!("Tables by Cache Hit Ratio (lowest first):");
        println!(
            "  {:3} {:>8} {:>8} {:>10} {:>10}  TABLE",
            "", "HEAP%", "IDX%", "HEAP_READ", "IDX_READ"
        );
        println!("  {}", "-".repeat(56));

        for table in &result.tables {
            let idx_ratio = table
                .idx_hit_ratio_pct
                .map(|r| format!("{:.1}%", r))
                .unwrap_or_else(|| "-".to_string());

            println!(
                "  {} {:>7.1}% {:>8} {:>10} {:>10}  {}.{}",
                table.status.emoji(),
                table.hit_ratio_pct,
                idx_ratio,
                format_number(table.heap_blks_read),
                format_number(table.idx_blks_read),
                table.schema,
                table.table
            );
        }
    }

    // Summary
    println!();
    let critical_count = result
        .tables
        .iter()
        .filter(|t| t.status == CacheStatus::Critical)
        .count();
    let warning_count = result
        .tables
        .iter()
        .filter(|t| t.status == CacheStatus::Warning)
        .count();

    if critical_count > 0 {
        println!(
            "  ✗ {} tables with <{}% cache hit ratio (CRITICAL)",
            critical_count, CACHE_CRITICAL_PCT
        );
        println!("    Consider increasing shared_buffers or adding more RAM.");
    }
    if warning_count > 0 {
        println!(
            "  ⚠ {} tables with <{}% cache hit ratio (WARNING)",
            warning_count, CACHE_WARNING_PCT
        );
    }
    if critical_count == 0 && warning_count == 0 {
        println!(
            "  ✓ All tables have healthy cache hit ratios (>={}%)",
            CACHE_WARNING_PCT
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

/// Print cache stats as JSON
pub fn print_json(
    result: &CacheResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        CacheStatus::Healthy => Severity::Healthy,
        CacheStatus::Warning => Severity::Warning,
        CacheStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::CACHE, result, severity, t),
        None => DiagnosticOutput::new(schema::CACHE, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_status_healthy() {
        assert_eq!(CacheStatus::from_hit_ratio(99.0), CacheStatus::Healthy);
        assert_eq!(CacheStatus::from_hit_ratio(95.0), CacheStatus::Healthy);
    }

    #[test]
    fn test_cache_status_warning() {
        assert_eq!(CacheStatus::from_hit_ratio(94.9), CacheStatus::Warning);
        assert_eq!(CacheStatus::from_hit_ratio(90.0), CacheStatus::Warning);
    }

    #[test]
    fn test_cache_status_critical() {
        assert_eq!(CacheStatus::from_hit_ratio(89.9), CacheStatus::Critical);
        assert_eq!(CacheStatus::from_hit_ratio(50.0), CacheStatus::Critical);
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(1500), "1.5K");
        assert_eq!(format_number(1_500_000), "1.5M");
        assert_eq!(format_number(1_500_000_000), "1.5B");
    }
}
