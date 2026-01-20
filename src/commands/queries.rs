//! Queries command: Top queries from pg_stat_statements.
//!
//! Shows the most expensive queries by execution time, helping identify
//! performance bottlenecks and optimization opportunities.

use anyhow::{Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

/// Status thresholds (in milliseconds)
const QUERY_WARNING_MS: f64 = 1000.0; // 1 second mean time
const QUERY_CRITICAL_MS: f64 = 5000.0; // 5 seconds mean time

/// Sort order for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QuerySortBy {
    /// Sort by total execution time (default)
    #[default]
    TotalTime,
    /// Sort by mean execution time per call
    MeanTime,
    /// Sort by number of calls
    Calls,
}

impl QuerySortBy {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "total" | "total_time" => Some(QuerySortBy::TotalTime),
            "mean" | "mean_time" | "avg" => Some(QuerySortBy::MeanTime),
            "calls" | "count" => Some(QuerySortBy::Calls),
            _ => None,
        }
    }

    fn order_by_clause(&self) -> &'static str {
        match self {
            QuerySortBy::TotalTime => "total_exec_time DESC",
            QuerySortBy::MeanTime => "mean_exec_time DESC",
            QuerySortBy::Calls => "calls DESC",
        }
    }
}

/// Query information from pg_stat_statements
#[derive(Debug, Clone, Serialize)]
pub struct QueryInfo {
    pub queryid: i64,
    pub query: String,
    pub calls: i64,
    pub total_exec_time_ms: f64,
    pub mean_exec_time_ms: f64,
    pub rows: i64,
    pub cache_hit_ratio: Option<f64>,
    pub status: QueryStatus,
}

/// Query status level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Healthy,
    Warning,
    Critical,
}

impl QueryStatus {
    pub fn from_mean_time(mean_ms: f64) -> Self {
        if mean_ms >= QUERY_CRITICAL_MS {
            QueryStatus::Critical
        } else if mean_ms >= QUERY_WARNING_MS {
            QueryStatus::Warning
        } else {
            QueryStatus::Healthy
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            QueryStatus::Healthy => "✓",
            QueryStatus::Warning => "⚠",
            QueryStatus::Critical => "✗",
        }
    }
}

/// Full queries results
#[derive(Debug, Serialize)]
pub struct QueriesResult {
    pub queries: Vec<QueryInfo>,
    pub overall_status: QueryStatus,
    pub extension_available: bool,
    pub stats_since: Option<String>,
    pub total_queries_tracked: i64,
}

/// Check if pg_stat_statements extension is installed and accessible
pub async fn check_extension(client: &Client) -> Result<bool> {
    let query = r#"
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
        )
    "#;

    let row = client
        .query_one(query, &[])
        .await
        .context("Failed to check pg_stat_statements extension")?;

    Ok(row.get::<_, bool>(0))
}

/// Get stats reset time if available
async fn get_stats_since(client: &Client) -> Option<String> {
    // pg_stat_statements_reset() time isn't directly available
    // We use pg_stat_statements_info if on PG14+, otherwise skip
    let query = r#"
        SELECT stats_reset::text
        FROM pg_stat_statements_info
        LIMIT 1
    "#;

    client
        .query_opt(query, &[])
        .await
        .ok()
        .flatten()
        .and_then(|row| row.get::<_, Option<String>>(0))
}

/// Get total count of tracked queries
async fn get_total_queries(client: &Client) -> i64 {
    let query = "SELECT COUNT(*)::bigint FROM pg_stat_statements";
    client
        .query_one(query, &[])
        .await
        .map(|r| r.get::<_, i64>(0))
        .unwrap_or(0)
}

/// Get top queries from pg_stat_statements
pub async fn get_queries(
    client: &Client,
    sort_by: QuerySortBy,
    limit: usize,
) -> Result<Vec<QueryInfo>> {
    // Build query with dynamic ORDER BY
    // Using pg_stat_statements columns available in PG13+
    let query = format!(
        r#"
        SELECT
            queryid,
            LEFT(query, 500) as query,
            calls,
            total_exec_time as total_exec_time_ms,
            mean_exec_time as mean_exec_time_ms,
            rows,
            CASE
                WHEN shared_blks_hit + shared_blks_read > 0
                THEN (100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read))::double precision
                ELSE NULL
            END as cache_hit_ratio
        FROM pg_stat_statements
        WHERE query NOT LIKE '%pg_stat_statements%'
        ORDER BY {}
        LIMIT $1
        "#,
        sort_by.order_by_clause()
    );

    let rows = client
        .query(&query, &[&(limit as i64)])
        .await
        .context("Failed to query pg_stat_statements")?;

    let mut queries = Vec::new();
    for row in rows {
        let mean_exec_time_ms: f64 = row.get("mean_exec_time_ms");
        queries.push(QueryInfo {
            queryid: row.get("queryid"),
            query: row.get("query"),
            calls: row.get("calls"),
            total_exec_time_ms: row.get("total_exec_time_ms"),
            mean_exec_time_ms,
            rows: row.get("rows"),
            cache_hit_ratio: row.get("cache_hit_ratio"),
            status: QueryStatus::from_mean_time(mean_exec_time_ms),
        });
    }

    Ok(queries)
}

/// Run full queries analysis
pub async fn run_queries(
    client: &Client,
    sort_by: QuerySortBy,
    limit: usize,
) -> Result<QueriesResult> {
    // Check extension availability first
    let extension_available = check_extension(client).await?;

    if !extension_available {
        return Ok(QueriesResult {
            queries: vec![],
            overall_status: QueryStatus::Healthy,
            extension_available: false,
            stats_since: None,
            total_queries_tracked: 0,
        });
    }

    let queries = get_queries(client, sort_by, limit).await?;
    let stats_since = get_stats_since(client).await;
    let total_queries_tracked = get_total_queries(client).await;

    // Overall status is worst of query statuses
    let overall_status = queries
        .iter()
        .map(|q| &q.status)
        .max_by_key(|s| match s {
            QueryStatus::Healthy => 0,
            QueryStatus::Warning => 1,
            QueryStatus::Critical => 2,
        })
        .cloned()
        .unwrap_or(QueryStatus::Healthy);

    Ok(QueriesResult {
        queries,
        overall_status,
        extension_available: true,
        stats_since,
        total_queries_tracked,
    })
}

/// Format duration in human-readable form
fn format_duration_ms(ms: f64) -> String {
    if ms >= 60000.0 {
        format!("{:.1}m", ms / 60000.0)
    } else if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else {
        format!("{:.1}ms", ms)
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

/// Truncate query for display
fn truncate_query(query: &str, max_len: usize) -> String {
    let clean = query.replace('\n', " ").replace("  ", " ");
    // Use chars for UTF-8 safe truncation
    if clean.chars().count() <= max_len {
        clean
    } else {
        format!("{}...", clean.chars().take(max_len - 3).collect::<String>())
    }
}

/// Print queries in human-readable format
pub fn print_human(result: &QueriesResult, quiet: bool) {
    if !result.extension_available {
        if !quiet {
            println!("pg_stat_statements extension not installed.");
            println!();
            println!("RECOMMENDATION: Enable pg_stat_statements on all PostgreSQL databases.");
            println!("It's the only way to see which queries consume the most time/resources.");
            println!(
                "Overhead is minimal (<2%), and it's essential for diagnosing \"why is it slow?\""
            );
            println!();
            println!("To enable:");
            println!(
                "  1. Add to postgresql.conf: shared_preload_libraries = 'pg_stat_statements'"
            );
            println!("  2. Restart PostgreSQL");
            println!("  3. Run: CREATE EXTENSION pg_stat_statements;");
            println!();
            println!(
                "Most managed PostgreSQL services (RDS, Cloud SQL, etc.) have this available."
            );
        }
        return;
    }

    if result.queries.is_empty() {
        if !quiet {
            println!("No queries recorded in pg_stat_statements.");
        }
        return;
    }

    println!("TOP QUERIES:");
    if let Some(ref since) = result.stats_since {
        println!("  Stats since: {}", since);
    }
    println!(
        "  Total tracked: {}",
        format_number(result.total_queries_tracked)
    );
    println!();

    // Header
    println!(
        "  {:3} {:>10} {:>10} {:>10} {:>8} {:>6}  QUERY",
        "", "CALLS", "TOTAL", "MEAN", "ROWS", "CACHE"
    );
    println!("  {}", "-".repeat(90));

    for query in &result.queries {
        let cache_str = query
            .cache_hit_ratio
            .map(|r| format!("{:.0}%", r))
            .unwrap_or_else(|| "-".to_string());

        println!(
            "  {} {:>10} {:>10} {:>10} {:>8} {:>6}  {}",
            query.status.emoji(),
            format_number(query.calls),
            format_duration_ms(query.total_exec_time_ms),
            format_duration_ms(query.mean_exec_time_ms),
            format_number(query.rows),
            cache_str,
            truncate_query(&query.query, 50)
        );
    }

    // Summary of issues
    let warning_count = result
        .queries
        .iter()
        .filter(|q| q.status == QueryStatus::Warning)
        .count();
    let critical_count = result
        .queries
        .iter()
        .filter(|q| q.status == QueryStatus::Critical)
        .count();

    if warning_count > 0 || critical_count > 0 {
        println!();
        if critical_count > 0 {
            println!(
                "  ✗ {} queries with mean time >{}s (CRITICAL)",
                critical_count,
                QUERY_CRITICAL_MS / 1000.0
            );
        }
        if warning_count > 0 {
            println!(
                "  ⚠ {} queries with mean time >{}s (WARNING)",
                warning_count,
                QUERY_WARNING_MS / 1000.0
            );
        }
    }
}

/// Print queries as JSON with schema versioning
pub fn print_json(
    result: &QueriesResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = if !result.extension_available {
        // Extension not available - report as healthy (not an error condition)
        Severity::Healthy
    } else {
        match result.overall_status {
            QueryStatus::Healthy => Severity::Healthy,
            QueryStatus::Warning => Severity::Warning,
            QueryStatus::Critical => Severity::Critical,
        }
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::QUERIES, result, severity, t),
        None => DiagnosticOutput::new(schema::QUERIES, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_status_healthy() {
        assert_eq!(QueryStatus::from_mean_time(500.0), QueryStatus::Healthy);
    }

    #[test]
    fn test_query_status_warning() {
        assert_eq!(QueryStatus::from_mean_time(1500.0), QueryStatus::Warning);
    }

    #[test]
    fn test_query_status_critical() {
        assert_eq!(QueryStatus::from_mean_time(6000.0), QueryStatus::Critical);
    }

    #[test]
    fn test_format_duration_ms() {
        assert_eq!(format_duration_ms(500.0), "500.0ms");
        assert_eq!(format_duration_ms(1500.0), "1.50s");
        assert_eq!(format_duration_ms(90000.0), "1.5m");
    }

    #[test]
    fn test_sort_by_from_str() {
        assert_eq!(QuerySortBy::from_str("total"), Some(QuerySortBy::TotalTime));
        assert_eq!(QuerySortBy::from_str("mean"), Some(QuerySortBy::MeanTime));
        assert_eq!(QuerySortBy::from_str("calls"), Some(QuerySortBy::Calls));
        assert_eq!(QuerySortBy::from_str("invalid"), None);
    }

    #[test]
    fn test_truncate_query() {
        let short = "SELECT 1";
        assert_eq!(truncate_query(short, 20), "SELECT 1");

        let long = "SELECT * FROM users WHERE id = 1 AND name = 'test'";
        let result = truncate_query(long, 20);
        assert!(result.ends_with("..."));
        assert!(result.chars().count() <= 20);
    }
}
