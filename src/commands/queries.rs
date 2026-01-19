//! Queries command: Top queries from pg_stat_statements.
//!
//! Shows top queries by execution time, mean time, or call count.
//! Requires pg_stat_statements extension to be installed.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Default limit when --all is specified (arbitrarily high)
pub const ALL_QUERIES_LIMIT: usize = 1000;

/// Sort order for queries
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
            "total" | "total_time" => Some(Self::TotalTime),
            "mean" | "mean_time" | "avg" => Some(Self::MeanTime),
            "calls" | "count" => Some(Self::Calls),
            _ => None,
        }
    }
}

/// Status level for individual queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Healthy,
    Warning,
    Critical,
}

impl QueryStatus {
    /// Derive status from mean execution time in milliseconds.
    /// - Critical: > 5000ms (5 seconds)
    /// - Warning: > 1000ms (1 second)
    /// - Healthy: <= 1000ms
    pub fn from_mean_time(mean_ms: f64) -> Self {
        if mean_ms > 5000.0 {
            QueryStatus::Critical
        } else if mean_ms > 1000.0 {
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

/// Information about a single query
#[derive(Debug, Clone, Serialize)]
pub struct QueryInfo {
    pub queryid: i64,
    pub query: String,
    pub calls: i64,
    pub total_exec_time_ms: f64,
    pub mean_exec_time_ms: f64,
    pub rows: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_hit_ratio: Option<f64>,
    pub status: QueryStatus,
}

impl QueryInfo {
    /// Redact the query text to remove string literals.
    pub fn redact_query(&mut self) {
        use crate::redact;
        self.query = redact::redact_query(&self.query);
    }
}

/// Full queries results
#[derive(Debug, Serialize)]
pub struct QueriesResult {
    pub queries: Vec<QueryInfo>,
    pub overall_status: QueryStatus,
    pub extension_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats_since: Option<String>,
    pub sort_by: String,
    pub limit: usize,
}

impl QueriesResult {
    /// Apply redaction to all query text in the result.
    pub fn redact(&mut self) {
        for q in &mut self.queries {
            q.redact_query();
        }
    }
}

/// Check if pg_stat_statements extension is available
pub async fn check_extension_available(client: &Client) -> bool {
    client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')",
            &[],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false)
}

/// Get stats reset timestamp if available
pub async fn get_stats_since(client: &Client) -> Option<String> {
    client
        .query_opt(
            "SELECT stats_reset::text FROM pg_stat_statements_info LIMIT 1",
            &[],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| r.get::<_, Option<String>>(0))
}

/// Get top queries from pg_stat_statements
pub async fn get_queries(
    client: &Client,
    sort_by: QuerySortBy,
    limit: usize,
) -> Result<QueriesResult> {
    // First check if extension is available
    let extension_available = check_extension_available(client).await;

    if !extension_available {
        let sort_by_str = match sort_by {
            QuerySortBy::TotalTime => "total_time",
            QuerySortBy::MeanTime => "mean_time",
            QuerySortBy::Calls => "calls",
        };
        return Ok(QueriesResult {
            queries: vec![],
            overall_status: QueryStatus::Healthy,
            extension_available: false,
            stats_since: None,
            sort_by: sort_by_str.to_string(),
            limit,
        });
    }

    // Get stats reset time
    let stats_since = get_stats_since(client).await;

    // Build query with appropriate ORDER BY
    let order_clause = match sort_by {
        QuerySortBy::TotalTime => "total_exec_time DESC",
        QuerySortBy::MeanTime => "mean_exec_time DESC",
        QuerySortBy::Calls => "calls DESC",
    };

    let query = format!(
        r#"
        SELECT
            queryid,
            left(query, 500) as query,
            calls,
            total_exec_time as total_exec_time_ms,
            mean_exec_time as mean_exec_time_ms,
            rows,
            shared_blks_hit,
            shared_blks_read
        FROM pg_stat_statements
        WHERE queryid IS NOT NULL
          AND query NOT LIKE '%pg_stat_statements%'
        ORDER BY {}
        LIMIT $1
        "#,
        order_clause
    );

    let rows = client.query(&query, &[&(limit as i64)]).await?;

    let mut queries = Vec::new();
    for row in rows {
        let mean_exec_time_ms: f64 = row.get("mean_exec_time_ms");
        let shared_blks_hit: i64 = row.get("shared_blks_hit");
        let shared_blks_read: i64 = row.get("shared_blks_read");

        let cache_hit_ratio = if shared_blks_hit + shared_blks_read > 0 {
            Some(100.0 * shared_blks_hit as f64 / (shared_blks_hit + shared_blks_read) as f64)
        } else {
            None
        };

        queries.push(QueryInfo {
            queryid: row.get("queryid"),
            query: row.get("query"),
            calls: row.get("calls"),
            total_exec_time_ms: row.get("total_exec_time_ms"),
            mean_exec_time_ms,
            rows: row.get("rows"),
            cache_hit_ratio,
            status: QueryStatus::from_mean_time(mean_exec_time_ms),
        });
    }

    // Calculate overall status (worst among all queries)
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

    let sort_by_str = match sort_by {
        QuerySortBy::TotalTime => "total_time",
        QuerySortBy::MeanTime => "mean_time",
        QuerySortBy::Calls => "calls",
    };

    Ok(QueriesResult {
        queries,
        overall_status,
        extension_available: true,
        stats_since,
        sort_by: sort_by_str.to_string(),
        limit,
    })
}

fn format_duration_ms(ms: f64) -> String {
    if ms < 1.0 {
        format!("{:.2}µs", ms * 1000.0)
    } else if ms < 1000.0 {
        format!("{:.1}ms", ms)
    } else if ms < 60000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else {
        format!("{:.1}m", ms / 60000.0)
    }
}

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

fn truncate_query(query: &str, max_len: usize) -> String {
    let clean = query.replace('\n', " ").replace("  ", " ");
    let char_count = clean.chars().count();
    if char_count <= max_len {
        return clean;
    }
    // Safe UTF-8 truncation using char boundaries
    let byte_pos = clean
        .char_indices()
        .nth(max_len.saturating_sub(3))
        .map(|(i, _)| i)
        .unwrap_or(clean.len());
    format!("{}...", &clean[..byte_pos])
}

/// Print queries in human-readable format
pub fn print_human(result: &QueriesResult, quiet: bool) {
    if !result.extension_available {
        if !quiet {
            println!("pg_stat_statements extension not available.");
            println!();
            println!("To enable query analysis:");
            println!("  1. Install extension: CREATE EXTENSION pg_stat_statements;");
            println!(
                "  2. Add to postgresql.conf: shared_preload_libraries = 'pg_stat_statements'"
            );
            println!("  3. Restart PostgreSQL");
        }
        return;
    }

    if result.queries.is_empty() {
        if !quiet {
            println!("No query statistics found.");
        }
        return;
    }

    println!("TOP QUERIES (by {}):", result.sort_by);
    if let Some(ref since) = result.stats_since {
        println!("Stats since: {}", since);
    }
    println!();

    // Header
    println!(
        "  {:3} {:>10} {:>10} {:>10} {:>8} {:>6} QUERY",
        "", "CALLS", "TOTAL", "MEAN", "ROWS", "CACHE"
    );
    println!("  {}", "-".repeat(90));

    for query in &result.queries {
        let cache_str = query
            .cache_hit_ratio
            .map(|r| format!("{:.0}%", r))
            .unwrap_or_else(|| "-".to_string());

        println!(
            "  {} {:>10} {:>10} {:>10} {:>8} {:>6} {}",
            query.status.emoji(),
            format_number(query.calls),
            format_duration_ms(query.total_exec_time_ms),
            format_duration_ms(query.mean_exec_time_ms),
            format_number(query.rows),
            cache_str,
            truncate_query(&query.query, 40)
        );
    }

    // Summary
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
            println!("  ✗ {} queries CRITICAL (mean > 5s)", critical_count);
        }
        if warning_count > 0 {
            println!("  ⚠ {} queries WARNING (mean > 1s)", warning_count);
        }
    }
}

/// Print queries as JSON with schema versioning
pub fn print_json(
    result: &QueriesResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Convert QueryStatus to Severity
    let severity = match result.overall_status {
        QueryStatus::Healthy => Severity::Healthy,
        QueryStatus::Warning => Severity::Warning,
        QueryStatus::Critical => Severity::Critical,
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
        assert_eq!(QueryStatus::from_mean_time(2000.0), QueryStatus::Warning);
    }

    #[test]
    fn test_query_status_critical() {
        assert_eq!(QueryStatus::from_mean_time(6000.0), QueryStatus::Critical);
    }

    #[test]
    fn test_sort_by_parsing() {
        assert_eq!(QuerySortBy::from_str("total"), Some(QuerySortBy::TotalTime));
        assert_eq!(QuerySortBy::from_str("mean"), Some(QuerySortBy::MeanTime));
        assert_eq!(QuerySortBy::from_str("calls"), Some(QuerySortBy::Calls));
        assert_eq!(QuerySortBy::from_str("invalid"), None);
    }

    #[test]
    fn test_format_duration_microseconds() {
        assert_eq!(format_duration_ms(0.5), "500.00µs");
    }

    #[test]
    fn test_format_duration_milliseconds() {
        assert_eq!(format_duration_ms(50.0), "50.0ms");
    }

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration_ms(5000.0), "5.00s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration_ms(120000.0), "2.0m");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(500), "500");
        assert_eq!(format_number(5000), "5.0K");
        assert_eq!(format_number(5000000), "5.0M");
        assert_eq!(format_number(5000000000), "5.0B");
    }
}
