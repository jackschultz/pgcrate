//! Stats-age command: Identify tables with stale statistics.
//!
//! PostgreSQL's query planner relies on table statistics to estimate row counts
//! and choose optimal join strategies. Stale statistics lead to poor query plans.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Default thresholds (in days)
const STATS_WARNING_DAYS: f64 = 7.0;
const STATS_CRITICAL_DAYS: f64 = 30.0;
const MIN_ROWS_TO_CARE: i64 = 1000;

/// Statistics freshness status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StatsStatus {
    Healthy,
    Warning,
    Critical,
}

impl StatsStatus {
    pub fn from_days(days: Option<f64>) -> Self {
        match days {
            None => StatsStatus::Critical, // Never analyzed
            Some(d) if d >= STATS_CRITICAL_DAYS => StatsStatus::Critical,
            Some(d) if d >= STATS_WARNING_DAYS => StatsStatus::Warning,
            Some(_) => StatsStatus::Healthy,
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            StatsStatus::Healthy => "✓",
            StatsStatus::Warning => "⚠",
            StatsStatus::Critical => "✗",
        }
    }
}

/// Information about a table's statistics age
#[derive(Debug, Clone, Serialize)]
pub struct TableStatsAge {
    pub schema: String,
    pub table: String,
    pub row_estimate: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_analyze: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_autoanalyze: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days_since_analyze: Option<f64>,
    pub status: StatsStatus,
}

/// Full stats-age results
#[derive(Debug, Serialize)]
pub struct StatsAgeResult {
    pub tables: Vec<TableStatsAge>,
    pub overall_status: StatsStatus,
    pub stale_count: usize,
    pub never_analyzed_count: usize,
}

/// Run stats-age analysis
pub async fn run_stats_age(
    client: &Client,
    threshold_days: Option<f64>,
    limit: usize,
) -> Result<StatsAgeResult> {
    let threshold = threshold_days.unwrap_or(STATS_WARNING_DAYS);

    let query = r#"
        SELECT
            schemaname,
            relname,
            n_live_tup AS row_estimate,
            last_analyze,
            last_autoanalyze,
            (EXTRACT(EPOCH FROM (now() - GREATEST(last_analyze, last_autoanalyze))) / 86400.0)::float8 AS days_since_analyze
        FROM pg_stat_user_tables
        WHERE n_live_tup > $1  -- Only tables with meaningful data
        ORDER BY
            CASE WHEN GREATEST(last_analyze, last_autoanalyze) IS NULL THEN 0 ELSE 1 END,
            GREATEST(last_analyze, last_autoanalyze) ASC NULLS FIRST
        LIMIT $2
    "#;

    let rows = client
        .query(query, &[&MIN_ROWS_TO_CARE, &(limit as i64)])
        .await?;

    let mut tables = Vec::new();
    for row in rows {
        let last_analyze: Option<chrono::DateTime<chrono::Utc>> = row.get("last_analyze");
        let last_autoanalyze: Option<chrono::DateTime<chrono::Utc>> = row.get("last_autoanalyze");
        let days_since: Option<f64> = row.get("days_since_analyze");

        let status = StatsStatus::from_days(days_since);

        tables.push(TableStatsAge {
            schema: row.get("schemaname"),
            table: row.get("relname"),
            row_estimate: row.get("row_estimate"),
            last_analyze: last_analyze.map(|t| t.to_rfc3339()),
            last_autoanalyze: last_autoanalyze.map(|t| t.to_rfc3339()),
            days_since_analyze: days_since,
            status,
        });
    }

    // Filter to only show tables exceeding threshold (or never analyzed)
    let tables: Vec<_> = tables
        .into_iter()
        .filter(|t| t.days_since_analyze.is_none_or(|d| d >= threshold))
        .collect();

    let stale_count = tables
        .iter()
        .filter(|t| t.status == StatsStatus::Warning || t.status == StatsStatus::Critical)
        .count();

    let never_analyzed_count = tables
        .iter()
        .filter(|t| t.days_since_analyze.is_none())
        .count();

    let overall_status = tables
        .iter()
        .map(|t| &t.status)
        .max_by_key(|s| match s {
            StatsStatus::Healthy => 0,
            StatsStatus::Warning => 1,
            StatsStatus::Critical => 2,
        })
        .cloned()
        .unwrap_or(StatsStatus::Healthy);

    Ok(StatsAgeResult {
        tables,
        overall_status,
        stale_count,
        never_analyzed_count,
    })
}

/// Format days for display
fn format_days(days: Option<f64>) -> String {
    match days {
        None => "never".to_string(),
        Some(d) if d < 1.0 => format!("{:.0} hours", d * 24.0),
        Some(d) if d < 7.0 => format!("{:.1} days", d),
        Some(d) => format!("{:.0} days", d),
    }
}

/// Print stats-age in human-readable format
pub fn print_human(result: &StatsAgeResult, quiet: bool) {
    if result.tables.is_empty() {
        if !quiet {
            println!("All tables have fresh statistics (analyzed within threshold).");
        }
        return;
    }

    println!("STATISTICS AGE");
    println!("==============");
    println!();
    println!("Tables with oldest statistics:");
    println!();

    for t in &result.tables {
        let age_str = format_days(t.days_since_analyze);
        let status_label = match t.status {
            StatsStatus::Critical => " (CRITICAL)",
            StatsStatus::Warning => "",
            StatsStatus::Healthy => "",
        };
        println!(
            "  {} {}.{:<30} last analyzed: {}{}",
            t.status.emoji(),
            t.schema,
            t.table,
            age_str,
            status_label
        );
    }

    // Summary
    println!();
    if result.never_analyzed_count > 0 {
        println!(
            "  ✗ {} tables have NEVER been analyzed",
            result.never_analyzed_count
        );
    }
    if result.stale_count > result.never_analyzed_count {
        println!(
            "  ⚠ {} tables have stale statistics (>{:.0} days)",
            result.stale_count - result.never_analyzed_count,
            STATS_WARNING_DAYS
        );
    }

    // Recommendations
    let critical_tables: Vec<_> = result
        .tables
        .iter()
        .filter(|t| t.status == StatsStatus::Critical)
        .collect();

    if !critical_tables.is_empty() && !quiet {
        println!();
        println!("Recommendation: Run ANALYZE on tables with stale statistics:");
        for t in critical_tables.iter().take(5) {
            println!("  ANALYZE {}.{};", t.schema, t.table);
        }
        if critical_tables.len() > 5 {
            println!("  ... and {} more tables", critical_tables.len() - 5);
        }
    }
}

/// Print stats-age as JSON with schema versioning
pub fn print_json(
    result: &StatsAgeResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        StatsStatus::Healthy => Severity::Healthy,
        StatsStatus::Warning => Severity::Warning,
        StatsStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::STATS_AGE, result, severity, t),
        None => DiagnosticOutput::new(schema::STATS_AGE, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_status_healthy() {
        assert_eq!(StatsStatus::from_days(Some(3.0)), StatsStatus::Healthy);
    }

    #[test]
    fn test_stats_status_warning() {
        assert_eq!(StatsStatus::from_days(Some(10.0)), StatsStatus::Warning);
    }

    #[test]
    fn test_stats_status_critical() {
        assert_eq!(StatsStatus::from_days(Some(45.0)), StatsStatus::Critical);
    }

    #[test]
    fn test_stats_status_never_analyzed() {
        assert_eq!(StatsStatus::from_days(None), StatsStatus::Critical);
    }

    #[test]
    fn test_stats_status_boundary() {
        assert_eq!(StatsStatus::from_days(Some(6.9)), StatsStatus::Healthy);
        assert_eq!(StatsStatus::from_days(Some(7.0)), StatsStatus::Warning);
        assert_eq!(StatsStatus::from_days(Some(29.9)), StatsStatus::Warning);
        assert_eq!(StatsStatus::from_days(Some(30.0)), StatsStatus::Critical);
    }

    #[test]
    fn test_format_days_never() {
        assert_eq!(format_days(None), "never");
    }

    #[test]
    fn test_format_days_hours() {
        assert_eq!(format_days(Some(0.5)), "12 hours");
    }

    #[test]
    fn test_format_days_days() {
        assert_eq!(format_days(Some(3.5)), "3.5 days");
    }

    #[test]
    fn test_format_days_weeks() {
        assert_eq!(format_days(Some(14.0)), "14 days");
    }
}
