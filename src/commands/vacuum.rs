//! Vacuum command: Monitor table bloat and vacuum health.
//!
//! Tables accumulate dead tuples from updates and deletes. VACUUM reclaims
//! this space. This command identifies tables needing vacuum attention.
//!
//! Two modes:
//! - Heuristic mode (always works): Uses pg_stat_user_tables dead tuple counts
//! - Full mode (requires pgstattuple): Accurate bloat measurement

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Default thresholds for vacuum warnings
const DEFAULT_WARNING_PCT: f64 = 10.0;
const DEFAULT_CRITICAL_PCT: f64 = 25.0;
const CRITICAL_DEAD_TUPLES: i64 = 1_000_000;

/// Table vacuum status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VacuumStatus {
    Healthy,
    Warning,
    Critical,
}

impl VacuumStatus {
    pub fn from_dead_pct(pct: f64, dead_tuples: i64) -> Self {
        if pct >= DEFAULT_CRITICAL_PCT || dead_tuples >= CRITICAL_DEAD_TUPLES {
            VacuumStatus::Critical
        } else if pct >= DEFAULT_WARNING_PCT {
            VacuumStatus::Warning
        } else {
            VacuumStatus::Healthy
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            VacuumStatus::Healthy => "✓",
            VacuumStatus::Warning => "⚠",
            VacuumStatus::Critical => "✗",
        }
    }
}

/// Information about a table's vacuum state
#[derive(Debug, Clone, Serialize)]
pub struct TableVacuumInfo {
    pub schema: String,
    pub table: String,
    pub dead_tuples: i64,
    pub live_tuples: i64,
    pub dead_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_vacuum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_autovacuum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_analyze: Option<String>,
    pub table_size: String,
    pub table_size_bytes: i64,
    pub status: VacuumStatus,
    /// Bloat estimate from pgstattuple (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bloat_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bloat_pct: Option<f64>,
    /// Method used to estimate bloat
    pub estimate_method: EstimateMethod,
}

/// How bloat was estimated
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EstimateMethod {
    /// Dead tuple count from pg_stat_user_tables
    Heuristic,
    /// Accurate measurement from pgstattuple extension
    Pgstattuple,
}

/// Full vacuum analysis results
#[derive(Debug, Serialize)]
pub struct VacuumResult {
    pub tables: Vec<TableVacuumInfo>,
    pub overall_status: VacuumStatus,
    /// Whether pgstattuple is available for accurate measurements
    pub pgstattuple_available: bool,
    /// Stats reset time (for confidence)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats_since: Option<String>,
}

/// Check if pgstattuple extension is available
async fn check_pgstattuple(client: &Client) -> bool {
    let query = r#"
        SELECT EXISTS(
            SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple'
        )
    "#;

    match client.query_one(query, &[]).await {
        Ok(row) => row.get::<_, bool>(0),
        Err(_) => false,
    }
}

/// Get stats reset time
async fn get_stats_since(client: &Client) -> Option<String> {
    let query = r#"
        SELECT stats_reset
        FROM pg_stat_database
        WHERE datname = current_database()
    "#;

    match client.query_opt(query, &[]).await {
        Ok(Some(row)) => {
            let reset: Option<chrono::DateTime<chrono::Utc>> = row.get("stats_reset");
            reset.map(|r| r.to_rfc3339())
        }
        _ => None,
    }
}

/// Get table vacuum info using heuristic method (pg_stat_user_tables)
async fn get_tables_heuristic(
    client: &Client,
    schema_filter: Option<&str>,
    table_filter: Option<&str>,
    threshold: f64,
) -> Result<Vec<TableVacuumInfo>> {
    let mut query = String::from(
        r#"
        SELECT
            schemaname,
            relname,
            n_dead_tup as dead_tuples,
            n_live_tup as live_tuples,
            CASE
                WHEN n_live_tup + n_dead_tup = 0 THEN 0
                ELSE round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)::float8
            END as dead_pct,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            pg_size_pretty(pg_total_relation_size(relid)) as table_size,
            pg_total_relation_size(relid) as table_size_bytes
        FROM pg_stat_user_tables
        WHERE 1=1
    "#,
    );

    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();

    if let Some(schema) = schema_filter {
        params.push(Box::new(schema.to_string()));
        query.push_str(&format!(" AND schemaname = ${}", params.len()));
    }

    if let Some(table) = table_filter {
        params.push(Box::new(table.to_string()));
        query.push_str(&format!(" AND relname = ${}", params.len()));
    }

    // Only include tables with some dead tuples or matching filter
    if schema_filter.is_none() && table_filter.is_none() {
        params.push(Box::new(threshold));
        query.push_str(&format!(
            " AND (n_dead_tup > 0 OR CASE WHEN n_live_tup + n_dead_tup = 0 THEN 0::float8 ELSE (100.0 * n_dead_tup / (n_live_tup + n_dead_tup))::float8 END >= ${})",
            params.len()
        ));
    }

    query.push_str(" ORDER BY dead_pct DESC, dead_tuples DESC LIMIT 50");

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&query, &param_refs).await?;
    let mut results = Vec::new();

    for row in rows {
        let dead_tuples: i64 = row.get("dead_tuples");
        let dead_pct: f64 = row.get("dead_pct");
        let status = VacuumStatus::from_dead_pct(dead_pct, dead_tuples);

        let last_vacuum: Option<chrono::DateTime<chrono::Utc>> = row.get("last_vacuum");
        let last_autovacuum: Option<chrono::DateTime<chrono::Utc>> = row.get("last_autovacuum");
        let last_analyze: Option<chrono::DateTime<chrono::Utc>> = row.get("last_analyze");

        results.push(TableVacuumInfo {
            schema: row.get("schemaname"),
            table: row.get("relname"),
            dead_tuples,
            live_tuples: row.get("live_tuples"),
            dead_pct,
            last_vacuum: last_vacuum.map(|t| t.to_rfc3339()),
            last_autovacuum: last_autovacuum.map(|t| t.to_rfc3339()),
            last_analyze: last_analyze.map(|t| t.to_rfc3339()),
            table_size: row.get("table_size"),
            table_size_bytes: row.get("table_size_bytes"),
            status,
            bloat_bytes: None,
            bloat_pct: None,
            estimate_method: EstimateMethod::Heuristic,
        });
    }

    Ok(results)
}

/// Get accurate bloat info using pgstattuple (for a specific table)
async fn get_table_pgstattuple(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Option<(i64, f64)>> {
    // pgstattuple returns detailed tuple-level statistics
    let query = format!(
        r#"
        SELECT
            dead_tuple_len,
            CASE
                WHEN table_len = 0 THEN 0
                ELSE round(100.0 * dead_tuple_len / table_len, 2)::float8
            END as bloat_pct
        FROM pgstattuple('"{}"."{}"')
    "#,
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    );

    match client.query_opt(&query, &[]).await {
        Ok(Some(row)) => {
            let dead_len: i64 = row.get("dead_tuple_len");
            let bloat_pct: f64 = row.get("bloat_pct");
            Ok(Some((dead_len, bloat_pct)))
        }
        Ok(None) => Ok(None),
        Err(_) => Ok(None), // pgstattuple may fail for some tables
    }
}

/// Run vacuum analysis
pub async fn run_vacuum(
    client: &Client,
    schema: Option<&str>,
    table: Option<&str>,
    threshold: Option<f64>,
) -> Result<VacuumResult> {
    let threshold = threshold.unwrap_or(DEFAULT_WARNING_PCT);
    let pgstattuple_available = check_pgstattuple(client).await;
    let stats_since = get_stats_since(client).await;

    let mut tables = get_tables_heuristic(client, schema, table, threshold).await?;

    // If pgstattuple is available and we're looking at specific tables, get accurate info
    if pgstattuple_available && (schema.is_some() || table.is_some()) {
        for t in &mut tables {
            if let Ok(Some((bloat_bytes, bloat_pct))) =
                get_table_pgstattuple(client, &t.schema, &t.table).await
            {
                t.bloat_bytes = Some(bloat_bytes);
                t.bloat_pct = Some(bloat_pct);
                t.estimate_method = EstimateMethod::Pgstattuple;
                // Update status based on more accurate bloat measurement
                t.status = VacuumStatus::from_dead_pct(bloat_pct, t.dead_tuples);
            }
        }
    }

    let overall_status = tables
        .iter()
        .map(|t| &t.status)
        .max_by_key(|s| match s {
            VacuumStatus::Healthy => 0,
            VacuumStatus::Warning => 1,
            VacuumStatus::Critical => 2,
        })
        .cloned()
        .unwrap_or(VacuumStatus::Healthy);

    Ok(VacuumResult {
        tables,
        overall_status,
        pgstattuple_available,
        stats_since,
    })
}

/// Format number for display
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

/// Print vacuum analysis in human-readable format
pub fn print_human(result: &VacuumResult, quiet: bool) {
    if result.tables.is_empty() {
        if !quiet {
            println!("No tables need vacuum attention (below threshold).");
            if !result.pgstattuple_available {
                println!();
                println!(
                    "Note: Install pgstattuple extension for more accurate bloat measurements."
                );
            }
        }
        return;
    }

    println!("VACUUM STATUS:");
    println!();

    // Header
    println!(
        "  {:3} {:40} {:>10} {:>10} {:>8} {:>10}",
        "", "TABLE", "DEAD", "LIVE", "DEAD %", "SIZE"
    );
    println!("  {}", "-".repeat(86));

    for t in &result.tables {
        let full_name = format!("{}.{}", t.schema, t.table);
        println!(
            "  {} {:40} {:>10} {:>10} {:>7.1}% {:>10}",
            t.status.emoji(),
            if full_name.len() > 40 {
                format!("{}...", &full_name[..37])
            } else {
                full_name
            },
            format_number(t.dead_tuples),
            format_number(t.live_tuples),
            t.dead_pct,
            t.table_size
        );

        // Show pgstattuple data if available
        if let (Some(bloat_bytes), Some(bloat_pct)) = (t.bloat_bytes, t.bloat_pct) {
            println!(
                "       (pgstattuple: {:.1}% bloat, {} bytes)",
                bloat_pct, bloat_bytes
            );
        }
    }

    // Summary
    let warning_count = result
        .tables
        .iter()
        .filter(|t| t.status == VacuumStatus::Warning)
        .count();
    let critical_count = result
        .tables
        .iter()
        .filter(|t| t.status == VacuumStatus::Critical)
        .count();

    if warning_count > 0 || critical_count > 0 {
        println!();
        if critical_count > 0 {
            println!(
                "  ✗ {} tables CRITICAL (>{:.0}% or >{} dead tuples)",
                critical_count, DEFAULT_CRITICAL_PCT, CRITICAL_DEAD_TUPLES
            );
        }
        if warning_count > 0 {
            println!(
                "  ⚠ {} tables WARNING (>{:.0}%)",
                warning_count, DEFAULT_WARNING_PCT
            );
        }
    }

    // Recommendations
    let critical_tables: Vec<_> = result
        .tables
        .iter()
        .filter(|t| t.status == VacuumStatus::Critical)
        .collect();

    if !critical_tables.is_empty() {
        println!();
        println!("RECOMMENDED ACTIONS:");
        println!();
        for t in critical_tables.iter().take(3) {
            println!("  VACUUM {}.{};", t.schema, t.table);
        }
        if critical_tables.len() > 3 {
            println!("  ... and {} more", critical_tables.len() - 3);
        }
    }

    if !result.pgstattuple_available && !quiet {
        println!();
        println!("Note: Install pgstattuple for accurate bloat measurements:");
        println!("  CREATE EXTENSION pgstattuple;");
    }
}

/// Print vacuum analysis as JSON with schema versioning.
pub fn print_json(
    result: &VacuumResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        VacuumStatus::Healthy => Severity::Healthy,
        VacuumStatus::Warning => Severity::Warning,
        VacuumStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::VACUUM, result, severity, t),
        None => DiagnosticOutput::new(schema::VACUUM, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vacuum_status_healthy() {
        assert_eq!(
            VacuumStatus::from_dead_pct(5.0, 1000),
            VacuumStatus::Healthy
        );
    }

    #[test]
    fn test_vacuum_status_warning() {
        assert_eq!(
            VacuumStatus::from_dead_pct(15.0, 50000),
            VacuumStatus::Warning
        );
    }

    #[test]
    fn test_vacuum_status_critical_pct() {
        assert_eq!(
            VacuumStatus::from_dead_pct(30.0, 50000),
            VacuumStatus::Critical
        );
    }

    #[test]
    fn test_vacuum_status_critical_count() {
        // Even with low percentage, high absolute count triggers critical
        assert_eq!(
            VacuumStatus::from_dead_pct(5.0, 2_000_000),
            VacuumStatus::Critical
        );
    }

    #[test]
    fn test_format_number_millions() {
        assert_eq!(format_number(1_500_000), "1.5M");
    }

    #[test]
    fn test_format_number_thousands() {
        assert_eq!(format_number(5_500), "5.5K");
    }

    #[test]
    fn test_format_number_small() {
        assert_eq!(format_number(500), "500");
    }
}
