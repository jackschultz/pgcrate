//! Autovacuum-progress command: Show currently running autovacuum operations.
//!
//! Uses pg_stat_progress_vacuum (PostgreSQL 9.6+) to show:
//! - Which tables are being vacuumed
//! - Current phase and progress
//! - Dead tuples collected
//!
//! This is purely informational - no status thresholds.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// A currently running autovacuum operation
#[derive(Debug, Clone, Serialize)]
pub struct AutovacuumProgress {
    pub pid: i32,
    pub database: String,
    pub table: String,
    pub phase: String,
    pub heap_blks_total: i64,
    pub heap_blks_scanned: i64,
    pub heap_blks_vacuumed: i64,
    pub progress_pct: f64,
    pub dead_tuples_collected: i64,
    pub index_vacuum_count: i64,
    pub running_seconds: f64,
}

/// Full autovacuum progress results
#[derive(Debug, Serialize)]
pub struct AutovacuumProgressResult {
    pub workers: Vec<AutovacuumProgress>,
    pub count: usize,
}

/// Run autovacuum progress check
pub async fn run_autovacuum_progress(client: &Client) -> Result<AutovacuumProgressResult> {
    // Check if pg_stat_progress_vacuum exists (PostgreSQL 9.6+)
    let version_check = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'pg_catalog'
            AND table_name = 'pg_stat_progress_vacuum'
        )
    "#;

    let has_view: bool = client.query_one(version_check, &[]).await?.get(0);

    if !has_view {
        // Return empty result for older PostgreSQL versions
        return Ok(AutovacuumProgressResult {
            workers: Vec::new(),
            count: 0,
        });
    }

    // Check PG version for column name compatibility
    // PG17+ renamed num_dead_tuples -> num_dead_item_ids
    let version_query = "SELECT current_setting('server_version_num')::int";
    let version_num: i32 = client.query_one(version_query, &[]).await?.get(0);

    // Use version-appropriate column name for dead tuple count
    let dead_tuple_col = if version_num >= 170000 {
        "p.num_dead_item_ids"
    } else {
        "p.num_dead_tuples"
    };

    let query = format!(
        r#"
        SELECT
            p.pid,
            d.datname AS database,
            p.relid::regclass::text AS table_name,
            p.phase,
            p.heap_blks_total,
            p.heap_blks_scanned,
            p.heap_blks_vacuumed,
            p.index_vacuum_count,
            {} AS num_dead_tuples,
            a.query_start,
            EXTRACT(EPOCH FROM (now() - a.query_start)) AS running_seconds
        FROM pg_stat_progress_vacuum p
        JOIN pg_stat_activity a ON a.pid = p.pid
        JOIN pg_database d ON d.oid = p.datid
        ORDER BY a.query_start
    "#,
        dead_tuple_col
    );

    let rows = client.query(&query, &[]).await?;

    let mut workers = Vec::new();
    for row in rows {
        let heap_blks_total: i64 = row.get("heap_blks_total");
        let heap_blks_scanned: i64 = row.get("heap_blks_scanned");

        let progress_pct = if heap_blks_total > 0 {
            (100.0 * heap_blks_scanned as f64) / heap_blks_total as f64
        } else {
            0.0
        };

        workers.push(AutovacuumProgress {
            pid: row.get("pid"),
            database: row.get("database"),
            table: row.get("table_name"),
            phase: row.get("phase"),
            heap_blks_total,
            heap_blks_scanned,
            heap_blks_vacuumed: row.get("heap_blks_vacuumed"),
            progress_pct,
            dead_tuples_collected: row.get("num_dead_tuples"),
            index_vacuum_count: row.get("index_vacuum_count"),
            running_seconds: row.get("running_seconds"),
        });
    }

    let count = workers.len();
    Ok(AutovacuumProgressResult { workers, count })
}

/// Format duration for display
fn format_duration(seconds: f64) -> String {
    if seconds >= 3600.0 {
        format!("{:.0} hours", seconds / 3600.0)
    } else if seconds >= 60.0 {
        let mins = (seconds / 60.0).floor();
        let secs = seconds % 60.0;
        format!("{:.0}m {:.0}s", mins, secs)
    } else {
        format!("{:.0} seconds", seconds)
    }
}

/// Format large numbers
fn format_number(n: i64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

/// Print autovacuum progress in human-readable format
pub fn print_human(result: &AutovacuumProgressResult, _quiet: bool) {
    println!("AUTOVACUUM IN PROGRESS");
    println!("======================");
    println!();

    if result.workers.is_empty() {
        println!("No autovacuum running. \u{2713} (all tables healthy)");
        return;
    }

    println!(
        "Currently running: {} autovacuum worker{}",
        result.count,
        if result.count == 1 { "" } else { "s" }
    );
    println!();

    for worker in &result.workers {
        println!("  {}", worker.table);
        println!("    Database: {}", worker.database);
        println!("    Phase: {}", worker.phase);
        println!("    Progress: {:.0}%", worker.progress_pct);
        println!(
            "    Heap blocks: {} / {} scanned",
            format_number(worker.heap_blks_scanned),
            format_number(worker.heap_blks_total)
        );
        println!(
            "    Dead tuples collected: {}",
            format_number(worker.dead_tuples_collected)
        );
        if worker.index_vacuum_count > 0 {
            println!("    Index vacuum passes: {}", worker.index_vacuum_count);
        }
        println!(
            "    Running for: {}",
            format_duration(worker.running_seconds)
        );
        println!("    PID: {}", worker.pid);
        println!();
    }
}

/// Print autovacuum progress as JSON with schema versioning
pub fn print_json(
    result: &AutovacuumProgressResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // This is purely informational, always healthy
    let severity = Severity::Healthy;

    let output = match timeouts {
        Some(t) => {
            DiagnosticOutput::with_timeouts(schema::AUTOVACUUM_PROGRESS, result, severity, t)
        }
        None => DiagnosticOutput::new(schema::AUTOVACUUM_PROGRESS, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(7200.0), "2 hours");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(125.0), "2m 5s");
    }

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(45.0), "45 seconds");
    }

    #[test]
    fn test_format_number_millions() {
        assert_eq!(format_number(2_500_000), "2.5M");
    }

    #[test]
    fn test_format_number_thousands() {
        assert_eq!(format_number(5_500), "5.5K");
    }

    #[test]
    fn test_format_number_small() {
        assert_eq!(format_number(42), "42");
    }
}
