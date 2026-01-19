//! XID command: Monitor transaction ID age to prevent wraparound.
//!
//! PostgreSQL transaction IDs are 32-bit integers that wrap around at ~2.1 billion.
//! If XID age gets too high, the database will shut down to prevent data corruption.
//! This command helps monitor XID age at database and table levels.

use anyhow::{Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

/// XID status thresholds (in transactions)
const XID_WARNING: i64 = 1_500_000_000; // 1.5 billion
const XID_CRITICAL: i64 = 1_800_000_000; // 1.8 billion

/// Database-level XID information
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseXid {
    pub datname: String,
    pub xid_age: i64,
    pub pct_used: f64,
    pub status: XidStatus,
}

/// Table-level XID information
#[derive(Debug, Clone, Serialize)]
pub struct TableXid {
    pub schema: String,
    pub table: String,
    pub xid_age: i64,
    pub size: String,
    pub status: XidStatus,
}

/// Autovacuum progress
#[derive(Debug, Clone, Serialize)]
pub struct VacuumProgress {
    pub table: String,
    pub phase: String,
    pub pct_done: f64,
}

/// XID status level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum XidStatus {
    Healthy,
    Warning,
    Critical,
}

impl XidStatus {
    pub fn from_age(age: i64) -> Self {
        if age >= XID_CRITICAL {
            XidStatus::Critical
        } else if age >= XID_WARNING {
            XidStatus::Warning
        } else {
            XidStatus::Healthy
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            XidStatus::Healthy => "✓",
            XidStatus::Warning => "⚠",
            XidStatus::Critical => "✗",
        }
    }
}

/// Full XID results
#[derive(Debug, Serialize)]
pub struct XidResult {
    pub databases: Vec<DatabaseXid>,
    pub tables: Vec<TableXid>,
    pub vacuum_progress: Vec<VacuumProgress>,
    pub overall_status: XidStatus,
}

/// Get database-level XID ages
pub async fn get_database_xid(client: &Client) -> Result<Vec<DatabaseXid>> {
    // Use explicit double precision cast to avoid deserialization issues
    let query = r#"
        SELECT
            datname,
            age(datfrozenxid)::bigint as xid_age,
            (100.0 * age(datfrozenxid)::double precision / 2147483647.0)::double precision as pct_used
        FROM pg_database
        WHERE datallowconn
        ORDER BY age(datfrozenxid) DESC
    "#;

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to query database XID ages")?;

    let mut results = Vec::new();

    for row in rows {
        let xid_age: i64 = row.get("xid_age");
        let pct_used: f64 = row.get("pct_used");
        results.push(DatabaseXid {
            datname: row.get("datname"),
            xid_age,
            pct_used,
            status: XidStatus::from_age(xid_age),
        });
    }

    Ok(results)
}

/// Get table-level XID ages (oldest unfrozen tables)
pub async fn get_table_xid(client: &Client, limit: usize) -> Result<Vec<TableXid>> {
    // Query user tables with XID age. Returns empty vec for databases with no tables.
    let query = r#"
        SELECT
            s.schemaname,
            s.relname,
            age(c.relfrozenxid)::bigint as xid_age,
            pg_size_pretty(pg_total_relation_size(s.relid)) as size
        FROM pg_stat_user_tables s
        JOIN pg_class c ON s.relid = c.oid
        WHERE c.relfrozenxid <> '0'::xid
        ORDER BY age(c.relfrozenxid) DESC
        LIMIT $1
    "#;

    let rows = client
        .query(query, &[&(limit as i64)])
        .await
        .context("Failed to query table XID ages (database may have no user tables)")?;

    let mut results = Vec::new();

    for row in rows {
        let xid_age: i64 = row.get("xid_age");
        results.push(TableXid {
            schema: row.get("schemaname"),
            table: row.get("relname"),
            xid_age,
            size: row.get("size"),
            status: XidStatus::from_age(xid_age),
        });
    }

    Ok(results)
}

/// Get autovacuum progress
pub async fn get_vacuum_progress(client: &Client) -> Result<Vec<VacuumProgress>> {
    let query = r#"
        SELECT
            p.relid::regclass::text as table_name,
            p.phase,
            CASE
                WHEN p.heap_blks_total > 0
                THEN round(100.0 * p.heap_blks_scanned / p.heap_blks_total, 1)
                ELSE 0
            END as pct_done
        FROM pg_stat_progress_vacuum p
        ORDER BY p.relid
    "#;

    let rows = client.query(query, &[]).await?;
    let mut results = Vec::new();

    for row in rows {
        results.push(VacuumProgress {
            table: row.get("table_name"),
            phase: row.get("phase"),
            pct_done: row.get("pct_done"),
        });
    }

    Ok(results)
}

/// Run full XID analysis
pub async fn run_xid(client: &Client, table_limit: usize) -> Result<XidResult> {
    let databases = get_database_xid(client).await?;
    let tables = get_table_xid(client, table_limit).await?;
    let vacuum_progress = get_vacuum_progress(client).await?;

    // Overall status is worst of database statuses
    let overall_status = databases
        .iter()
        .map(|d| &d.status)
        .max_by_key(|s| match s {
            XidStatus::Healthy => 0,
            XidStatus::Warning => 1,
            XidStatus::Critical => 2,
        })
        .cloned()
        .unwrap_or(XidStatus::Healthy);

    Ok(XidResult {
        databases,
        tables,
        vacuum_progress,
        overall_status,
    })
}

/// Format XID age for display
fn format_xid(age: i64) -> String {
    if age >= 1_000_000_000 {
        format!("{:.2}B", age as f64 / 1_000_000_000.0)
    } else if age >= 1_000_000 {
        format!("{:.1}M", age as f64 / 1_000_000.0)
    } else {
        format!("{}", age)
    }
}

/// Print XID results in human-readable format
pub fn print_human(result: &XidResult) {
    // Database-level XID
    println!("DATABASE XID AGE:");
    println!();

    for db in &result.databases {
        println!(
            "  {} {:20} {:>10} / 2.1B ({:>5.1}%)   {}",
            db.status.emoji(),
            db.datname,
            format_xid(db.xid_age),
            db.pct_used,
            match db.status {
                XidStatus::Healthy => "healthy",
                XidStatus::Warning => "WARNING",
                XidStatus::Critical => "CRITICAL",
            }
        );
    }

    // Table-level XID
    if !result.tables.is_empty() {
        println!();
        println!("OLDEST UNFROZEN TABLES:");
        println!();

        for table in &result.tables {
            println!(
                "  {} {}.{:30} {:>10}   ({})",
                table.status.emoji(),
                table.schema,
                table.table,
                format_xid(table.xid_age),
                table.size
            );
        }
    }

    // Vacuum progress
    if !result.vacuum_progress.is_empty() {
        println!();
        println!("AUTOVACUUM IN PROGRESS:");
        println!();

        for vac in &result.vacuum_progress {
            println!("  {:40} {:20} {:>5.1}%", vac.table, vac.phase, vac.pct_done);
        }
    }

    // Actions
    if result.overall_status != XidStatus::Healthy {
        println!();
        println!("RECOMMENDED ACTIONS:");

        if result.overall_status == XidStatus::Critical {
            println!("  ✗ URGENT: Run VACUUM FREEZE on oldest tables");
            println!("  ✗ Consider emergency maintenance window");
        } else {
            println!("  ⚠ Schedule VACUUM FREEZE during low-traffic period");
            println!("  ⚠ Review autovacuum settings");
        }

        if !result.tables.is_empty() {
            let oldest = &result.tables[0];
            println!();
            println!("  Example command:");
            println!(
                "    VACUUM (FREEZE, VERBOSE) {}.{};",
                oldest.schema, oldest.table
            );
        }
    }
}

/// Print XID results as JSON with schema versioning.
pub fn print_json(
    result: &XidResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Convert XidStatus to Severity
    let severity = match result.overall_status {
        XidStatus::Healthy => Severity::Healthy,
        XidStatus::Warning => Severity::Warning,
        XidStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::XID, result, severity, t),
        None => DiagnosticOutput::new(schema::XID, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xid_status_healthy() {
        assert_eq!(XidStatus::from_age(1_000_000_000), XidStatus::Healthy);
    }

    #[test]
    fn test_xid_status_warning() {
        assert_eq!(XidStatus::from_age(1_500_000_000), XidStatus::Warning);
        assert_eq!(XidStatus::from_age(1_700_000_000), XidStatus::Warning);
    }

    #[test]
    fn test_xid_status_critical() {
        assert_eq!(XidStatus::from_age(1_800_000_000), XidStatus::Critical);
        assert_eq!(XidStatus::from_age(2_000_000_000), XidStatus::Critical);
    }

    #[test]
    fn test_format_xid_billions() {
        assert_eq!(format_xid(1_500_000_000), "1.50B");
    }

    #[test]
    fn test_format_xid_millions() {
        assert_eq!(format_xid(500_000_000), "500.0M");
    }

    #[test]
    fn test_format_xid_small() {
        assert_eq!(format_xid(12345), "12345");
    }
}
