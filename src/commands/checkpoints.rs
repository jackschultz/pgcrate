//! Checkpoints command: Analyze checkpoint frequency and health.
//!
//! Frequent checkpoints indicate:
//! - checkpoint_timeout too low
//! - max_wal_size too small
//! - Heavy write workload overwhelming WAL
//!
//! This command helps diagnose WAL-related performance issues.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Thresholds for checkpoint health
const REQUESTED_PCT_WARNING: f64 = 20.0;
const REQUESTED_PCT_CRITICAL: f64 = 50.0;
const BACKEND_WRITE_PCT_WARNING: f64 = 10.0;

/// Checkpoint status level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointStatus {
    Healthy,
    Warning,
    Critical,
}

impl CheckpointStatus {
    pub fn emoji(&self) -> &'static str {
        match self {
            CheckpointStatus::Healthy => "✓",
            CheckpointStatus::Warning => "⚠",
            CheckpointStatus::Critical => "✗",
        }
    }
}

/// Checkpoint statistics from pg_stat_bgwriter
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointStats {
    /// Number of scheduled checkpoints (timed)
    pub checkpoints_timed: i64,
    /// Number of requested checkpoints (forced, e.g., WAL full)
    pub checkpoints_requested: i64,
    /// Percentage of checkpoints that were forced
    pub requested_pct: f64,
    /// Total time spent writing checkpoint data (ms)
    pub checkpoint_write_time_ms: f64,
    /// Total time spent syncing checkpoint data (ms)
    pub checkpoint_sync_time_ms: f64,
    /// Buffers written during checkpoints
    pub buffers_checkpoint: i64,
    /// Buffers written by background writer
    pub buffers_bgwriter: i64,
    /// Buffers written directly by backends (bad for performance)
    pub buffers_backend: i64,
    /// Percentage of buffers written by backends
    pub backend_write_pct: f64,
    /// Times bgwriter stopped due to maxwritten_clean
    pub maxwritten_clean: i64,
    /// When stats were last reset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats_since: Option<String>,
    /// Overall status
    pub status: CheckpointStatus,
    /// Specific warnings found
    pub warnings: Vec<String>,
}

/// Full checkpoint analysis results
#[derive(Debug, Serialize)]
pub struct CheckpointsResult {
    pub stats: CheckpointStats,
    pub overall_status: CheckpointStatus,
}

/// Run checkpoint analysis
pub async fn run_checkpoints(client: &Client) -> Result<CheckpointsResult> {
    // Check PG version - PG17+ moved checkpoint stats to pg_stat_checkpointer
    let version_query = "SELECT current_setting('server_version_num')::int";
    let version_num: i32 = client.query_one(version_query, &[]).await?.get(0);

    let (
        checkpoints_timed,
        checkpoints_requested,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        stats_reset,
    ): (
        i64,
        i64,
        f64,
        f64,
        i64,
        Option<chrono::DateTime<chrono::Utc>>,
    );

    if version_num >= 170000 {
        // PostgreSQL 17+: checkpoint stats in pg_stat_checkpointer
        let query = r#"
            SELECT
                num_timed,
                num_requested,
                write_time,
                sync_time,
                buffers_written,
                stats_reset
            FROM pg_stat_checkpointer
        "#;
        let row = client.query_one(query, &[]).await?;
        checkpoints_timed = row.get("num_timed");
        checkpoints_requested = row.get("num_requested");
        checkpoint_write_time = row.get("write_time");
        checkpoint_sync_time = row.get("sync_time");
        buffers_checkpoint = row.get("buffers_written");
        stats_reset = row.get("stats_reset");
    } else {
        // PostgreSQL <17: checkpoint stats in pg_stat_bgwriter
        let query = r#"
            SELECT
                checkpoints_timed,
                checkpoints_req,
                checkpoint_write_time,
                checkpoint_sync_time,
                buffers_checkpoint,
                stats_reset
            FROM pg_stat_bgwriter
        "#;
        let row = client.query_one(query, &[]).await?;
        checkpoints_timed = row.get("checkpoints_timed");
        checkpoints_requested = row.get("checkpoints_req");
        checkpoint_write_time = row.get("checkpoint_write_time");
        checkpoint_sync_time = row.get("checkpoint_sync_time");
        buffers_checkpoint = row.get("buffers_checkpoint");
        stats_reset = row.get("stats_reset");
    }

    // Buffer stats are always in pg_stat_bgwriter
    let bgwriter_query = r#"
        SELECT
            buffers_clean,
            buffers_alloc,
            maxwritten_clean
        FROM pg_stat_bgwriter
    "#;
    let bgwriter_row = client.query_one(bgwriter_query, &[]).await?;
    let buffers_bgwriter: i64 = bgwriter_row.get("buffers_clean");
    let maxwritten_clean: i64 = bgwriter_row.get("maxwritten_clean");

    // For backends writing, we need pg_stat_io in PG16+ or estimate from buffers_backend
    // In PG17+, buffers_backend was removed - we'll use 0 as a fallback
    let buffers_backend: i64 = if version_num >= 160000 {
        // PG16+ has pg_stat_io but structure is complex; use 0 for now
        0
    } else {
        // Try to get from pg_stat_bgwriter if available
        let backend_query = "SELECT COALESCE((SELECT buffers_backend FROM pg_stat_bgwriter), 0)";
        client
            .query_one(backend_query, &[])
            .await
            .map(|r| r.get(0))
            .unwrap_or(0)
    };

    let total_checkpoints = checkpoints_timed + checkpoints_requested;
    let requested_pct = if total_checkpoints > 0 {
        (100.0 * checkpoints_requested as f64) / total_checkpoints as f64
    } else {
        0.0
    };

    let total_buffers = buffers_checkpoint + buffers_bgwriter + buffers_backend;
    let backend_write_pct = if total_buffers > 0 {
        (100.0 * buffers_backend as f64) / total_buffers as f64
    } else {
        0.0
    };

    // Determine status and collect warnings
    let mut warnings = Vec::new();
    let mut status = CheckpointStatus::Healthy;

    if requested_pct >= REQUESTED_PCT_CRITICAL {
        status = CheckpointStatus::Critical;
        warnings.push(format!(
            "{:.0}% of checkpoints are forced (requested) - max_wal_size likely too small",
            requested_pct
        ));
    } else if requested_pct >= REQUESTED_PCT_WARNING {
        status = CheckpointStatus::Warning;
        warnings.push(format!(
            "{:.0}% of checkpoints are forced - consider increasing max_wal_size",
            requested_pct
        ));
    }

    if backend_write_pct >= BACKEND_WRITE_PCT_WARNING {
        if status == CheckpointStatus::Healthy {
            status = CheckpointStatus::Warning;
        }
        warnings.push(format!(
            "Backends writing {:.0}% of buffers directly - bgwriter may need tuning",
            backend_write_pct
        ));
    }

    if maxwritten_clean > 0 {
        if status == CheckpointStatus::Healthy {
            status = CheckpointStatus::Warning;
        }
        warnings.push(format!(
            "bgwriter stopped {} times due to maxwritten_clean - consider increasing bgwriter_lru_maxpages",
            maxwritten_clean
        ));
    }

    let stats = CheckpointStats {
        checkpoints_timed,
        checkpoints_requested,
        requested_pct,
        checkpoint_write_time_ms: checkpoint_write_time,
        checkpoint_sync_time_ms: checkpoint_sync_time,
        buffers_checkpoint,
        buffers_bgwriter,
        buffers_backend,
        backend_write_pct,
        maxwritten_clean,
        stats_since: stats_reset.map(|t| t.to_rfc3339()),
        status,
        warnings,
    };

    Ok(CheckpointsResult {
        stats,
        overall_status: status,
    })
}

/// Format bytes for display
fn format_bytes(bytes: i64) -> String {
    // PostgreSQL buffer = 8KB
    let total_bytes = bytes * 8192;
    if total_bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", total_bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if total_bytes >= 1024 * 1024 {
        format!("{:.1} MB", total_bytes as f64 / (1024.0 * 1024.0))
    } else if total_bytes >= 1024 {
        format!("{:.1} KB", total_bytes as f64 / 1024.0)
    } else {
        format!("{} bytes", total_bytes)
    }
}

/// Format duration for display
fn format_duration(ms: f64) -> String {
    if ms >= 3600000.0 {
        format!("{:.1} hours", ms / 3600000.0)
    } else if ms >= 60000.0 {
        format!("{:.1} min", ms / 60000.0)
    } else if ms >= 1000.0 {
        format!("{:.1} sec", ms / 1000.0)
    } else {
        format!("{:.0} ms", ms)
    }
}

/// Print checkpoints in human-readable format
pub fn print_human(result: &CheckpointsResult, _quiet: bool) {
    let stats = &result.stats;

    println!("CHECKPOINT ANALYSIS");
    println!("===================");
    println!();

    if let Some(ref since) = stats.stats_since {
        println!("Statistics since: {}", since);
        println!();
    }

    println!("Checkpoint Frequency:");
    let total = stats.checkpoints_timed + stats.checkpoints_requested;
    println!("  Total checkpoints:     {}", total);
    println!(
        "  Timed (scheduled):     {} ({:.0}%)",
        stats.checkpoints_timed,
        if total > 0 {
            100.0 - stats.requested_pct
        } else {
            0.0
        }
    );
    println!(
        "  Requested (forced):    {} ({:.0}%)",
        stats.checkpoints_requested, stats.requested_pct
    );
    println!();

    println!("Checkpoint Performance:");
    println!(
        "  Write time:            {}",
        format_duration(stats.checkpoint_write_time_ms)
    );
    println!(
        "  Sync time:             {}",
        format_duration(stats.checkpoint_sync_time_ms)
    );
    println!();

    println!("Buffer Writes:");
    println!(
        "  By checkpoints:        {}",
        format_bytes(stats.buffers_checkpoint)
    );
    println!(
        "  By bgwriter:           {}",
        format_bytes(stats.buffers_bgwriter)
    );
    let backend_marker = if stats.backend_write_pct >= BACKEND_WRITE_PCT_WARNING {
        " ⚠"
    } else {
        ""
    };
    println!(
        "  By backends:           {} ({:.0}%){}",
        format_bytes(stats.buffers_backend),
        stats.backend_write_pct,
        backend_marker
    );

    // Warnings
    if !stats.warnings.is_empty() {
        println!();
        println!("{} Warnings:", stats.status.emoji());
        for warning in &stats.warnings {
            println!("  - {}", warning);
        }
    } else {
        println!();
        println!("{} Checkpoint health looks good", stats.status.emoji());
    }
}

/// Print checkpoints as JSON with schema versioning
pub fn print_json(
    result: &CheckpointsResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        CheckpointStatus::Healthy => Severity::Healthy,
        CheckpointStatus::Warning => Severity::Warning,
        CheckpointStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::CHECKPOINTS, result, severity, t),
        None => DiagnosticOutput::new(schema::CHECKPOINTS, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes_gb() {
        // 1GB / 8KB per buffer = 131072 buffers
        assert_eq!(format_bytes(131072), "1.0 GB");
    }

    #[test]
    fn test_format_bytes_mb() {
        // 100MB / 8KB per buffer = 12800 buffers
        assert_eq!(format_bytes(12800), "100.0 MB");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(7200000.0), "2.0 hours");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(120000.0), "2.0 min");
    }

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(5000.0), "5.0 sec");
    }

    #[test]
    fn test_format_duration_ms() {
        assert_eq!(format_duration(500.0), "500 ms");
    }
}
