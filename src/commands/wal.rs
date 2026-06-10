//! WAL command: Monitor Write-Ahead Log health.
//!
//! Shows WAL generation rate, archiving status, and disk consumption.
//! Helps identify WAL accumulation issues and archive lag.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

// Thresholds for severity
const WAL_DIR_WARNING_BYTES: i64 = 10_737_418_240; // 10GB
const WAL_DIR_CRITICAL_BYTES: i64 = 53_687_091_200; // 50GB
const ARCHIVE_LAG_WARNING_FILES: i64 = 10;
const ARCHIVE_LAG_CRITICAL_FILES: i64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WalStatus {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalDirectory {
    pub size_bytes: Option<i64>,
    pub segment_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveStatus {
    pub enabled: bool,
    pub archive_mode: String,
    pub archive_command: Option<String>,
    pub last_archived_wal: Option<String>,
    pub last_archived_time: Option<String>,
    pub failed_count: i64,
    pub last_failed_wal: Option<String>,
    pub last_failed_time: Option<String>,
    pub pending_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GenerationRate {
    pub bytes_per_second: Option<f64>,
    pub measurement_note: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalIssue {
    pub code: String,
    pub message: String,
    pub severity: WalStatus,
}

#[derive(Debug, Serialize)]
pub struct WalResult {
    pub wal_level: String,
    pub current_wal_lsn: String,
    pub wal_segment_size_bytes: i64,
    pub wal_directory: WalDirectory,
    pub archiving: ArchiveStatus,
    pub generation_rate: GenerationRate,
    pub issues: Vec<WalIssue>,
    pub overall_status: WalStatus,
}

async fn get_wal_settings(client: &Client) -> Result<(String, i64)> {
    let query = r#"
SELECT
    current_setting('wal_level') AS wal_level,
    pg_size_bytes(current_setting('wal_segment_size'))::bigint AS wal_segment_size
"#;
    let row = client.query_one(query, &[]).await?;
    let wal_level: String = row.get("wal_level");
    let wal_segment_size: i64 = row.get("wal_segment_size");
    Ok((wal_level, wal_segment_size))
}

async fn get_current_lsn(client: &Client) -> Result<String> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text AS lsn", &[])
        .await?;
    Ok(row.get("lsn"))
}

async fn get_wal_directory_info(client: &Client) -> Result<WalDirectory> {
    // Try to get WAL directory stats using pg_ls_waldir() - requires superuser or pg_monitor
    let query = r#"
SELECT
    COALESCE(SUM(size), 0)::bigint AS total_size,
    COUNT(*)::bigint AS segment_count
FROM pg_ls_waldir()
"#;

    match client.query_one(query, &[]).await {
        Ok(row) => Ok(WalDirectory {
            size_bytes: Some(row.get("total_size")),
            segment_count: Some(row.get("segment_count")),
        }),
        Err(_) => {
            // Fallback: no access to pg_ls_waldir
            Ok(WalDirectory {
                size_bytes: None,
                segment_count: None,
            })
        }
    }
}

async fn get_archive_status(client: &Client) -> Result<ArchiveStatus> {
    // Get archive settings
    let settings_query = r#"
SELECT
    current_setting('archive_mode') AS archive_mode,
    current_setting('archive_command') AS archive_command
"#;
    let settings_row = client.query_one(settings_query, &[]).await?;
    let archive_mode: String = settings_row.get("archive_mode");
    let archive_command: String = settings_row.get("archive_command");

    let enabled = archive_mode == "on" || archive_mode == "always";

    // Get archiver stats
    let stats_query = r#"
SELECT
    last_archived_wal,
    last_archived_time::text,
    failed_count,
    last_failed_wal,
    last_failed_time::text
FROM pg_stat_archiver
"#;
    let stats_row = client.query_one(stats_query, &[]).await?;

    // Calculate pending files (WAL files not yet archived)
    let pending_count = if enabled {
        let pending_query = r#"
SELECT COUNT(*)::bigint AS pending
FROM pg_ls_waldir()
WHERE name ~ '^[0-9A-F]{24}$'
  AND name > COALESCE(
      (SELECT last_archived_wal FROM pg_stat_archiver),
      '000000000000000000000000'
  )
"#;
        match client.query_one(pending_query, &[]).await {
            Ok(row) => Some(row.get::<_, i64>("pending")),
            Err(_) => None, // No access to pg_ls_waldir
        }
    } else {
        None
    };

    Ok(ArchiveStatus {
        enabled,
        archive_mode,
        archive_command: if archive_command.is_empty() {
            None
        } else {
            Some(archive_command)
        },
        last_archived_wal: stats_row.get("last_archived_wal"),
        last_archived_time: stats_row.get("last_archived_time"),
        failed_count: stats_row.get("failed_count"),
        last_failed_wal: stats_row.get("last_failed_wal"),
        last_failed_time: stats_row.get("last_failed_time"),
        pending_count,
    })
}

async fn get_generation_rate(client: &Client) -> Result<GenerationRate> {
    // Try to estimate WAL generation from pg_stat_wal (PG14+)
    // Note: wal_bytes is numeric type, cast to bigint for Rust compatibility
    let query = r#"
SELECT
    wal_bytes::bigint AS wal_bytes,
    EXTRACT(EPOCH FROM (now() - stats_reset))::float8 AS seconds_since_reset
FROM pg_stat_wal
"#;

    match client.query_one(query, &[]).await {
        Ok(row) => {
            let wal_bytes: i64 = row.get("wal_bytes");
            let seconds: Option<f64> = row.get("seconds_since_reset");

            let bytes_per_second = seconds.and_then(|s| {
                if s > 0.0 {
                    Some(wal_bytes as f64 / s)
                } else {
                    None
                }
            });

            let note = match seconds {
                Some(s) if s > 86400.0 => {
                    format!("Average over {:.1} days since stats reset", s / 86400.0)
                }
                Some(s) if s > 3600.0 => {
                    format!("Average over {:.1} hours since stats reset", s / 3600.0)
                }
                Some(s) => format!("Average over {:.0} seconds since stats reset", s),
                None => "Stats reset time unknown".to_string(),
            };

            Ok(GenerationRate {
                bytes_per_second,
                measurement_note: note,
            })
        }
        Err(_) => {
            // pg_stat_wal not available (PG < 14)
            Ok(GenerationRate {
                bytes_per_second: None,
                measurement_note: "pg_stat_wal not available (requires PostgreSQL 14+)".to_string(),
            })
        }
    }
}

fn analyze_issues(
    wal_dir: &WalDirectory,
    archive: &ArchiveStatus,
    wal_level: &str,
) -> Vec<WalIssue> {
    let mut issues = Vec::new();

    // Check WAL directory size
    if let Some(size) = wal_dir.size_bytes {
        if size >= WAL_DIR_CRITICAL_BYTES {
            issues.push(WalIssue {
                code: "wal_dir_critical".to_string(),
                message: format!(
                    "WAL directory is {} - check replication slots and archive status",
                    format_bytes(size)
                ),
                severity: WalStatus::Critical,
            });
        } else if size >= WAL_DIR_WARNING_BYTES {
            issues.push(WalIssue {
                code: "wal_dir_large".to_string(),
                message: format!(
                    "WAL directory is {} - monitor for growth",
                    format_bytes(size)
                ),
                severity: WalStatus::Warning,
            });
        }
    }

    // Check archive failures
    if archive.enabled && archive.failed_count > 0 {
        issues.push(WalIssue {
            code: "archive_failures".to_string(),
            message: format!(
                "{} archive failures - check archive_command and destination",
                archive.failed_count
            ),
            severity: if archive.failed_count > 10 {
                WalStatus::Critical
            } else {
                WalStatus::Warning
            },
        });
    }

    // Check archive lag
    if let Some(pending) = archive.pending_count {
        if pending >= ARCHIVE_LAG_CRITICAL_FILES {
            issues.push(WalIssue {
                code: "archive_lag_critical".to_string(),
                message: format!("{} WAL files pending archive", pending),
                severity: WalStatus::Critical,
            });
        } else if pending >= ARCHIVE_LAG_WARNING_FILES {
            issues.push(WalIssue {
                code: "archive_lag".to_string(),
                message: format!("{} WAL files pending archive", pending),
                severity: WalStatus::Warning,
            });
        }
    }

    // Check wal_level for replication capability
    if wal_level == "minimal" {
        issues.push(WalIssue {
            code: "wal_level_minimal".to_string(),
            message: "wal_level=minimal - replication and PITR not possible".to_string(),
            severity: WalStatus::Warning,
        });
    }

    // Check archive disabled with replication-capable wal_level
    if !archive.enabled && (wal_level == "replica" || wal_level == "logical") {
        issues.push(WalIssue {
            code: "archive_disabled".to_string(),
            message: "Archiving disabled - point-in-time recovery not possible".to_string(),
            severity: WalStatus::Warning,
        });
    }

    issues
}

fn calculate_overall_status(issues: &[WalIssue]) -> WalStatus {
    if issues.iter().any(|i| i.severity == WalStatus::Critical) {
        WalStatus::Critical
    } else if issues.iter().any(|i| i.severity == WalStatus::Warning) {
        WalStatus::Warning
    } else {
        WalStatus::Healthy
    }
}

pub async fn get_wal(client: &Client) -> Result<WalResult> {
    let (wal_level, wal_segment_size) = get_wal_settings(client).await?;
    let current_lsn = get_current_lsn(client).await?;
    let wal_directory = get_wal_directory_info(client).await?;
    let archiving = get_archive_status(client).await?;
    let generation_rate = get_generation_rate(client).await?;

    let issues = analyze_issues(&wal_directory, &archiving, &wal_level);
    let overall_status = calculate_overall_status(&issues);

    Ok(WalResult {
        wal_level,
        current_wal_lsn: current_lsn,
        wal_segment_size_bytes: wal_segment_size,
        wal_directory,
        archiving,
        generation_rate,
        issues,
        overall_status,
    })
}

fn format_bytes(bytes: i64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn status_emoji(status: &WalStatus) -> &'static str {
    match status {
        WalStatus::Healthy => "✓",
        WalStatus::Warning => "⚠",
        WalStatus::Critical => "✗",
    }
}

pub fn print_human(result: &WalResult, quiet: bool) {
    if !quiet {
        println!(
            "WAL HEALTH: {} {}",
            status_emoji(&result.overall_status),
            match result.overall_status {
                WalStatus::Healthy => "healthy",
                WalStatus::Warning => "warning",
                WalStatus::Critical => "critical",
            }
        );
        println!();
    }

    // WAL Settings
    println!("WAL CONFIGURATION:");
    println!("  wal_level:         {}", result.wal_level);
    println!(
        "  segment_size:      {}",
        format_bytes(result.wal_segment_size_bytes)
    );
    println!("  current_lsn:       {}", result.current_wal_lsn);
    println!();

    // WAL Directory
    println!("WAL DIRECTORY:");
    match (
        result.wal_directory.size_bytes,
        result.wal_directory.segment_count,
    ) {
        (Some(size), Some(count)) => {
            println!("  size:              {}", format_bytes(size));
            println!("  segment_count:     {}", count);
        }
        _ => {
            println!("  (requires pg_monitor or superuser to read pg_ls_waldir)");
        }
    }
    println!();

    // Archive Status
    println!("ARCHIVING:");
    println!(
        "  mode:              {}",
        if result.archiving.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    if let Some(ref cmd) = result.archiving.archive_command {
        let cmd_display = if cmd.len() > 50 {
            format!("{}...", &cmd[..47])
        } else {
            cmd.clone()
        };
        println!("  command:           {}", cmd_display);
    }
    if result.archiving.enabled {
        if let Some(ref wal) = result.archiving.last_archived_wal {
            println!("  last_archived:     {}", wal);
        }
        if let Some(pending) = result.archiving.pending_count {
            println!("  pending_files:     {}", pending);
        }
        if result.archiving.failed_count > 0 {
            println!("  failed_count:      {}", result.archiving.failed_count);
            if let Some(ref wal) = result.archiving.last_failed_wal {
                println!("  last_failed:       {}", wal);
            }
        }
    }
    println!();

    // Generation Rate
    println!("WAL GENERATION:");
    match result.generation_rate.bytes_per_second {
        Some(rate) => {
            println!("  rate:              {}/s", format_bytes(rate as i64));
            println!(
                "  note:              {}",
                result.generation_rate.measurement_note
            );
        }
        None => {
            println!("  {}", result.generation_rate.measurement_note);
        }
    }

    // Issues
    if !result.issues.is_empty() {
        println!();
        println!("ISSUES:");
        for issue in &result.issues {
            println!("  {} {}", status_emoji(&issue.severity), issue.message);
        }
    }
}

pub fn print_json(
    result: &WalResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        WalStatus::Healthy => Severity::Healthy,
        WalStatus::Warning => Severity::Warning,
        WalStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::WAL, result, severity, t),
        None => DiagnosticOutput::new(schema::WAL, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1_500_000), "1.4 MB");
        assert_eq!(format_bytes(2_000_000_000), "1.9 GB");
    }

    #[test]
    fn test_calculate_overall_status() {
        let empty: Vec<WalIssue> = vec![];
        assert_eq!(calculate_overall_status(&empty), WalStatus::Healthy);

        let warning = vec![WalIssue {
            code: "test".to_string(),
            message: "test".to_string(),
            severity: WalStatus::Warning,
        }];
        assert_eq!(calculate_overall_status(&warning), WalStatus::Warning);

        let critical = vec![WalIssue {
            code: "test".to_string(),
            message: "test".to_string(),
            severity: WalStatus::Critical,
        }];
        assert_eq!(calculate_overall_status(&critical), WalStatus::Critical);
    }
}
