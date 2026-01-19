//! Sequences command: Monitor sequence exhaustion risk.
//!
//! Sequences have maximum values based on their data type. When exhausted,
//! inserts fail. This command identifies sequences approaching their limits.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Default warning threshold (percentage)
const DEFAULT_WARNING_PCT: i32 = 70;
const DEFAULT_CRITICAL_PCT: i32 = 85;

/// Sequence information
#[derive(Debug, Clone, Serialize)]
pub struct SequenceInfo {
    pub schema: String,
    pub name: String,
    pub data_type: String,
    pub last_value: i64,
    pub max_value: i64,
    pub pct_used: f64,
    pub status: SeqStatus,
    /// Estimated time until exhaustion (if we have rate data)
    pub estimated_exhaustion: Option<String>,
}

/// Sequence status level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SeqStatus {
    Healthy,
    Warning,
    Critical,
}

impl SeqStatus {
    pub fn from_pct(pct: f64, warn_threshold: i32, crit_threshold: i32) -> Self {
        if pct >= crit_threshold as f64 {
            SeqStatus::Critical
        } else if pct >= warn_threshold as f64 {
            SeqStatus::Warning
        } else {
            SeqStatus::Healthy
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            SeqStatus::Healthy => "✓",
            SeqStatus::Warning => "⚠",
            SeqStatus::Critical => "✗",
        }
    }
}

/// Full sequences results
#[derive(Debug, Serialize)]
pub struct SequencesResult {
    pub sequences: Vec<SequenceInfo>,
    pub overall_status: SeqStatus,
    pub warning_threshold: i32,
    pub critical_threshold: i32,
}

/// Get all sequences with their usage
pub async fn get_sequences(
    client: &Client,
    warn_threshold: i32,
    crit_threshold: i32,
) -> Result<SequencesResult> {
    // Query sequences with type-aware max value calculation
    let query = r#"
        SELECT
            schemaname,
            sequencename,
            data_type::text as data_type,
            COALESCE(last_value, 0) as last_value,
            max_value,
            CASE
                WHEN increment_by > 0 AND max_value > 0 AND last_value IS NOT NULL
                THEN round(100.0 * last_value / max_value, 2)::float8
                ELSE 0::float8
            END as pct_used
        FROM pg_sequences
        ORDER BY pct_used DESC
    "#;

    let rows = client.query(query, &[]).await?;
    let mut sequences = Vec::new();

    for row in rows {
        let pct_used: f64 = row.get("pct_used");
        let status = SeqStatus::from_pct(pct_used, warn_threshold, crit_threshold);

        sequences.push(SequenceInfo {
            schema: row.get("schemaname"),
            name: row.get("sequencename"),
            data_type: row.get("data_type"),
            last_value: row.get("last_value"),
            max_value: row.get("max_value"),
            pct_used,
            status,
            estimated_exhaustion: None, // Would require historical data
        });
    }

    let overall_status = sequences
        .iter()
        .map(|s| &s.status)
        .max_by_key(|s| match s {
            SeqStatus::Healthy => 0,
            SeqStatus::Warning => 1,
            SeqStatus::Critical => 2,
        })
        .cloned()
        .unwrap_or(SeqStatus::Healthy);

    Ok(SequencesResult {
        sequences,
        overall_status,
        warning_threshold: warn_threshold,
        critical_threshold: crit_threshold,
    })
}

/// Format large numbers for display
fn format_number(n: i64) -> String {
    if n >= 1_000_000_000_000 {
        format!("{:.1}T", n as f64 / 1_000_000_000_000.0)
    } else if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

/// Print sequences in human-readable format
pub fn print_human(result: &SequencesResult, quiet: bool, show_all: bool) {
    let to_show: Vec<_> = if show_all {
        result.sequences.iter().collect()
    } else {
        // Only show non-healthy or top 10
        result
            .sequences
            .iter()
            .filter(|s| s.status != SeqStatus::Healthy || result.sequences.len() <= 10)
            .take(20)
            .collect()
    };

    if to_show.is_empty() {
        if !quiet {
            println!(
                "All sequences healthy (below {}% threshold)",
                result.warning_threshold
            );
        }
        return;
    }

    println!("SEQUENCES:");
    println!();

    // Header
    println!(
        "  {:3} {:40} {:>12} {:>12} {:>7}",
        "", "SEQUENCE", "CURRENT", "MAX", "USED"
    );
    println!("  {}", "-".repeat(78));

    for seq in to_show {
        let full_name = format!("{}.{}", seq.schema, seq.name);
        // Use chars().count() for UTF-8 safe length check
        let display_name = if full_name.chars().count() > 40 {
            format!("{}...", full_name.chars().take(37).collect::<String>())
        } else {
            full_name
        };
        println!(
            "  {} {:40} {:>12} {:>12} {:>6.1}%",
            seq.status.emoji(),
            display_name,
            format_number(seq.last_value),
            format_number(seq.max_value),
            seq.pct_used
        );
    }

    // Summary
    let warning_count = result
        .sequences
        .iter()
        .filter(|s| s.status == SeqStatus::Warning)
        .count();
    let critical_count = result
        .sequences
        .iter()
        .filter(|s| s.status == SeqStatus::Critical)
        .count();

    if warning_count > 0 || critical_count > 0 {
        println!();
        if critical_count > 0 {
            println!(
                "  ✗ {} sequences CRITICAL (>{}%)",
                critical_count, result.critical_threshold
            );
        }
        if warning_count > 0 {
            println!(
                "  ⚠ {} sequences WARNING (>{}%)",
                warning_count, result.warning_threshold
            );
        }
    }

    // Actions for critical sequences
    let critical_seqs: Vec<_> = result
        .sequences
        .iter()
        .filter(|s| s.status == SeqStatus::Critical)
        .collect();

    if !critical_seqs.is_empty() {
        println!();
        println!("RECOMMENDED ACTIONS:");
        println!();

        for seq in critical_seqs.iter().take(3) {
            // For bigint, suggest increasing max or resetting
            if seq.data_type == "bigint" {
                println!(
                    "  -- {}.{} is at {:.1}% of bigint max",
                    seq.schema, seq.name, seq.pct_used
                );
                println!(
                    "  -- Consider: is this sequence actually exhausting, or is last_value stale?"
                );
            } else {
                // For int/smallint, suggest upgrading to bigint
                println!(
                    "  -- Upgrade {}.{} from {} to bigint:",
                    seq.schema, seq.name, seq.data_type
                );
                println!("  ALTER SEQUENCE {}.{} AS bigint;", seq.schema, seq.name);
            }
            println!();
        }
    }
}

/// Print sequences as JSON with schema versioning.
pub fn print_json(
    result: &SequencesResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Convert SeqStatus to Severity
    let severity = match result.overall_status {
        SeqStatus::Healthy => Severity::Healthy,
        SeqStatus::Warning => Severity::Warning,
        SeqStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::SEQUENCES, result, severity, t),
        None => DiagnosticOutput::new(schema::SEQUENCES, result, severity),
    };
    output.print()?;
    Ok(())
}

/// Run sequences check with default thresholds
pub async fn run_sequences(
    client: &Client,
    warn_threshold: Option<i32>,
    crit_threshold: Option<i32>,
) -> Result<SequencesResult> {
    let warn = warn_threshold.unwrap_or(DEFAULT_WARNING_PCT);
    let crit = crit_threshold.unwrap_or(DEFAULT_CRITICAL_PCT);
    get_sequences(client, warn, crit).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_status_healthy() {
        assert_eq!(SeqStatus::from_pct(50.0, 70, 85), SeqStatus::Healthy);
    }

    #[test]
    fn test_seq_status_warning() {
        assert_eq!(SeqStatus::from_pct(75.0, 70, 85), SeqStatus::Warning);
    }

    #[test]
    fn test_seq_status_critical() {
        assert_eq!(SeqStatus::from_pct(90.0, 70, 85), SeqStatus::Critical);
    }

    #[test]
    fn test_format_number_trillion() {
        assert_eq!(format_number(1_500_000_000_000), "1.5T");
    }

    #[test]
    fn test_format_number_billion() {
        assert_eq!(format_number(2_147_483_647), "2.1B");
    }

    #[test]
    fn test_format_number_million() {
        assert_eq!(format_number(5_500_000), "5.5M");
    }

    #[test]
    fn test_format_number_thousand() {
        assert_eq!(format_number(12_500), "12.5K");
    }

    #[test]
    fn test_format_number_small() {
        assert_eq!(format_number(500), "500");
    }
}
