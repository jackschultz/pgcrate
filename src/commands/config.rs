//! Config command: Review PostgreSQL configuration settings.
//!
//! Compares current settings against common best practices. Note that optimal
//! settings depend heavily on workload (OLTP vs OLAP vs mixed) and hardware.
//!
//! IMPORTANT: Recommendations are suggestions, not requirements. Always test
//! changes in non-production first.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Status for config settings - never Critical (these are just suggestions)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigStatus {
    /// Setting looks reasonable
    Ok,
    /// Suggestion for potential improvement (not a problem)
    Suggestion,
}

impl ConfigStatus {
    pub fn emoji(&self) -> &'static str {
        match self {
            ConfigStatus::Ok => "✓",
            ConfigStatus::Suggestion => "⚠",
        }
    }
}

/// A configuration setting with optional recommendation
#[derive(Debug, Clone, Serialize)]
pub struct ConfigSetting {
    pub name: String,
    pub current_value: String,
    pub current_value_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<String>,
    pub status: ConfigStatus,
    pub category: String,
    /// True if changing this setting requires a server restart
    pub requires_restart: bool,
}

/// Full configuration review results
#[derive(Debug, Serialize)]
pub struct ConfigResult {
    pub settings: Vec<ConfigSetting>,
    pub postgres_version: String,
    pub has_suggestions: bool,
    /// Always included disclaimer
    pub disclaimer: String,
}

/// Settings we review and their categories
const SETTINGS_TO_CHECK: &[(&str, &str)] = &[
    // Memory
    ("shared_buffers", "memory"),
    ("effective_cache_size", "memory"),
    ("work_mem", "memory"),
    ("maintenance_work_mem", "memory"),
    // Connections
    ("max_connections", "connections"),
    // WAL
    ("wal_buffers", "wal"),
    ("checkpoint_timeout", "wal"),
    ("max_wal_size", "wal"),
    ("min_wal_size", "wal"),
    // Planner
    ("random_page_cost", "planner"),
    ("effective_io_concurrency", "planner"),
    // Parallelism
    ("max_worker_processes", "parallelism"),
    ("max_parallel_workers", "parallelism"),
    ("max_parallel_workers_per_gather", "parallelism"),
];

/// Parse PostgreSQL memory setting to bytes
fn parse_memory_setting(value: &str, unit: Option<&str>) -> Option<i64> {
    let value = value.trim();

    // If unit is provided separately (from pg_settings), use it
    if let Some(u) = unit {
        if let Ok(n) = value.parse::<i64>() {
            return match u {
                "8kB" => Some(n * 8 * 1024),
                "kB" => Some(n * 1024),
                "MB" => Some(n * 1024 * 1024),
                "GB" => Some(n * 1024 * 1024 * 1024),
                _ => Some(n),
            };
        }
    }

    // Try parsing with suffix
    if let Some(n) = value.strip_suffix("GB") {
        n.trim().parse::<i64>().ok().map(|v| v * 1024 * 1024 * 1024)
    } else if let Some(n) = value.strip_suffix("MB") {
        n.trim().parse::<i64>().ok().map(|v| v * 1024 * 1024)
    } else if let Some(n) = value.strip_suffix("kB") {
        n.trim().parse::<i64>().ok().map(|v| v * 1024)
    } else {
        value.parse::<i64>().ok()
    }
}

/// Format bytes for display
fn format_bytes(bytes: i64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{} GB", bytes / (1024 * 1024 * 1024))
    } else if bytes >= 1024 * 1024 {
        format!("{} MB", bytes / (1024 * 1024))
    } else if bytes >= 1024 {
        format!("{} KB", bytes / 1024)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Simple word-wrap for long strings
fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current_line = String::new();

    for word in text.split_whitespace() {
        if current_line.is_empty() {
            current_line = word.to_string();
        } else if current_line.len() + 1 + word.len() <= width {
            current_line.push(' ');
            current_line.push_str(word);
        } else {
            lines.push(current_line);
            current_line = word.to_string();
        }
    }

    if !current_line.is_empty() {
        lines.push(current_line);
    }

    lines
}

/// Generate recommendation for a setting
fn get_recommendation(
    name: &str,
    value: &str,
    _unit: Option<&str>,
    bytes: Option<i64>,
) -> Option<(String, ConfigStatus)> {
    match name {
        "shared_buffers" => {
            // 128MB is the typical default, often too low for production
            if let Some(b) = bytes {
                if b <= 128 * 1024 * 1024 {
                    return Some((
                        "Default value (128MB). For dedicated database servers, 25% of RAM (max ~8GB) is typical.".to_string(),
                        ConfigStatus::Suggestion,
                    ));
                }
            }
            None
        }
        "effective_cache_size" => {
            // Should be ~50-75% of available RAM
            if let Some(b) = bytes {
                if b <= 512 * 1024 * 1024 {
                    return Some((
                        "Low value. Should typically be 50-75% of total system RAM.".to_string(),
                        ConfigStatus::Suggestion,
                    ));
                }
            }
            None
        }
        "work_mem" => {
            // 4MB is default, often fine but depends on query complexity
            if let Some(b) = bytes {
                if b <= 4 * 1024 * 1024 {
                    return Some((
                        "Default value (4MB). Increase for complex sorts/hashes, but be careful: this is per-operation, not per-connection.".to_string(),
                        ConfigStatus::Ok, // Just informational, not a suggestion
                    ));
                }
            }
            None
        }
        "maintenance_work_mem" => {
            // 64MB is default, often too low for large tables
            if let Some(b) = bytes {
                if b <= 64 * 1024 * 1024 {
                    return Some((
                        "Default value (64MB). For servers with large tables, 256MB-1GB can speed up VACUUM and index creation.".to_string(),
                        ConfigStatus::Suggestion,
                    ));
                }
            }
            None
        }
        "max_connections" => {
            // 100 is default, often either too low or too high
            if let Ok(n) = value.parse::<i32>() {
                if n > 200 {
                    return Some((
                        "High value. Consider using a connection pooler (PgBouncer) instead of increasing max_connections.".to_string(),
                        ConfigStatus::Suggestion,
                    ));
                }
            }
            None
        }
        "max_wal_size" => {
            // 1GB is default, may cause frequent checkpoints under write load
            if let Some(b) = bytes {
                if b <= 1024 * 1024 * 1024 {
                    return Some((
                        "Default value (1GB). For write-heavy workloads, 2-4GB can reduce checkpoint frequency.".to_string(),
                        ConfigStatus::Suggestion,
                    ));
                }
            }
            None
        }
        "random_page_cost" => {
            // 4.0 is default, tuned for spinning disks
            if let Ok(n) = value.parse::<f64>() {
                if n >= 4.0 {
                    return Some((
                        "Default value (4.0) is tuned for spinning disks. For SSDs, 1.1-1.5 is more appropriate.".to_string(),
                        ConfigStatus::Suggestion,
                    ));
                }
            }
            None
        }
        "effective_io_concurrency" => {
            // 1 is default on many systems, too low for SSDs
            if let Ok(n) = value.parse::<i32>() {
                if n <= 1 {
                    return Some((
                        "Default value (1). For SSDs, 200 is a common recommendation.".to_string(),
                        ConfigStatus::Suggestion,
                    ));
                }
            }
            None
        }
        _ => None,
    }
}

/// Run configuration review
pub async fn run_config(client: &Client) -> Result<ConfigResult> {
    // Get PostgreSQL version
    let version_query = "SELECT version()";
    let version_row = client.query_one(version_query, &[]).await?;
    let postgres_version: String = version_row.get(0);

    // Get settings
    let names: Vec<&str> = SETTINGS_TO_CHECK.iter().map(|(n, _)| *n).collect();
    let query = r#"
        SELECT name, setting, unit, short_desc, context
        FROM pg_settings
        WHERE name = ANY($1)
    "#;

    let rows = client.query(query, &[&names]).await?;

    let mut settings = Vec::new();
    let mut has_suggestions = false;

    // Create a map for category lookup
    let category_map: std::collections::HashMap<&str, &str> =
        SETTINGS_TO_CHECK.iter().copied().collect();

    for row in rows {
        let name: String = row.get("name");
        let value: String = row.get("setting");
        let unit: Option<String> = row.get("unit");
        let context: String = row.get("context");

        // postmaster context means restart required; sighup means reload only
        let requires_restart = context == "postmaster";

        let bytes = parse_memory_setting(&value, unit.as_deref());
        let (recommendation, status) = get_recommendation(&name, &value, unit.as_deref(), bytes)
            .unwrap_or((String::new(), ConfigStatus::Ok));

        let recommendation = if recommendation.is_empty() {
            None
        } else {
            if status == ConfigStatus::Suggestion {
                has_suggestions = true;
            }
            Some(recommendation)
        };

        let category = category_map
            .get(name.as_str())
            .unwrap_or(&"other")
            .to_string();

        // Format current value with unit for display
        let display_value = if let Some(ref u) = unit {
            if let Some(b) = bytes {
                format!("{} ({})", value, format_bytes(b))
            } else {
                format!("{} {}", value, u)
            }
        } else {
            value.clone()
        };

        settings.push(ConfigSetting {
            name,
            current_value: display_value,
            current_value_bytes: bytes,
            unit,
            recommendation,
            status,
            category,
            requires_restart,
        });
    }

    // Sort by category then name
    settings.sort_by(|a, b| {
        let cat_order = ["memory", "connections", "wal", "planner", "parallelism"];
        let a_idx = cat_order
            .iter()
            .position(|&c| c == a.category)
            .unwrap_or(99);
        let b_idx = cat_order
            .iter()
            .position(|&c| c == b.category)
            .unwrap_or(99);
        (a_idx, &a.name).cmp(&(b_idx, &b.name))
    });

    Ok(ConfigResult {
        settings,
        postgres_version,
        has_suggestions,
        disclaimer: "Recommendations are suggestions based on common patterns. Optimal settings depend on your specific workload, hardware, and requirements. Always test changes in non-production first.".to_string(),
    })
}

/// Print config in human-readable format
pub fn print_human(result: &ConfigResult, _quiet: bool) {
    println!("CONFIGURATION REVIEW");
    println!("====================");
    println!();
    println!(
        "PostgreSQL: {}",
        result
            .postgres_version
            .lines()
            .next()
            .unwrap_or(&result.postgres_version)
    );
    println!();

    let mut current_category = String::new();

    for setting in &result.settings {
        // Print category header
        if setting.category != current_category {
            current_category = setting.category.clone();
            let header = match current_category.as_str() {
                "memory" => "Memory Settings:",
                "connections" => "Connection Settings:",
                "wal" => "WAL Settings:",
                "planner" => "Planner Settings:",
                "parallelism" => "Parallelism Settings:",
                _ => "Other Settings:",
            };
            println!("{}", header);
        }

        // Print setting (with restart indicator if needed)
        let restart_marker = if setting.requires_restart {
            " [restart]"
        } else {
            ""
        };
        println!(
            "  {} {:30} {}{}",
            setting.status.emoji(),
            format!("{}:", setting.name),
            setting.current_value,
            restart_marker
        );

        // Print recommendation if any
        if let Some(ref rec) = setting.recommendation {
            // Wrap long recommendations
            for line in wrap_text(rec, 60) {
                println!("       {}", line);
            }
        }
    }

    // Disclaimer
    println!();
    println!("Note: {}", result.disclaimer);
}

/// Print config as JSON with schema versioning
pub fn print_json(
    result: &ConfigResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Config review is never Critical - just suggestions
    let severity = if result.has_suggestions {
        Severity::Warning
    } else {
        Severity::Healthy
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::CONFIG, result, severity, t),
        None => DiagnosticOutput::new(schema::CONFIG, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_setting_with_unit() {
        // pg_settings returns value in 8kB units for shared_buffers
        assert_eq!(
            parse_memory_setting("16384", Some("8kB")),
            Some(16384 * 8 * 1024)
        );
    }

    #[test]
    fn test_parse_memory_setting_mb() {
        assert_eq!(parse_memory_setting("256MB", None), Some(256 * 1024 * 1024));
    }

    #[test]
    fn test_parse_memory_setting_gb() {
        assert_eq!(
            parse_memory_setting("4GB", None),
            Some(4 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn test_format_bytes_gb() {
        assert_eq!(format_bytes(2 * 1024 * 1024 * 1024), "2 GB");
    }

    #[test]
    fn test_format_bytes_mb() {
        assert_eq!(format_bytes(256 * 1024 * 1024), "256 MB");
    }

    #[test]
    fn test_shared_buffers_default_suggestion() {
        let (rec, status) = get_recommendation(
            "shared_buffers",
            "16384",
            Some("8kB"),
            Some(128 * 1024 * 1024),
        )
        .unwrap();
        assert_eq!(status, ConfigStatus::Suggestion);
        assert!(rec.contains("128MB"));
    }

    #[test]
    fn test_shared_buffers_large_ok() {
        // 4GB shared_buffers should not get a suggestion
        let result = get_recommendation(
            "shared_buffers",
            "524288",
            Some("8kB"),
            Some(4 * 1024 * 1024 * 1024),
        );
        assert!(result.is_none());
    }
}
