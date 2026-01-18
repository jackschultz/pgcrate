//! Triage command: Quick health check showing the 20 lines you check first.
//!
//! Runs multiple diagnostic checks in parallel, aggregates results,
//! and provides a summary with drill-down suggestions.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Result of a single triage check
#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    /// Check name (e.g., "blocking_locks")
    pub name: &'static str,
    /// Display label (e.g., "BLOCKING LOCKS")
    pub label: &'static str,
    /// Status: healthy, warning, or critical
    pub status: CheckStatus,
    /// One-line summary
    pub summary: String,
    /// Optional detailed information
    pub details: Option<String>,
    /// Suggested drill-down command
    pub action: Option<String>,
}

/// Status level for a check
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Healthy,
    Warning,
    Critical,
    Error,
}

impl CheckStatus {
    pub fn emoji(&self) -> &'static str {
        match self {
            CheckStatus::Healthy => "✓",
            CheckStatus::Warning => "⚠",
            CheckStatus::Critical => "✗",
            CheckStatus::Error => "?",
        }
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            CheckStatus::Healthy => 0,
            CheckStatus::Warning => 1,
            CheckStatus::Critical => 2,
            CheckStatus::Error => 3,
        }
    }
}

/// Full triage results
#[derive(Debug, Serialize)]
pub struct TriageResults {
    pub checks: Vec<CheckResult>,
    pub overall_status: CheckStatus,
}

impl TriageResults {
    pub fn new(checks: Vec<CheckResult>) -> Self {
        let overall_status = checks
            .iter()
            .map(|c| &c.status)
            .max_by_key(|s| match s {
                CheckStatus::Healthy => 0,
                CheckStatus::Warning => 1,
                CheckStatus::Critical => 2,
                CheckStatus::Error => 3,
            })
            .cloned()
            .unwrap_or(CheckStatus::Healthy);

        Self {
            checks,
            overall_status,
        }
    }

    pub fn exit_code(&self) -> i32 {
        self.overall_status.exit_code()
    }
}

/// Run all triage checks and return aggregated results
pub async fn run_triage(client: &Client) -> TriageResults {
    // Run checks sequentially (sharing connection).
    // For true parallelism we'd need a connection pool, but sequential
    // is fine for diagnostic queries that complete quickly.
    let checks = vec![
        check_blocking_locks(client).await,
        check_long_transactions(client).await,
        check_xid_age(client).await,
        check_sequences(client).await,
        check_connections(client).await,
        check_replication_lag(client).await,
        check_stats_age(client).await,
    ];

    TriageResults::new(checks)
}

/// Check for blocking lock chains
async fn check_blocking_locks(client: &Client) -> CheckResult {
    let name = "blocking_locks";
    let label = "BLOCKING LOCKS";

    let query = r#"
        SELECT
            count(*) as blocked_count,
            max(extract(epoch from now() - a.query_start))::int as oldest_seconds
        FROM pg_stat_activity a
        WHERE a.wait_event_type = 'Lock'
          AND a.state != 'idle'
          AND cardinality(pg_blocking_pids(a.pid)) > 0
    "#;

    match client.query_one(query, &[]).await {
        Ok(row) => {
            let blocked_count: i64 = row.get("blocked_count");
            let oldest_seconds: Option<i32> = row.get("oldest_seconds");

            if blocked_count == 0 {
                CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "No blocking locks".to_string(),
                    details: None,
                    action: None,
                }
            } else {
                let oldest_min = oldest_seconds.unwrap_or(0) / 60;
                let status = if oldest_min > 30 {
                    CheckStatus::Critical
                } else {
                    CheckStatus::Warning
                };

                CheckResult {
                    name,
                    label,
                    status,
                    summary: format!("{} blocked (oldest: {} min)", blocked_count, oldest_min),
                    details: None,
                    action: Some("pgcrate locks --blocking".to_string()),
                }
            }
        }
        Err(e) => CheckResult {
            name,
            label,
            status: CheckStatus::Error,
            summary: format!("Query failed: {}", e),
            details: None,
            action: None,
        },
    }
}

/// Check for long-running transactions
async fn check_long_transactions(client: &Client) -> CheckResult {
    let name = "long_transactions";
    let label = "LONG TRANSACTIONS";

    let query = r#"
        SELECT
            count(*) as count,
            max(extract(epoch from now() - xact_start))::int as oldest_seconds
        FROM pg_stat_activity
        WHERE state != 'idle'
          AND xact_start IS NOT NULL
          AND extract(epoch from now() - xact_start) > 300  -- > 5 minutes
    "#;

    match client.query_one(query, &[]).await {
        Ok(row) => {
            let count: i64 = row.get("count");
            let oldest_seconds: Option<i32> = row.get("oldest_seconds");

            if count == 0 {
                CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "No long transactions (>5 min)".to_string(),
                    details: None,
                    action: None,
                }
            } else {
                let oldest_min = oldest_seconds.unwrap_or(0) / 60;
                let status = if oldest_min > 30 {
                    CheckStatus::Critical
                } else {
                    CheckStatus::Warning
                };

                CheckResult {
                    name,
                    label,
                    status,
                    summary: format!("{} long transactions (oldest: {} min)", count, oldest_min),
                    details: None,
                    action: Some("pgcrate sql \"SELECT pid, state, query_start, query FROM pg_stat_activity WHERE xact_start < now() - interval '5 minutes'\"".to_string()),
                }
            }
        }
        Err(e) => CheckResult {
            name,
            label,
            status: CheckStatus::Error,
            summary: format!("Query failed: {}", e),
            details: None,
            action: None,
        },
    }
}

/// Check transaction ID (XID) age for wraparound risk
async fn check_xid_age(client: &Client) -> CheckResult {
    let name = "xid_age";
    let label = "XID AGE";

    let query = r#"
        SELECT
            datname,
            age(datfrozenxid) as xid_age
        FROM pg_database
        WHERE datallowconn
        ORDER BY age(datfrozenxid) DESC
        LIMIT 1
    "#;

    match client.query_one(query, &[]).await {
        Ok(row) => {
            let datname: String = row.get("datname");
            let xid_age: i32 = row.get("xid_age");

            // XID wraparound happens at 2^31 (~2.1 billion)
            let max_xid: i64 = 2_147_483_648;
            let pct = (xid_age as f64 / max_xid as f64 * 100.0) as i32;

            let status = if xid_age > 1_800_000_000 {
                CheckStatus::Critical
            } else if xid_age > 1_500_000_000 {
                CheckStatus::Warning
            } else {
                CheckStatus::Healthy
            };

            let summary = format!(
                "{:.1}B / 2.1B ({}%) in {}",
                xid_age as f64 / 1_000_000_000.0,
                pct,
                datname
            );

            let action = if status != CheckStatus::Healthy {
                Some("VACUUM FREEZE or autovacuum tuning needed".to_string())
            } else {
                None
            };

            CheckResult {
                name,
                label,
                status,
                summary,
                details: None,
                action,
            }
        }
        Err(e) => CheckResult {
            name,
            label,
            status: CheckStatus::Error,
            summary: format!("Query failed: {}", e),
            details: None,
            action: None,
        },
    }
}

/// Check sequence exhaustion risk
async fn check_sequences(client: &Client) -> CheckResult {
    let name = "sequences";
    let label = "SEQUENCES";

    // Check sequences that are >70% exhausted
    let query = r#"
        SELECT
            schemaname || '.' || sequencename as seq_name,
            last_value,
            CASE
                WHEN increment_by > 0 THEN
                    (last_value::numeric / max_value::numeric * 100)::int
                ELSE
                    ((min_value::numeric - last_value::numeric) / (min_value::numeric - max_value::numeric) * 100)::int
            END as pct_used
        FROM pg_sequences
        WHERE last_value IS NOT NULL
        ORDER BY pct_used DESC
        LIMIT 5
    "#;

    match client.query(query, &[]).await {
        Ok(rows) => {
            let critical: Vec<_> = rows
                .iter()
                .filter(|r| {
                    let pct: i32 = r.get("pct_used");
                    pct > 85
                })
                .collect();

            let warning: Vec<_> = rows
                .iter()
                .filter(|r| {
                    let pct: i32 = r.get("pct_used");
                    pct > 70 && pct <= 85
                })
                .collect();

            if !critical.is_empty() {
                let seq_name: String = critical[0].get("seq_name");
                let pct: i32 = critical[0].get("pct_used");
                CheckResult {
                    name,
                    label,
                    status: CheckStatus::Critical,
                    summary: format!("{} at {}% (+ {} more)", seq_name, pct, critical.len() - 1),
                    details: None,
                    action: Some("pgcrate sequences".to_string()),
                }
            } else if !warning.is_empty() {
                let seq_name: String = warning[0].get("seq_name");
                let pct: i32 = warning[0].get("pct_used");
                CheckResult {
                    name,
                    label,
                    status: CheckStatus::Warning,
                    summary: format!("{} at {}%", seq_name, pct),
                    details: None,
                    action: Some("pgcrate sequences".to_string()),
                }
            } else {
                CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "All sequences healthy".to_string(),
                    details: None,
                    action: None,
                }
            }
        }
        Err(e) => CheckResult {
            name,
            label,
            status: CheckStatus::Error,
            summary: format!("Query failed: {}", e),
            details: None,
            action: None,
        },
    }
}

/// Check connection usage
async fn check_connections(client: &Client) -> CheckResult {
    let name = "connections";
    let label = "CONNECTIONS";

    let query = r#"
        SELECT
            (SELECT count(*) FROM pg_stat_activity) as current,
            (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max
    "#;

    match client.query_one(query, &[]).await {
        Ok(row) => {
            let current: i64 = row.get("current");
            let max: i32 = row.get("max");
            let pct = (current as f64 / max as f64 * 100.0) as i32;

            let status = if pct > 95 {
                CheckStatus::Critical
            } else if pct > 80 {
                CheckStatus::Warning
            } else {
                CheckStatus::Healthy
            };

            CheckResult {
                name,
                label,
                status,
                summary: format!("{} / {} ({}%)", current, max, pct),
                details: None,
                action: if status != CheckStatus::Healthy {
                    Some("pgcrate sql \"SELECT usename, count(*) FROM pg_stat_activity GROUP BY usename ORDER BY count DESC\"".to_string())
                } else {
                    None
                },
            }
        }
        Err(e) => CheckResult {
            name,
            label,
            status: CheckStatus::Error,
            summary: format!("Query failed: {}", e),
            details: None,
            action: None,
        },
    }
}

/// Check replication lag
async fn check_replication_lag(client: &Client) -> CheckResult {
    let name = "replication";
    let label = "REPLICATION";

    // First check if this is a primary with replicas
    let query = r#"
        SELECT
            client_addr,
            state,
            COALESCE(
                extract(epoch from replay_lag)::int,
                extract(epoch from write_lag)::int,
                0
            ) as lag_seconds
        FROM pg_stat_replication
        ORDER BY lag_seconds DESC
        LIMIT 1
    "#;

    match client.query_opt(query, &[]).await {
        Ok(Some(row)) => {
            let client_addr: Option<std::net::IpAddr> = row.get("client_addr");
            let state: String = row.get("state");
            let lag_seconds: i32 = row.get("lag_seconds");

            let status = if lag_seconds > 300 {
                CheckStatus::Critical
            } else if lag_seconds > 30 {
                CheckStatus::Warning
            } else {
                CheckStatus::Healthy
            };

            let addr_str = client_addr
                .map(|a| a.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            CheckResult {
                name,
                label,
                status,
                summary: format!("{}: {} lag {}s", addr_str, state, lag_seconds),
                details: None,
                action: if status != CheckStatus::Healthy {
                    Some("Check replica health and network".to_string())
                } else {
                    None
                },
            }
        }
        Ok(None) => {
            // No replicas - check if we're a replica ourselves
            let is_replica_query = "SELECT pg_is_in_recovery()";
            match client.query_one(is_replica_query, &[]).await {
                Ok(row) => {
                    let is_replica: bool = row.get(0);
                    if is_replica {
                        CheckResult {
                            name,
                            label,
                            status: CheckStatus::Healthy,
                            summary: "This is a replica".to_string(),
                            details: None,
                            action: None,
                        }
                    } else {
                        CheckResult {
                            name,
                            label,
                            status: CheckStatus::Healthy,
                            summary: "No replicas configured".to_string(),
                            details: None,
                            action: None,
                        }
                    }
                }
                Err(_) => CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "No replicas configured".to_string(),
                    details: None,
                    action: None,
                },
            }
        }
        Err(e) => CheckResult {
            name,
            label,
            status: CheckStatus::Error,
            summary: format!("Query failed: {}", e),
            details: None,
            action: None,
        },
    }
}

/// Check if stats have been recently reset (too fresh to be useful)
async fn check_stats_age(client: &Client) -> CheckResult {
    let name = "stats_age";
    let label = "STATS AGE";

    let query = r#"
        SELECT
            extract(epoch from now() - stats_reset)::int as age_seconds
        FROM pg_stat_database
        WHERE datname = current_database()
    "#;

    match client.query_one(query, &[]).await {
        Ok(row) => {
            let age_seconds: Option<i32> = row.get("age_seconds");

            match age_seconds {
                Some(age) if age < 3600 => {
                    let age_min = age / 60;
                    CheckResult {
                        name,
                        label,
                        status: CheckStatus::Warning,
                        summary: format!("Stats reset {} min ago (too fresh)", age_min),
                        details: Some("Stats may not be representative".to_string()),
                        action: None,
                    }
                }
                Some(age) => {
                    let age_hours = age / 3600;
                    let age_days = age / 86400;
                    let summary = if age_days > 0 {
                        format!("Stats age: {} days", age_days)
                    } else {
                        format!("Stats age: {} hours", age_hours)
                    };
                    CheckResult {
                        name,
                        label,
                        status: CheckStatus::Healthy,
                        summary,
                        details: None,
                        action: None,
                    }
                }
                None => CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "Stats never reset".to_string(),
                    details: None,
                    action: None,
                },
            }
        }
        Err(e) => CheckResult {
            name,
            label,
            status: CheckStatus::Error,
            summary: format!("Query failed: {}", e),
            details: None,
            action: None,
        },
    }
}

/// Print triage results in human-readable format
pub fn print_human(results: &TriageResults, quiet: bool) {
    if quiet {
        // In quiet mode, only show non-healthy checks
        for check in &results.checks {
            if check.status != CheckStatus::Healthy {
                println!(
                    "{} {}: {}",
                    check.status.emoji(),
                    check.label,
                    check.summary
                );
                if let Some(ref action) = check.action {
                    println!("  → {}", action);
                }
            }
        }
        return;
    }

    // Find longest label for alignment
    let max_label = results
        .checks
        .iter()
        .map(|c| c.label.len())
        .max()
        .unwrap_or(0);

    for check in &results.checks {
        let status_str = match check.status {
            CheckStatus::Healthy => format!("{}  healthy", check.status.emoji()),
            CheckStatus::Warning => format!("{}  WARNING", check.status.emoji()),
            CheckStatus::Critical => format!("{}  CRITICAL", check.status.emoji()),
            CheckStatus::Error => format!("{}  ERROR", check.status.emoji()),
        };

        println!(
            "{:width$}  {:40} {}",
            check.label,
            check.summary,
            status_str,
            width = max_label
        );
    }

    // Print drill-down suggestions
    let actionable: Vec<_> = results
        .checks
        .iter()
        .filter(|c| c.action.is_some() && c.status != CheckStatus::Healthy)
        .collect();

    if !actionable.is_empty() {
        println!();
        println!("DRILL DOWN:");
        for check in actionable {
            if let Some(ref action) = check.action {
                println!("  {} → {}", check.label, action);
            }
        }
    }
}

/// Print triage results as JSON with schema versioning.
pub fn print_json(results: &TriageResults) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput};
    let output = DiagnosticOutput::new(schema::TRIAGE, results);
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_status_ordering() {
        let results = TriageResults::new(vec![
            CheckResult {
                name: "test1",
                label: "TEST1",
                status: CheckStatus::Healthy,
                summary: "ok".to_string(),
                details: None,
                action: None,
            },
            CheckResult {
                name: "test2",
                label: "TEST2",
                status: CheckStatus::Warning,
                summary: "warn".to_string(),
                details: None,
                action: None,
            },
        ]);
        assert_eq!(results.overall_status, CheckStatus::Warning);
        assert_eq!(results.exit_code(), 1);
    }

    #[test]
    fn test_check_status_critical_wins() {
        let results = TriageResults::new(vec![
            CheckResult {
                name: "test1",
                label: "TEST1",
                status: CheckStatus::Warning,
                summary: "warn".to_string(),
                details: None,
                action: None,
            },
            CheckResult {
                name: "test2",
                label: "TEST2",
                status: CheckStatus::Critical,
                summary: "crit".to_string(),
                details: None,
                action: None,
            },
        ]);
        assert_eq!(results.overall_status, CheckStatus::Critical);
        assert_eq!(results.exit_code(), 2);
    }

    #[test]
    fn test_all_healthy() {
        let results = TriageResults::new(vec![CheckResult {
            name: "test1",
            label: "TEST1",
            status: CheckStatus::Healthy,
            summary: "ok".to_string(),
            details: None,
            action: None,
        }]);
        assert_eq!(results.overall_status, CheckStatus::Healthy);
        assert_eq!(results.exit_code(), 0);
    }

    #[test]
    fn test_status_emoji() {
        assert_eq!(CheckStatus::Healthy.emoji(), "✓");
        assert_eq!(CheckStatus::Warning.emoji(), "⚠");
        assert_eq!(CheckStatus::Critical.emoji(), "✗");
        assert_eq!(CheckStatus::Error.emoji(), "?");
    }
}
