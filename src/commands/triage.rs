//! Triage command: Quick health check showing the 20 lines you check first.
//!
//! Runs multiple diagnostic checks, aggregates results, and provides
//! actionable next steps. Degrades gracefully when checks cannot run.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

use crate::reason_codes::ReasonCode;

/// A check that could not be executed.
#[derive(Debug, Clone, Serialize)]
pub struct SkippedCheck {
    /// Check identifier (e.g., "blocking_locks")
    pub check_id: &'static str,
    /// Why the check was skipped (stable enum for automation)
    pub reason_code: ReasonCode,
    /// Human-readable explanation
    pub reason_human: String,
}

/// A structured next action (runnable suggestion).
#[derive(Debug, Clone, Serialize)]
pub struct NextAction {
    /// Command to run (e.g., "pgcrate")
    pub command: &'static str,
    /// Command arguments
    pub args: Vec<&'static str>,
    /// Why this action helps
    pub rationale: &'static str,
}

impl NextAction {
    /// Create a pgcrate command suggestion.
    pub fn pgcrate(args: &[&'static str], rationale: &'static str) -> Self {
        Self {
            command: "pgcrate",
            args: args.to_vec(),
            rationale,
        }
    }

    /// Format as a runnable command string.
    pub fn to_command_string(&self) -> String {
        if self.args.is_empty() {
            self.command.to_string()
        } else {
            format!("{} {}", self.command, self.args.join(" "))
        }
    }
}

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
    /// Suggested next actions (structured, runnable)
    pub next_actions: Vec<NextAction>,
}

/// Status level for a check
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Healthy,
    Warning,
    Critical,
}

impl CheckStatus {
    pub fn emoji(&self) -> &'static str {
        match self {
            CheckStatus::Healthy => "✓",
            CheckStatus::Warning => "⚠",
            CheckStatus::Critical => "✗",
        }
    }

    /// Exit code for findings: 0=healthy, 1=warning, 2=critical.
    /// Operational failures use separate codes >= 10 (see exit_codes module).
    pub fn exit_code(&self) -> i32 {
        match self {
            CheckStatus::Healthy => 0,
            CheckStatus::Warning => 1,
            CheckStatus::Critical => 2,
        }
    }
}

/// Full triage results
#[derive(Debug, Serialize)]
pub struct TriageResults {
    /// Checks that ran successfully, sorted by severity (critical first)
    pub checks: Vec<CheckResult>,
    /// Checks that could not run
    pub skipped_checks: Vec<SkippedCheck>,
    /// Worst status across all checks
    pub overall_status: CheckStatus,
}

impl TriageResults {
    pub fn new(mut checks: Vec<CheckResult>, skipped_checks: Vec<SkippedCheck>) -> Self {
        // Sort by severity: critical first, then warning, then healthy
        checks.sort_by_key(|c| match c.status {
            CheckStatus::Critical => 0,
            CheckStatus::Warning => 1,
            CheckStatus::Healthy => 2,
        });

        let overall_status = checks
            .iter()
            .map(|c| &c.status)
            .max_by_key(|s| match s {
                CheckStatus::Healthy => 0,
                CheckStatus::Warning => 1,
                CheckStatus::Critical => 2,
            })
            .cloned()
            .unwrap_or(CheckStatus::Healthy);

        Self {
            checks,
            skipped_checks,
            overall_status,
        }
    }

    pub fn exit_code(&self) -> i32 {
        self.overall_status.exit_code()
    }
}

fn classify_error(err: &tokio_postgres::Error) -> (ReasonCode, String) {
    let msg = err.to_string();
    let code = ReasonCode::from_postgres_error(err);
    (code, msg)
}

/// Result of running a single check: either success or skip.
enum CheckOutcome {
    Ok(CheckResult),
    Skip(SkippedCheck),
}

/// Run all triage checks and return aggregated results.
/// Checks that fail due to permissions/timeouts are captured as skipped, not errors.
pub async fn run_triage(client: &Client) -> TriageResults {
    let mut checks = Vec::new();
    let mut skipped = Vec::new();

    // Run checks sequentially (sharing connection).
    let outcomes = vec![
        check_blocking_locks(client).await,
        check_long_transactions(client).await,
        check_xid_age(client).await,
        check_sequences(client).await,
        check_connections(client).await,
        check_replication_lag(client).await,
        check_stats_age(client).await,
    ];

    for outcome in outcomes {
        match outcome {
            CheckOutcome::Ok(result) => checks.push(result),
            CheckOutcome::Skip(skip) => skipped.push(skip),
        }
    }

    TriageResults::new(checks, skipped)
}

/// Check for blocking lock chains
async fn check_blocking_locks(client: &Client) -> CheckOutcome {
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
                CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "No blocking locks".to_string(),
                    details: None,
                    next_actions: vec![],
                })
            } else {
                let oldest_min = oldest_seconds.unwrap_or(0) / 60;
                let status = if oldest_min > 30 {
                    CheckStatus::Critical
                } else {
                    CheckStatus::Warning
                };

                CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status,
                    summary: format!("{} blocked (oldest: {} min)", blocked_count, oldest_min),
                    details: None,
                    next_actions: vec![NextAction::pgcrate(
                        &["locks", "--blocking"],
                        "Show blocking chains and candidate PIDs for cancellation",
                    )],
                })
            }
        }
        Err(e) => {
            let (reason_code, reason_human) = classify_error(&e);
            CheckOutcome::Skip(SkippedCheck {
                check_id: name,
                reason_code,
                reason_human,
            })
        }
    }
}

/// Check for long-running transactions
async fn check_long_transactions(client: &Client) -> CheckOutcome {
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
                CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "No long transactions (>5 min)".to_string(),
                    details: None,
                    next_actions: vec![],
                })
            } else {
                let oldest_min = oldest_seconds.unwrap_or(0) / 60;
                let status = if oldest_min > 30 {
                    CheckStatus::Critical
                } else {
                    CheckStatus::Warning
                };

                CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status,
                    summary: format!("{} long transactions (oldest: {} min)", count, oldest_min),
                    details: None,
                    next_actions: vec![NextAction::pgcrate(
                        &["locks", "--long-tx", "5"],
                        "List transactions running longer than 5 minutes",
                    )],
                })
            }
        }
        Err(e) => {
            let (reason_code, reason_human) = classify_error(&e);
            CheckOutcome::Skip(SkippedCheck {
                check_id: name,
                reason_code,
                reason_human,
            })
        }
    }
}

/// Check transaction ID (XID) age for wraparound risk
async fn check_xid_age(client: &Client) -> CheckOutcome {
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

            let next_actions = if status != CheckStatus::Healthy {
                vec![NextAction::pgcrate(
                    &["xid"],
                    "Show detailed XID age by database and table",
                )]
            } else {
                vec![]
            };

            CheckOutcome::Ok(CheckResult {
                name,
                label,
                status,
                summary,
                details: None,
                next_actions,
            })
        }
        Err(e) => {
            let (reason_code, reason_human) = classify_error(&e);
            CheckOutcome::Skip(SkippedCheck {
                check_id: name,
                reason_code,
                reason_human,
            })
        }
    }
}

/// Check sequence exhaustion risk
async fn check_sequences(client: &Client) -> CheckOutcome {
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
                CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status: CheckStatus::Critical,
                    summary: format!("{} at {}% (+ {} more)", seq_name, pct, critical.len() - 1),
                    details: None,
                    next_actions: vec![NextAction::pgcrate(
                        &["sequences"],
                        "Show all sequences with exhaustion risk",
                    )],
                })
            } else if !warning.is_empty() {
                let seq_name: String = warning[0].get("seq_name");
                let pct: i32 = warning[0].get("pct_used");
                CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status: CheckStatus::Warning,
                    summary: format!("{} at {}%", seq_name, pct),
                    details: None,
                    next_actions: vec![NextAction::pgcrate(
                        &["sequences"],
                        "Show all sequences with exhaustion risk",
                    )],
                })
            } else {
                CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "All sequences healthy".to_string(),
                    details: None,
                    next_actions: vec![],
                })
            }
        }
        Err(e) => {
            let (reason_code, reason_human) = classify_error(&e);
            CheckOutcome::Skip(SkippedCheck {
                check_id: name,
                reason_code,
                reason_human,
            })
        }
    }
}

/// Check connection usage
async fn check_connections(client: &Client) -> CheckOutcome {
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

            let next_actions = if status != CheckStatus::Healthy {
                vec![NextAction::pgcrate(
                    &["sql", "SELECT usename, count(*) FROM pg_stat_activity GROUP BY usename ORDER BY count DESC"],
                    "Show connection count by user to identify source",
                )]
            } else {
                vec![]
            };

            CheckOutcome::Ok(CheckResult {
                name,
                label,
                status,
                summary: format!("{} / {} ({}%)", current, max, pct),
                details: None,
                next_actions,
            })
        }
        Err(e) => {
            let (reason_code, reason_human) = classify_error(&e);
            CheckOutcome::Skip(SkippedCheck {
                check_id: name,
                reason_code,
                reason_human,
            })
        }
    }
}

/// Check replication lag
async fn check_replication_lag(client: &Client) -> CheckOutcome {
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

            // No pgcrate command for replication yet - leave next_actions empty
            let next_actions = vec![];

            CheckOutcome::Ok(CheckResult {
                name,
                label,
                status,
                summary: format!("{}: {} lag {}s", addr_str, state, lag_seconds),
                details: None,
                next_actions,
            })
        }
        Ok(None) => {
            // No replicas - check if we're a replica ourselves
            let is_replica_query = "SELECT pg_is_in_recovery()";
            match client.query_one(is_replica_query, &[]).await {
                Ok(row) => {
                    let is_replica: bool = row.get(0);
                    let summary = if is_replica {
                        "This is a replica"
                    } else {
                        "No replicas configured"
                    };
                    CheckOutcome::Ok(CheckResult {
                        name,
                        label,
                        status: CheckStatus::Healthy,
                        summary: summary.to_string(),
                        details: None,
                        next_actions: vec![],
                    })
                }
                Err(e) => {
                    let (reason_code, reason_human) = classify_error(&e);
                    CheckOutcome::Skip(SkippedCheck {
                        check_id: name,
                        reason_code,
                        reason_human,
                    })
                }
            }
        }
        Err(e) => {
            let (reason_code, reason_human) = classify_error(&e);
            CheckOutcome::Skip(SkippedCheck {
                check_id: name,
                reason_code,
                reason_human,
            })
        }
    }
}

/// Check if stats have been recently reset (too fresh to be useful)
async fn check_stats_age(client: &Client) -> CheckOutcome {
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
                    CheckOutcome::Ok(CheckResult {
                        name,
                        label,
                        status: CheckStatus::Warning,
                        summary: format!("Stats reset {} min ago (too fresh)", age_min),
                        details: Some("Stats may not be representative".to_string()),
                        next_actions: vec![],
                    })
                }
                Some(age) => {
                    let age_hours = age / 3600;
                    let age_days = age / 86400;
                    let summary = if age_days > 0 {
                        format!("Stats age: {} days", age_days)
                    } else {
                        format!("Stats age: {} hours", age_hours)
                    };
                    CheckOutcome::Ok(CheckResult {
                        name,
                        label,
                        status: CheckStatus::Healthy,
                        summary,
                        details: None,
                        next_actions: vec![],
                    })
                }
                None => CheckOutcome::Ok(CheckResult {
                    name,
                    label,
                    status: CheckStatus::Healthy,
                    summary: "Stats never reset".to_string(),
                    details: None,
                    next_actions: vec![],
                }),
            }
        }
        Err(e) => {
            let (reason_code, reason_human) = classify_error(&e);
            CheckOutcome::Skip(SkippedCheck {
                check_id: name,
                reason_code,
                reason_human,
            })
        }
    }
}

/// Print triage results in human-readable format
pub fn print_human(results: &TriageResults, quiet: bool) {
    if quiet {
        // In quiet mode, only show non-healthy checks and skipped
        for check in &results.checks {
            if check.status != CheckStatus::Healthy {
                println!(
                    "{} {}: {}",
                    check.status.emoji(),
                    check.label,
                    check.summary
                );
                for action in &check.next_actions {
                    println!("  → {}", action.to_command_string());
                }
            }
        }
        for skip in &results.skipped_checks {
            println!(
                "- {}: skipped ({})",
                skip.check_id,
                skip.reason_code.description()
            );
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

    // Print checks (already sorted by severity)
    for check in &results.checks {
        let status_str = match check.status {
            CheckStatus::Healthy => format!("{}  healthy", check.status.emoji()),
            CheckStatus::Warning => format!("{}  WARNING", check.status.emoji()),
            CheckStatus::Critical => format!("{}  CRITICAL", check.status.emoji()),
        };

        println!(
            "{:width$}  {:40} {}",
            check.label,
            check.summary,
            status_str,
            width = max_label
        );
    }

    // Print skipped checks if any
    if !results.skipped_checks.is_empty() {
        println!();
        println!("SKIPPED:");
        for skip in &results.skipped_checks {
            println!("  {} - {}", skip.check_id, skip.reason_code.description());
        }
    }

    // Print drill-down suggestions
    let actionable: Vec<_> = results
        .checks
        .iter()
        .filter(|c| !c.next_actions.is_empty() && c.status != CheckStatus::Healthy)
        .collect();

    if !actionable.is_empty() {
        println!();
        println!("NEXT ACTIONS:");
        for check in actionable {
            for action in &check.next_actions {
                println!(
                    "  {} → {} ({})",
                    check.label,
                    action.to_command_string(),
                    action.rationale
                );
            }
        }
    }
}

/// Print triage results as JSON with schema versioning.
pub fn print_json(
    results: &TriageResults,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};
    use crate::reason_codes::ReasonInfo;

    // Derive severity from overall status
    let severity = Severity::from_check_status(&results.overall_status);

    // Convert skipped checks to warnings
    let warnings: Vec<ReasonInfo> = results
        .skipped_checks
        .iter()
        .map(|skip| {
            ReasonInfo::new(
                skip.reason_code,
                format!("{}: {}", skip.check_id, skip.reason_human),
            )
        })
        .collect();

    let partial = !results.skipped_checks.is_empty();

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::TRIAGE, results, severity, t),
        None => DiagnosticOutput::new(schema::TRIAGE, results, severity),
    };
    let output = output.with_partial(partial).with_warnings(warnings);
    output.print()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(name: &'static str, label: &'static str, status: CheckStatus) -> CheckResult {
        CheckResult {
            name,
            label,
            status,
            summary: format!("{:?}", status),
            details: None,
            next_actions: vec![],
        }
    }

    #[test]
    fn test_check_status_ordering() {
        let results = TriageResults::new(
            vec![
                check("test1", "TEST1", CheckStatus::Healthy),
                check("test2", "TEST2", CheckStatus::Warning),
            ],
            vec![],
        );
        assert_eq!(results.overall_status, CheckStatus::Warning);
        assert_eq!(results.exit_code(), 1);
    }

    #[test]
    fn test_check_status_critical_wins() {
        let results = TriageResults::new(
            vec![
                check("test1", "TEST1", CheckStatus::Warning),
                check("test2", "TEST2", CheckStatus::Critical),
            ],
            vec![],
        );
        assert_eq!(results.overall_status, CheckStatus::Critical);
        assert_eq!(results.exit_code(), 2);
    }

    #[test]
    fn test_all_healthy() {
        let results =
            TriageResults::new(vec![check("test1", "TEST1", CheckStatus::Healthy)], vec![]);
        assert_eq!(results.overall_status, CheckStatus::Healthy);
        assert_eq!(results.exit_code(), 0);
    }

    #[test]
    fn test_status_emoji() {
        assert_eq!(CheckStatus::Healthy.emoji(), "✓");
        assert_eq!(CheckStatus::Warning.emoji(), "⚠");
        assert_eq!(CheckStatus::Critical.emoji(), "✗");
    }

    #[test]
    fn test_severity_sorting() {
        // Checks should be sorted: critical first, then warning, then healthy
        let results = TriageResults::new(
            vec![
                check("healthy", "HEALTHY", CheckStatus::Healthy),
                check("warning", "WARNING", CheckStatus::Warning),
                check("critical", "CRITICAL", CheckStatus::Critical),
            ],
            vec![],
        );
        assert_eq!(results.checks[0].name, "critical");
        assert_eq!(results.checks[1].name, "warning");
        assert_eq!(results.checks[2].name, "healthy");
    }

    #[test]
    fn test_skipped_checks() {
        let results = TriageResults::new(
            vec![check("test1", "TEST1", CheckStatus::Healthy)],
            vec![SkippedCheck {
                check_id: "replication",
                reason_code: ReasonCode::MissingPrivilege,
                reason_human: "permission denied".to_string(),
            }],
        );
        assert_eq!(results.skipped_checks.len(), 1);
        assert_eq!(results.skipped_checks[0].check_id, "replication");
    }

    #[test]
    fn test_next_action_command_string() {
        let action = NextAction::pgcrate(&["locks", "--blocking"], "test");
        assert_eq!(action.to_command_string(), "pgcrate locks --blocking");
    }
}
