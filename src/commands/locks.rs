//! Locks command: Drill-down for blocking chains and long transactions.
//!
//! Provides visibility into lock contention, idle-in-transaction sessions,
//! and the ability to cancel or terminate problematic connections.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// A process involved in locking (either blocking or blocked)
#[derive(Debug, Clone, Serialize)]
pub struct LockProcess {
    pub pid: i32,
    pub usename: String,
    pub application_name: String,
    pub client_addr: Option<String>,
    pub state: String,
    pub wait_event_type: Option<String>,
    pub wait_event: Option<String>,
    pub duration_seconds: i64,
    pub query: String,
    pub blocking_pids: Vec<i32>,
    pub blocked_count: i32,
}

/// A blocking chain (root blocker and its victims)
#[derive(Debug, Clone, Serialize)]
pub struct BlockingChain {
    pub root: LockProcess,
    pub blocked: Vec<LockProcess>,
    pub total_blocked: i32,
    pub oldest_blocked_seconds: i64,
}

/// Results from the locks command
#[derive(Debug, Serialize)]
pub struct LocksResult {
    pub blocking_chains: Vec<BlockingChain>,
    pub long_transactions: Vec<LockProcess>,
    pub idle_in_transaction: Vec<LockProcess>,
}

impl LocksResult {
    /// Apply redaction to all query text in the result.
    pub fn redact(&mut self) {
        for chain in &mut self.blocking_chains {
            chain.root.redact_query();
            for p in &mut chain.blocked {
                p.redact_query();
            }
        }
        for p in &mut self.long_transactions {
            p.redact_query();
        }
        for p in &mut self.idle_in_transaction {
            p.redact_query();
        }
    }
}

impl LockProcess {
    /// Redact the query text to remove string literals.
    pub fn redact_query(&mut self) {
        use crate::redact;
        self.query = redact::redact_query(&self.query);
    }
}

/// Get all blocking chains
pub async fn get_blocking_chains(client: &Client) -> Result<Vec<BlockingChain>> {
    // First, get all blocked processes and their blockers
    let query = r#"
        SELECT
            blocked.pid as blocked_pid,
            blocked.usename as blocked_user,
            blocked.application_name as blocked_app,
            blocked.client_addr::text as blocked_addr,
            blocked.state as blocked_state,
            blocked.wait_event_type,
            blocked.wait_event,
            extract(epoch from now() - blocked.query_start)::bigint as blocked_duration,
            left(blocked.query, 500) as blocked_query,
            blocker.pid as blocker_pid,
            blocker.usename as blocker_user,
            blocker.application_name as blocker_app,
            blocker.client_addr::text as blocker_addr,
            blocker.state as blocker_state,
            extract(epoch from now() - blocker.query_start)::bigint as blocker_duration,
            left(blocker.query, 500) as blocker_query
        FROM pg_stat_activity blocked
        CROSS JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS blocker_pid
        JOIN pg_stat_activity blocker ON blocker.pid = blocker_pid
        WHERE blocked.wait_event_type = 'Lock'
        ORDER BY blocker.query_start, blocked.query_start
    "#;

    let rows = client.query(query, &[]).await?;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    // Build chains: group by root blocker
    use std::collections::{HashMap, HashSet};

    let mut blockers: HashMap<i32, LockProcess> = HashMap::new();
    let mut blocked_by: HashMap<i32, Vec<LockProcess>> = HashMap::new();
    let mut all_blocked: HashSet<i32> = HashSet::new();

    for row in &rows {
        let blocker_pid: i32 = row.get("blocker_pid");
        let blocked_pid: i32 = row.get("blocked_pid");

        all_blocked.insert(blocked_pid);

        // Record the blocker
        blockers.entry(blocker_pid).or_insert_with(|| LockProcess {
            pid: blocker_pid,
            usename: row.get("blocker_user"),
            application_name: row.get("blocker_app"),
            client_addr: row.get("blocker_addr"),
            state: row.get("blocker_state"),
            wait_event_type: None,
            wait_event: None,
            duration_seconds: row.get::<_, Option<i64>>("blocker_duration").unwrap_or(0),
            query: row.get("blocker_query"),
            blocking_pids: vec![],
            blocked_count: 0,
        });

        // Record the blocked process
        let blocked_proc = LockProcess {
            pid: blocked_pid,
            usename: row.get("blocked_user"),
            application_name: row.get("blocked_app"),
            client_addr: row.get("blocked_addr"),
            state: row.get("blocked_state"),
            wait_event_type: row.get("wait_event_type"),
            wait_event: row.get("wait_event"),
            duration_seconds: row.get::<_, Option<i64>>("blocked_duration").unwrap_or(0),
            query: row.get("blocked_query"),
            blocking_pids: vec![blocker_pid],
            blocked_count: 0,
        };

        blocked_by
            .entry(blocker_pid)
            .or_default()
            .push(blocked_proc);
    }

    // Find root blockers (blockers that are not themselves blocked)
    let root_blockers: Vec<i32> = blockers
        .keys()
        .filter(|pid| !all_blocked.contains(pid))
        .copied()
        .collect();

    // Build chains from root blockers
    let mut chains = Vec::new();
    for root_pid in root_blockers {
        if let Some(root) = blockers.get(&root_pid) {
            let blocked = blocked_by.get(&root_pid).cloned().unwrap_or_default();
            let total_blocked = blocked.len() as i32;
            let oldest_blocked_seconds = blocked
                .iter()
                .map(|p| p.duration_seconds)
                .max()
                .unwrap_or(0);

            chains.push(BlockingChain {
                root: LockProcess {
                    blocked_count: total_blocked,
                    ..root.clone()
                },
                blocked,
                total_blocked,
                oldest_blocked_seconds,
            });
        }
    }

    // Sort by impact (most blocked first)
    chains.sort_by(|a, b| b.total_blocked.cmp(&a.total_blocked));

    Ok(chains)
}

/// Get long-running transactions
pub async fn get_long_transactions(client: &Client, min_minutes: u64) -> Result<Vec<LockProcess>> {
    let query = r#"
        SELECT
            pid,
            usename,
            application_name,
            client_addr::text,
            state,
            wait_event_type,
            wait_event,
            extract(epoch from now() - xact_start)::bigint as duration_seconds,
            left(query, 500) as query,
            cardinality(pg_blocking_pids(pid)) as being_blocked
        FROM pg_stat_activity
        WHERE xact_start IS NOT NULL
          AND state != 'idle'
          AND extract(epoch from now() - xact_start) > $1
        ORDER BY xact_start
    "#;

    let min_seconds = (min_minutes * 60) as f64;
    let rows = client.query(query, &[&min_seconds]).await?;

    let mut results = Vec::new();
    for row in rows {
        results.push(LockProcess {
            pid: row.get("pid"),
            usename: row.get("usename"),
            application_name: row.get("application_name"),
            client_addr: row.get("client_addr"),
            state: row.get("state"),
            wait_event_type: row.get("wait_event_type"),
            wait_event: row.get("wait_event"),
            duration_seconds: row.get("duration_seconds"),
            query: row.get("query"),
            blocking_pids: vec![],
            blocked_count: row.get::<_, i32>("being_blocked"),
        });
    }

    Ok(results)
}

/// Get idle-in-transaction sessions
pub async fn get_idle_in_transaction(client: &Client) -> Result<Vec<LockProcess>> {
    let query = r#"
        SELECT
            pid,
            usename,
            application_name,
            client_addr::text,
            state,
            wait_event_type,
            wait_event,
            extract(epoch from now() - state_change)::bigint as duration_seconds,
            left(query, 500) as query
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
           OR state = 'idle in transaction (aborted)'
        ORDER BY state_change
    "#;

    let rows = client.query(query, &[]).await?;

    let mut results = Vec::new();
    for row in rows {
        results.push(LockProcess {
            pid: row.get("pid"),
            usename: row.get("usename"),
            application_name: row.get("application_name"),
            client_addr: row.get("client_addr"),
            state: row.get("state"),
            wait_event_type: row.get("wait_event_type"),
            wait_event: row.get("wait_event"),
            duration_seconds: row.get("duration_seconds"),
            query: row.get("query"),
            blocking_pids: vec![],
            blocked_count: 0,
        });
    }

    Ok(results)
}

/// Get info about a specific PID (for cancel/kill display)
pub async fn get_pid_info(client: &Client, pid: i32) -> Result<LockProcess> {
    let query = r#"
        SELECT
            pid,
            usename,
            application_name,
            client_addr::text,
            state,
            wait_event_type,
            wait_event,
            extract(epoch from now() - COALESCE(query_start, backend_start))::bigint as duration_seconds,
            left(query, 500) as query,
            (SELECT count(*)::int FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0
             AND $1 = ANY(pg_blocking_pids(pid))) as blocked_count
        FROM pg_stat_activity
        WHERE pid = $1
    "#;

    let row = client
        .query_opt(query, &[&pid])
        .await?
        .ok_or_else(|| anyhow::anyhow!("PID {} not found", pid))?;

    Ok(LockProcess {
        pid: row.get("pid"),
        usename: row.get::<_, Option<String>>("usename").unwrap_or_default(),
        application_name: row
            .get::<_, Option<String>>("application_name")
            .unwrap_or_default(),
        client_addr: row.get("client_addr"),
        state: row
            .get::<_, Option<String>>("state")
            .unwrap_or_else(|| "unknown".to_string()),
        wait_event_type: row.get("wait_event_type"),
        wait_event: row.get("wait_event"),
        duration_seconds: row.get::<_, Option<i64>>("duration_seconds").unwrap_or(0),
        query: row.get::<_, Option<String>>("query").unwrap_or_default(),
        blocking_pids: vec![],
        blocked_count: row.get::<_, Option<i32>>("blocked_count").unwrap_or(0),
    })
}

/// Cancel a query (pg_cancel_backend)
pub async fn cancel_query(client: &Client, pid: i32, execute: bool, redact: bool) -> Result<bool> {
    let mut info = get_pid_info(client, pid).await?;
    if redact {
        info.redact_query();
    }

    eprintln!("=== CANCEL QUERY ===");
    print_pid_info(&info);

    if !execute {
        eprintln!();
        eprintln!("Dry-run mode. Add --execute to actually cancel.");
        return Ok(false);
    }

    let result: bool = client
        .query_one("SELECT pg_cancel_backend($1)", &[&pid])
        .await?
        .get(0);

    if result {
        eprintln!("✓ Query cancelled");
    } else {
        eprintln!("✗ Cancel failed (process may have already ended)");
    }

    Ok(result)
}

/// Terminate a connection (pg_terminate_backend)
pub async fn terminate_connection(
    client: &Client,
    pid: i32,
    execute: bool,
    redact: bool,
) -> Result<bool> {
    let mut info = get_pid_info(client, pid).await?;
    if redact {
        info.redact_query();
    }

    eprintln!("=== TERMINATE CONNECTION ===");
    print_pid_info(&info);

    if info.blocked_count > 0 {
        eprintln!(
            "⚠ WARNING: This PID is blocking {} other queries",
            info.blocked_count
        );
    }

    if !execute {
        eprintln!();
        eprintln!("Dry-run mode. Add --execute to actually terminate.");
        return Ok(false);
    }

    let result: bool = client
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await?
        .get(0);

    if result {
        eprintln!("✓ Connection terminated");
    } else {
        eprintln!("✗ Terminate failed (process may have already ended)");
    }

    Ok(result)
}

/// Print info about a PID
fn print_pid_info(info: &LockProcess) {
    eprintln!("PID:   {}", info.pid);
    eprintln!("User:  {}", info.usename);
    eprintln!("App:   {}", info.application_name);
    if let Some(ref addr) = info.client_addr {
        eprintln!("Addr:  {}", addr);
    }
    eprintln!("State: {}", info.state);
    eprintln!("Duration: {}", format_duration(info.duration_seconds));
    eprintln!("Query: {}", truncate_query(&info.query, 80));
}

/// Format duration in human-readable form
fn format_duration(seconds: i64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    }
}

/// Truncate query for display
fn truncate_query(query: &str, max_len: usize) -> String {
    let clean = query.replace('\n', " ").replace("  ", " ");
    if clean.len() <= max_len {
        clean
    } else {
        format!("{}...", &clean[..max_len - 3])
    }
}

/// Print blocking chains in human-readable format
pub fn print_blocking_chains(chains: &[BlockingChain], quiet: bool) {
    if chains.is_empty() {
        if !quiet {
            println!("No blocking locks found.");
        }
        return;
    }

    for (i, chain) in chains.iter().enumerate() {
        if i > 0 {
            println!();
        }

        println!(
            "CHAIN {} — {} blocked, oldest waiting {}",
            i + 1,
            chain.total_blocked,
            format_duration(chain.oldest_blocked_seconds)
        );
        println!();

        // Root blocker
        println!(
            "  BLOCKER: PID {} ({}) — {} for {}",
            chain.root.pid,
            chain.root.usename,
            chain.root.state,
            format_duration(chain.root.duration_seconds)
        );
        println!("    App:   {}", chain.root.application_name);
        println!("    Query: {}", truncate_query(&chain.root.query, 60));

        // Blocked processes
        for blocked in &chain.blocked {
            println!();
            println!(
                "    BLOCKED: PID {} ({}) — waiting {}",
                blocked.pid,
                blocked.usename,
                format_duration(blocked.duration_seconds)
            );
            println!("      App:   {}", blocked.application_name);
            println!("      Query: {}", truncate_query(&blocked.query, 58));
        }
    }

    println!();
    println!("Actions:");
    for chain in chains {
        println!(
            "  pgcrate locks --cancel {} --execute   # Cancel blocker's query",
            chain.root.pid
        );
        println!(
            "  pgcrate locks --kill {} --execute     # Terminate blocker's connection",
            chain.root.pid
        );
    }
}

/// Print long transactions in human-readable format
pub fn print_long_transactions(procs: &[LockProcess], quiet: bool) {
    if procs.is_empty() {
        if !quiet {
            println!("No long-running transactions found.");
        }
        return;
    }

    println!("LONG-RUNNING TRANSACTIONS:");
    println!();

    for proc in procs {
        println!(
            "  PID {} ({}) — {} for {}",
            proc.pid,
            proc.usename,
            proc.state,
            format_duration(proc.duration_seconds)
        );
        println!("    App:   {}", proc.application_name);
        println!("    Query: {}", truncate_query(&proc.query, 60));
        if proc.blocked_count > 0 {
            println!(
                "    ⚠ Being blocked by {} other queries",
                proc.blocked_count
            );
        }
        println!();
    }
}

/// Print idle-in-transaction sessions in human-readable format
pub fn print_idle_in_transaction(procs: &[LockProcess], quiet: bool) {
    if procs.is_empty() {
        if !quiet {
            println!("No idle-in-transaction sessions found.");
        }
        return;
    }

    println!("IDLE-IN-TRANSACTION SESSIONS:");
    println!();

    for proc in procs {
        let state_label = if proc.state.contains("aborted") {
            "ABORTED"
        } else {
            "idle"
        };

        println!(
            "  PID {} ({}) — {} for {}",
            proc.pid,
            proc.usename,
            state_label,
            format_duration(proc.duration_seconds)
        );
        println!("    App:   {}", proc.application_name);
        println!("    Last:  {}", truncate_query(&proc.query, 60));
        println!();
    }

    println!("Actions:");
    println!("  pgcrate locks --cancel <PID> --execute   # Cancel (if still running)");
    println!("  pgcrate locks --kill <PID> --execute     # Terminate connection");
}

/// Print results as JSON with schema versioning.
pub fn print_json(
    result: &LocksResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Derive severity from findings
    let severity = if result
        .blocking_chains
        .iter()
        .any(|c| c.oldest_blocked_seconds > 1800)
    {
        // Any lock blocked > 30 min is critical
        Severity::Critical
    } else if !result.blocking_chains.is_empty()
        || result
            .long_transactions
            .iter()
            .any(|t| t.duration_seconds > 1800)
    {
        // Blocking locks or transactions > 30 min are warnings
        Severity::Warning
    } else if !result.long_transactions.is_empty() || !result.idle_in_transaction.is_empty() {
        Severity::Warning
    } else {
        Severity::Healthy
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::LOCKS, result, severity, t),
        None => DiagnosticOutput::new(schema::LOCKS, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(45), "45s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(125), "2m 5s");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(3665), "1h 1m");
    }

    #[test]
    fn test_truncate_query_short() {
        let query = "SELECT 1";
        assert_eq!(truncate_query(query, 20), "SELECT 1");
    }

    #[test]
    fn test_truncate_query_long() {
        let query = "SELECT * FROM users WHERE id = 1 AND name = 'test'";
        let result = truncate_query(query, 20);
        assert!(result.ends_with("..."));
        assert!(result.len() <= 20);
    }

    #[test]
    fn test_truncate_query_newlines() {
        let query = "SELECT\n  *\nFROM\n  users";
        let result = truncate_query(query, 50);
        assert!(!result.contains('\n'));
    }
}
