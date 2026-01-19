//! Connections command: Connection analysis vs max_connections.
//!
//! Shows connection usage vs limits, with breakdowns by state,
//! user, and database.

use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use tokio_postgres::Client;

/// Status level for connection usage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Healthy,
    Warning,
    Critical,
}

impl ConnectionStatus {
    /// Derive status from usage percentage.
    /// - Critical: > 90%
    /// - Warning: > 75%
    /// - Healthy: <= 75%
    pub fn from_usage_pct(pct: f64) -> Self {
        if pct > 90.0 {
            ConnectionStatus::Critical
        } else if pct > 75.0 {
            ConnectionStatus::Warning
        } else {
            ConnectionStatus::Healthy
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            ConnectionStatus::Healthy => "✓",
            ConnectionStatus::Warning => "⚠",
            ConnectionStatus::Critical => "✗",
        }
    }
}

/// Connection statistics summary
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionStats {
    pub total: i32,
    pub max_connections: i32,
    pub usage_pct: f64,
    pub reserved_superuser: i32,
    pub available: i32,
    pub by_state: HashMap<String, i32>,
    pub status: ConnectionStatus,
}

/// Connections grouped by user
#[derive(Debug, Clone, Serialize)]
pub struct UserConnections {
    pub username: String,
    pub total: i32,
    pub by_state: HashMap<String, i32>,
}

/// Connections grouped by database
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseConnections {
    pub database: String,
    pub total: i32,
    pub by_state: HashMap<String, i32>,
}

/// Full connections results
#[derive(Debug, Serialize)]
pub struct ConnectionsResult {
    pub stats: ConnectionStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_user: Option<Vec<UserConnections>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_database: Option<Vec<DatabaseConnections>>,
    pub overall_status: ConnectionStatus,
}

async fn get_max_connections(client: &Client) -> Result<i32> {
    let row = client
        .query_one(
            "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'",
            &[],
        )
        .await?;
    Ok(row.get(0))
}

async fn get_reserved_superuser(client: &Client) -> Result<i32> {
    let row = client
        .query_one(
            "SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections'",
            &[],
        )
        .await?;
    Ok(row.get(0))
}

/// Get connection statistics
pub async fn get_connections(
    client: &Client,
    include_by_user: bool,
    include_by_database: bool,
) -> Result<ConnectionsResult> {
    let max_connections = get_max_connections(client).await?;
    let reserved_superuser = get_reserved_superuser(client).await?;

    // Get connection counts by state
    let state_query = r#"
        SELECT
            COALESCE(state, 'unknown') as state,
            count(*)::int as count
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        GROUP BY state
        ORDER BY count DESC
    "#;

    let state_rows = client.query(state_query, &[]).await?;

    let mut by_state: HashMap<String, i32> = HashMap::new();
    let mut total = 0i32;

    for row in state_rows {
        let state: String = row.get("state");
        let count: i32 = row.get("count");
        by_state.insert(state, count);
        total += count;
    }

    // Add 1 for our own connection
    total += 1;

    let usage_pct = if max_connections > 0 {
        100.0 * total as f64 / max_connections as f64
    } else {
        0.0
    };

    let available = max_connections - reserved_superuser - total;
    let status = ConnectionStatus::from_usage_pct(usage_pct);

    let stats = ConnectionStats {
        total,
        max_connections,
        usage_pct,
        reserved_superuser,
        available: available.max(0),
        by_state,
        status,
    };

    // Get by user if requested
    let by_user = if include_by_user {
        let user_query = r#"
            SELECT
                COALESCE(usename, 'unknown') as username,
                COALESCE(state, 'unknown') as state,
                count(*)::int as count
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
            GROUP BY usename, state
            ORDER BY usename, count DESC
        "#;

        let user_rows = client.query(user_query, &[]).await?;

        let mut user_map: HashMap<String, UserConnections> = HashMap::new();

        for row in user_rows {
            let username: String = row.get("username");
            let state: String = row.get("state");
            let count: i32 = row.get("count");

            let entry = user_map.entry(username.clone()).or_insert(UserConnections {
                username,
                total: 0,
                by_state: HashMap::new(),
            });

            entry.total += count;
            entry.by_state.insert(state, count);
        }

        let mut users: Vec<_> = user_map.into_values().collect();
        users.sort_by(|a, b| b.total.cmp(&a.total));
        Some(users)
    } else {
        None
    };

    // Get by database if requested
    let by_database = if include_by_database {
        let db_query = r#"
            SELECT
                COALESCE(datname, 'unknown') as database,
                COALESCE(state, 'unknown') as state,
                count(*)::int as count
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
            GROUP BY datname, state
            ORDER BY datname, count DESC
        "#;

        let db_rows = client.query(db_query, &[]).await?;

        let mut db_map: HashMap<String, DatabaseConnections> = HashMap::new();

        for row in db_rows {
            let database: String = row.get("database");
            let state: String = row.get("state");
            let count: i32 = row.get("count");

            let entry = db_map
                .entry(database.clone())
                .or_insert(DatabaseConnections {
                    database,
                    total: 0,
                    by_state: HashMap::new(),
                });

            entry.total += count;
            entry.by_state.insert(state, count);
        }

        let mut dbs: Vec<_> = db_map.into_values().collect();
        dbs.sort_by(|a, b| b.total.cmp(&a.total));
        Some(dbs)
    } else {
        None
    };

    Ok(ConnectionsResult {
        stats,
        by_user,
        by_database,
        overall_status: status,
    })
}

/// Print connections in human-readable format
pub fn print_human(result: &ConnectionsResult, quiet: bool) {
    let stats = &result.stats;

    println!("CONNECTIONS:");
    println!();

    // Summary line
    println!(
        "  {} {}/{} connections ({:.1}%)",
        stats.status.emoji(),
        stats.total,
        stats.max_connections,
        stats.usage_pct
    );
    println!(
        "     Reserved for superuser: {}, Available: {}",
        stats.reserved_superuser, stats.available
    );
    println!();

    // By state breakdown
    println!("  BY STATE:");
    let mut states: Vec<_> = stats.by_state.iter().collect();
    states.sort_by(|a, b| b.1.cmp(a.1));

    for (state, count) in states {
        let state_display = match state.as_str() {
            "idle in transaction (aborted)" => "idle in tx (aborted)",
            s => s,
        };
        println!("    {:25} {:>5}", state_display, count);
    }

    // By user if available
    if let Some(ref by_user) = result.by_user {
        println!();
        println!("  BY USER:");
        for user in by_user.iter().take(10) {
            let states_str: String = user
                .by_state
                .iter()
                .map(|(s, c)| format!("{}:{}", abbreviate_state(s), c))
                .collect::<Vec<_>>()
                .join(", ");
            println!(
                "    {:25} {:>5}  ({})",
                user.username, user.total, states_str
            );
        }
        if by_user.len() > 10 && !quiet {
            println!("    ... and {} more users", by_user.len() - 10);
        }
    }

    // By database if available
    if let Some(ref by_database) = result.by_database {
        println!();
        println!("  BY DATABASE:");
        for db in by_database.iter().take(10) {
            let states_str: String = db
                .by_state
                .iter()
                .map(|(s, c)| format!("{}:{}", abbreviate_state(s), c))
                .collect::<Vec<_>>()
                .join(", ");
            println!("    {:25} {:>5}  ({})", db.database, db.total, states_str);
        }
        if by_database.len() > 10 && !quiet {
            println!("    ... and {} more databases", by_database.len() - 10);
        }
    }

    // Status-based recommendations
    if stats.status == ConnectionStatus::Critical {
        println!();
        println!("  CRITICAL: Connection pool near exhaustion!");
        println!("  Recommendations:");
        println!("    - Check for connection leaks in application code");
        println!("    - Review idle connections that could be closed");
        println!("    - Consider using a connection pooler (PgBouncer, pgpool)");
        println!("    - Increase max_connections if resources allow");
    } else if stats.status == ConnectionStatus::Warning {
        println!();
        println!("  WARNING: Connection usage above 75%");
        println!("  Monitor closely and consider optimization.");
    }
}

fn abbreviate_state(state: &str) -> &str {
    match state {
        "active" => "act",
        "idle" => "idl",
        "idle in transaction" => "itx",
        "idle in transaction (aborted)" => "abt",
        "fastpath function call" => "fpc",
        "disabled" => "dis",
        _ => state,
    }
}

/// Print connections as JSON with schema versioning
pub fn print_json(
    result: &ConnectionsResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Convert ConnectionStatus to Severity
    let severity = match result.overall_status {
        ConnectionStatus::Healthy => Severity::Healthy,
        ConnectionStatus::Warning => Severity::Warning,
        ConnectionStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::CONNECTIONS, result, severity, t),
        None => DiagnosticOutput::new(schema::CONNECTIONS, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_status_healthy() {
        assert_eq!(
            ConnectionStatus::from_usage_pct(50.0),
            ConnectionStatus::Healthy
        );
        assert_eq!(
            ConnectionStatus::from_usage_pct(75.0),
            ConnectionStatus::Healthy
        );
    }

    #[test]
    fn test_connection_status_warning() {
        assert_eq!(
            ConnectionStatus::from_usage_pct(76.0),
            ConnectionStatus::Warning
        );
        assert_eq!(
            ConnectionStatus::from_usage_pct(90.0),
            ConnectionStatus::Warning
        );
    }

    #[test]
    fn test_connection_status_critical() {
        assert_eq!(
            ConnectionStatus::from_usage_pct(91.0),
            ConnectionStatus::Critical
        );
        assert_eq!(
            ConnectionStatus::from_usage_pct(100.0),
            ConnectionStatus::Critical
        );
    }

    #[test]
    fn test_abbreviate_state() {
        assert_eq!(abbreviate_state("active"), "act");
        assert_eq!(abbreviate_state("idle"), "idl");
        assert_eq!(abbreviate_state("idle in transaction"), "itx");
    }
}
