//! Connections command: Connection usage analysis.
//!
//! Monitors connection pool usage against max_connections to identify
//! connection exhaustion risks and connection management issues.

use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::HashMap;
use tokio_postgres::Client;

/// Status thresholds (percentage of max_connections)
const CONN_WARNING_PCT: f64 = 75.0;
const CONN_CRITICAL_PCT: f64 = 90.0;

/// Connection status level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Healthy,
    Warning,
    Critical,
}

impl ConnectionStatus {
    pub fn from_pct(pct: f64) -> Self {
        if pct >= CONN_CRITICAL_PCT {
            ConnectionStatus::Critical
        } else if pct >= CONN_WARNING_PCT {
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

/// Connection statistics
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionStats {
    pub total: i32,
    pub max_connections: i32,
    pub reserved_connections: i32,
    pub available: i32,
    pub usage_pct: f64,
    pub by_state: HashMap<String, i32>,
    pub status: ConnectionStatus,
}

/// Connections grouped by user
#[derive(Debug, Clone, Serialize)]
pub struct UserConnections {
    pub username: String,
    pub count: i32,
    pub by_state: HashMap<String, i32>,
}

/// Connections grouped by database
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseConnections {
    pub database: String,
    pub count: i32,
    pub by_state: HashMap<String, i32>,
}

/// Connections grouped by application
#[derive(Debug, Clone, Serialize)]
pub struct ApplicationConnections {
    pub application_name: String,
    pub count: i32,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_application: Option<Vec<ApplicationConnections>>,
    pub overall_status: ConnectionStatus,
}

/// Get max_connections setting
async fn get_max_connections(client: &Client) -> Result<i32> {
    let query = "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'";
    let row = client
        .query_one(query, &[])
        .await
        .context("Failed to get max_connections")?;
    Ok(row.get::<_, i32>(0))
}

/// Get superuser_reserved_connections setting
async fn get_reserved_connections(client: &Client) -> Result<i32> {
    let query = "SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections'";
    let row = client
        .query_one(query, &[])
        .await
        .context("Failed to get superuser_reserved_connections")?;
    Ok(row.get::<_, i32>(0))
}

/// Get current connection count
async fn get_total_connections(client: &Client) -> Result<i32> {
    let query = "SELECT COUNT(*)::int FROM pg_stat_activity WHERE pid != pg_backend_pid()";
    let row = client
        .query_one(query, &[])
        .await
        .context("Failed to count connections")?;
    Ok(row.get::<_, i32>(0))
}

/// Get connection counts by state
async fn get_connections_by_state(client: &Client) -> Result<HashMap<String, i32>> {
    let query = r#"
        SELECT
            COALESCE(state, 'null') as state,
            COUNT(*)::int as count
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        GROUP BY state
        ORDER BY count DESC
    "#;

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to get connections by state")?;

    let mut by_state = HashMap::new();
    for row in rows {
        let state: String = row.get("state");
        let count: i32 = row.get("count");
        by_state.insert(state, count);
    }

    Ok(by_state)
}

/// Get connection counts grouped by user
async fn get_connections_by_user(client: &Client) -> Result<Vec<UserConnections>> {
    let query = r#"
        SELECT
            COALESCE(usename, '<none>') as username,
            COALESCE(state, 'null') as state,
            COUNT(*)::int as count
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        GROUP BY usename, state
        ORDER BY usename, count DESC
    "#;

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to get connections by user")?;

    // Aggregate into UserConnections
    let mut user_map: HashMap<String, UserConnections> = HashMap::new();
    for row in rows {
        let username: String = row.get("username");
        let state: String = row.get("state");
        let count: i32 = row.get("count");

        let entry = user_map.entry(username.clone()).or_insert_with(|| UserConnections {
            username,
            count: 0,
            by_state: HashMap::new(),
        });
        entry.count += count;
        entry.by_state.insert(state, count);
    }

    let mut users: Vec<_> = user_map.into_values().collect();
    users.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(users)
}

/// Get connection counts grouped by database
async fn get_connections_by_database(client: &Client) -> Result<Vec<DatabaseConnections>> {
    let query = r#"
        SELECT
            COALESCE(datname, '<none>') as database,
            COALESCE(state, 'null') as state,
            COUNT(*)::int as count
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        GROUP BY datname, state
        ORDER BY datname, count DESC
    "#;

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to get connections by database")?;

    // Aggregate into DatabaseConnections
    let mut db_map: HashMap<String, DatabaseConnections> = HashMap::new();
    for row in rows {
        let database: String = row.get("database");
        let state: String = row.get("state");
        let count: i32 = row.get("count");

        let entry = db_map.entry(database.clone()).or_insert_with(|| DatabaseConnections {
            database,
            count: 0,
            by_state: HashMap::new(),
        });
        entry.count += count;
        entry.by_state.insert(state, count);
    }

    let mut dbs: Vec<_> = db_map.into_values().collect();
    dbs.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(dbs)
}

/// Get connection counts grouped by application
async fn get_connections_by_application(client: &Client) -> Result<Vec<ApplicationConnections>> {
    let query = r#"
        SELECT
            COALESCE(NULLIF(application_name, ''), '<none>') as app_name,
            COALESCE(state, 'null') as state,
            COUNT(*)::int as count
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        GROUP BY application_name, state
        ORDER BY application_name, count DESC
    "#;

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to get connections by application")?;

    // Aggregate into ApplicationConnections
    let mut app_map: HashMap<String, ApplicationConnections> = HashMap::new();
    for row in rows {
        let application_name: String = row.get("app_name");
        let state: String = row.get("state");
        let count: i32 = row.get("count");

        let entry = app_map
            .entry(application_name.clone())
            .or_insert_with(|| ApplicationConnections {
                application_name,
                count: 0,
                by_state: HashMap::new(),
            });
        entry.count += count;
        entry.by_state.insert(state, count);
    }

    let mut apps: Vec<_> = app_map.into_values().collect();
    apps.sort_by(|a, b| b.count.cmp(&a.count));
    Ok(apps)
}

/// Run full connections analysis
pub async fn run_connections(
    client: &Client,
    include_by_user: bool,
    include_by_database: bool,
    include_by_application: bool,
) -> Result<ConnectionsResult> {
    let max_connections = get_max_connections(client).await?;
    let reserved_connections = get_reserved_connections(client).await?;
    let total = get_total_connections(client).await?;
    let by_state = get_connections_by_state(client).await?;

    let available = max_connections - reserved_connections;
    let usage_pct = if available > 0 {
        (100.0 * total as f64) / available as f64
    } else {
        0.0
    };
    let status = ConnectionStatus::from_pct(usage_pct);

    let stats = ConnectionStats {
        total,
        max_connections,
        reserved_connections,
        available,
        usage_pct,
        by_state,
        status,
    };

    let by_user = if include_by_user {
        Some(get_connections_by_user(client).await?)
    } else {
        None
    };

    let by_database = if include_by_database {
        Some(get_connections_by_database(client).await?)
    } else {
        None
    };

    let by_application = if include_by_application {
        Some(get_connections_by_application(client).await?)
    } else {
        None
    };

    Ok(ConnectionsResult {
        stats,
        by_user,
        by_database,
        by_application,
        overall_status: status,
    })
}

/// Print connections in human-readable format
pub fn print_human(result: &ConnectionsResult, quiet: bool) {
    let stats = &result.stats;

    println!("CONNECTIONS:");
    println!();
    println!(
        "  {} {}/{} ({:.1}%)",
        stats.status.emoji(),
        stats.total,
        stats.available,
        stats.usage_pct
    );
    println!();
    println!(
        "    max_connections: {}",
        stats.max_connections
    );
    println!(
        "    superuser_reserved: {}",
        stats.reserved_connections
    );
    println!(
        "    available: {}",
        stats.available
    );
    println!("    in use: {}", stats.total);

    // State breakdown
    if !stats.by_state.is_empty() {
        println!();
        println!("  BY STATE:");
        let mut states: Vec<_> = stats.by_state.iter().collect();
        states.sort_by(|a, b| b.1.cmp(a.1));
        for (state, count) in states {
            let state_display = if state == "null" { "null (backend)" } else { state };
            println!("    {:30} {:>5}", state_display, count);
        }
    }

    // By user breakdown
    if let Some(ref users) = result.by_user {
        println!();
        println!("  BY USER:");
        for user in users.iter().take(10) {
            let states: String = user
                .by_state
                .iter()
                .map(|(s, c)| format!("{}:{}", s, c))
                .collect::<Vec<_>>()
                .join(", ");
            println!("    {:30} {:>5}  ({})", user.username, user.count, states);
        }
        if users.len() > 10 {
            println!("    ... and {} more users", users.len() - 10);
        }
    }

    // By database breakdown
    if let Some(ref dbs) = result.by_database {
        println!();
        println!("  BY DATABASE:");
        for db in dbs.iter().take(10) {
            let states: String = db
                .by_state
                .iter()
                .map(|(s, c)| format!("{}:{}", s, c))
                .collect::<Vec<_>>()
                .join(", ");
            println!("    {:30} {:>5}  ({})", db.database, db.count, states);
        }
        if dbs.len() > 10 {
            println!("    ... and {} more databases", dbs.len() - 10);
        }
    }

    // By application breakdown
    if let Some(ref apps) = result.by_application {
        println!();
        println!("  BY APPLICATION:");
        for app in apps.iter().take(10) {
            let app_name = if app.application_name.chars().count() > 30 {
                format!("{}...", app.application_name.chars().take(27).collect::<String>())
            } else {
                app.application_name.clone()
            };
            println!("    {:30} {:>5}", app_name, app.count);
        }
        if apps.len() > 10 {
            println!("    ... and {} more applications", apps.len() - 10);
        }
    }

    // Warnings
    if !quiet {
        match stats.status {
            ConnectionStatus::Critical => {
                println!();
                println!("  ✗ CRITICAL: Connection usage >{}%", CONN_CRITICAL_PCT);
                println!("    Consider increasing max_connections or investigating connection leaks");
            }
            ConnectionStatus::Warning => {
                println!();
                println!("  ⚠ WARNING: Connection usage >{}%", CONN_WARNING_PCT);
                println!("    Monitor for potential connection exhaustion");
            }
            ConnectionStatus::Healthy => {}
        }

        // Check for idle in transaction
        if let Some(idle_in_tx) = stats.by_state.get("idle in transaction") {
            if *idle_in_tx > 5 {
                println!();
                println!(
                    "  ⚠ {} connections idle in transaction (potential lock holders)",
                    idle_in_tx
                );
            }
        }
    }
}

/// Print connections as JSON with schema versioning
pub fn print_json(
    result: &ConnectionsResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

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
        assert_eq!(ConnectionStatus::from_pct(50.0), ConnectionStatus::Healthy);
    }

    #[test]
    fn test_connection_status_warning() {
        assert_eq!(ConnectionStatus::from_pct(80.0), ConnectionStatus::Warning);
    }

    #[test]
    fn test_connection_status_critical() {
        assert_eq!(ConnectionStatus::from_pct(95.0), ConnectionStatus::Critical);
    }

    #[test]
    fn test_connection_status_boundary() {
        assert_eq!(ConnectionStatus::from_pct(74.9), ConnectionStatus::Healthy);
        assert_eq!(ConnectionStatus::from_pct(75.0), ConnectionStatus::Warning);
        assert_eq!(ConnectionStatus::from_pct(89.9), ConnectionStatus::Warning);
        assert_eq!(ConnectionStatus::from_pct(90.0), ConnectionStatus::Critical);
    }
}
