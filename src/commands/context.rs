//! Context command: Show connection context and server information.
//!
//! Provides information about the current connection, server capabilities,
//! installed extensions, and effective privileges. Useful for understanding
//! what pgcrate can do in the current environment.

use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use tokio_postgres::Client;

/// Connection target information
#[derive(Debug, Serialize)]
pub struct TargetInfo {
    /// Host (redacted by default for security)
    pub host: String,
    /// Port number
    pub port: u16,
    /// Database name
    pub database: String,
    /// Connected user
    pub user: String,
    /// Whether connection is read-only
    pub readonly: bool,
}

/// Server information
#[derive(Debug, Serialize)]
pub struct ServerInfo {
    /// Full version string (e.g., "PostgreSQL 16.1 on x86_64-linux")
    pub version: String,
    /// Numeric version (e.g., 160001 for 16.0.1)
    pub version_num: i32,
    /// Major version (e.g., 16)
    pub version_major: i32,
    /// Whether server is a replica (in recovery mode)
    pub in_recovery: bool,
    /// Data directory (if readable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_directory: Option<String>,
}

/// Privilege information for diagnostic capabilities
#[derive(Debug, Serialize)]
pub struct PrivilegeInfo {
    /// Can read pg_stat_activity (for locks, sessions)
    pub pg_stat_activity_select: bool,
    /// Can call pg_cancel_backend (for canceling queries)
    pub pg_cancel_backend_execute: bool,
    /// Can call pg_terminate_backend (for killing connections)
    pub pg_terminate_backend_execute: bool,
    /// Can read pg_stat_statements (for query analysis)
    pub pg_stat_statements_select: bool,
    /// Whether user is superuser
    pub is_superuser: bool,
    /// Current user roles
    pub roles: Vec<String>,
}

/// Full context data
#[derive(Debug, Serialize)]
pub struct ContextData {
    pub target: TargetInfo,
    pub server: ServerInfo,
    /// Installed extensions (name -> version available)
    pub extensions: HashMap<String, String>,
    /// Effective privileges
    pub privileges: PrivilegeInfo,
}

/// Context results wrapper
#[derive(Debug, Serialize)]
pub struct ContextResult {
    #[serde(flatten)]
    pub context: ContextData,
}

/// Query target information from the connection
pub async fn get_target_info(
    client: &Client,
    connection_url: &str,
    read_only: bool,
    no_redact: bool,
) -> Result<TargetInfo> {
    // Parse connection URL to get host/port
    let url = url::Url::parse(connection_url)?;
    let host = if no_redact {
        url.host_str().unwrap_or("localhost").to_string()
    } else {
        // Redact to just show it's configured
        url.host_str()
            .map(|h| {
                if h == "localhost" || h == "127.0.0.1" {
                    h.to_string()
                } else {
                    format!("{}...", &h.chars().take(4).collect::<String>())
                }
            })
            .unwrap_or_else(|| "***".to_string())
    };
    let port = url.port().unwrap_or(5432);

    let row = client
        .query_one("SELECT current_database(), current_user", &[])
        .await?;
    let database: String = row.get(0);
    let user: String = row.get(1);

    Ok(TargetInfo {
        host,
        port,
        database,
        user,
        readonly: read_only,
    })
}

/// Query server information
pub async fn get_server_info(client: &Client, no_redact: bool) -> Result<ServerInfo> {
    let row = client
        .query_one(
            r#"
            SELECT
                version(),
                current_setting('server_version_num')::int,
                pg_is_in_recovery()
            "#,
            &[],
        )
        .await?;

    let version: String = row.get(0);
    let version_num: i32 = row.get(1);
    let in_recovery: bool = row.get(2);

    // Extract major version from version_num (e.g., 160001 -> 16)
    let version_major = version_num / 10000;

    // Data directory is sensitive (reveals filesystem paths) - only show with --no-redact
    let data_directory = if no_redact {
        match client
            .query_one("SELECT current_setting('data_directory')", &[])
            .await
        {
            Ok(row) => {
                let dir: String = row.get(0);
                Some(dir)
            }
            Err(_) => None,
        }
    } else {
        None
    };

    Ok(ServerInfo {
        version,
        version_num,
        version_major,
        in_recovery,
        data_directory,
    })
}

/// Query installed extensions
pub async fn get_extensions(client: &Client) -> Result<HashMap<String, String>> {
    let rows = client
        .query(
            "SELECT extname, extversion FROM pg_extension ORDER BY extname",
            &[],
        )
        .await?;

    let mut extensions = HashMap::new();
    for row in rows {
        let name: String = row.get(0);
        let version: String = row.get(1);
        extensions.insert(name, version);
    }

    Ok(extensions)
}

/// Query privilege information
pub async fn get_privileges(client: &Client) -> Result<PrivilegeInfo> {
    // Check various privileges in parallel
    let pg_stat_activity = client
        .query_one(
            "SELECT has_table_privilege('pg_stat_activity', 'SELECT')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    let pg_cancel = client
        .query_one(
            "SELECT has_function_privilege('pg_cancel_backend(int)', 'EXECUTE')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    let pg_terminate = client
        .query_one(
            "SELECT has_function_privilege('pg_terminate_backend(int)', 'EXECUTE')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    // Check if pg_stat_statements extension exists and is accessible
    let pg_stat_statements = client
        .query_one(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
            ) AND has_table_privilege('pg_stat_statements', 'SELECT')
            "#,
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    // Check superuser status
    let is_superuser: bool = client
        .query_one("SELECT current_setting('is_superuser') = 'on'", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    // Get user roles
    let role_rows = client
        .query(
            r#"
            SELECT r.rolname
            FROM pg_roles r
            JOIN pg_auth_members m ON r.oid = m.roleid
            JOIN pg_roles u ON m.member = u.oid
            WHERE u.rolname = current_user
            ORDER BY r.rolname
            "#,
            &[],
        )
        .await
        .unwrap_or_default();

    let roles: Vec<String> = role_rows.iter().map(|r| r.get(0)).collect();

    Ok(PrivilegeInfo {
        pg_stat_activity_select: pg_stat_activity,
        pg_cancel_backend_execute: pg_cancel,
        pg_terminate_backend_execute: pg_terminate,
        pg_stat_statements_select: pg_stat_statements,
        is_superuser,
        roles,
    })
}

/// Run full context analysis
pub async fn run_context(
    client: &Client,
    connection_url: &str,
    read_only: bool,
    no_redact: bool,
) -> Result<ContextResult> {
    let target = get_target_info(client, connection_url, read_only, no_redact).await?;
    let server = get_server_info(client, no_redact).await?;
    let extensions = get_extensions(client).await?;
    let privileges = get_privileges(client).await?;

    Ok(ContextResult {
        context: ContextData {
            target,
            server,
            extensions,
            privileges,
        },
    })
}

/// Print context in human-readable format
pub fn print_human(result: &ContextResult) {
    let ctx = &result.context;

    println!("CONNECTION:");
    println!("  Host:     {}", ctx.target.host);
    println!("  Port:     {}", ctx.target.port);
    println!("  Database: {}", ctx.target.database);
    println!("  User:     {}", ctx.target.user);
    println!(
        "  Mode:     {}",
        if ctx.target.readonly {
            "read-only"
        } else {
            "read-write"
        }
    );

    println!();
    println!("SERVER:");
    println!(
        "  Version:     {} ({})",
        ctx.server.version_major, ctx.server.version_num
    );
    println!(
        "  Recovery:    {}",
        if ctx.server.in_recovery {
            "yes (replica)"
        } else {
            "no (primary)"
        }
    );
    if let Some(ref dir) = ctx.server.data_directory {
        println!("  Data dir:    {}", dir);
    }

    println!();
    println!("EXTENSIONS ({}):", ctx.extensions.len());
    let mut exts: Vec<_> = ctx.extensions.iter().collect();
    exts.sort_by_key(|(name, _)| name.as_str());
    for (name, version) in exts.iter().take(10) {
        println!("  {} ({})", name, version);
    }
    if ctx.extensions.len() > 10 {
        println!("  ... and {} more", ctx.extensions.len() - 10);
    }

    println!();
    println!("PRIVILEGES:");
    println!(
        "  Superuser:          {}",
        if ctx.privileges.is_superuser {
            "yes"
        } else {
            "no"
        }
    );
    println!(
        "  pg_stat_activity:   {}",
        if ctx.privileges.pg_stat_activity_select {
            "✓"
        } else {
            "✗"
        }
    );
    println!(
        "  pg_cancel_backend:  {}",
        if ctx.privileges.pg_cancel_backend_execute {
            "✓"
        } else {
            "✗"
        }
    );
    println!(
        "  pg_terminate:       {}",
        if ctx.privileges.pg_terminate_backend_execute {
            "✓"
        } else {
            "✗"
        }
    );
    println!(
        "  pg_stat_statements: {}",
        if ctx.privileges.pg_stat_statements_select {
            "✓"
        } else {
            "✗"
        }
    );

    if !ctx.privileges.roles.is_empty() {
        println!();
        println!("ROLES:");
        for role in &ctx.privileges.roles {
            println!("  {}", role);
        }
    }
}

/// Print context as JSON with schema versioning
pub fn print_json(
    result: &ContextResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Context is informational, always healthy unless there's an error
    let severity = Severity::Healthy;

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::CONTEXT, result, severity, t),
        None => DiagnosticOutput::new(schema::CONTEXT, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_version_major() {
        // 160001 should be major version 16
        let major = 160001 / 10000;
        assert_eq!(major, 16);

        // 150005 should be major version 15
        let major = 150005 / 10000;
        assert_eq!(major, 15);
    }
}
