//! Connection management for pgcrate.
//!
//! Supports named connections with:
//! - Environment variable expansion (`${VAR}`)
//! - Command execution for dynamic URLs
//! - Primary/replica role distinction
//! - Read-only mode enforcement

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;
use url::Url;

/// Connection configuration from pgcrate.toml
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ConnectionConfig {
    /// Database URL (can contain `${VAR}` for env var expansion)
    pub url: Option<String>,
    /// Command to execute to get URL (argv array)
    pub command: Option<Vec<String>>,
    /// Connection role
    #[serde(default)]
    pub role: ConnectionRole,
    /// Force read-only mode
    #[serde(default)]
    pub readonly: Option<bool>,
}

/// Connection role (primary or replica)
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionRole {
    #[default]
    Primary,
    Replica,
}

impl std::fmt::Display for ConnectionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionRole::Primary => write!(f, "primary"),
            ConnectionRole::Replica => write!(f, "replica"),
        }
    }
}

/// Policy configuration for connection restrictions
#[derive(Deserialize, Debug, Clone, Default)]
pub struct PolicyConfig {
    /// Forbid --primary flag entirely
    pub allow_primary: Option<bool>,
    /// All connections must be read-only
    pub require_readonly: Option<bool>,
    /// Whitelist of allowed connection names
    pub allowed_connections: Option<Vec<String>>,
}

/// A fully resolved connection ready for use
#[derive(Debug, Clone)]
pub struct ResolvedConnection {
    /// Connection name (e.g., "prod-ro", "local")
    pub name: String,
    /// Resolved database URL (never log this!)
    pub url: String,
    /// Host extracted from URL (safe to display)
    pub host: String,
    /// Port (default 5432)
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub user: String,
    /// Connection role
    pub role: ConnectionRole,
    /// Whether this connection is read-only
    pub readonly: bool,
}

impl ResolvedConnection {
    /// Display string for banner (never includes password)
    pub fn display(&self) -> String {
        format!("{}:{}/{}", self.host, self.port, self.database)
    }

    /// Print connection banner to stderr
    pub fn print_banner(&self) {
        let role_str = match self.role {
            ConnectionRole::Primary => "primary",
            ConnectionRole::Replica => "replica",
        };
        let mode_str = if self.readonly { "read-only" } else { "read-write" };
        eprintln!(
            "pgcrate: {} ({}, {}) as {}",
            self.display(),
            role_str,
            mode_str,
            self.user
        );
    }
}

/// Resolve a connection by name from the connections config
pub fn resolve_connection(
    name: &str,
    connections: &HashMap<String, ConnectionConfig>,
    policy: Option<&PolicyConfig>,
) -> Result<ResolvedConnection> {
    // Check policy whitelist
    if let Some(policy) = policy {
        if let Some(ref allowed) = policy.allowed_connections {
            if !allowed.iter().any(|a| a == name) {
                bail!(
                    "Connection '{}' not in allowed list. Allowed: {}",
                    name,
                    allowed.join(", ")
                );
            }
        }
    }

    let config = connections
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("Connection '{}' not found in pgcrate.toml", name))?;

    let url = resolve_url(name, config)?;
    let parsed = parse_connection_url(&url)?;

    let readonly = config.readonly.unwrap_or(config.role == ConnectionRole::Replica);

    Ok(ResolvedConnection {
        name: name.to_string(),
        url,
        host: parsed.host,
        port: parsed.port,
        database: parsed.database,
        user: parsed.user,
        role: config.role,
        readonly,
    })
}

/// Resolve a connection from an environment variable name
pub fn resolve_from_env_var(env_var: &str) -> Result<ResolvedConnection> {
    let url = std::env::var(env_var).with_context(|| {
        format!(
            "Environment variable '{}' not set.\nHint: Set the variable or use -c <connection> instead.",
            env_var
        )
    })?;

    let parsed = parse_connection_url(&url)?;

    Ok(ResolvedConnection {
        name: env_var.to_string(),
        url,
        host: parsed.host,
        port: parsed.port,
        database: parsed.database,
        user: parsed.user,
        role: ConnectionRole::Primary, // Assume primary for env var connections
        readonly: false,
    })
}

/// Resolve URL from ConnectionConfig (handles env vars and commands)
fn resolve_url(name: &str, config: &ConnectionConfig) -> Result<String> {
    // Command takes precedence over url
    if let Some(ref cmd) = config.command {
        return execute_command(cmd);
    }

    if let Some(ref url_template) = config.url {
        return expand_env_vars(name, url_template);
    }

    bail!(
        "Connection '{}' has neither 'url' nor 'command' defined",
        name
    );
}

/// Expand environment variables in a string (${VAR} syntax)
fn expand_env_vars(conn_name: &str, template: &str) -> Result<String> {
    let mut result = template.to_string();
    let mut start = 0;

    while let Some(var_start) = result[start..].find("${") {
        let var_start = start + var_start;
        let var_end = result[var_start..]
            .find('}')
            .ok_or_else(|| anyhow::anyhow!("Unclosed ${{ in connection '{}' url", conn_name))?;
        let var_end = var_start + var_end;

        let var_name = &result[var_start + 2..var_end];
        let var_value = std::env::var(var_name).with_context(|| {
            format!(
                "Connection '{}' references undefined environment variable '{}'.\n\
                 Hint: Set the variable or update pgcrate.toml",
                conn_name, var_name
            )
        })?;

        result = format!("{}{}{}", &result[..var_start], var_value, &result[var_end + 1..]);
        start = var_start + var_value.len();
    }

    Ok(result)
}

/// Execute a command and capture stdout as the URL
fn execute_command(argv: &[String]) -> Result<String> {
    if argv.is_empty() {
        bail!("Connection command cannot be empty");
    }

    let program = &argv[0];
    let args = &argv[1..];

    // Spawn the command
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to execute command: {}", argv.join(" ")))?;

    // Set up timeout (30 seconds)
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();

    // Wait for the command with timeout
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    // Capture stderr for error message
                    let stderr = child.stderr.take();
                    let stderr_msg = if let Some(stderr) = stderr {
                        let reader = BufReader::new(stderr);
                        reader.lines().filter_map(|l| l.ok()).collect::<Vec<_>>().join("\n")
                    } else {
                        String::new()
                    };

                    bail!(
                        "Connection command failed with exit code {}:\n  Command: {}\n  Error: {}",
                        status.code().unwrap_or(-1),
                        argv.join(" "),
                        stderr_msg
                    );
                }
                break;
            }
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    bail!(
                        "Connection command timed out after {}s: {}",
                        timeout.as_secs(),
                        argv.join(" ")
                    );
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                bail!("Failed to wait for command: {}", e);
            }
        }
    }

    // Read stdout
    let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("No stdout from command"))?;
    let reader = BufReader::new(stdout);
    let url = reader
        .lines()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Command produced no output"))??
        .trim()
        .to_string();

    if url.is_empty() {
        bail!("Connection command produced empty output: {}", argv.join(" "));
    }

    Ok(url)
}

/// Parsed URL components
struct ParsedUrl {
    host: String,
    port: u16,
    database: String,
    user: String,
}

/// Parse a database URL to extract components (safe for display)
fn parse_connection_url(url: &str) -> Result<ParsedUrl> {
    let parsed = Url::parse(url).with_context(|| "Invalid database URL format")?;

    let host = parsed.host_str().unwrap_or("localhost").to_string();
    let port = parsed.port().unwrap_or(5432);
    let database = parsed.path().trim_start_matches('/').to_string();
    let user = parsed.username().to_string();

    if database.is_empty() {
        bail!("Database URL must include a database name");
    }

    Ok(ParsedUrl {
        host,
        port,
        database,
        user: if user.is_empty() {
            "postgres".to_string()
        } else {
            user
        },
    })
}

/// Check if a connection requires --primary flag
pub fn requires_primary_flag(conn: &ResolvedConnection) -> bool {
    conn.role == ConnectionRole::Primary
}

/// Result of full connection resolution
#[derive(Debug)]
pub struct ConnectionResult {
    /// The database URL to connect with (may include read-only options)
    pub url: String,
    /// The resolved connection (if using named connection)
    pub connection: Option<ResolvedConnection>,
    /// Whether this connection is in read-only mode
    pub readonly: bool,
}

/// Append read-only session option to a database URL
fn make_readonly_url(url: &str) -> String {
    // PostgreSQL connection strings support options via the 'options' parameter
    // We need to set: options=-c default_transaction_read_only=on
    let option = "options=-c%20default_transaction_read_only%3Don";

    if url.contains('?') {
        format!("{}&{}", url, option)
    } else {
        format!("{}?{}", url, option)
    }
}

/// Fully resolve and validate a connection.
///
/// This is the main entry point for commands that need database connections.
/// It handles:
/// 1. Resolution via -d, -c, --env, DATABASE_URL, or config
/// 2. Policy enforcement
/// 3. Primary flag requirement check
/// 4. Read-only mode determination
/// 5. Banner printing (to stderr, unless quiet)
pub fn resolve_and_validate(
    config: &crate::config::Config,
    cli_url: Option<&str>,
    connection_name: Option<&str>,
    env_var_name: Option<&str>,
    allow_primary: bool,
    read_write: bool,
    quiet: bool,
) -> Result<ConnectionResult> {
    let (url, maybe_conn) = config.resolve_database_url(cli_url, connection_name, env_var_name)?;

    // If we have a resolved connection, perform additional checks
    if let Some(ref conn) = maybe_conn {
        // Check policy
        check_policy(conn, config.policy.as_ref(), allow_primary, read_write)?;

        // Primary databases require explicit --primary flag
        if requires_primary_flag(conn) && !allow_primary {
            bail!(
                "Connection '{}' is a primary database.\n\
                 Use --primary to confirm you want to connect to a primary database.",
                conn.name
            );
        }

        // Determine readonly mode
        // Default: connection's readonly setting
        // Override: --read-write flag forces read-write
        let readonly = if read_write { false } else { conn.readonly };

        // Print banner unless quiet
        if !quiet {
            // Create a modified connection with the effective readonly setting
            let display_conn = ResolvedConnection {
                readonly,
                ..conn.clone()
            };
            display_conn.print_banner();
        }

        let final_url = if readonly {
            make_readonly_url(&url)
        } else {
            url
        };

        return Ok(ConnectionResult {
            url: final_url,
            connection: Some(ResolvedConnection {
                readonly,
                ..conn.clone()
            }),
            readonly,
        });
    }

    // Direct URL or DATABASE_URL - no special handling
    // Default to read-only unless --read-write specified
    let readonly = !read_write;

    let final_url = if readonly {
        make_readonly_url(&url)
    } else {
        url
    };

    Ok(ConnectionResult {
        url: final_url,
        connection: None,
        readonly,
    })
}

/// Enforce policy restrictions
pub fn check_policy(
    conn: &ResolvedConnection,
    policy: Option<&PolicyConfig>,
    allow_primary_flag: bool,
    read_write_flag: bool,
) -> Result<()> {
    let policy = match policy {
        Some(p) => p,
        None => return Ok(()),
    };

    // Check allow_primary policy
    if policy.allow_primary == Some(false) && allow_primary_flag {
        bail!("Policy forbids --primary flag");
    }

    // Check require_readonly policy
    if policy.require_readonly == Some(true) && read_write_flag {
        bail!("Policy requires all connections to be read-only");
    }

    // Check if connecting to primary without permission
    if policy.allow_primary == Some(false) && conn.role == ConnectionRole::Primary {
        bail!(
            "Policy forbids connecting to primary databases.\n\
             Connection '{}' has role=primary.",
            conn.name
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_env_vars_simple() {
        std::env::set_var("TEST_DB_HOST", "localhost");
        let result = expand_env_vars("test", "postgres://${TEST_DB_HOST}/mydb").unwrap();
        assert_eq!(result, "postgres://localhost/mydb");
        std::env::remove_var("TEST_DB_HOST");
    }

    #[test]
    fn test_expand_env_vars_multiple() {
        std::env::set_var("TEST_HOST", "db.example.com");
        std::env::set_var("TEST_DB", "appdb");
        let result = expand_env_vars("test", "postgres://${TEST_HOST}/${TEST_DB}").unwrap();
        assert_eq!(result, "postgres://db.example.com/appdb");
        std::env::remove_var("TEST_HOST");
        std::env::remove_var("TEST_DB");
    }

    #[test]
    fn test_expand_env_vars_undefined_error() {
        let result = expand_env_vars("myconn", "postgres://${UNDEFINED_VAR_12345}/db");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("UNDEFINED_VAR_12345"));
        assert!(err.contains("myconn"));
    }

    #[test]
    fn test_expand_env_vars_no_vars() {
        let result = expand_env_vars("test", "postgres://localhost/mydb").unwrap();
        assert_eq!(result, "postgres://localhost/mydb");
    }

    #[test]
    fn test_parse_connection_url_simple() {
        let parsed = parse_connection_url("postgres://localhost/mydb").unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 5432);
        assert_eq!(parsed.database, "mydb");
    }

    #[test]
    fn test_parse_connection_url_with_credentials() {
        let parsed = parse_connection_url("postgres://user:pass@host.example.com:5433/appdb").unwrap();
        assert_eq!(parsed.host, "host.example.com");
        assert_eq!(parsed.port, 5433);
        assert_eq!(parsed.database, "appdb");
        assert_eq!(parsed.user, "user");
    }

    #[test]
    fn test_parse_connection_url_no_database() {
        let result = parse_connection_url("postgres://localhost/");
        assert!(result.is_err());
    }

    #[test]
    fn test_connection_role_default() {
        let role = ConnectionRole::default();
        assert_eq!(role, ConnectionRole::Primary);
    }

    #[test]
    fn test_connection_role_display() {
        assert_eq!(format!("{}", ConnectionRole::Primary), "primary");
        assert_eq!(format!("{}", ConnectionRole::Replica), "replica");
    }

    #[test]
    fn test_requires_primary_flag() {
        let conn = ResolvedConnection {
            name: "test".to_string(),
            url: "postgres://localhost/db".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            database: "db".to_string(),
            user: "postgres".to_string(),
            role: ConnectionRole::Primary,
            readonly: false,
        };
        assert!(requires_primary_flag(&conn));

        let replica_conn = ResolvedConnection {
            role: ConnectionRole::Replica,
            ..conn
        };
        assert!(!requires_primary_flag(&replica_conn));
    }

    #[test]
    fn test_policy_forbids_primary() {
        let conn = ResolvedConnection {
            name: "test".to_string(),
            url: "postgres://localhost/db".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            database: "db".to_string(),
            user: "postgres".to_string(),
            role: ConnectionRole::Primary,
            readonly: false,
        };
        let policy = PolicyConfig {
            allow_primary: Some(false),
            require_readonly: None,
            allowed_connections: None,
        };

        // Should fail when trying to use --primary flag
        let result = check_policy(&conn, Some(&policy), true, false);
        assert!(result.is_err());

        // Should also fail for primary role without explicit flag
        let result = check_policy(&conn, Some(&policy), false, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_policy_requires_readonly() {
        let conn = ResolvedConnection {
            name: "test".to_string(),
            url: "postgres://localhost/db".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            database: "db".to_string(),
            user: "postgres".to_string(),
            role: ConnectionRole::Replica,
            readonly: true,
        };
        let policy = PolicyConfig {
            allow_primary: None,
            require_readonly: Some(true),
            allowed_connections: None,
        };

        // Should fail when trying to use --read-write flag
        let result = check_policy(&conn, Some(&policy), false, true);
        assert!(result.is_err());

        // Should succeed without --read-write
        let result = check_policy(&conn, Some(&policy), false, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_resolved_connection_display() {
        let conn = ResolvedConnection {
            name: "prod-ro".to_string(),
            url: "postgres://user:secret@db.example.com:5432/appdb".to_string(),
            host: "db.example.com".to_string(),
            port: 5432,
            database: "appdb".to_string(),
            user: "user".to_string(),
            role: ConnectionRole::Replica,
            readonly: true,
        };
        // Should not contain password
        let display = conn.display();
        assert!(!display.contains("secret"));
        assert!(display.contains("db.example.com"));
        assert!(display.contains("appdb"));
    }

    #[test]
    fn test_make_readonly_url_simple() {
        let url = make_readonly_url("postgres://localhost/db");
        assert!(url.contains("options="));
        assert!(url.contains("default_transaction_read_only"));
        assert!(url.starts_with("postgres://localhost/db?"));
    }

    #[test]
    fn test_make_readonly_url_with_existing_params() {
        let url = make_readonly_url("postgres://localhost/db?sslmode=require");
        assert!(url.contains("sslmode=require"));
        assert!(url.contains("&options="));
        assert!(url.contains("default_transaction_read_only"));
    }

    #[test]
    fn test_make_readonly_url_preserves_original() {
        let original = "postgres://user:pass@host:5432/db";
        let url = make_readonly_url(original);
        assert!(url.starts_with(original));
    }
}
