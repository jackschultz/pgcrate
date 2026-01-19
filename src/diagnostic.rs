//! Diagnostic session management with safety rails.
//!
//! Provides bounded database sessions for diagnostic commands.
//! All diagnostic operations should go through DiagnosticSession to ensure:
//! - Connection timeout (fast fail on unreachable hosts)
//! - Statement timeout (bounded query runtime)
//! - Lock timeout (never wait on locks)
//! - Ctrl+C cancellation (best-effort query cancellation)

use anyhow::{Context, Result};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_postgres::{CancelToken, Client, NoTls};

/// Default timeout values for diagnostic sessions.
/// These are conservative defaults for production safety.
pub mod defaults {
    use std::time::Duration;

    /// Connection timeout: fail fast on unreachable hosts
    pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Statement timeout: bound query runtime
    pub const STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Lock timeout: never wait on locks (diagnostics should be non-blocking)
    pub const LOCK_TIMEOUT: Duration = Duration::from_millis(500);
}

/// Timeout configuration for a diagnostic session.
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Connection timeout (how long to wait for initial connection)
    pub connect_timeout: Duration,
    /// Statement timeout (max query runtime)
    pub statement_timeout: Duration,
    /// Lock timeout (max time to wait for locks)
    pub lock_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect_timeout: defaults::CONNECT_TIMEOUT,
            statement_timeout: defaults::STATEMENT_TIMEOUT,
            lock_timeout: defaults::LOCK_TIMEOUT,
        }
    }
}

impl TimeoutConfig {
    /// Create a new TimeoutConfig with custom values.
    pub fn new(
        connect_timeout: Option<Duration>,
        statement_timeout: Option<Duration>,
        lock_timeout: Option<Duration>,
    ) -> Self {
        Self {
            connect_timeout: connect_timeout.unwrap_or(defaults::CONNECT_TIMEOUT),
            statement_timeout: statement_timeout.unwrap_or(defaults::STATEMENT_TIMEOUT),
            lock_timeout: lock_timeout.unwrap_or(defaults::LOCK_TIMEOUT),
        }
    }

    /// Format as Postgres-compatible duration string (milliseconds).
    fn format_pg_duration(d: Duration) -> String {
        format!("{}ms", d.as_millis())
    }

    /// SQL to set session-level timeouts.
    pub fn session_setup_sql(&self) -> String {
        format!(
            "SET statement_timeout = '{}'; SET lock_timeout = '{}';",
            Self::format_pg_duration(self.statement_timeout),
            Self::format_pg_duration(self.lock_timeout),
        )
    }
}

/// A diagnostic session with safety rails.
///
/// Wraps a tokio_postgres Client with enforced timeouts at session level.
/// Connection drops cleanly when the session is dropped.
pub struct DiagnosticSession {
    client: Client,
    pub timeouts: TimeoutConfig,
    /// Cancel token for aborting running queries (Ctrl+C support)
    cancel_token: CancelToken,
    /// Sender to signal connection task to stop (triggers on drop)
    _shutdown_tx: oneshot::Sender<()>,
}

impl DiagnosticSession {
    /// Connect with timeout enforcement.
    ///
    /// Sets session-level statement_timeout and lock_timeout after connecting.
    pub async fn connect(database_url: &str, timeouts: TimeoutConfig) -> Result<Self> {
        let connect_future = tokio_postgres::connect(database_url, NoTls);
        let (client, connection) = tokio::time::timeout(timeouts.connect_timeout, connect_future)
            .await
            .with_context(|| format!("Connection timed out after {:?}", timeouts.connect_timeout))?
            .with_context(|| "Failed to connect to database")?;

        // Get cancel token before spawning connection task
        let cancel_token = client.cancel_token();

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Spawn connection handler that exits on shutdown or error
        tokio::spawn(async move {
            tokio::select! {
                _ = connection => {}
                _ = shutdown_rx => {}
            }
        });

        // Set session-level timeouts
        client
            .batch_execute(&timeouts.session_setup_sql())
            .await
            .context("Failed to set session timeouts")?;

        Ok(Self {
            client,
            timeouts,
            cancel_token,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Get a cloneable cancel token for Ctrl+C handling.
    ///
    /// The cancel token can be used to cancel running queries from a signal handler.
    pub fn cancel_token(&self) -> CancelToken {
        self.cancel_token.clone()
    }

    /// Get a reference to the underlying client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get effective timeout values for display/logging.
    pub fn effective_timeouts(&self) -> EffectiveTimeouts {
        EffectiveTimeouts {
            connect_timeout_ms: self.timeouts.connect_timeout.as_millis() as u64,
            statement_timeout_ms: self.timeouts.statement_timeout.as_millis() as u64,
            lock_timeout_ms: self.timeouts.lock_timeout.as_millis() as u64,
        }
    }
}

/// Effective timeout values for output (JSON-serializable).
#[derive(Debug, Clone, serde::Serialize)]
pub struct EffectiveTimeouts {
    pub connect_timeout_ms: u64,
    pub statement_timeout_ms: u64,
    pub lock_timeout_ms: u64,
}

impl std::fmt::Display for EffectiveTimeouts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "connect={}ms, statement={}ms, lock={}ms",
            self.connect_timeout_ms, self.statement_timeout_ms, self.lock_timeout_ms
        )
    }
}

/// Parse a duration string like "5s", "500ms", "1m".
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("Empty duration string");
    }

    // Try to find the unit suffix
    let (num_part, unit) = if let Some(stripped) = s.strip_suffix("ms") {
        (stripped, "ms")
    } else if let Some(stripped) = s.strip_suffix('s') {
        (stripped, "s")
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, "m")
    } else {
        // Default to seconds if no unit
        (s, "s")
    };

    let num: u64 = num_part
        .trim()
        .parse()
        .with_context(|| format!("Invalid duration number: '{}'", num_part))?;

    let duration = match unit {
        "ms" => Duration::from_millis(num),
        "s" => Duration::from_secs(num),
        "m" => Duration::from_secs(num * 60),
        _ => anyhow::bail!("Unknown duration unit: '{}'", unit),
    };

    Ok(duration)
}

/// Set up Ctrl+C (SIGINT) handling for graceful query cancellation.
///
/// When Ctrl+C is pressed:
/// 1. Attempts to cancel any running query via the cancel token
/// 2. Exits with the INTERRUPTED exit code
///
/// This should be called after establishing a database connection.
/// The cancel token is obtained from `DiagnosticSession::cancel_token()`.
pub fn setup_ctrlc_handler(cancel_token: CancelToken) {
    use crate::exit_codes;

    // Spawn a task to handle Ctrl+C
    tokio::spawn(async move {
        // Wait for Ctrl+C signal
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("Failed to listen for Ctrl+C: {}", e);
            return;
        }

        // Signal received - attempt to cancel running query
        eprintln!("\nInterrupted (Ctrl+C). Cancelling query...");

        // Cancel the query (best effort - may fail if already completed)
        // Note: cancel_query requires TLS parameter matching the connection
        if let Err(e) = cancel_token.cancel_query(NoTls).await {
            eprintln!("Warning: Failed to cancel query: {}", e);
        }

        // Exit with interrupted code
        std::process::exit(exit_codes::INTERRUPTED);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_timeouts() {
        let config = TimeoutConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.statement_timeout, Duration::from_secs(30));
        assert_eq!(config.lock_timeout, Duration::from_millis(500));
    }

    #[test]
    fn test_timeout_config_new_with_overrides() {
        let config = TimeoutConfig::new(
            Some(Duration::from_secs(10)),
            None, // Use default
            Some(Duration::from_millis(100)),
        );
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.statement_timeout, Duration::from_secs(30)); // default
        assert_eq!(config.lock_timeout, Duration::from_millis(100));
    }

    #[test]
    fn test_session_setup_sql() {
        let config = TimeoutConfig::default();
        let sql = config.session_setup_sql();
        assert!(sql.contains("statement_timeout = '30000ms'"));
        assert!(sql.contains("lock_timeout = '500ms'"));
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("100ms").unwrap(), Duration::from_millis(100));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_no_unit_defaults_to_seconds() {
        assert_eq!(parse_duration("10").unwrap(), Duration::from_secs(10));
    }

    #[test]
    fn test_parse_duration_with_whitespace() {
        assert_eq!(parse_duration("  5s  ").unwrap(), Duration::from_secs(5));
        assert_eq!(
            parse_duration(" 500 ms").unwrap(),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("-5s").is_err());
    }

    #[test]
    fn test_effective_timeouts_display() {
        let timeouts = EffectiveTimeouts {
            connect_timeout_ms: 5000,
            statement_timeout_ms: 30000,
            lock_timeout_ms: 500,
        };
        let display = format!("{}", timeouts);
        assert!(display.contains("connect=5000ms"));
        assert!(display.contains("statement=30000ms"));
        assert!(display.contains("lock=500ms"));
    }
}
