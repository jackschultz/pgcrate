//! Reason codes for diagnostic outputs.
//!
//! Provides a taxonomy of reasons why operations failed, degraded, or were skipped.
//! These are stable identifiers for automation - the enum variants are the contract.

use serde::Serialize;

/// Reason code taxonomy for diagnostic outputs.
///
/// Categorized into three groups:
/// - **Operational**: Runtime conditions (timeouts, connection issues)
/// - **Policy**: Intentional restrictions (safety rails, confirmation required)
/// - **Capability**: Missing prerequisites (extensions, privileges, features)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    // =========================================================================
    // Operational: Runtime conditions that prevented operation
    // =========================================================================
    /// Connection to database timed out
    ConnectionTimeout,
    /// Statement execution timed out (statement_timeout)
    StatementTimeout,
    /// Could not acquire lock within timeout (lock_timeout)
    LockTimeout,
    /// Connection to database failed
    ConnectionFailed,
    /// Query was cancelled (e.g., by Ctrl+C)
    QueryCancelled,
    /// Database server is shutting down
    ServerShutdown,
    /// Too many connections to server
    TooManyConnections,
    /// Out of memory on server
    OutOfMemory,
    /// Disk full on server
    DiskFull,
    /// Unexpected internal error
    InternalError,

    // =========================================================================
    // Policy: Intentional restrictions enforced by pgcrate
    // =========================================================================
    /// Operation requires --primary flag to confirm primary database access
    PrimaryRequiresAck,
    /// Operation requires --read-write flag (default is read-only)
    RequiresReadWrite,
    /// Operation is too dangerous without explicit confirmation
    DangerousOperation,
    /// Operation not allowed on replica/standby
    ReplicaNotAllowed,
    /// Operation not allowed on primary
    PrimaryNotAllowed,
    /// Feature is disabled in configuration
    FeatureDisabled,

    // =========================================================================
    // Capability: Missing prerequisites to run the operation
    // =========================================================================
    /// Required PostgreSQL extension not installed
    MissingExtension,
    /// Insufficient database privileges
    MissingPrivilege,
    /// Required database role does not exist
    MissingRole,
    /// Required table or relation does not exist
    MissingTable,
    /// Required schema does not exist
    MissingSchema,
    /// Required function does not exist
    MissingFunction,
    /// PostgreSQL version is too old for this feature
    UnsupportedVersion,
    /// Feature not applicable to current database configuration
    NotApplicable,
    /// Required configuration parameter not set
    MissingConfig,
    /// Feature requires superuser privileges
    RequiresSuperuser,
    /// Feature requires replication privileges
    RequiresReplication,
}

impl ReasonCode {
    /// Human-readable description of the reason code.
    pub fn description(&self) -> &'static str {
        match self {
            // Operational
            ReasonCode::ConnectionTimeout => "connection timed out",
            ReasonCode::StatementTimeout => "statement timeout exceeded",
            ReasonCode::LockTimeout => "lock timeout exceeded",
            ReasonCode::ConnectionFailed => "connection failed",
            ReasonCode::QueryCancelled => "query was cancelled",
            ReasonCode::ServerShutdown => "server is shutting down",
            ReasonCode::TooManyConnections => "too many connections",
            ReasonCode::OutOfMemory => "out of memory",
            ReasonCode::DiskFull => "disk full",
            ReasonCode::InternalError => "internal error",

            // Policy
            ReasonCode::PrimaryRequiresAck => "requires --primary flag to confirm",
            ReasonCode::RequiresReadWrite => "requires --read-write flag",
            ReasonCode::DangerousOperation => "dangerous operation requires confirmation",
            ReasonCode::ReplicaNotAllowed => "operation not allowed on replica",
            ReasonCode::PrimaryNotAllowed => "operation not allowed on primary",
            ReasonCode::FeatureDisabled => "feature is disabled",

            // Capability
            ReasonCode::MissingExtension => "required extension not installed",
            ReasonCode::MissingPrivilege => "insufficient privileges",
            ReasonCode::MissingRole => "required role does not exist",
            ReasonCode::MissingTable => "required table does not exist",
            ReasonCode::MissingSchema => "required schema does not exist",
            ReasonCode::MissingFunction => "required function does not exist",
            ReasonCode::UnsupportedVersion => "PostgreSQL version not supported",
            ReasonCode::NotApplicable => "not applicable to this configuration",
            ReasonCode::MissingConfig => "required configuration not set",
            ReasonCode::RequiresSuperuser => "requires superuser privileges",
            ReasonCode::RequiresReplication => "requires replication privileges",
        }
    }

    /// Category of the reason code.
    pub fn category(&self) -> ReasonCategory {
        match self {
            ReasonCode::ConnectionTimeout
            | ReasonCode::StatementTimeout
            | ReasonCode::LockTimeout
            | ReasonCode::ConnectionFailed
            | ReasonCode::QueryCancelled
            | ReasonCode::ServerShutdown
            | ReasonCode::TooManyConnections
            | ReasonCode::OutOfMemory
            | ReasonCode::DiskFull
            | ReasonCode::InternalError => ReasonCategory::Operational,

            ReasonCode::PrimaryRequiresAck
            | ReasonCode::RequiresReadWrite
            | ReasonCode::DangerousOperation
            | ReasonCode::ReplicaNotAllowed
            | ReasonCode::PrimaryNotAllowed
            | ReasonCode::FeatureDisabled => ReasonCategory::Policy,

            ReasonCode::MissingExtension
            | ReasonCode::MissingPrivilege
            | ReasonCode::MissingRole
            | ReasonCode::MissingTable
            | ReasonCode::MissingSchema
            | ReasonCode::MissingFunction
            | ReasonCode::UnsupportedVersion
            | ReasonCode::NotApplicable
            | ReasonCode::MissingConfig
            | ReasonCode::RequiresSuperuser
            | ReasonCode::RequiresReplication => ReasonCategory::Capability,
        }
    }

    /// Classify a tokio_postgres error into a reason code.
    pub fn from_postgres_error(err: &tokio_postgres::Error) -> Self {
        let msg = err.to_string().to_lowercase();

        // Check for SQLSTATE codes first (most reliable)
        if let Some(db_err) = err.as_db_error() {
            let code = db_err.code().code();
            return match code {
                // Class 08 - Connection Exception
                "08000" | "08003" | "08006" => ReasonCode::ConnectionFailed,
                "08001" => ReasonCode::ConnectionFailed, // sqlclient_unable_to_establish_sqlconnection
                "08004" => ReasonCode::ConnectionFailed, // sqlserver_rejected_establishment_of_sqlconnection

                // Class 42 - Syntax/Access
                "42501" => ReasonCode::MissingPrivilege, // insufficient_privilege
                "42883" => ReasonCode::MissingFunction,  // undefined_function
                "42P01" => ReasonCode::MissingTable,     // undefined_table
                "3F000" => ReasonCode::MissingSchema,    // invalid_schema_name

                // Class 53 - Insufficient Resources
                "53000" => ReasonCode::OutOfMemory, // insufficient_resources
                "53100" => ReasonCode::DiskFull,    // disk_full
                "53200" => ReasonCode::OutOfMemory, // out_of_memory
                "53300" => ReasonCode::TooManyConnections, // too_many_connections

                // Class 55 - Object Not In Prerequisite State
                "55P03" => ReasonCode::LockTimeout, // lock_not_available

                // Class 57 - Operator Intervention
                "57014" => ReasonCode::QueryCancelled,  // query_canceled (includes statement_timeout)
                "57P01" => ReasonCode::ServerShutdown,  // admin_shutdown
                "57P02" => ReasonCode::ServerShutdown,  // crash_shutdown
                "57P03" => ReasonCode::ServerShutdown,  // cannot_connect_now

                _ => Self::classify_message(&msg),
            };
        }

        // Fallback to message heuristics
        Self::classify_message(&msg)
    }

    /// Classify an error message into a reason code using heuristics.
    fn classify_message(msg: &str) -> Self {
        if msg.contains("permission denied") || msg.contains("must be superuser") {
            ReasonCode::MissingPrivilege
        } else if msg.contains("statement timeout") || msg.contains("canceling statement") {
            ReasonCode::StatementTimeout
        } else if msg.contains("lock timeout") || msg.contains("could not obtain lock") {
            ReasonCode::LockTimeout
        } else if msg.contains("connection refused")
            || msg.contains("could not connect")
            || msg.contains("connection timed out")
        {
            ReasonCode::ConnectionTimeout
        } else if msg.contains("does not exist") && msg.contains("extension") {
            ReasonCode::MissingExtension
        } else if msg.contains("does not exist") && msg.contains("relation") {
            ReasonCode::MissingTable
        } else if msg.contains("does not exist") && msg.contains("schema") {
            ReasonCode::MissingSchema
        } else if msg.contains("does not exist") && msg.contains("role") {
            ReasonCode::MissingRole
        } else if msg.contains("too many connections") {
            ReasonCode::TooManyConnections
        } else if msg.contains("out of memory") {
            ReasonCode::OutOfMemory
        } else if msg.contains("disk full") || msg.contains("no space left") {
            ReasonCode::DiskFull
        } else {
            ReasonCode::InternalError
        }
    }
}

/// Category of reason codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasonCategory {
    /// Runtime conditions (timeouts, connection issues)
    Operational,
    /// Intentional restrictions (safety rails)
    Policy,
    /// Missing prerequisites (extensions, privileges)
    Capability,
}

/// Structured reason information for JSON output.
#[derive(Debug, Clone, Serialize)]
pub struct ReasonInfo {
    /// Stable reason code for automation
    pub code: ReasonCode,
    /// Human-readable message
    pub message: String,
    /// Optional additional details (structured data)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ReasonInfo {
    /// Create a new reason info with just a code and message.
    pub fn new(code: ReasonCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    /// Create a reason info with additional structured details.
    pub fn with_details(
        code: ReasonCode,
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            details: Some(details),
        }
    }

    /// Create from a tokio_postgres error.
    pub fn from_postgres_error(err: &tokio_postgres::Error) -> Self {
        let code = ReasonCode::from_postgres_error(err);
        Self {
            code,
            message: err.to_string(),
            details: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reason_code_descriptions() {
        assert_eq!(
            ReasonCode::ConnectionTimeout.description(),
            "connection timed out"
        );
        assert_eq!(
            ReasonCode::MissingPrivilege.description(),
            "insufficient privileges"
        );
        assert_eq!(
            ReasonCode::RequiresReadWrite.description(),
            "requires --read-write flag"
        );
    }

    #[test]
    fn test_reason_code_categories() {
        assert_eq!(
            ReasonCode::ConnectionTimeout.category(),
            ReasonCategory::Operational
        );
        assert_eq!(
            ReasonCode::RequiresReadWrite.category(),
            ReasonCategory::Policy
        );
        assert_eq!(
            ReasonCode::MissingExtension.category(),
            ReasonCategory::Capability
        );
    }

    #[test]
    fn test_classify_message_permission() {
        assert_eq!(
            ReasonCode::classify_message("permission denied for table foo"),
            ReasonCode::MissingPrivilege
        );
        assert_eq!(
            ReasonCode::classify_message("must be superuser to do this"),
            ReasonCode::MissingPrivilege
        );
    }

    #[test]
    fn test_classify_message_timeout() {
        assert_eq!(
            ReasonCode::classify_message("canceling statement due to statement timeout"),
            ReasonCode::StatementTimeout
        );
        assert_eq!(
            ReasonCode::classify_message("could not obtain lock on relation"),
            ReasonCode::LockTimeout
        );
    }

    #[test]
    fn test_classify_message_connection() {
        assert_eq!(
            ReasonCode::classify_message("connection refused"),
            ReasonCode::ConnectionTimeout
        );
        assert_eq!(
            ReasonCode::classify_message("could not connect to server"),
            ReasonCode::ConnectionTimeout
        );
    }

    #[test]
    fn test_classify_message_missing() {
        assert_eq!(
            ReasonCode::classify_message("extension \"foo\" does not exist"),
            ReasonCode::MissingExtension
        );
        assert_eq!(
            ReasonCode::classify_message("relation \"foo\" does not exist"),
            ReasonCode::MissingTable
        );
    }

    #[test]
    fn test_reason_info_new() {
        let info = ReasonInfo::new(ReasonCode::MissingPrivilege, "permission denied for pg_stat_activity");
        assert_eq!(info.code, ReasonCode::MissingPrivilege);
        assert_eq!(info.message, "permission denied for pg_stat_activity");
        assert!(info.details.is_none());
    }

    #[test]
    fn test_reason_info_with_details() {
        let details = serde_json::json!({
            "required_privilege": "SELECT",
            "object": "pg_stat_activity"
        });
        let info = ReasonInfo::with_details(
            ReasonCode::MissingPrivilege,
            "permission denied",
            details.clone(),
        );
        assert_eq!(info.code, ReasonCode::MissingPrivilege);
        assert_eq!(info.details, Some(details));
    }

    #[test]
    fn test_reason_info_serialization() {
        let info = ReasonInfo::new(ReasonCode::StatementTimeout, "query timed out");
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"code\":\"statement_timeout\""));
        assert!(json.contains("\"message\":\"query timed out\""));
    }
}
