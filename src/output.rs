//! Output layer for pgcrate CLI.
//!
//! Centralizes stdout/stderr separation and human vs JSON output modes.
//! - stdout: data (the "answer" - results, JSON)
//! - stderr: diagnostics (progress, debug messages, human-mode errors)

use serde::Serialize;
use std::io::{self, Write};

/// Output mode for the CLI
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutputMode {
    Human,
    Json,
}

/// Output helper that centralizes all CLI output
#[derive(Debug, Clone)]
pub struct Output {
    pub mode: OutputMode,
    pub quiet: bool,
    pub verbose: bool,
}

impl Output {
    pub fn new(json: bool, quiet: bool, verbose: bool) -> Self {
        Self {
            mode: if json {
                OutputMode::Json
            } else {
                OutputMode::Human
            },
            quiet,
            verbose,
        }
    }

    /// Write data to stdout (the command's "answer")
    /// In JSON mode, this is the only thing that goes to stdout
    #[allow(dead_code)]
    pub fn data(&self, message: &str) {
        println!("{}", message);
    }

    /// Write JSON data to stdout
    /// Returns error if serialization fails (should be propagated to become JSON error)
    pub fn json<T: Serialize>(&self, data: &T) -> Result<(), serde_json::Error> {
        let json = serde_json::to_string_pretty(data)?;
        println!("{}", json);
        Ok(())
    }

    /// Write a diagnostic/progress message to stderr
    /// Suppressed in JSON mode and when --quiet is set
    #[allow(dead_code)]
    pub fn info(&self, message: &str) {
        if self.mode == OutputMode::Json || self.quiet {
            return;
        }
        eprintln!("{}", message);
    }

    /// Write a verbose diagnostic message to stderr
    /// Only shown with --verbose in human mode
    pub fn verbose(&self, message: &str) {
        if self.mode == OutputMode::Json || self.quiet || !self.verbose {
            return;
        }
        eprintln!("{}", message);
    }

    /// Write a warning to stderr
    /// Shown in human mode unless --quiet, suppressed in JSON mode
    #[allow(dead_code)]
    pub fn warn(&self, message: &str) {
        if self.mode == OutputMode::Json || self.quiet {
            return;
        }
        eprintln!("{}", message);
    }

    /// Check if we're in JSON mode
    pub fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    /// Check if we're in quiet mode
    pub fn is_quiet(&self) -> bool {
        self.quiet
    }

    /// Flush stdout (useful before exiting)
    #[allow(dead_code)]
    pub fn flush(&self) {
        let _ = io::stdout().flush();
    }
}

// =============================================================================
// JSON Response Types
// =============================================================================

/// JSON error response (written to stdout with non-zero exit)
#[derive(Debug, Serialize)]
pub struct JsonError {
    pub ok: bool,
    pub error: JsonErrorDetails,
}

#[derive(Debug, Serialize)]
pub struct JsonErrorDetails {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl JsonError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: JsonErrorDetails {
                message: message.into(),
                details: None,
            },
        }
    }

    pub fn with_details(message: impl Into<String>, details: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: JsonErrorDetails {
                message: message.into(),
                details: Some(details.into()),
            },
        }
    }

    /// Print this error as JSON to stdout
    /// Panics if serialization fails (should never happen for JsonError)
    pub fn print(&self) {
        let json =
            serde_json::to_string_pretty(self).expect("JsonError serialization should never fail");
        println!("{}", json);
    }
}

/// JSON success response wrapper for status command
#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub ok: bool,
    pub applied: Vec<MigrationInfo>,
    pub pending: Vec<MigrationInfo>,
    pub counts: StatusCounts,
}

#[derive(Debug, Serialize)]
pub struct MigrationInfo {
    pub version: String,
    pub name: String,
    pub has_down: bool,
}

#[derive(Debug, Serialize)]
pub struct StatusCounts {
    pub applied: usize,
    pub pending: usize,
    pub total: usize,
}

/// JSON success response wrapper for diff command
#[derive(Debug, Serialize)]
pub struct DiffResponse {
    pub ok: bool,
    pub identical: bool,
    pub summary: DiffSummaryJson,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted_diff: Option<String>,
}

#[derive(Debug, Serialize, Default)]
pub struct DiffSummaryJson {
    pub tables: usize,
    pub columns: usize,
    pub indexes: usize,
    pub constraints: usize,
    pub enums: usize,
    pub functions: usize,
    pub views: usize,
    pub triggers: usize,
    pub sequences: usize,
    pub extensions: usize,
    pub schemas: usize,
    pub materialized_views: usize,
}

impl From<&crate::diff::DiffSummary> for DiffSummaryJson {
    fn from(s: &crate::diff::DiffSummary) -> Self {
        Self {
            tables: s.tables,
            columns: s.columns,
            indexes: s.indexes,
            constraints: s.constraints,
            enums: s.enums,
            functions: s.functions,
            views: s.views,
            triggers: s.triggers,
            sequences: s.sequences,
            extensions: s.extensions,
            schemas: s.schemas,
            materialized_views: s.materialized_views,
        }
    }
}

/// JSON success response wrapper for describe command
#[derive(Debug, Serialize)]
pub struct DescribeResponse {
    pub ok: bool,
    pub schema: String,
    pub name: String,
    pub table: crate::describe::TableDescribe,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependents: Option<crate::describe::Dependents>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<crate::describe::Dependencies>,
}

// =============================================================================
// Diagnostic Output (versioned JSON for diagnostic commands)
// =============================================================================

/// Schema version for diagnostic JSON outputs.
/// Follows semver: breaking=major, additive=minor, bugfix=patch.
pub const DIAGNOSTIC_SCHEMA_VERSION: &str = "1.0.0";

/// Wrapper for diagnostic command JSON output.
/// Includes schema metadata for stable automation and versioning.
#[derive(Debug, Serialize)]
pub struct DiagnosticOutput<T: Serialize> {
    pub ok: bool,
    pub schema_id: &'static str,
    pub schema_version: &'static str,
    /// Effective timeout configuration used for this diagnostic
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeouts: Option<TimeoutsJson>,
    #[serde(flatten)]
    pub data: T,
}

/// Timeout configuration in JSON output (milliseconds).
#[derive(Debug, Clone, Serialize)]
pub struct TimeoutsJson {
    pub connect_ms: u64,
    pub statement_ms: u64,
    pub lock_ms: u64,
}

impl<T: Serialize> DiagnosticOutput<T> {
    /// Create a new diagnostic output with the given schema ID and data.
    pub fn new(schema_id: &'static str, data: T) -> Self {
        Self {
            ok: true,
            schema_id,
            schema_version: DIAGNOSTIC_SCHEMA_VERSION,
            timeouts: None,
            data,
        }
    }

    /// Create a new diagnostic output with timeouts included.
    pub fn with_timeouts(
        schema_id: &'static str,
        data: T,
        timeouts: crate::diagnostic::EffectiveTimeouts,
    ) -> Self {
        Self {
            ok: true,
            schema_id,
            schema_version: DIAGNOSTIC_SCHEMA_VERSION,
            timeouts: Some(TimeoutsJson {
                connect_ms: timeouts.connect_timeout_ms,
                statement_ms: timeouts.statement_timeout_ms,
                lock_ms: timeouts.lock_timeout_ms,
            }),
            data,
        }
    }

    /// Print this output as JSON to stdout.
    pub fn print(&self) -> Result<(), serde_json::Error> {
        let json = serde_json::to_string_pretty(self)?;
        println!("{}", json);
        Ok(())
    }
}

/// Schema IDs for diagnostic commands.
pub mod schema {
    pub const TRIAGE: &str = "pgcrate.diagnostics.triage";
    pub const LOCKS: &str = "pgcrate.diagnostics.locks";
    pub const XID: &str = "pgcrate.diagnostics.xid";
    pub const SEQUENCES: &str = "pgcrate.diagnostics.sequences";
    pub const INDEXES: &str = "pgcrate.diagnostics.indexes";
}

/// Diagnostic-specific error response.
/// Distinguishes "tool failed" from "critical finding" by including schema metadata.
#[derive(Debug, Serialize)]
pub struct DiagnosticError {
    pub ok: bool,
    /// Which diagnostic command failed
    pub schema_id: &'static str,
    pub schema_version: &'static str,
    /// Stable error code for automation
    pub error_code: DiagnosticErrorCode,
    /// Human-readable error message
    pub message: String,
    /// Optional additional details (error chain, hints)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// Error codes for diagnostic failures (stable enum for automation).
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticErrorCode {
    /// Could not connect to database
    ConnectionFailed,
    /// Query timed out (statement_timeout)
    StatementTimeout,
    /// Could not acquire lock (lock_timeout)
    LockTimeout,
    /// Insufficient privileges to run diagnostic
    PermissionDenied,
    /// Internal error in pgcrate
    InternalError,
}

impl DiagnosticError {
    pub fn new(
        schema_id: &'static str,
        error_code: DiagnosticErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            ok: false,
            schema_id,
            schema_version: DIAGNOSTIC_SCHEMA_VERSION,
            error_code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(
        schema_id: &'static str,
        error_code: DiagnosticErrorCode,
        message: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self {
            ok: false,
            schema_id,
            schema_version: DIAGNOSTIC_SCHEMA_VERSION,
            error_code,
            message: message.into(),
            details: Some(details.into()),
        }
    }

    /// Classify an anyhow error into a diagnostic error code.
    pub fn classify_error(err: &anyhow::Error) -> DiagnosticErrorCode {
        let msg = err.to_string().to_lowercase();

        if msg.contains("connection refused")
            || msg.contains("could not connect")
            || msg.contains("connection timed out")
        {
            DiagnosticErrorCode::ConnectionFailed
        } else if msg.contains("statement timeout") || msg.contains("canceling statement") {
            DiagnosticErrorCode::StatementTimeout
        } else if msg.contains("lock timeout") || msg.contains("could not obtain lock") {
            DiagnosticErrorCode::LockTimeout
        } else if msg.contains("permission denied") || msg.contains("must be superuser") {
            DiagnosticErrorCode::PermissionDenied
        } else {
            DiagnosticErrorCode::InternalError
        }
    }

    /// Print this error as JSON to stdout.
    pub fn print(&self) {
        let json = serde_json::to_string_pretty(self)
            .expect("DiagnosticError serialization should never fail");
        println!("{}", json);
    }
}

// =============================================================================
// Meta UX JSON Response Types (--help, --version, --help-llm)
// =============================================================================

/// JSON response for --help flag
#[derive(Debug, Serialize)]
pub struct HelpResponse {
    pub ok: bool,
    pub help: String,
}

impl HelpResponse {
    pub fn new(help_text: String) -> Self {
        Self {
            ok: true,
            help: help_text,
        }
    }

    pub fn print(&self) {
        let json = serde_json::to_string_pretty(self)
            .expect("HelpResponse serialization should never fail");
        println!("{}", json);
    }
}

/// JSON response for --version flag
#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub ok: bool,
    pub version: String,
}

impl VersionResponse {
    pub fn new(version: String) -> Self {
        Self { ok: true, version }
    }

    pub fn print(&self) {
        let json = serde_json::to_string_pretty(self)
            .expect("VersionResponse serialization should never fail");
        println!("{}", json);
    }
}

/// JSON response for --help-llm flag
#[derive(Debug, Serialize)]
pub struct LlmHelpResponse {
    pub ok: bool,
    pub llm_help: String,
}

impl LlmHelpResponse {
    pub fn new(llm_help: String) -> Self {
        Self { ok: true, llm_help }
    }

    pub fn print(&self) {
        let json = serde_json::to_string_pretty(self)
            .expect("LlmHelpResponse serialization should never fail");
        println!("{}", json);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_error_basic() {
        let err = JsonError::new("Something went wrong");
        assert!(!err.ok);
        assert_eq!(err.error.message, "Something went wrong");
        assert!(err.error.details.is_none());
    }

    #[test]
    fn test_json_error_with_details() {
        let err = JsonError::with_details("Connection failed", "Host not found");
        assert!(!err.ok);
        assert_eq!(err.error.message, "Connection failed");
        assert_eq!(err.error.details, Some("Host not found".to_string()));
    }

    #[test]
    fn test_output_mode_json() {
        let output = Output::new(true, false, false);
        assert!(output.is_json());
        assert_eq!(output.mode, OutputMode::Json);
    }

    #[test]
    fn test_output_mode_human() {
        let output = Output::new(false, false, false);
        assert!(!output.is_json());
        assert_eq!(output.mode, OutputMode::Human);
    }

    #[test]
    fn test_output_quiet() {
        let output = Output::new(false, true, false);
        assert!(output.is_quiet());
    }
}
