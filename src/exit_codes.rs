//! Exit code policy for pgcrate.
//!
#![allow(dead_code)] // Constants defined for policy documentation, used selectively
//!
//! ## Findings (0-2)
//!
//! Diagnostic commands return exit codes based on findings:
//! - `0` = Healthy (no issues found)
//! - `1` = Warning (non-critical issues found)
//! - `2` = Critical (critical issues found)
//!
//! ## Operational Failures (10+)
//!
//! Operational failures (connection issues, invalid config, internal errors)
//! use codes >= 10 to distinguish from findings:
//! - `10` = General operational failure
//! - `11` = Connection failure
//! - `12` = Configuration error
//! - `13` = Permission denied (can't run at all, not a finding)
//!
//! This separation allows automation to distinguish between:
//! - "The database has problems" (findings, 1-2)
//! - "We couldn't check the database" (operational failure, 10+)

/// Exit code: healthy findings (no issues)
pub const HEALTHY: i32 = 0;

/// Exit code: warning findings (non-critical issues)
pub const WARNING: i32 = 1;

/// Exit code: critical findings
pub const CRITICAL: i32 = 2;

/// Exit code: general operational failure
pub const OPERATIONAL_FAILURE: i32 = 10;

/// Exit code: connection failure
pub const CONNECTION_FAILURE: i32 = 11;

/// Exit code: configuration error
pub const CONFIG_ERROR: i32 = 12;

/// Exit code: permission denied (operational, not a finding)
pub const PERMISSION_DENIED: i32 = 13;

/// Exit code: interrupted by Ctrl+C (SIGINT)
pub const INTERRUPTED: i32 = 130;
