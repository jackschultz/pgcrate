//! Capabilities command: Permission-aware feature discovery.
//!
//! Reports which pgcrate diagnostic and fix capabilities are available
//! in the current environment based on privileges, extensions, and
//! connection mode.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

use crate::reason_codes::{ReasonCode, ReasonInfo};

/// Status of a capability
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CapabilityStatus {
    /// Capability is fully available
    Available,
    /// Capability works but with limitations
    Degraded,
    /// Capability is not available
    Unavailable,
    /// Could not determine availability
    Unknown,
}

/// A requirement for a capability
#[derive(Debug, Clone, Serialize)]
pub struct Requirement {
    /// What is required (e.g., "pg_stat_activity SELECT")
    pub what: String,
    /// Whether this requirement is met
    pub met: bool,
}

/// Information about a single capability
#[derive(Debug, Clone, Serialize)]
pub struct CapabilityInfo {
    /// Capability identifier (e.g., "diagnostics.triage")
    pub id: &'static str,
    /// Human-readable name
    pub name: &'static str,
    /// Brief description
    pub description: &'static str,
    /// Availability status
    pub status: CapabilityStatus,
    /// Reasons for degraded/unavailable status
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<ReasonInfo>,
    /// Requirements and their status
    pub requirements: Vec<Requirement>,
    /// Limitations when degraded
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub limitations: Vec<String>,
}

/// Full capabilities results
#[derive(Debug, Serialize)]
pub struct CapabilitiesResult {
    pub capabilities: Vec<CapabilityInfo>,
    /// Count of capabilities by status
    pub summary: CapabilitySummary,
}

#[derive(Debug, Serialize)]
pub struct CapabilitySummary {
    pub available: usize,
    pub degraded: usize,
    pub unavailable: usize,
    pub unknown: usize,
}

/// Check capabilities based on privileges and connection mode
pub async fn run_capabilities(client: &Client, read_only: bool) -> Result<CapabilitiesResult> {
    // Probe privileges
    let has_pg_stat_activity = check_privilege(client, "pg_stat_activity", "SELECT").await;
    let has_pg_stat_user_tables = check_privilege(client, "pg_stat_user_tables", "SELECT").await;
    let has_pg_stat_user_indexes = check_privilege(client, "pg_stat_user_indexes", "SELECT").await;
    let has_pg_sequences = check_privilege(client, "pg_sequences", "SELECT").await;
    let has_pg_database = check_privilege(client, "pg_database", "SELECT").await;
    let has_pg_cancel = check_function_privilege(client, "pg_cancel_backend(int)").await;
    let has_pg_terminate = check_function_privilege(client, "pg_terminate_backend(int)").await;
    let has_pg_stat_statements = check_extension_and_privilege(client, "pg_stat_statements").await;

    let mut capabilities = Vec::new();

    // diagnostics.triage - always available (uses minimal queries)
    capabilities.push(CapabilityInfo {
        id: "diagnostics.triage",
        name: "Triage",
        description: "Quick database health check",
        status: CapabilityStatus::Available,
        reasons: vec![],
        requirements: vec![],
        limitations: vec![],
    });

    // diagnostics.locks - needs pg_stat_activity
    capabilities.push(check_locks_capability(
        has_pg_stat_activity,
        has_pg_cancel,
        has_pg_terminate,
        read_only,
    ));

    // diagnostics.sequences - needs pg_sequences
    capabilities.push(check_sequences_capability(has_pg_sequences, read_only));

    // diagnostics.indexes - needs pg_stat_user_indexes
    capabilities.push(check_indexes_capability(
        has_pg_stat_user_indexes,
        has_pg_stat_user_tables,
    ));

    // diagnostics.xid - needs pg_database
    capabilities.push(check_xid_capability(has_pg_database));

    // diagnostics.context - always available
    capabilities.push(CapabilityInfo {
        id: "diagnostics.context",
        name: "Context",
        description: "Connection and server information",
        status: CapabilityStatus::Available,
        reasons: vec![],
        requirements: vec![],
        limitations: vec![],
    });

    // diagnostics.queries - needs pg_stat_statements (Phase 3, not yet implemented)
    capabilities.push(check_queries_capability(has_pg_stat_statements));

    // fix.sequence - needs write access and pg_sequences
    capabilities.push(check_fix_sequence_capability(has_pg_sequences, read_only));

    // fix.cancel - needs pg_cancel_backend
    capabilities.push(check_fix_cancel_capability(has_pg_cancel, read_only));

    // fix.terminate - needs pg_terminate_backend
    capabilities.push(check_fix_terminate_capability(has_pg_terminate, read_only));

    // Calculate summary
    let summary = CapabilitySummary {
        available: capabilities
            .iter()
            .filter(|c| c.status == CapabilityStatus::Available)
            .count(),
        degraded: capabilities
            .iter()
            .filter(|c| c.status == CapabilityStatus::Degraded)
            .count(),
        unavailable: capabilities
            .iter()
            .filter(|c| c.status == CapabilityStatus::Unavailable)
            .count(),
        unknown: capabilities
            .iter()
            .filter(|c| c.status == CapabilityStatus::Unknown)
            .count(),
    };

    Ok(CapabilitiesResult {
        capabilities,
        summary,
    })
}

async fn check_privilege(client: &Client, table: &str, privilege: &str) -> bool {
    let query = format!(
        "SELECT has_table_privilege('{}', '{}')",
        table, privilege
    );
    client
        .query_one(&query, &[])
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false)
}

async fn check_function_privilege(client: &Client, function: &str) -> bool {
    let query = format!(
        "SELECT has_function_privilege('{}', 'EXECUTE')",
        function
    );
    client
        .query_one(&query, &[])
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false)
}

async fn check_extension_and_privilege(client: &Client, extension: &str) -> bool {
    // Check if extension exists and we can read from it
    let query = format!(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = '{}'
        ) AND has_table_privilege('{}', 'SELECT')
        "#,
        extension, extension
    );
    client
        .query_one(&query, &[])
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false)
}

fn check_locks_capability(
    has_pg_stat_activity: bool,
    has_pg_cancel: bool,
    has_pg_terminate: bool,
    read_only: bool,
) -> CapabilityInfo {
    let mut requirements = vec![
        Requirement {
            what: "pg_stat_activity SELECT".to_string(),
            met: has_pg_stat_activity,
        },
    ];

    let mut reasons = vec![];
    let mut limitations = vec![];

    let status = if !has_pg_stat_activity {
        reasons.push(ReasonInfo::new(
            ReasonCode::MissingPrivilege,
            "Cannot read pg_stat_activity",
        ));
        CapabilityStatus::Unavailable
    } else {
        // Check if cancel/terminate are available
        if !has_pg_cancel || !has_pg_terminate || read_only {
            if read_only {
                limitations.push("Cancel/terminate actions not available in read-only mode".to_string());
            }
            if !has_pg_cancel {
                limitations.push("pg_cancel_backend not available".to_string());
            }
            if !has_pg_terminate {
                limitations.push("pg_terminate_backend not available".to_string());
            }
            CapabilityStatus::Degraded
        } else {
            CapabilityStatus::Available
        }
    };

    requirements.push(Requirement {
        what: "pg_cancel_backend EXECUTE".to_string(),
        met: has_pg_cancel,
    });
    requirements.push(Requirement {
        what: "pg_terminate_backend EXECUTE".to_string(),
        met: has_pg_terminate,
    });
    if !read_only {
        requirements.push(Requirement {
            what: "read-write mode".to_string(),
            met: true,
        });
    }

    CapabilityInfo {
        id: "diagnostics.locks",
        name: "Locks",
        description: "Blocking lock detection and analysis",
        status,
        reasons,
        requirements,
        limitations,
    }
}

fn check_sequences_capability(has_pg_sequences: bool, read_only: bool) -> CapabilityInfo {
    let mut requirements = vec![Requirement {
        what: "pg_sequences SELECT".to_string(),
        met: has_pg_sequences,
    }];

    let mut reasons = vec![];
    let mut limitations = vec![];

    let status = if !has_pg_sequences {
        reasons.push(ReasonInfo::new(
            ReasonCode::MissingPrivilege,
            "Cannot read pg_sequences",
        ));
        CapabilityStatus::Unavailable
    } else {
        if read_only {
            limitations.push("Cannot suggest ALTER SEQUENCE in read-only mode".to_string());
        }
        CapabilityStatus::Available
    };

    if read_only {
        requirements.push(Requirement {
            what: "read-write mode for fixes".to_string(),
            met: false,
        });
    }

    CapabilityInfo {
        id: "diagnostics.sequences",
        name: "Sequences",
        description: "Sequence exhaustion monitoring",
        status,
        reasons,
        requirements,
        limitations,
    }
}

fn check_indexes_capability(
    has_pg_stat_user_indexes: bool,
    has_pg_stat_user_tables: bool,
) -> CapabilityInfo {
    let requirements = vec![
        Requirement {
            what: "pg_stat_user_indexes SELECT".to_string(),
            met: has_pg_stat_user_indexes,
        },
        Requirement {
            what: "pg_stat_user_tables SELECT".to_string(),
            met: has_pg_stat_user_tables,
        },
    ];

    let mut reasons = vec![];
    let mut limitations = vec![];

    let status = if !has_pg_stat_user_indexes || !has_pg_stat_user_tables {
        if !has_pg_stat_user_indexes {
            reasons.push(ReasonInfo::new(
                ReasonCode::MissingPrivilege,
                "Cannot read pg_stat_user_indexes",
            ));
        }
        if !has_pg_stat_user_tables {
            reasons.push(ReasonInfo::new(
                ReasonCode::MissingPrivilege,
                "Cannot read pg_stat_user_tables",
            ));
            limitations.push("Missing index detection not available".to_string());
        }
        if has_pg_stat_user_indexes {
            CapabilityStatus::Degraded
        } else {
            CapabilityStatus::Unavailable
        }
    } else {
        CapabilityStatus::Available
    };

    CapabilityInfo {
        id: "diagnostics.indexes",
        name: "Indexes",
        description: "Index health analysis",
        status,
        reasons,
        requirements,
        limitations,
    }
}

fn check_xid_capability(has_pg_database: bool) -> CapabilityInfo {
    let requirements = vec![Requirement {
        what: "pg_database SELECT".to_string(),
        met: has_pg_database,
    }];

    let (status, reasons) = if !has_pg_database {
        (
            CapabilityStatus::Unavailable,
            vec![ReasonInfo::new(
                ReasonCode::MissingPrivilege,
                "Cannot read pg_database",
            )],
        )
    } else {
        (CapabilityStatus::Available, vec![])
    };

    CapabilityInfo {
        id: "diagnostics.xid",
        name: "XID Age",
        description: "Transaction ID wraparound monitoring",
        status,
        reasons,
        requirements,
        limitations: vec![],
    }
}

fn check_queries_capability(has_pg_stat_statements: bool) -> CapabilityInfo {
    let requirements = vec![Requirement {
        what: "pg_stat_statements extension".to_string(),
        met: has_pg_stat_statements,
    }];

    let (status, reasons) = if !has_pg_stat_statements {
        (
            CapabilityStatus::Unavailable,
            vec![ReasonInfo::new(
                ReasonCode::MissingExtension,
                "pg_stat_statements extension not installed or accessible",
            )],
        )
    } else {
        // Even with the extension, this capability is not yet implemented
        (
            CapabilityStatus::Unavailable,
            vec![ReasonInfo::new(
                ReasonCode::NotApplicable,
                "Query analysis not yet implemented (planned for Phase 3)",
            )],
        )
    };

    CapabilityInfo {
        id: "diagnostics.queries",
        name: "Query Analysis",
        description: "Slow query identification (pg_stat_statements)",
        status,
        reasons,
        requirements,
        limitations: vec!["Not yet implemented".to_string()],
    }
}

fn check_fix_sequence_capability(has_pg_sequences: bool, read_only: bool) -> CapabilityInfo {
    let requirements = vec![
        Requirement {
            what: "pg_sequences SELECT".to_string(),
            met: has_pg_sequences,
        },
        Requirement {
            what: "read-write mode".to_string(),
            met: !read_only,
        },
    ];

    let mut reasons = vec![];

    let status = if !has_pg_sequences {
        reasons.push(ReasonInfo::new(
            ReasonCode::MissingPrivilege,
            "Cannot read pg_sequences",
        ));
        CapabilityStatus::Unavailable
    } else if read_only {
        reasons.push(ReasonInfo::new(
            ReasonCode::RequiresReadWrite,
            "Sequence fixes require read-write mode",
        ));
        CapabilityStatus::Unavailable
    } else {
        // Not yet implemented
        reasons.push(ReasonInfo::new(
            ReasonCode::NotApplicable,
            "Sequence fixes not yet implemented",
        ));
        CapabilityStatus::Unavailable
    };

    CapabilityInfo {
        id: "fix.sequence",
        name: "Fix Sequence",
        description: "Upgrade sequences to bigint",
        status,
        reasons,
        requirements,
        limitations: vec!["Not yet implemented".to_string()],
    }
}

fn check_fix_cancel_capability(has_pg_cancel: bool, read_only: bool) -> CapabilityInfo {
    let requirements = vec![
        Requirement {
            what: "pg_cancel_backend EXECUTE".to_string(),
            met: has_pg_cancel,
        },
        Requirement {
            what: "read-write mode".to_string(),
            met: !read_only,
        },
    ];

    let mut reasons = vec![];

    let status = if !has_pg_cancel {
        reasons.push(ReasonInfo::new(
            ReasonCode::MissingPrivilege,
            "Cannot execute pg_cancel_backend",
        ));
        CapabilityStatus::Unavailable
    } else if read_only {
        reasons.push(ReasonInfo::new(
            ReasonCode::RequiresReadWrite,
            "Query cancellation requires read-write mode",
        ));
        CapabilityStatus::Unavailable
    } else {
        CapabilityStatus::Available
    };

    CapabilityInfo {
        id: "fix.cancel",
        name: "Cancel Query",
        description: "Cancel a running query by PID",
        status,
        reasons,
        requirements,
        limitations: vec![],
    }
}

fn check_fix_terminate_capability(has_pg_terminate: bool, read_only: bool) -> CapabilityInfo {
    let requirements = vec![
        Requirement {
            what: "pg_terminate_backend EXECUTE".to_string(),
            met: has_pg_terminate,
        },
        Requirement {
            what: "read-write mode".to_string(),
            met: !read_only,
        },
    ];

    let mut reasons = vec![];

    let status = if !has_pg_terminate {
        reasons.push(ReasonInfo::new(
            ReasonCode::MissingPrivilege,
            "Cannot execute pg_terminate_backend",
        ));
        CapabilityStatus::Unavailable
    } else if read_only {
        reasons.push(ReasonInfo::new(
            ReasonCode::RequiresReadWrite,
            "Connection termination requires read-write mode",
        ));
        CapabilityStatus::Unavailable
    } else {
        CapabilityStatus::Available
    };

    CapabilityInfo {
        id: "fix.terminate",
        name: "Terminate Connection",
        description: "Terminate a connection by PID",
        status,
        reasons,
        requirements,
        limitations: vec![],
    }
}

/// Print capabilities in human-readable format
pub fn print_human(result: &CapabilitiesResult, _quiet: bool) {
    println!("CAPABILITIES:");
    println!();

    for cap in &result.capabilities {
        let status_str = match cap.status {
            CapabilityStatus::Available => "✓ available",
            CapabilityStatus::Degraded => "⚠ degraded",
            CapabilityStatus::Unavailable => "✗ unavailable",
            CapabilityStatus::Unknown => "? unknown",
        };

        println!("  {:25} {}", cap.id, status_str);

        if !cap.limitations.is_empty() {
            for lim in &cap.limitations {
                println!("    - {}", lim);
            }
        }

        if !cap.reasons.is_empty() && cap.status != CapabilityStatus::Available {
            for reason in &cap.reasons {
                println!("    - {}", reason.message);
            }
        }
    }

    println!();
    println!(
        "SUMMARY: {} available, {} degraded, {} unavailable",
        result.summary.available, result.summary.degraded, result.summary.unavailable
    );
}

/// Print capabilities as JSON with schema versioning
pub fn print_json(
    result: &CapabilitiesResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Derive severity from capability status
    let severity = if result.summary.unavailable > result.summary.available {
        Severity::Warning
    } else {
        Severity::Healthy
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::CAPABILITIES, result, severity, t),
        None => DiagnosticOutput::new(schema::CAPABILITIES, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capability_status_serialization() {
        let json = serde_json::to_string(&CapabilityStatus::Available).unwrap();
        assert_eq!(json, "\"available\"");

        let json = serde_json::to_string(&CapabilityStatus::Degraded).unwrap();
        assert_eq!(json, "\"degraded\"");
    }
}
