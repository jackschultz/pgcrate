//! Action schema types for structured fix suggestions.
//!
//! Actions are structured commands that can be executed to address
//! findings from diagnostic commands. They include metadata about
//! risks, prerequisites, and verification steps.

use serde::Serialize;

/// Risk level for lock acquisition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LockRisk {
    /// No locks required
    None,
    /// Row-level locks only
    Low,
    /// Table-level locks that might block
    Medium,
    /// Exclusive locks that will block writes
    High,
    /// AccessExclusive locks that block all access
    Extreme,
}

/// Gates that must pass before an action can be executed
#[derive(Debug, Clone, Default, Serialize)]
pub struct ActionGates {
    /// Requires --read-write mode
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub requires_write: bool,
    /// Requires --primary flag
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub requires_primary: bool,
    /// Requires --yes flag for confirmation
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub requires_confirmation: bool,
    /// Required capability (e.g., "fix.sequence")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_capability: Option<&'static str>,
    /// Minimum PostgreSQL version required
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_pg_version: Option<i32>,
}

/// A step to verify the action completed successfully
#[derive(Debug, Clone, Serialize)]
pub struct VerifyStep {
    /// Human-readable description of what to verify
    pub description: String,
    /// Command to run for verification
    pub command: String,
    /// Expected outcome
    pub expected: String,
}

/// A structured action that can be taken to address a finding
#[derive(Debug, Clone, Serialize)]
pub struct Action {
    /// Unique action identifier (e.g., "fix-sequence-bigint-public.users_id_seq")
    pub action_id: String,
    /// Type of action (investigate, fix, monitor)
    pub action_type: ActionType,
    /// Command to run (e.g., "pgcrate")
    pub command: &'static str,
    /// Command arguments
    pub args: Vec<String>,
    /// Full command string for display
    pub command_string: String,
    /// Human-readable description of what this action does
    pub description: String,
    /// Which capability is required (for availability checking)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_capability_id: Option<&'static str>,
    /// Whether this action is available in the current context
    pub available: bool,
    /// Reason why action is unavailable
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
    /// Whether this action mutates data
    pub mutates: bool,
    /// SQL that would be executed (preview)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sql_preview: Vec<String>,
    /// Lock acquisition risk
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_risk: Option<LockRisk>,
    /// Risk level description
    pub risk: &'static str,
    /// Evidence that led to this recommendation (e.g., sequence data)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<serde_json::Value>,
    /// Gates that must pass before execution
    pub gates: ActionGates,
    /// Steps to verify success after execution
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub verify: Vec<VerifyStep>,
}

/// Type of action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ActionType {
    /// Investigative action (e.g., drill down for more info)
    Investigate,
    /// Remediation action (e.g., fix a problem)
    Fix,
    /// Monitoring action (e.g., set up alerting)
    Monitor,
}

impl Action {
    /// Create a new investigate action
    pub fn investigate(
        action_id: impl Into<String>,
        description: impl Into<String>,
        args: Vec<String>,
    ) -> Self {
        let command_string = format!("pgcrate {}", args.join(" "));
        Self {
            action_id: action_id.into(),
            action_type: ActionType::Investigate,
            command: "pgcrate",
            args,
            command_string,
            description: description.into(),
            requires_capability_id: None,
            available: true,
            unavailable_reason: None,
            mutates: false,
            sql_preview: vec![],
            lock_risk: None,
            risk: "none",
            evidence: None,
            gates: ActionGates::default(),
            verify: vec![],
        }
    }

    /// Create a new fix action
    pub fn fix(
        action_id: impl Into<String>,
        description: impl Into<String>,
        args: Vec<String>,
        sql_preview: Vec<String>,
    ) -> Self {
        let command_string = format!("pgcrate {}", args.join(" "));
        Self {
            action_id: action_id.into(),
            action_type: ActionType::Fix,
            command: "pgcrate",
            args,
            command_string,
            description: description.into(),
            requires_capability_id: None,
            available: true,
            unavailable_reason: None,
            mutates: true,
            sql_preview,
            lock_risk: None,
            risk: "medium",
            evidence: None,
            gates: ActionGates {
                requires_write: true,
                requires_confirmation: true,
                ..Default::default()
            },
            verify: vec![],
        }
    }

    /// Set the required capability
    pub fn with_capability(mut self, capability_id: &'static str) -> Self {
        self.requires_capability_id = Some(capability_id);
        self
    }

    /// Set availability status
    pub fn with_availability(mut self, available: bool, reason: Option<String>) -> Self {
        self.available = available;
        self.unavailable_reason = reason;
        self
    }

    /// Set lock risk
    pub fn with_lock_risk(mut self, risk: LockRisk) -> Self {
        self.lock_risk = Some(risk);
        self.risk = match risk {
            LockRisk::None => "none",
            LockRisk::Low => "low",
            LockRisk::Medium => "medium",
            LockRisk::High => "high",
            LockRisk::Extreme => "extreme",
        };
        self
    }

    /// Set evidence
    pub fn with_evidence(mut self, evidence: serde_json::Value) -> Self {
        self.evidence = Some(evidence);
        self
    }

    /// Add verification step
    pub fn with_verify(mut self, description: impl Into<String>, command: impl Into<String>, expected: impl Into<String>) -> Self {
        self.verify.push(VerifyStep {
            description: description.into(),
            command: command.into(),
            expected: expected.into(),
        });
        self
    }

    /// Set gates
    pub fn with_gates(mut self, gates: ActionGates) -> Self {
        self.gates = gates;
        self
    }
}

/// Generate actions for blocking locks
pub fn blocking_locks_actions(blocking_count: i32, oldest_blocked_seconds: i64) -> Vec<Action> {
    let mut actions = vec![];

    // Always suggest investigation
    actions.push(
        Action::investigate(
            "investigate-blocking-locks",
            "Investigate blocking lock chains",
            vec!["locks".to_string(), "--blocking".to_string()],
        )
        .with_evidence(serde_json::json!({
            "blocked_count": blocking_count,
            "oldest_blocked_seconds": oldest_blocked_seconds,
        })),
    );

    actions
}

/// Generate actions for long transactions
pub fn long_transaction_actions(count: i64, oldest_seconds: i64) -> Vec<Action> {
    let mut actions = vec![];

    actions.push(
        Action::investigate(
            "investigate-long-transactions",
            "List long-running transactions",
            vec!["locks".to_string(), "--long-tx".to_string(), "5".to_string()],
        )
        .with_evidence(serde_json::json!({
            "count": count,
            "oldest_seconds": oldest_seconds,
        })),
    );

    actions
}

/// Generate actions for sequence exhaustion
pub fn sequence_exhaustion_actions(
    schema: &str,
    name: &str,
    data_type: &str,
    pct_used: f64,
    read_only: bool,
) -> Vec<Action> {
    let mut actions = vec![];

    // Investigation action
    actions.push(
        Action::investigate(
            format!("investigate-sequence-{}.{}", schema, name),
            format!("Show details for sequence {}.{}", schema, name),
            vec!["sequences".to_string()],
        )
        .with_evidence(serde_json::json!({
            "schema": schema,
            "name": name,
            "data_type": data_type,
            "pct_used": pct_used,
        })),
    );

    // For non-bigint sequences, suggest upgrade
    if data_type != "bigint" {
        let sql = format!("ALTER SEQUENCE {}.{} AS bigint;", schema, name);
        let (available, reason) = if read_only {
            (false, Some("Requires --read-write mode".to_string()))
        } else {
            // Fix not yet implemented
            (false, Some("Sequence fix not yet implemented".to_string()))
        };

        actions.push(
            Action::fix(
                format!("fix-sequence-bigint-{}.{}", schema, name),
                format!("Upgrade {}.{} from {} to bigint", schema, name, data_type),
                vec![
                    "fix".to_string(),
                    "sequence".to_string(),
                    "--upgrade-to".to_string(),
                    "bigint".to_string(),
                    format!("{}.{}", schema, name),
                ],
                vec![sql],
            )
            .with_capability("fix.sequence")
            .with_availability(available, reason)
            .with_lock_risk(LockRisk::High)
            .with_gates(ActionGates {
                requires_write: true,
                requires_primary: true,
                requires_confirmation: true,
                requires_capability: Some("fix.sequence"),
                min_pg_version: None,
            })
            .with_verify(
                "Verify sequence upgraded",
                format!("pgcrate sql -c \"SELECT data_type FROM pg_sequences WHERE schemaname='{}' AND sequencename='{}'\"", schema, name),
                "bigint",
            )
            .with_evidence(serde_json::json!({
                "schema": schema,
                "name": name,
                "current_type": data_type,
                "pct_used": pct_used,
            })),
        );
    }

    actions
}

/// Generate actions for XID age warnings
pub fn xid_age_actions(datname: &str, xid_age: i64) -> Vec<Action> {
    let mut actions = vec![];

    actions.push(
        Action::investigate(
            format!("investigate-xid-{}", datname),
            format!("Show XID age details for {}", datname),
            vec!["xid".to_string()],
        )
        .with_evidence(serde_json::json!({
            "database": datname,
            "xid_age": xid_age,
        })),
    );

    actions
}

/// Generate actions for connection warnings
pub fn connection_actions(current: i64, max: i32, pct: i32) -> Vec<Action> {
    let mut actions = vec![];

    actions.push(
        Action::investigate(
            "investigate-connections",
            "Show connections by user",
            vec![
                "sql".to_string(),
                "-c".to_string(),
                "SELECT usename, count(*) FROM pg_stat_activity GROUP BY usename ORDER BY count DESC".to_string(),
            ],
        )
        .with_evidence(serde_json::json!({
            "current": current,
            "max": max,
            "pct_used": pct,
        })),
    );

    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_investigate() {
        let action = Action::investigate(
            "test-action",
            "Test action",
            vec!["locks".to_string(), "--blocking".to_string()],
        );
        assert_eq!(action.action_type, ActionType::Investigate);
        assert!(!action.mutates);
        assert_eq!(action.command_string, "pgcrate locks --blocking");
    }

    #[test]
    fn test_action_fix() {
        let action = Action::fix(
            "fix-test",
            "Fix something",
            vec!["fix".to_string(), "sequence".to_string()],
            vec!["ALTER SEQUENCE foo AS bigint;".to_string()],
        );
        assert_eq!(action.action_type, ActionType::Fix);
        assert!(action.mutates);
        assert!(action.gates.requires_write);
    }

    #[test]
    fn test_lock_risk_serialization() {
        let json = serde_json::to_string(&LockRisk::High).unwrap();
        assert_eq!(json, "\"high\"");
    }
}
