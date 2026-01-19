//! Common types for fix commands.
//!
//! These types match the action.schema.json contract for structured actions.

use serde::Serialize;

/// Action type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ActionType {
    /// Remediation fix
    Fix,
}

/// Risk level for an action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Risk {
    /// Low risk (non-blocking, reversible)
    Low,
    /// Medium risk (may affect performance, requires confirmation)
    Medium,
    /// High risk (may block operations, requires explicit confirmation)
    High,
}

/// Gates that must be satisfied before an action can execute
#[derive(Debug, Clone, Default, Serialize)]
pub struct ActionGates {
    /// Requires --read-write flag (not in read-only mode)
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub requires_write: bool,
    /// Requires --primary flag (confirmed connection to primary)
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub requires_primary: bool,
    /// Requires --yes flag (explicit confirmation)
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub requires_confirmation: bool,
}

impl ActionGates {
    pub fn write_primary() -> Self {
        Self {
            requires_write: true,
            requires_primary: true,
            ..Default::default()
        }
    }

    pub fn write_primary_confirm() -> Self {
        Self {
            requires_write: true,
            requires_primary: true,
            requires_confirmation: true,
        }
    }
}

/// A verification step to run after a fix
#[derive(Debug, Clone, Serialize)]
pub struct VerifyStep {
    /// Human-readable description
    pub description: String,
    /// Command to run (e.g., "pgcrate sequences --json")
    pub command: String,
    /// JSONPath expression for expected result
    pub expected: String,
}

/// A structured action that can be executed
#[derive(Debug, Clone, Serialize)]
pub struct StructuredAction {
    /// Unique identifier (e.g., "fix.sequence.upgrade-bigint.public.order_seq")
    pub action_id: String,
    /// Type of action
    pub action_type: ActionType,
    /// Command to run
    pub command: &'static str,
    /// Command arguments
    pub args: Vec<String>,
    /// Human-readable description
    pub description: String,
    /// Whether this action is available (gates are satisfied)
    pub available: bool,
    /// Whether this action mutates state
    pub mutates: bool,
    /// Risk level
    pub risk: Risk,
    /// Gate requirements
    pub gates: ActionGates,
    /// SQL that will be executed (for preview)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_preview: Option<Vec<String>>,
    /// Evidence supporting this action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<serde_json::Value>,
    /// Verification steps to run after fix
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify: Option<Vec<VerifyStep>>,
    /// Reason action is blocked (if available=false)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
}

impl StructuredAction {
    /// Create a new action builder
    pub fn builder(action_id: impl Into<String>, action_type: ActionType) -> ActionBuilder {
        ActionBuilder::new(action_id.into(), action_type)
    }
}

/// Builder for StructuredAction
pub struct ActionBuilder {
    action_id: String,
    action_type: ActionType,
    command: &'static str,
    args: Vec<String>,
    description: String,
    mutates: bool,
    risk: Risk,
    gates: ActionGates,
    sql_preview: Option<Vec<String>>,
    evidence: Option<serde_json::Value>,
    verify: Option<Vec<VerifyStep>>,
}

impl ActionBuilder {
    pub fn new(action_id: String, action_type: ActionType) -> Self {
        Self {
            action_id,
            action_type,
            command: "pgcrate",
            args: Vec::new(),
            description: String::new(),
            mutates: false,
            risk: Risk::Low,
            gates: ActionGates::default(),
            sql_preview: None,
            evidence: None,
            verify: None,
        }
    }

    pub fn command(mut self, command: &'static str) -> Self {
        self.command = command;
        self
    }

    pub fn args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    pub fn mutates(mut self, mutates: bool) -> Self {
        self.mutates = mutates;
        self
    }

    pub fn risk(mut self, risk: Risk) -> Self {
        self.risk = risk;
        self
    }

    pub fn gates(mut self, gates: ActionGates) -> Self {
        self.gates = gates;
        self
    }

    pub fn sql_preview(mut self, sql: Vec<String>) -> Self {
        self.sql_preview = Some(sql);
        self
    }

    pub fn evidence(mut self, evidence: serde_json::Value) -> Self {
        self.evidence = Some(evidence);
        self
    }

    pub fn verify(mut self, steps: Vec<VerifyStep>) -> Self {
        self.verify = Some(steps);
        self
    }

    /// Build the action, checking gates against current state
    pub fn build(self, read_write: bool, is_primary: bool, confirmed: bool) -> StructuredAction {
        let gate_result = check_gates(&self.gates, read_write, is_primary, confirmed);

        StructuredAction {
            action_id: self.action_id,
            action_type: self.action_type,
            command: self.command,
            args: self.args,
            description: self.description,
            available: gate_result.passed,
            mutates: self.mutates,
            risk: self.risk,
            gates: self.gates,
            sql_preview: self.sql_preview,
            evidence: self.evidence,
            verify: self.verify,
            blocked_reason: gate_result.blocked_reason,
        }
    }
}

/// Result of checking action gates
#[derive(Debug, Clone)]
pub struct GateCheckResult {
    pub passed: bool,
    pub blocked_reason: Option<String>,
}

/// Check whether gates are satisfied
fn check_gates(
    gates: &ActionGates,
    read_write: bool,
    is_primary: bool,
    confirmed: bool,
) -> GateCheckResult {
    let mut missing_flags = Vec::new();

    if gates.requires_write && !read_write {
        missing_flags.push("--read-write");
    }
    if gates.requires_primary && !is_primary {
        missing_flags.push("--primary");
    }
    if gates.requires_confirmation && !confirmed {
        missing_flags.push("--yes");
    }

    if missing_flags.is_empty() {
        GateCheckResult {
            passed: true,
            blocked_reason: None,
        }
    } else {
        GateCheckResult {
            passed: false,
            blocked_reason: Some(format!("Missing required flags: {}", missing_flags.join(", "))),
        }
    }
}

/// Result of executing a fix command
#[derive(Debug, Clone, Serialize)]
pub struct FixResult {
    /// Whether the fix was executed (vs dry-run)
    pub executed: bool,
    /// Whether the fix succeeded
    pub success: bool,
    /// SQL that was (or would be) executed
    pub sql: Vec<String>,
    /// Human-readable summary
    pub summary: String,
    /// Optional error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Verification results (if --verify was used)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification: Option<VerificationResult>,
}

/// Result of verification steps
#[derive(Debug, Clone, Serialize)]
pub struct VerificationResult {
    /// Whether all verification steps passed
    pub passed: bool,
    /// Individual step results
    pub steps: Vec<VerifyStepResult>,
}

/// Result of a single verification step
#[derive(Debug, Clone, Serialize)]
pub struct VerifyStepResult {
    pub description: String,
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gates_all_pass() {
        let gates = ActionGates::write_primary_confirm();
        let result = check_gates(&gates, true, true, true);
        assert!(result.passed);
        assert!(result.blocked_reason.is_none());
    }

    #[test]
    fn test_gates_missing_write() {
        let gates = ActionGates::write_primary();
        let result = check_gates(&gates, false, true, true);
        assert!(!result.passed);
        assert!(result.blocked_reason.unwrap().contains("--read-write"));
    }

    #[test]
    fn test_gates_missing_multiple() {
        let gates = ActionGates::write_primary_confirm();
        let result = check_gates(&gates, false, false, false);
        assert!(!result.passed);
        let reason = result.blocked_reason.unwrap();
        assert!(reason.contains("--read-write"));
        assert!(reason.contains("--primary"));
        assert!(reason.contains("--yes"));
    }

    #[test]
    fn test_action_builder() {
        let action = StructuredAction::builder("test.action", ActionType::Fix)
            .description("Test action")
            .mutates(true)
            .risk(Risk::Low)
            .gates(ActionGates::write_primary())
            .sql_preview(vec!["SELECT 1".to_string()])
            .build(true, true, true);

        assert_eq!(action.action_id, "test.action");
        assert!(action.available);
        assert!(action.mutates);
        assert_eq!(action.risk, Risk::Low);
    }
}
