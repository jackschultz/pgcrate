//! Fix commands: Remediation actions for diagnostics findings.
//!
//! Fix commands are separate from diagnostic commands because they mutate state.
//! They follow a diagnose → fix → verify workflow with proper gating.

pub mod common;
pub mod index;
pub mod sequence;
pub mod vacuum;
pub mod verify;

// Re-export StructuredAction for triage --include-fixes
pub use common::StructuredAction;
