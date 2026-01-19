//! Fix sequence command: Upgrade sequence types to prevent exhaustion.
//!
//! Sequences can exhaust their maximum value based on data type:
//! - smallint: 32,767
//! - integer: 2,147,483,647
//! - bigint: 9,223,372,036,854,775,807
//!
//! This command upgrades sequences to larger types (ALTER SEQUENCE ... AS type).

use anyhow::{bail, Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

use super::common::{
    print_fix_result, ActionGates, ActionType, FixResult, Risk, StructuredAction, VerifyStep,
};
use crate::sql::quote_ident;

/// Sequence type hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SequenceType {
    SmallInt,
    Integer,
    BigInt,
}

impl SequenceType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "smallint" => Some(SequenceType::SmallInt),
            "integer" | "int" | "int4" => Some(SequenceType::Integer),
            "bigint" | "int8" => Some(SequenceType::BigInt),
            _ => None,
        }
    }

    pub fn to_sql(self) -> &'static str {
        match self {
            SequenceType::SmallInt => "smallint",
            SequenceType::Integer => "integer",
            SequenceType::BigInt => "bigint",
        }
    }

    pub fn max_value(&self) -> i64 {
        match self {
            SequenceType::SmallInt => 32_767,
            SequenceType::Integer => 2_147_483_647,
            SequenceType::BigInt => 9_223_372_036_854_775_807,
        }
    }
}

/// Evidence for sequence upgrade action
#[derive(Debug, Clone, Serialize)]
pub struct SequenceEvidence {
    pub current_type: String,
    pub target_type: String,
    pub current_pct: f64,
    pub last_value: i64,
    pub max_value: i64,
    pub upgrade_max_value: i64,
}

/// Information about a sequence for fix operations
#[derive(Debug, Clone, Serialize)]
pub struct SequenceFixInfo {
    pub schema: String,
    pub name: String,
    pub data_type: String,
    pub last_value: i64,
    pub max_value: i64,
    pub pct_used: f64,
}

/// Get information about a specific sequence
pub async fn get_sequence_info(
    client: &Client,
    schema: &str,
    name: &str,
) -> Result<SequenceFixInfo> {
    let query = r#"
        SELECT
            schemaname,
            sequencename,
            data_type::text as data_type,
            COALESCE(last_value, 0) as last_value,
            max_value,
            CASE
                WHEN increment_by > 0 AND max_value > 0 AND last_value IS NOT NULL
                THEN round(100.0 * last_value / max_value, 2)::float8
                ELSE 0::float8
            END as pct_used
        FROM pg_sequences
        WHERE schemaname = $1 AND sequencename = $2
    "#;

    let row = client
        .query_opt(query, &[&schema, &name])
        .await
        .context("Failed to query sequence")?
        .ok_or_else(|| anyhow::anyhow!("Sequence {}.{} not found", schema, name))?;

    Ok(SequenceFixInfo {
        schema: row.get("schemaname"),
        name: row.get("sequencename"),
        data_type: row.get("data_type"),
        last_value: row.get("last_value"),
        max_value: row.get("max_value"),
        pct_used: row.get("pct_used"),
    })
}

/// Generate SQL for upgrading a sequence type
pub fn generate_upgrade_sql(schema: &str, name: &str, target_type: SequenceType) -> String {
    format!(
        "ALTER SEQUENCE {}.{} AS {};",
        quote_ident(schema),
        quote_ident(name),
        target_type.to_sql()
    )
}

/// Execute sequence upgrade
pub async fn execute_upgrade(
    client: &Client,
    schema: &str,
    name: &str,
    target_type: SequenceType,
    dry_run: bool,
) -> Result<FixResult> {
    // Get current state
    let info = get_sequence_info(client, schema, name).await?;

    // Validate upgrade path
    let current_type = SequenceType::from_str(&info.data_type)
        .ok_or_else(|| anyhow::anyhow!("Unknown sequence type: {}", info.data_type))?;

    if target_type <= current_type {
        bail!(
            "Cannot downgrade sequence from {} to {}",
            current_type.to_sql(),
            target_type.to_sql()
        );
    }

    let sql = generate_upgrade_sql(schema, name, target_type);

    if dry_run {
        return Ok(FixResult {
            executed: false,
            success: true,
            sql: vec![sql],
            summary: format!(
                "Would upgrade {}.{} from {} to {}",
                schema,
                name,
                current_type.to_sql(),
                target_type.to_sql()
            ),
            error: None,
            verification: None,
        });
    }

    // Execute the upgrade
    match client.batch_execute(&sql).await {
        Ok(_) => Ok(FixResult {
            executed: true,
            success: true,
            sql: vec![sql],
            summary: format!(
                "Upgraded {}.{} from {} to {}",
                schema,
                name,
                current_type.to_sql(),
                target_type.to_sql()
            ),
            error: None,
            verification: None,
        }),
        Err(e) => Ok(FixResult {
            executed: true,
            success: false,
            sql: vec![sql],
            summary: format!("Failed to upgrade {}.{}", schema, name),
            error: Some(e.to_string()),
            verification: None,
        }),
    }
}

/// Get verification steps for sequence upgrade.
pub fn get_verify_steps(schema: &str, name: &str) -> Vec<VerifyStep> {
    vec![VerifyStep {
        description: format!("Verify {}.{} is no longer critical", schema, name),
        command: "pgcrate sequences --all --json".to_string(),
        expected: format!(
            "$.data.sequences[?(@.name=='{}')].status != 'critical'",
            name
        ),
    }]
}

/// Create a structured action for sequence upgrade
pub fn create_upgrade_action(
    info: &SequenceFixInfo,
    target_type: SequenceType,
    read_write: bool,
    is_primary: bool,
    confirmed: bool,
) -> StructuredAction {
    let current_type = SequenceType::from_str(&info.data_type).unwrap_or(SequenceType::Integer);

    let action_id = format!(
        "fix.sequence.upgrade-{}.{}.{}",
        target_type.to_sql(),
        info.schema,
        info.name
    );

    let sql = generate_upgrade_sql(&info.schema, &info.name, target_type);

    let evidence = SequenceEvidence {
        current_type: current_type.to_sql().to_string(),
        target_type: target_type.to_sql().to_string(),
        current_pct: info.pct_used,
        last_value: info.last_value,
        max_value: info.max_value,
        upgrade_max_value: target_type.max_value(),
    };

    let verify_steps = get_verify_steps(&info.schema, &info.name);

    StructuredAction::builder(action_id, ActionType::Fix)
        .command("pgcrate")
        .args(vec![
            "fix".to_string(),
            "sequence".to_string(),
            format!("{}.{}", info.schema, info.name),
            "--upgrade-to".to_string(),
            target_type.to_sql().to_string(),
        ])
        .description(format!(
            "Upgrade sequence {}.{} from {} to {} (currently at {:.1}%)",
            info.schema,
            info.name,
            current_type.to_sql(),
            target_type.to_sql(),
            info.pct_used
        ))
        .mutates(true)
        .risk(Risk::Low) // ALTER SEQUENCE is non-blocking and safe
        .gates(ActionGates::write_primary())
        .sql_preview(vec![sql])
        .evidence(serde_json::to_value(evidence).unwrap_or_default())
        .verify(verify_steps)
        .build(read_write, is_primary, confirmed)
}

/// Print fix result in human-readable format
pub fn print_human(result: &FixResult, quiet: bool) {
    print_fix_result(result, quiet, None);
}

/// Print fix result as JSON
pub fn print_json(
    result: &FixResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{DiagnosticOutput, Severity};

    let severity = if result.success {
        Severity::Healthy
    } else {
        Severity::Error
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts("pgcrate.fix.sequence", result, severity, t),
        None => DiagnosticOutput::new("pgcrate.fix.sequence", result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_type_ordering() {
        assert!(SequenceType::SmallInt < SequenceType::Integer);
        assert!(SequenceType::Integer < SequenceType::BigInt);
    }

    #[test]
    fn test_sequence_type_from_str() {
        assert_eq!(
            SequenceType::from_str("smallint"),
            Some(SequenceType::SmallInt)
        );
        assert_eq!(
            SequenceType::from_str("integer"),
            Some(SequenceType::Integer)
        );
        assert_eq!(SequenceType::from_str("int"), Some(SequenceType::Integer));
        assert_eq!(SequenceType::from_str("int4"), Some(SequenceType::Integer));
        assert_eq!(SequenceType::from_str("bigint"), Some(SequenceType::BigInt));
        assert_eq!(SequenceType::from_str("int8"), Some(SequenceType::BigInt));
        assert_eq!(SequenceType::from_str("unknown"), None);
    }

    #[test]
    fn test_generate_upgrade_sql() {
        let sql = generate_upgrade_sql("public", "order_seq", SequenceType::BigInt);
        assert_eq!(sql, "ALTER SEQUENCE \"public\".\"order_seq\" AS bigint;");
    }

    #[test]
    fn test_generate_upgrade_sql_special_chars() {
        let sql = generate_upgrade_sql("My Schema", "Order-Seq", SequenceType::BigInt);
        assert_eq!(sql, "ALTER SEQUENCE \"My Schema\".\"Order-Seq\" AS bigint;");
    }
}
