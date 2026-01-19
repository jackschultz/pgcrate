//! Fix index command: Safely drop unused/duplicate indexes.
//!
//! Indexes that are never used waste disk space and slow down writes.
//! This command provides safe index dropping with comprehensive evidence
//! and safety checks.

use anyhow::{bail, Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

use super::common::{
    print_fix_result, ActionGates, ActionType, FixResult, Risk, StructuredAction, VerifyStep,
};
use crate::sql::quote_ident;

/// Evidence for index drop action
#[derive(Debug, Clone, Serialize)]
pub struct IndexDropEvidence {
    pub schema: String,
    pub index_name: String,
    pub table_name: String,
    pub idx_scan: i64,
    pub idx_tup_read: i64,
    pub size_bytes: i64,
    pub size_pretty: String,
    pub is_unique: bool,
    pub is_primary_key: bool,
    pub is_replica_identity: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backing_constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats_age_days: Option<i32>,
}

/// Safety check result
#[derive(Debug, Clone)]
pub struct SafetyCheck {
    pub can_drop: bool,
    pub reason: Option<String>,
}

/// Get detailed information about an index for dropping
pub async fn get_index_info(
    client: &Client,
    schema: &str,
    name: &str,
) -> Result<IndexDropEvidence> {
    // Get index details including safety information
    let query = r#"
        SELECT
            n.nspname as schema_name,
            i.relname as index_name,
            t.relname as table_name,
            COALESCE(s.idx_scan, 0) as idx_scan,
            COALESCE(s.idx_tup_read, 0) as idx_tup_read,
            pg_relation_size(i.oid) as size_bytes,
            pg_size_pretty(pg_relation_size(i.oid)) as size_pretty,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary_key,
            ix.indisreplident as is_replica_identity,
            c.conname as constraint_name
        FROM pg_class i
        JOIN pg_index ix ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = i.relnamespace
        LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
        LEFT JOIN pg_constraint c ON c.conindid = i.oid
        WHERE n.nspname = $1 AND i.relname = $2
    "#;

    let row = client
        .query_opt(query, &[&schema, &name])
        .await
        .context("Failed to query index")?
        .ok_or_else(|| anyhow::anyhow!("Index {}.{} not found", schema, name))?;

    // Get stats reset time
    let stats_query = r#"
        SELECT
            stats_reset,
            (EXTRACT(EPOCH FROM (now() - stats_reset)) / 86400)::int as days_old
        FROM pg_stat_database
        WHERE datname = current_database()
    "#;

    let (stats_since, stats_age_days) = match client.query_opt(stats_query, &[]).await? {
        Some(stats_row) => {
            let reset: Option<chrono::DateTime<chrono::Utc>> = stats_row.get("stats_reset");
            let days: Option<i32> = stats_row.get("days_old");
            (reset.map(|r| r.to_rfc3339()), days)
        }
        None => (None, None),
    };

    Ok(IndexDropEvidence {
        schema: row.get("schema_name"),
        index_name: row.get("index_name"),
        table_name: row.get("table_name"),
        idx_scan: row.get("idx_scan"),
        idx_tup_read: row.get("idx_tup_read"),
        size_bytes: row.get("size_bytes"),
        size_pretty: row.get("size_pretty"),
        is_unique: row.get("is_unique"),
        is_primary_key: row.get("is_primary_key"),
        is_replica_identity: row.get("is_replica_identity"),
        backing_constraint: row.get("constraint_name"),
        stats_since,
        stats_age_days,
    })
}

/// Check if an index is safe to drop
pub fn check_safety(evidence: &IndexDropEvidence) -> SafetyCheck {
    if evidence.is_primary_key {
        return SafetyCheck {
            can_drop: false,
            reason: Some(
                "Cannot drop primary key index. Drop the primary key constraint instead."
                    .to_string(),
            ),
        };
    }

    if evidence.is_replica_identity {
        return SafetyCheck {
            can_drop: false,
            reason: Some(
                "Cannot drop replica identity index. It is used for logical replication."
                    .to_string(),
            ),
        };
    }

    if let Some(constraint) = &evidence.backing_constraint {
        return SafetyCheck {
            can_drop: false,
            reason: Some(format!(
                "Cannot drop index directly. It backs constraint '{}'. Drop the constraint instead.",
                constraint
            )),
        };
    }

    SafetyCheck {
        can_drop: true,
        reason: None,
    }
}

/// Generate SQL for dropping an index
pub fn generate_drop_sql(schema: &str, name: &str, concurrent: bool) -> String {
    if concurrent {
        format!(
            "DROP INDEX CONCURRENTLY {}.{};",
            quote_ident(schema),
            quote_ident(name)
        )
    } else {
        format!("DROP INDEX {}.{};", quote_ident(schema), quote_ident(name))
    }
}

/// Execute index drop
pub async fn execute_drop(
    client: &Client,
    schema: &str,
    name: &str,
    dry_run: bool,
) -> Result<FixResult> {
    // Get current state and check safety
    let evidence = get_index_info(client, schema, name).await?;
    let safety = check_safety(&evidence);

    if !safety.can_drop {
        bail!(
            "{}",
            safety
                .reason
                .unwrap_or_else(|| "Cannot drop index".to_string())
        );
    }

    // Use CONCURRENTLY to avoid blocking
    let sql = generate_drop_sql(schema, name, true);

    if dry_run {
        return Ok(FixResult {
            executed: false,
            success: true,
            sql: vec![sql],
            summary: format!(
                "Would drop index {}.{} ({}, {} scans)",
                schema, name, evidence.size_pretty, evidence.idx_scan
            ),
            error: None,
            verification: None,
        });
    }

    // Execute the drop
    // Note: DROP INDEX CONCURRENTLY cannot run in a transaction
    match client.batch_execute(&sql).await {
        Ok(_) => Ok(FixResult {
            executed: true,
            success: true,
            sql: vec![sql],
            summary: format!(
                "Dropped index {}.{} ({} reclaimed)",
                schema, name, evidence.size_pretty
            ),
            error: None,
            verification: None,
        }),
        Err(e) => Ok(FixResult {
            executed: true,
            success: false,
            sql: vec![sql],
            summary: format!("Failed to drop index {}.{}", schema, name),
            error: Some(e.to_string()),
            verification: None,
        }),
    }
}

/// Get verification steps for index drop.
pub fn get_verify_steps(index_name: &str) -> Vec<VerifyStep> {
    vec![VerifyStep {
        description: format!(
            "Verify index {} no longer exists in unused list",
            index_name
        ),
        command: "pgcrate indexes --json".to_string(),
        expected: format!("$.data.unused[?(@.index=='{}')].length() == 0", index_name),
    }]
}

/// Create a structured action for index drop
pub fn create_drop_action(
    evidence: &IndexDropEvidence,
    read_write: bool,
    is_primary: bool,
    confirmed: bool,
) -> StructuredAction {
    let action_id = format!("fix.index.drop.{}.{}", evidence.schema, evidence.index_name);

    let safety = check_safety(evidence);
    let sql = generate_drop_sql(&evidence.schema, &evidence.index_name, true);

    let verify_steps = get_verify_steps(&evidence.index_name);

    let builder = StructuredAction::builder(action_id, ActionType::Fix)
        .command("pgcrate")
        .args(vec![
            "fix".to_string(),
            "index".to_string(),
            "--drop".to_string(),
            format!("{}.{}", evidence.schema, evidence.index_name),
        ])
        .description(format!(
            "Drop unused index {}.{} ({}, {} scans since stats reset)",
            evidence.schema, evidence.index_name, evidence.size_pretty, evidence.idx_scan
        ))
        .mutates(true)
        .risk(Risk::Medium) // Requires confirmation, concurrent drop
        .gates(ActionGates::write_primary_confirm())
        .sql_preview(vec![sql])
        .evidence(serde_json::to_value(evidence).unwrap_or_default())
        .verify(verify_steps);

    // If safety check fails, we still build the action but mark it unavailable
    let mut action = builder.build(read_write, is_primary, confirmed);

    if !safety.can_drop {
        action.available = false;
        action.blocked_reason = safety.reason;
    }

    action
}

/// Print fix result in human-readable format
pub fn print_human(result: &FixResult, quiet: bool) {
    print_fix_result(
        result,
        quiet,
        Some("Note: Uses DROP INDEX CONCURRENTLY to avoid blocking."),
    );
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
        Some(t) => DiagnosticOutput::with_timeouts("pgcrate.fix.index", result, severity, t),
        None => DiagnosticOutput::new("pgcrate.fix.index", result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_evidence() -> IndexDropEvidence {
        IndexDropEvidence {
            schema: "public".to_string(),
            index_name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            idx_scan: 0,
            idx_tup_read: 0,
            size_bytes: 1048576,
            size_pretty: "1 MB".to_string(),
            is_unique: false,
            is_primary_key: false,
            is_replica_identity: false,
            backing_constraint: None,
            stats_since: Some("2024-01-01T00:00:00Z".to_string()),
            stats_age_days: Some(30),
        }
    }

    #[test]
    fn test_safety_check_normal_index() {
        let evidence = test_evidence();
        let safety = check_safety(&evidence);
        assert!(safety.can_drop);
        assert!(safety.reason.is_none());
    }

    #[test]
    fn test_safety_check_primary_key() {
        let mut evidence = test_evidence();
        evidence.is_primary_key = true;
        let safety = check_safety(&evidence);
        assert!(!safety.can_drop);
        assert!(safety.reason.unwrap().contains("primary key"));
    }

    #[test]
    fn test_safety_check_replica_identity() {
        let mut evidence = test_evidence();
        evidence.is_replica_identity = true;
        let safety = check_safety(&evidence);
        assert!(!safety.can_drop);
        assert!(safety.reason.unwrap().contains("replica identity"));
    }

    #[test]
    fn test_safety_check_backing_constraint() {
        let mut evidence = test_evidence();
        evidence.backing_constraint = Some("users_email_key".to_string());
        let safety = check_safety(&evidence);
        assert!(!safety.can_drop);
        assert!(safety.reason.unwrap().contains("constraint"));
    }

    #[test]
    fn test_generate_drop_sql_concurrent() {
        let sql = generate_drop_sql("public", "idx_test", true);
        assert_eq!(sql, "DROP INDEX CONCURRENTLY \"public\".\"idx_test\";");
    }

    #[test]
    fn test_generate_drop_sql_blocking() {
        let sql = generate_drop_sql("public", "idx_test", false);
        assert_eq!(sql, "DROP INDEX \"public\".\"idx_test\";");
    }
}
