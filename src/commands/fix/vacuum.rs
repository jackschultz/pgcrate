//! Fix vacuum command: Trigger vacuum operations on tables.
//!
//! VACUUM reclaims storage from dead tuples and updates statistics.
//! Options:
//! - Regular VACUUM: Online, non-blocking
//! - VACUUM FREEZE: Also freezes old XIDs
//! - VACUUM FULL: Rewrites table, requires ACCESS EXCLUSIVE lock
//! - VACUUM ANALYZE: Also updates statistics

use anyhow::{Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

use super::common::{
    print_fix_result, ActionGates, ActionType, FixResult, Risk, StructuredAction, VerifyStep,
};
use crate::sql::quote_ident;

/// VACUUM options
#[derive(Debug, Clone, Default)]
pub struct VacuumOptions {
    pub freeze: bool,
    pub full: bool,
    pub analyze: bool,
}

#[allow(dead_code)] // Used by --include-fixes
impl VacuumOptions {
    pub fn risk(&self) -> Risk {
        if self.full {
            Risk::High // ACCESS EXCLUSIVE lock
        } else {
            Risk::Low // Non-blocking
        }
    }

    pub fn to_sql_options(&self) -> String {
        let mut opts = Vec::new();
        if self.freeze {
            opts.push("FREEZE");
        }
        if self.full {
            opts.push("FULL");
        }
        if self.analyze {
            opts.push("ANALYZE");
        }
        if opts.is_empty() {
            String::new()
        } else {
            format!("({})", opts.join(", "))
        }
    }
}

/// Evidence for vacuum action
#[derive(Debug, Clone, Serialize)]
pub struct VacuumEvidence {
    pub schema: String,
    pub table: String,
    pub dead_tuples: i64,
    pub live_tuples: i64,
    pub dead_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_vacuum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_autovacuum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_analyze: Option<String>,
    pub table_size: String,
    pub table_size_bytes: i64,
}

/// Get table vacuum status
pub async fn get_table_vacuum_info(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<VacuumEvidence> {
    let query = r#"
        SELECT
            schemaname,
            relname,
            n_dead_tup as dead_tuples,
            n_live_tup as live_tuples,
            CASE
                WHEN n_live_tup + n_dead_tup = 0 THEN 0
                ELSE round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)::float8
            END as dead_pct,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            pg_size_pretty(pg_total_relation_size(relid)) as table_size,
            pg_total_relation_size(relid) as table_size_bytes
        FROM pg_stat_user_tables
        WHERE schemaname = $1 AND relname = $2
    "#;

    let row = client
        .query_opt(query, &[&schema, &table])
        .await
        .context("Failed to query table stats")?
        .ok_or_else(|| anyhow::anyhow!("Table {}.{} not found", schema, table))?;

    let last_vacuum: Option<chrono::DateTime<chrono::Utc>> = row.get("last_vacuum");
    let last_autovacuum: Option<chrono::DateTime<chrono::Utc>> = row.get("last_autovacuum");
    let last_analyze: Option<chrono::DateTime<chrono::Utc>> = row.get("last_analyze");

    Ok(VacuumEvidence {
        schema: row.get("schemaname"),
        table: row.get("relname"),
        dead_tuples: row.get("dead_tuples"),
        live_tuples: row.get("live_tuples"),
        dead_pct: row.get("dead_pct"),
        last_vacuum: last_vacuum.map(|t| t.to_rfc3339()),
        last_autovacuum: last_autovacuum.map(|t| t.to_rfc3339()),
        last_analyze: last_analyze.map(|t| t.to_rfc3339()),
        table_size: row.get("table_size"),
        table_size_bytes: row.get("table_size_bytes"),
    })
}

/// Generate SQL for vacuum operation
pub fn generate_vacuum_sql(schema: &str, table: &str, options: &VacuumOptions) -> String {
    let opts = options.to_sql_options();
    if opts.is_empty() {
        format!("VACUUM {}.{};", quote_ident(schema), quote_ident(table))
    } else {
        format!(
            "VACUUM {} {}.{};",
            opts,
            quote_ident(schema),
            quote_ident(table)
        )
    }
}

/// Execute vacuum operation
pub async fn execute_vacuum(
    client: &Client,
    schema: &str,
    table: &str,
    options: &VacuumOptions,
    dry_run: bool,
) -> Result<FixResult> {
    // Get current state
    let evidence = get_table_vacuum_info(client, schema, table).await?;

    let sql = generate_vacuum_sql(schema, table, options);

    let mode = if options.full {
        "VACUUM FULL"
    } else if options.freeze {
        "VACUUM FREEZE"
    } else if options.analyze {
        "VACUUM ANALYZE"
    } else {
        "VACUUM"
    };

    if dry_run {
        let warning = if options.full {
            "\n\nWARNING: VACUUM FULL requires ACCESS EXCLUSIVE lock and will block all operations on the table."
        } else {
            ""
        };

        return Ok(FixResult {
            executed: false,
            success: true,
            sql: vec![sql],
            summary: format!(
                "Would run {} on {}.{} ({} dead tuples, {:.1}%){}",
                mode, schema, table, evidence.dead_tuples, evidence.dead_pct, warning
            ),
            error: None,
            verification: None,
        });
    }

    // Execute vacuum
    // Note: VACUUM cannot run in a transaction, so we use batch_execute
    match client.batch_execute(&sql).await {
        Ok(_) => Ok(FixResult {
            executed: true,
            success: true,
            sql: vec![sql],
            summary: format!("{} completed on {}.{}", mode, schema, table),
            error: None,
            verification: None,
        }),
        Err(e) => Ok(FixResult {
            executed: true,
            success: false,
            sql: vec![sql],
            summary: format!("Failed to {} {}.{}", mode, schema, table),
            error: Some(e.to_string()),
            verification: None,
        }),
    }
}

/// Get verification steps for vacuum.
/// Note: Vacuum verification is limited - we check overall status improved,
/// not the specific table, since JSONPath comparison operators are limited.
pub fn get_verify_steps() -> Vec<VerifyStep> {
    vec![VerifyStep {
        description: "Verify vacuum status is not critical".to_string(),
        command: "pgcrate vacuum --json".to_string(),
        expected: "$.data.overall_status != 'critical'".to_string(),
    }]
}

/// Create a structured action for vacuum
pub fn create_vacuum_action(
    evidence: &VacuumEvidence,
    options: &VacuumOptions,
    read_write: bool,
    is_primary: bool,
    confirmed: bool,
) -> StructuredAction {
    let mode = if options.full {
        "full"
    } else if options.freeze {
        "freeze"
    } else if options.analyze {
        "analyze"
    } else {
        "regular"
    };

    let action_id = format!("fix.vacuum.{}.{}.{}", mode, evidence.schema, evidence.table);

    let sql = generate_vacuum_sql(&evidence.schema, &evidence.table, options);
    let risk = options.risk();

    // VACUUM FULL requires confirmation due to ACCESS EXCLUSIVE lock
    let gates = if options.full {
        ActionGates::write_primary_confirm()
    } else {
        ActionGates::write_primary()
    };

    let mut args = vec![
        "fix".to_string(),
        "vacuum".to_string(),
        format!("{}.{}", evidence.schema, evidence.table),
    ];
    if options.freeze {
        args.push("--freeze".to_string());
    }
    if options.full {
        args.push("--full".to_string());
    }
    if options.analyze {
        args.push("--analyze".to_string());
    }

    let verify_steps = get_verify_steps();

    let mode_desc = if options.full {
        "VACUUM FULL (requires ACCESS EXCLUSIVE lock)"
    } else if options.freeze {
        "VACUUM FREEZE"
    } else if options.analyze {
        "VACUUM ANALYZE"
    } else {
        "VACUUM"
    };

    StructuredAction::builder(action_id, ActionType::Fix)
        .command("pgcrate")
        .args(args)
        .description(format!(
            "{} on {}.{} ({} dead tuples, {:.1}% bloat)",
            mode_desc, evidence.schema, evidence.table, evidence.dead_tuples, evidence.dead_pct
        ))
        .mutates(true)
        .risk(risk)
        .gates(gates)
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
        Some(t) => DiagnosticOutput::with_timeouts("pgcrate.fix.vacuum", result, severity, t),
        None => DiagnosticOutput::new("pgcrate.fix.vacuum", result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vacuum_options_regular() {
        let opts = VacuumOptions::default();
        assert_eq!(opts.to_sql_options(), "");
        assert_eq!(opts.risk(), Risk::Low);
    }

    #[test]
    fn test_vacuum_options_freeze() {
        let opts = VacuumOptions {
            freeze: true,
            ..Default::default()
        };
        assert_eq!(opts.to_sql_options(), "(FREEZE)");
        assert_eq!(opts.risk(), Risk::Low);
    }

    #[test]
    fn test_vacuum_options_full() {
        let opts = VacuumOptions {
            full: true,
            ..Default::default()
        };
        assert_eq!(opts.to_sql_options(), "(FULL)");
        assert_eq!(opts.risk(), Risk::High);
    }

    #[test]
    fn test_vacuum_options_analyze() {
        let opts = VacuumOptions {
            analyze: true,
            ..Default::default()
        };
        assert_eq!(opts.to_sql_options(), "(ANALYZE)");
        assert_eq!(opts.risk(), Risk::Low);
    }

    #[test]
    fn test_vacuum_options_multiple() {
        let opts = VacuumOptions {
            freeze: true,
            full: true,
            analyze: true,
        };
        assert_eq!(opts.to_sql_options(), "(FREEZE, FULL, ANALYZE)");
    }

    #[test]
    fn test_generate_vacuum_sql_regular() {
        let opts = VacuumOptions::default();
        let sql = generate_vacuum_sql("public", "orders", &opts);
        assert_eq!(sql, "VACUUM \"public\".\"orders\";");
    }

    #[test]
    fn test_generate_vacuum_sql_full() {
        let opts = VacuumOptions {
            full: true,
            ..Default::default()
        };
        let sql = generate_vacuum_sql("public", "orders", &opts);
        assert_eq!(sql, "VACUUM (FULL) \"public\".\"orders\";");
    }

    #[test]
    fn test_generate_vacuum_sql_analyze() {
        let opts = VacuumOptions {
            analyze: true,
            ..Default::default()
        };
        let sql = generate_vacuum_sql("public", "orders", &opts);
        assert_eq!(sql, "VACUUM (ANALYZE) \"public\".\"orders\";");
    }
}
