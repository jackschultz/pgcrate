//! Fix bloat command: Rebuild bloated indexes via REINDEX.
//!
//! Index bloat accumulates from page splits and deletions. Unlike table bloat
//! (handled by VACUUM), index bloat requires REINDEX to reclaim space.
//!
//! Options:
//! - REINDEX CONCURRENTLY: Non-blocking (PostgreSQL 12+), creates new index in background
//! - REINDEX: Blocking, requires ACCESS EXCLUSIVE lock (fallback for older PG)

use anyhow::{bail, Context, Result};
use serde::Serialize;
use tokio_postgres::Client;

use super::common::{
    print_fix_result, ActionGates, ActionType, FixResult, Risk, StructuredAction, VerifyStep,
};
use crate::sql::quote_ident;

/// Evidence for index reindex action
#[derive(Debug, Clone, Serialize)]
pub struct ReindexEvidence {
    pub schema: String,
    pub index_name: String,
    pub table_name: String,
    pub size_bytes: i64,
    pub size_pretty: String,
    pub bloat_bytes: i64,
    pub bloat_pct: f64,
    pub is_primary_key: bool,
    pub is_unique: bool,
    /// PostgreSQL version (major)
    pub pg_version: i32,
    /// Whether REINDEX CONCURRENTLY is available (PG 12+)
    pub concurrent_available: bool,
}

/// Get PostgreSQL major version
async fn get_pg_version(client: &Client) -> Result<i32> {
    let row = client
        .query_one("SHOW server_version_num", &[])
        .await
        .context("Failed to get PostgreSQL version")?;

    let version_num: String = row.get(0);
    let version: i32 = version_num.parse().unwrap_or(0);
    Ok(version / 10000) // Major version
}

/// Get detailed information about an index for reindexing
pub async fn get_index_bloat_info(
    client: &Client,
    schema: &str,
    name: &str,
) -> Result<ReindexEvidence> {
    let pg_version = get_pg_version(client).await?;

    // Query index details and estimate bloat using the same method as dba bloat
    let query = r#"
WITH btree_index_atts AS (
    SELECT
        n.nspname,
        ci.relname AS index_relname,
        ct.relname AS table_relname,
        i.indexrelid,
        i.indrelid,
        i.indisprimary,
        i.indisunique,
        pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS indkeys
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
    JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = ct.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = ct.oid AND a.attnum = ANY(i.indkey)
    WHERE n.nspname = $1 AND ci.relname = $2
      AND ci.relam = (SELECT oid FROM pg_am WHERE amname = 'btree')
    GROUP BY n.nspname, ci.relname, ct.relname, i.indexrelid, i.indrelid, i.indisprimary, i.indisunique
),
index_stats AS (
    SELECT
        b.nspname AS schema_name,
        b.table_relname AS table_name,
        b.index_relname AS index_name,
        b.indisprimary AS is_primary_key,
        b.indisunique AS is_unique,
        pg_relation_size(b.indexrelid) AS size_bytes,
        pg_size_pretty(pg_relation_size(b.indexrelid)) AS size_pretty,
        ci.relpages,
        ci.reltuples,
        coalesce(
            ceil(
                ci.reltuples * (
                    sum(coalesce(s.avg_width, 8)) + 8 + 6
                ) / (
                    current_setting('block_size')::int - 24
                )
            ),
            0
        )::bigint AS est_pages
    FROM btree_index_atts b
    JOIN pg_catalog.pg_class ci ON ci.oid = b.indexrelid
    LEFT JOIN pg_catalog.pg_stats s ON s.schemaname = b.nspname
        AND s.tablename = b.table_relname
        AND s.attname = ANY(b.indkeys)
    GROUP BY b.nspname, b.table_relname, b.index_relname, b.indexrelid,
             b.indisprimary, b.indisunique, ci.relpages, ci.reltuples
)
SELECT
    schema_name,
    table_name,
    index_name,
    is_primary_key,
    is_unique,
    size_bytes,
    size_pretty,
    GREATEST(0, (relpages - est_pages) * current_setting('block_size')::int)::bigint AS bloat_bytes,
    CASE WHEN relpages > 0
        THEN round(100.0 * GREATEST(0, relpages - est_pages) / relpages, 1)
        ELSE 0
    END::float8 AS bloat_pct
FROM index_stats
"#;

    let row = client
        .query_opt(query, &[&schema, &name])
        .await
        .context("Failed to query index bloat")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Index {}.{} not found or is not a btree index",
                schema,
                name
            )
        })?;

    Ok(ReindexEvidence {
        schema: row.get("schema_name"),
        index_name: row.get("index_name"),
        table_name: row.get("table_name"),
        size_bytes: row.get("size_bytes"),
        size_pretty: row.get("size_pretty"),
        bloat_bytes: row.get("bloat_bytes"),
        bloat_pct: row.get("bloat_pct"),
        is_primary_key: row.get("is_primary_key"),
        is_unique: row.get("is_unique"),
        pg_version,
        concurrent_available: pg_version >= 12,
    })
}

/// Generate SQL for reindexing
pub fn generate_reindex_sql(schema: &str, name: &str, concurrent: bool) -> String {
    if concurrent {
        format!(
            "REINDEX INDEX CONCURRENTLY {}.{};",
            quote_ident(schema),
            quote_ident(name)
        )
    } else {
        format!(
            "REINDEX INDEX {}.{};",
            quote_ident(schema),
            quote_ident(name)
        )
    }
}

/// Execute reindex operation
pub async fn execute_reindex(
    client: &Client,
    schema: &str,
    name: &str,
    dry_run: bool,
    force_blocking: bool,
) -> Result<FixResult> {
    let evidence = get_index_bloat_info(client, schema, name).await?;

    // Determine if we can use CONCURRENTLY
    let use_concurrent = evidence.concurrent_available && !force_blocking;
    let sql = generate_reindex_sql(schema, name, use_concurrent);

    let mode = if use_concurrent {
        "REINDEX CONCURRENTLY"
    } else {
        "REINDEX"
    };

    if dry_run {
        let warning = if !use_concurrent {
            if evidence.concurrent_available {
                "\n\nNote: Using blocking REINDEX due to --blocking flag."
            } else {
                "\n\nWARNING: REINDEX requires ACCESS EXCLUSIVE lock (blocks reads and writes). \
                 PostgreSQL 12+ supports REINDEX CONCURRENTLY for non-blocking rebuilds."
            }
        } else {
            ""
        };

        return Ok(FixResult {
            executed: false,
            success: true,
            sql: vec![sql],
            summary: format!(
                "Would {} on {}.{} ({}, {:.1}% bloat, ~{} reclaimable){}",
                mode,
                schema,
                name,
                evidence.size_pretty,
                evidence.bloat_pct,
                format_bytes(evidence.bloat_bytes),
                warning
            ),
            error: None,
            verification: None,
        });
    }

    // Validate we have actual bloat worth fixing
    if evidence.bloat_pct < 10.0 {
        bail!(
            "Index {}.{} has only {:.1}% estimated bloat. \
             Reindexing indexes with <10% bloat is not recommended.",
            schema,
            name,
            evidence.bloat_pct
        );
    }

    // Execute reindex
    // Note: REINDEX CONCURRENTLY cannot run in a transaction
    match client.batch_execute(&sql).await {
        Ok(_) => {
            // Get new size to calculate savings
            let new_info = get_index_bloat_info(client, schema, name).await.ok();
            let savings = new_info
                .map(|i| evidence.size_bytes - i.size_bytes)
                .unwrap_or(0);

            Ok(FixResult {
                executed: true,
                success: true,
                sql: vec![sql],
                summary: format!(
                    "{} completed on {}.{} ({} reclaimed)",
                    mode,
                    schema,
                    name,
                    format_bytes(savings.max(0))
                ),
                error: None,
                verification: None,
            })
        }
        Err(e) => Ok(FixResult {
            executed: true,
            success: false,
            sql: vec![sql],
            summary: format!("Failed to {} {}.{}", mode, schema, name),
            error: Some(e.to_string()),
            verification: None,
        }),
    }
}

/// Format bytes for human display
fn format_bytes(bytes: i64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Get verification steps for reindex
pub fn get_verify_steps(schema: &str, index: &str) -> Vec<VerifyStep> {
    vec![VerifyStep {
        description: format!("Verify {}.{} bloat reduced", schema, index),
        command: "pgcrate dba bloat --json".to_string(),
        expected: format!("$.data.indexes[?(@.index=='{}')].bloat_pct < 20", index),
    }]
}

/// Create a structured action for reindex
#[allow(dead_code)] // Used by triage --include-fixes (PGC-71)
pub fn create_reindex_action(
    evidence: &ReindexEvidence,
    read_write: bool,
    is_primary: bool,
    confirmed: bool,
) -> StructuredAction {
    let action_id = format!(
        "fix.bloat.reindex.{}.{}",
        evidence.schema, evidence.index_name
    );

    let use_concurrent = evidence.concurrent_available;
    let sql = generate_reindex_sql(&evidence.schema, &evidence.index_name, use_concurrent);

    // REINDEX CONCURRENTLY is medium risk (still does work, can fail)
    // Blocking REINDEX is high risk (locks table)
    let risk = if use_concurrent {
        Risk::Medium
    } else {
        Risk::High
    };

    // Require confirmation for all reindex operations
    let gates = ActionGates::write_primary_confirm();

    let verify_steps = get_verify_steps(&evidence.schema, &evidence.index_name);

    let mode = if use_concurrent {
        "REINDEX CONCURRENTLY"
    } else {
        "REINDEX (blocking)"
    };

    StructuredAction::builder(action_id, ActionType::Fix)
        .command("pgcrate")
        .args(vec![
            "dba".to_string(),
            "fix".to_string(),
            "bloat".to_string(),
            format!("{}.{}", evidence.schema, evidence.index_name),
        ])
        .description(format!(
            "{} on {}.{} ({}, {:.1}% bloat, ~{} reclaimable)",
            mode,
            evidence.schema,
            evidence.index_name,
            evidence.size_pretty,
            evidence.bloat_pct,
            format_bytes(evidence.bloat_bytes)
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
    let note = Some("Note: REINDEX CONCURRENTLY builds a new index without blocking writes.");
    print_fix_result(result, quiet, note);
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
        Some(t) => DiagnosticOutput::with_timeouts("pgcrate.fix.bloat", result, severity, t),
        None => DiagnosticOutput::new("pgcrate.fix.bloat", result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_reindex_sql_concurrent() {
        let sql = generate_reindex_sql("public", "idx_users_email", true);
        assert_eq!(
            sql,
            "REINDEX INDEX CONCURRENTLY \"public\".\"idx_users_email\";"
        );
    }

    #[test]
    fn test_generate_reindex_sql_blocking() {
        let sql = generate_reindex_sql("public", "idx_users_email", false);
        assert_eq!(sql, "REINDEX INDEX \"public\".\"idx_users_email\";");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1_500_000), "1.4 MB");
        assert_eq!(format_bytes(2_000_000_000), "1.9 GB");
    }

    fn test_evidence() -> ReindexEvidence {
        ReindexEvidence {
            schema: "public".to_string(),
            index_name: "idx_orders_created".to_string(),
            table_name: "orders".to_string(),
            size_bytes: 104857600, // 100 MB
            size_pretty: "100 MB".to_string(),
            bloat_bytes: 52428800, // 50 MB
            bloat_pct: 50.0,
            is_primary_key: false,
            is_unique: false,
            pg_version: 15,
            concurrent_available: true,
        }
    }

    #[test]
    fn test_create_action_concurrent() {
        let evidence = test_evidence();
        let action = create_reindex_action(&evidence, true, true, true);

        assert!(action.available);
        assert_eq!(action.risk, Risk::Medium);
        assert!(action.sql_preview.unwrap()[0].contains("CONCURRENTLY"));
    }

    #[test]
    fn test_create_action_old_pg() {
        let mut evidence = test_evidence();
        evidence.pg_version = 11;
        evidence.concurrent_available = false;

        let action = create_reindex_action(&evidence, true, true, true);

        assert!(action.available);
        assert_eq!(action.risk, Risk::High);
        assert!(!action.sql_preview.unwrap()[0].contains("CONCURRENTLY"));
    }
}
