//! Bloat command: Estimate table and index bloat.
//!
//! Shows wasted disk space from fragmentation. Tables bloat from updates/deletes;
//! indexes bloat from page splits and deletions.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

const WARNING_PCT: f64 = 20.0;
const CRITICAL_PCT: f64 = 50.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BloatStatus {
    Healthy,
    Warning,
    Critical,
}

impl BloatStatus {
    fn from_pct(pct: f64) -> Self {
        if pct >= CRITICAL_PCT {
            BloatStatus::Critical
        } else if pct >= WARNING_PCT {
            BloatStatus::Warning
        } else {
            BloatStatus::Healthy
        }
    }

    fn emoji(&self) -> &'static str {
        match self {
            BloatStatus::Healthy => "✓",
            BloatStatus::Warning => "⚠",
            BloatStatus::Critical => "✗",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TableBloat {
    pub schema: String,
    pub table: String,
    pub size_bytes: i64,
    pub bloat_bytes: i64,
    pub bloat_pct: f64,
    pub status: BloatStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexBloat {
    pub schema: String,
    pub table: String,
    pub index: String,
    pub size_bytes: i64,
    pub bloat_bytes: i64,
    pub bloat_pct: f64,
    pub status: BloatStatus,
}

#[derive(Debug, Serialize)]
pub struct BloatResult {
    pub tables: Vec<TableBloat>,
    pub indexes: Vec<IndexBloat>,
    pub total_table_bloat_bytes: i64,
    pub total_index_bloat_bytes: i64,
    pub overall_status: BloatStatus,
}

/// Statistical index bloat estimation (ioguix-style).
/// Works without extensions by using pg_class and pg_stats.
async fn get_index_bloat(client: &Client, limit: usize) -> Result<Vec<IndexBloat>> {
    // This query estimates index bloat using statistics.
    // It calculates expected index size based on tuple counts and widths,
    // then compares to actual size.
    let query = r#"
WITH btree_index_atts AS (
    SELECT
        n.nspname,
        ci.relname AS index_relname,
        ct.relname AS table_relname,
        i.indexrelid,
        i.indrelid,
        pg_catalog.array_agg(a.attnum ORDER BY a.attnum) AS indkey,
        pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS indkeys
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
    JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = ct.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = ct.oid AND a.attnum = ANY(i.indkey)
    WHERE ci.relam = (SELECT oid FROM pg_am WHERE amname = 'btree')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    GROUP BY n.nspname, ci.relname, ct.relname, i.indexrelid, i.indrelid
),
index_stats AS (
    SELECT
        b.nspname AS schema,
        b.table_relname AS "table",
        b.index_relname AS index,
        pg_relation_size(b.indexrelid) AS size_bytes,
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
    GROUP BY b.nspname, b.table_relname, b.index_relname, b.indexrelid, ci.relpages, ci.reltuples
)
SELECT
    schema,
    "table",
    index,
    size_bytes,
    GREATEST(0, (relpages - est_pages) * current_setting('block_size')::int)::bigint AS bloat_bytes,
    CASE WHEN relpages > 0
        THEN round(100.0 * GREATEST(0, relpages - est_pages) / relpages, 1)
        ELSE 0
    END::float8 AS bloat_pct
FROM index_stats
WHERE relpages > 1
  AND size_bytes > 65536
ORDER BY bloat_bytes DESC
LIMIT $1
"#;

    let rows = client.query(query, &[&(limit as i64)]).await?;
    let mut results = Vec::with_capacity(rows.len());

    for row in rows {
        let bloat_pct: f64 = row.get("bloat_pct");
        results.push(IndexBloat {
            schema: row.get("schema"),
            table: row.get("table"),
            index: row.get("index"),
            size_bytes: row.get("size_bytes"),
            bloat_bytes: row.get("bloat_bytes"),
            bloat_pct,
            status: BloatStatus::from_pct(bloat_pct),
        });
    }

    Ok(results)
}

/// Table bloat from dead tuples (pg_stat_user_tables).
/// For accurate measurement, use pgstattuple extension.
async fn get_table_bloat(client: &Client, limit: usize) -> Result<Vec<TableBloat>> {
    let query = r#"
SELECT
    schemaname AS schema,
    relname AS "table",
    pg_total_relation_size(relid) AS size_bytes,
    CASE WHEN n_live_tup + n_dead_tup > 0
        THEN (n_dead_tup::float8 / (n_live_tup + n_dead_tup) * pg_total_relation_size(relid))::bigint
        ELSE 0
    END AS bloat_bytes,
    CASE WHEN n_live_tup + n_dead_tup > 0
        THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 1)
        ELSE 0
    END::float8 AS bloat_pct
FROM pg_stat_user_tables
WHERE n_dead_tup > 0
  AND pg_total_relation_size(relid) > 65536
ORDER BY bloat_bytes DESC
LIMIT $1
"#;

    let rows = client.query(query, &[&(limit as i64)]).await?;
    let mut results = Vec::with_capacity(rows.len());

    for row in rows {
        let bloat_pct: f64 = row.get("bloat_pct");
        results.push(TableBloat {
            schema: row.get("schema"),
            table: row.get("table"),
            size_bytes: row.get("size_bytes"),
            bloat_bytes: row.get("bloat_bytes"),
            bloat_pct,
            status: BloatStatus::from_pct(bloat_pct),
        });
    }

    Ok(results)
}

pub async fn get_bloat(client: &Client, limit: usize) -> Result<BloatResult> {
    let tables = get_table_bloat(client, limit).await?;
    let indexes = get_index_bloat(client, limit).await?;

    let total_table_bloat: i64 = tables.iter().map(|t| t.bloat_bytes).sum();
    let total_index_bloat: i64 = indexes.iter().map(|i| i.bloat_bytes).sum();

    let worst_table = tables.iter().map(|t| &t.status).max_by_key(|s| match s {
        BloatStatus::Healthy => 0,
        BloatStatus::Warning => 1,
        BloatStatus::Critical => 2,
    });

    let worst_index = indexes.iter().map(|i| &i.status).max_by_key(|s| match s {
        BloatStatus::Healthy => 0,
        BloatStatus::Warning => 1,
        BloatStatus::Critical => 2,
    });

    let overall_status = match (worst_table, worst_index) {
        (Some(t), Some(i)) => {
            if matches!(t, BloatStatus::Critical) || matches!(i, BloatStatus::Critical) {
                BloatStatus::Critical
            } else if matches!(t, BloatStatus::Warning) || matches!(i, BloatStatus::Warning) {
                BloatStatus::Warning
            } else {
                BloatStatus::Healthy
            }
        }
        (Some(s), None) | (None, Some(s)) => *s,
        (None, None) => BloatStatus::Healthy,
    };

    Ok(BloatResult {
        tables,
        indexes,
        total_table_bloat_bytes: total_table_bloat,
        total_index_bloat_bytes: total_index_bloat,
        overall_status,
    })
}

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

pub fn print_human(result: &BloatResult, quiet: bool) {
    if result.tables.is_empty() && result.indexes.is_empty() {
        if !quiet {
            println!("No significant bloat detected.");
        }
        return;
    }

    let total_bloat = result.total_table_bloat_bytes + result.total_index_bloat_bytes;
    println!(
        "BLOAT SUMMARY: {} estimated reclaimable",
        format_bytes(total_bloat)
    );
    println!();

    // Tables
    if !result.tables.is_empty() {
        println!(
            "TABLES ({} reclaimable):",
            format_bytes(result.total_table_bloat_bytes)
        );
        println!(
            "  {:3} {:40} {:>10} {:>10} {:>6}",
            "", "TABLE", "SIZE", "BLOAT", "%"
        );
        println!("  {}", "-".repeat(74));

        for t in &result.tables {
            let name = format!("{}.{}", t.schema, t.table);
            let display_name = if name.chars().count() > 40 {
                let truncated: String = name.chars().take(37).collect();
                format!("{}...", truncated)
            } else {
                name
            };
            println!(
                "  {} {:40} {:>10} {:>10} {:>5.1}%",
                t.status.emoji(),
                display_name,
                format_bytes(t.size_bytes),
                format_bytes(t.bloat_bytes),
                t.bloat_pct
            );
        }
        println!();
    }

    // Indexes
    if !result.indexes.is_empty() {
        println!(
            "INDEXES ({} reclaimable):",
            format_bytes(result.total_index_bloat_bytes)
        );
        println!(
            "  {:3} {:40} {:>10} {:>10} {:>6}",
            "", "INDEX", "SIZE", "BLOAT", "%"
        );
        println!("  {}", "-".repeat(74));

        for i in &result.indexes {
            let name = format!("{}.{}", i.schema, i.index);
            let display_name = if name.chars().count() > 40 {
                let truncated: String = name.chars().take(37).collect();
                format!("{}...", truncated)
            } else {
                name
            };
            println!(
                "  {} {:40} {:>10} {:>10} {:>5.1}%",
                i.status.emoji(),
                display_name,
                format_bytes(i.size_bytes),
                format_bytes(i.bloat_bytes),
                i.bloat_pct
            );
        }
    }

    // Recommendations
    let critical_tables: Vec<_> = result
        .tables
        .iter()
        .filter(|t| t.status == BloatStatus::Critical)
        .collect();
    let critical_indexes: Vec<_> = result
        .indexes
        .iter()
        .filter(|i| i.status == BloatStatus::Critical)
        .collect();

    if !critical_tables.is_empty() || !critical_indexes.is_empty() {
        println!();
        println!("RECOMMENDATIONS:");
        for t in critical_tables.iter().take(3) {
            println!("  VACUUM FULL {}.{};", t.schema, t.table);
        }
        for i in critical_indexes.iter().take(3) {
            println!("  REINDEX INDEX {}.{};", i.schema, i.index);
        }
    }
}

pub fn print_json(
    result: &BloatResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        BloatStatus::Healthy => Severity::Healthy,
        BloatStatus::Warning => Severity::Warning,
        BloatStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::BLOAT, result, severity, t),
        None => DiagnosticOutput::new(schema::BLOAT, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_thresholds() {
        assert_eq!(BloatStatus::from_pct(10.0), BloatStatus::Healthy);
        assert_eq!(BloatStatus::from_pct(20.0), BloatStatus::Warning);
        assert_eq!(BloatStatus::from_pct(50.0), BloatStatus::Critical);
        assert_eq!(BloatStatus::from_pct(75.0), BloatStatus::Critical);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1_500_000), "1.4 MB");
        assert_eq!(format_bytes(2_000_000_000), "1.9 GB");
    }
}
