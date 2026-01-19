//! Indexes command: Identify missing, unused, and duplicate indexes.
//!
//! Indexes are critical for query performance but come with costs:
//! - Missing indexes cause slow sequential scans
//! - Unused indexes waste space and slow writes
//! - Duplicate indexes provide no benefit over their counterparts

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

/// Thresholds for index recommendations
const MIN_SEQ_SCANS_FOR_MISSING: i64 = 1000;
const MIN_TABLE_SIZE_BYTES: i64 = 10 * 1024 * 1024; // 10MB

/// A table that may need an index
#[derive(Debug, Clone, Serialize)]
pub struct MissingIndexCandidate {
    pub schema: String,
    pub table: String,
    pub seq_scan: i64,
    pub seq_tup_read: i64,
    pub idx_scan: i64,
    pub table_size: String,
    pub table_size_bytes: i64,
    /// Ratio of sequential to index scans (higher = worse)
    pub scan_ratio: f64,
}

/// An index that hasn't been used
#[derive(Debug, Clone, Serialize)]
pub struct UnusedIndex {
    pub schema: String,
    pub table: String,
    pub index: String,
    pub index_size: String,
    pub index_size_bytes: i64,
    pub idx_scan: i64,
    /// Whether this is a unique constraint (keep for data integrity)
    pub is_unique: bool,
    /// Whether this is a primary key (keep for data integrity)
    pub is_primary: bool,
}

/// Indexes that cover the same columns
#[derive(Debug, Clone, Serialize)]
pub struct DuplicateIndexSet {
    pub schema: String,
    pub table: String,
    pub columns: String,
    pub indexes: Vec<DuplicateIndexInfo>,
    pub wasted_bytes: i64,
    pub wasted_size: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DuplicateIndexInfo {
    pub name: String,
    pub size: String,
    pub size_bytes: i64,
    pub is_unique: bool,
    pub is_primary: bool,
    pub idx_scan: i64,
}

/// Full index analysis results
#[derive(Debug, Serialize)]
pub struct IndexesResult {
    pub missing: Vec<MissingIndexCandidate>,
    pub unused: Vec<UnusedIndex>,
    pub duplicates: Vec<DuplicateIndexSet>,
    pub total_unused_bytes: i64,
    pub total_unused_size: String,
    pub total_duplicate_bytes: i64,
    pub total_duplicate_size: String,
}

/// Get tables that may benefit from indexes
pub async fn get_missing_index_candidates(
    client: &Client,
    limit: usize,
) -> Result<Vec<MissingIndexCandidate>> {
    // Find tables with high sequential scans relative to index scans
    let query = r#"
        SELECT
            schemaname,
            relname,
            seq_scan,
            seq_tup_read,
            COALESCE(idx_scan, 0) as idx_scan,
            pg_size_pretty(pg_total_relation_size(relid)) as table_size,
            pg_total_relation_size(relid) as table_size_bytes,
            CASE
                WHEN COALESCE(idx_scan, 0) = 0 THEN seq_scan::float
                ELSE seq_scan::float / idx_scan
            END as scan_ratio
        FROM pg_stat_user_tables
        WHERE seq_scan > $1
          AND pg_total_relation_size(relid) > $2
        ORDER BY seq_tup_read DESC
        LIMIT $3
    "#;

    let rows = client
        .query(
            query,
            &[
                &MIN_SEQ_SCANS_FOR_MISSING,
                &MIN_TABLE_SIZE_BYTES,
                &(limit as i64),
            ],
        )
        .await?;

    let mut results = Vec::new();
    for row in rows {
        results.push(MissingIndexCandidate {
            schema: row.get("schemaname"),
            table: row.get("relname"),
            seq_scan: row.get("seq_scan"),
            seq_tup_read: row.get("seq_tup_read"),
            idx_scan: row.get("idx_scan"),
            table_size: row.get("table_size"),
            table_size_bytes: row.get("table_size_bytes"),
            scan_ratio: row.get("scan_ratio"),
        });
    }

    Ok(results)
}

/// Get indexes that haven't been used since stats reset
pub async fn get_unused_indexes(client: &Client, limit: usize) -> Result<Vec<UnusedIndex>> {
    let query = r#"
        SELECT
            s.schemaname,
            s.relname as tablename,
            s.indexrelname as indexname,
            pg_size_pretty(pg_relation_size(s.indexrelid)) as index_size,
            pg_relation_size(s.indexrelid) as index_size_bytes,
            s.idx_scan,
            i.indisunique as is_unique,
            i.indisprimary as is_primary
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        WHERE s.idx_scan = 0
          AND pg_relation_size(s.indexrelid) > 0
        ORDER BY pg_relation_size(s.indexrelid) DESC
        LIMIT $1
    "#;

    let rows = client.query(query, &[&(limit as i64)]).await?;

    let mut results = Vec::new();
    for row in rows {
        results.push(UnusedIndex {
            schema: row.get("schemaname"),
            table: row.get("tablename"),
            index: row.get("indexname"),
            index_size: row.get("index_size"),
            index_size_bytes: row.get("index_size_bytes"),
            idx_scan: row.get("idx_scan"),
            is_unique: row.get("is_unique"),
            is_primary: row.get("is_primary"),
        });
    }

    Ok(results)
}

/// Get duplicate indexes (same columns on same table)
pub async fn get_duplicate_indexes(client: &Client) -> Result<Vec<DuplicateIndexSet>> {
    // Find indexes with identical column sets
    let query = r#"
        WITH index_cols AS (
            SELECT
                n.nspname as schema_name,
                t.relname as table_name,
                i.relname as index_name,
                ix.indisunique,
                ix.indisprimary,
                pg_relation_size(i.oid) as index_size,
                pg_size_pretty(pg_relation_size(i.oid)) as index_size_pretty,
                COALESCE(s.idx_scan, 0) as idx_scan,
                array_to_string(
                    array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)),
                    ', '
                ) as columns
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = ix.indexrelid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT ix.indisexclusion
              AND NOT (0 = ANY(ix.indkey))  -- exclude expression indexes
              AND ix.indpred IS NULL        -- exclude partial indexes
            GROUP BY n.nspname, t.relname, i.relname, ix.indisunique, ix.indisprimary,
                     i.oid, s.idx_scan
        ),
        duplicates AS (
            SELECT
                schema_name,
                table_name,
                columns,
                count(*) as num_indexes
            FROM index_cols
            GROUP BY schema_name, table_name, columns
            HAVING count(*) > 1
        )
        SELECT
            ic.schema_name,
            ic.table_name,
            ic.columns,
            ic.index_name,
            ic.index_size,
            ic.index_size_pretty,
            ic.indisunique,
            ic.indisprimary,
            ic.idx_scan
        FROM index_cols ic
        JOIN duplicates d ON ic.schema_name = d.schema_name
                         AND ic.table_name = d.table_name
                         AND ic.columns = d.columns
        ORDER BY ic.schema_name, ic.table_name, ic.columns, ic.index_size DESC
    "#;

    let rows = client.query(query, &[]).await?;

    // Group by schema/table/columns
    let mut sets: std::collections::HashMap<(String, String, String), Vec<DuplicateIndexInfo>> =
        std::collections::HashMap::new();

    for row in rows {
        let key = (
            row.get::<_, String>("schema_name"),
            row.get::<_, String>("table_name"),
            row.get::<_, String>("columns"),
        );

        let info = DuplicateIndexInfo {
            name: row.get("index_name"),
            size: row.get("index_size_pretty"),
            size_bytes: row.get("index_size"),
            is_unique: row.get("indisunique"),
            is_primary: row.get("indisprimary"),
            idx_scan: row.get("idx_scan"),
        };

        sets.entry(key).or_default().push(info);
    }

    let mut results: Vec<DuplicateIndexSet> = sets
        .into_iter()
        .map(|((schema, table, columns), indexes)| {
            // Wasted = total size minus largest (keep the biggest one)
            let total_size: i64 = indexes.iter().map(|i| i.size_bytes).sum();
            let max_size = indexes.iter().map(|i| i.size_bytes).max().unwrap_or(0);
            let wasted = total_size - max_size;

            DuplicateIndexSet {
                schema,
                table,
                columns,
                indexes,
                wasted_bytes: wasted,
                wasted_size: format_bytes(wasted),
            }
        })
        .collect();

    // Sort by wasted space descending
    results.sort_by(|a, b| b.wasted_bytes.cmp(&a.wasted_bytes));

    Ok(results)
}

/// Run full index analysis
pub async fn run_indexes(
    client: &Client,
    missing_limit: usize,
    unused_limit: usize,
) -> Result<IndexesResult> {
    let missing = get_missing_index_candidates(client, missing_limit).await?;
    let unused = get_unused_indexes(client, unused_limit).await?;
    let duplicates = get_duplicate_indexes(client).await?;

    let total_unused_bytes: i64 = unused.iter().map(|u| u.index_size_bytes).sum();
    let total_duplicate_bytes: i64 = duplicates.iter().map(|d| d.wasted_bytes).sum();

    Ok(IndexesResult {
        missing,
        unused,
        duplicates,
        total_unused_bytes,
        total_unused_size: format_bytes(total_unused_bytes),
        total_duplicate_bytes,
        total_duplicate_size: format_bytes(total_duplicate_bytes),
    })
}

/// Format bytes for display
fn format_bytes(bytes: i64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Format large numbers
fn format_number(n: i64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

/// Print index analysis in human-readable format
pub fn print_human(result: &IndexesResult, verbose: bool) {
    let mut has_output = false;

    // Missing indexes
    if !result.missing.is_empty() {
        has_output = true;
        println!("MISSING INDEX CANDIDATES:");
        println!();
        println!(
            "  {:40} {:>10} {:>12} {:>10}",
            "TABLE", "SEQ SCANS", "ROWS READ", "SIZE"
        );
        println!("  {}", "-".repeat(76));

        for m in &result.missing {
            let full_name = format!("{}.{}", m.schema, m.table);
            println!(
                "  {:40} {:>10} {:>12} {:>10}",
                if full_name.len() > 40 {
                    format!("{}...", &full_name[..37])
                } else {
                    full_name
                },
                format_number(m.seq_scan),
                format_number(m.seq_tup_read),
                m.table_size
            );

            if verbose {
                println!(
                    "    idx_scan: {}  ratio: {:.1}x",
                    format_number(m.idx_scan),
                    m.scan_ratio
                );
            }
        }
        println!();
    }

    // Unused indexes
    if !result.unused.is_empty() {
        has_output = true;
        println!("UNUSED INDEXES:");
        println!();
        println!("  {:40} {:>12} {:>8}", "INDEX", "SIZE", "KEEP?");
        println!("  {}", "-".repeat(64));

        for u in &result.unused {
            let full_name = format!("{}.{}", u.schema, u.index);
            let keep = if u.is_primary {
                "PK"
            } else if u.is_unique {
                "UNIQ"
            } else {
                ""
            };

            println!(
                "  {:40} {:>12} {:>8}",
                if full_name.len() > 40 {
                    format!("{}...", &full_name[..37])
                } else {
                    full_name
                },
                u.index_size,
                keep
            );

            if verbose {
                println!("    table: {}.{}", u.schema, u.table);
            }
        }

        let droppable: Vec<_> = result
            .unused
            .iter()
            .filter(|u| !u.is_primary && !u.is_unique)
            .collect();

        if !droppable.is_empty() {
            let droppable_bytes: i64 = droppable.iter().map(|u| u.index_size_bytes).sum();
            println!();
            println!(
                "  {} unused indexes can be dropped ({} reclaimable)",
                droppable.len(),
                format_bytes(droppable_bytes)
            );
        }
        println!();
    }

    // Duplicate indexes
    if !result.duplicates.is_empty() {
        has_output = true;
        println!("DUPLICATE INDEXES:");
        println!();

        for dup in &result.duplicates {
            println!("  {}.{} ({})", dup.schema, dup.table, dup.columns);

            for idx in &dup.indexes {
                let marker = if idx.is_primary {
                    " [PK]"
                } else if idx.is_unique {
                    " [UNIQ]"
                } else {
                    ""
                };
                println!(
                    "    {} {:>10} {:>8} scans{}",
                    idx.name,
                    idx.size,
                    format_number(idx.idx_scan),
                    marker
                );
            }
            println!("    wasted: {}", dup.wasted_size);
            println!();
        }
    }

    // Summary
    if has_output {
        println!("SUMMARY:");
        println!();
        if !result.unused.is_empty() {
            println!(
                "  Unused indexes:    {} ({} total)",
                result.unused.len(),
                result.total_unused_size
            );
        }
        if !result.duplicates.is_empty() {
            println!(
                "  Duplicate sets:    {} ({} wasted)",
                result.duplicates.len(),
                result.total_duplicate_size
            );
        }
        if !result.missing.is_empty() {
            println!("  Missing candidates: {}", result.missing.len());
        }
    } else {
        println!("No index issues found.");
    }

    // Actions
    let droppable_unused: Vec<_> = result
        .unused
        .iter()
        .filter(|u| !u.is_primary && !u.is_unique)
        .collect();

    if !droppable_unused.is_empty() || !result.duplicates.is_empty() {
        println!();
        println!("RECOMMENDED ACTIONS:");
        println!();

        // Collect indexes to drop as duplicates (so we can dedupe against unused)
        let mut duplicate_drops: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        for dup in &result.duplicates {
            let keeper = dup.indexes.iter().max_by_key(|i| {
                let priority = if i.is_primary {
                    2
                } else if i.is_unique {
                    1
                } else {
                    0
                };
                (priority, i.idx_scan)
            });
            if let Some(keep) = keeper {
                for idx in &dup.indexes {
                    if idx.name != keep.name && !idx.is_primary {
                        duplicate_drops.insert((dup.schema.clone(), idx.name.clone()));
                    }
                }
            }
        }

        // Show drop commands for unused indexes (excluding those already in duplicates)
        for u in droppable_unused.iter().take(5) {
            if !duplicate_drops.contains(&(u.schema.clone(), u.index.clone())) {
                println!(
                    "  DROP INDEX {}.{};  -- {} unused",
                    u.schema, u.index, u.index_size
                );
            }
        }

        // Show drop commands for duplicates (keep the one with most scans or PK/unique)
        for dup in result.duplicates.iter().take(3) {
            let keeper = dup.indexes.iter().max_by_key(|i| {
                let priority = if i.is_primary {
                    2
                } else if i.is_unique {
                    1
                } else {
                    0
                };
                (priority, i.idx_scan)
            });

            if let Some(keep) = keeper {
                for idx in &dup.indexes {
                    if idx.name != keep.name && !idx.is_primary {
                        println!(
                            "  DROP INDEX {}.{};  -- duplicate of {}",
                            dup.schema, idx.name, keep.name
                        );
                    }
                }
            }
        }

        println!();
        println!("  Note: Verify indexes aren't needed for specific queries before dropping.");
        println!("  Check pg_stat_statements for query patterns if available.");
    }
}

/// Print index analysis as JSON with schema versioning.
pub fn print_json(
    result: &IndexesResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Derive severity from findings
    // Missing indexes with high seq scans are concerning
    // Large amounts of wasted space from unused/duplicates also warrant attention
    let severity = if result
        .missing
        .iter()
        .any(|m| m.seq_scan > 10000 && m.scan_ratio > 100.0)
    {
        // High sequential scans with very poor index coverage
        Severity::Warning
    } else if result.total_unused_bytes > 1_000_000_000
        || result.total_duplicate_bytes > 500_000_000
    {
        // Over 1GB wasted on unused indexes or 500MB on duplicates
        Severity::Warning
    } else if !result.missing.is_empty()
        || !result.unused.is_empty()
        || !result.duplicates.is_empty()
    {
        // Some findings - report as warning so automation knows there's something to review
        Severity::Warning
    } else {
        Severity::Healthy
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::INDEXES, result, severity, t),
        None => DiagnosticOutput::new(schema::INDEXES, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes_gb() {
        assert_eq!(format_bytes(2_147_483_648), "2.0 GB");
    }

    #[test]
    fn test_format_bytes_mb() {
        assert_eq!(format_bytes(52_428_800), "50.0 MB");
    }

    #[test]
    fn test_format_bytes_kb() {
        assert_eq!(format_bytes(10_240), "10.0 KB");
    }

    #[test]
    fn test_format_bytes_small() {
        assert_eq!(format_bytes(512), "512 bytes");
    }

    #[test]
    fn test_format_number_billions() {
        assert_eq!(format_number(1_500_000_000), "1.5B");
    }

    #[test]
    fn test_format_number_millions() {
        assert_eq!(format_number(2_500_000), "2.5M");
    }

    #[test]
    fn test_format_number_thousands() {
        assert_eq!(format_number(5_500), "5.5K");
    }

    #[test]
    fn test_format_number_small() {
        assert_eq!(format_number(42), "42");
    }
}
