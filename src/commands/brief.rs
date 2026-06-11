//! Brief command: the whole database in one dense, sub-second screen.
//!
//! This is the command an agent runs first, every session. It answers the
//! orientation questions that otherwise cost three or four manual pg_catalog
//! round-trips: which instance am I on, which schemas exist, what tables with
//! how many rows, what's hiding outside `public`, what relates to what, is the
//! database migrated, and are there any glance-level hazards.
//!
//! Everything here is catalog/statistics only — `reltuples` estimates, never
//! `count(*)`, no table scans, no `pg_stat_statements` dependency. The target
//! is sub-second on a typical database.
//!
//! Health flags are *displayed*, not scored: brief is orientation, not triage.
//! The exit code stays 0 for any finding; severity lives in `dba triage`.

use anyhow::Result;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use tokio_postgres::Client;

use crate::commands::context::{
    get_extensions, get_server_info, get_target_info, ServerInfo, TargetInfo,
};
use crate::config::Config;
use crate::migrations::discover_migrations;
use crate::output::Density;

/// Sequence usage warning threshold (percent) for the at-a-glance health line.
const SEQ_WARN_PCT: f64 = 75.0;
/// XID age at which wraparound becomes worth flagging at a glance.
const XID_WARN_AGE: i64 = 1_500_000_000;
/// Per-schema table cap for human output. A schema with hundreds of tables
/// (orphan test fixtures, partition children) would bury the headline; brief
/// shows the largest `SCHEMA_TABLE_CAP` and folds the rest into one line. JSON
/// is never capped — automation gets the full set.
const SCHEMA_TABLE_CAP: usize = 40;

/// One table's headline shape: estimated rows and total on-disk size.
#[derive(Debug, Clone, Serialize)]
pub struct TableBrief {
    pub schema: String,
    pub name: String,
    /// Estimated live rows from `reltuples`. `None` means never analyzed
    /// (`reltuples` is -1 on modern PG) — rendered as `?`, never as -1.
    pub est_rows: Option<i64>,
    pub size: String,
    pub size_bytes: i64,
}

/// A schema and the tables it holds, in the order brief prints them.
#[derive(Debug, Clone, Serialize)]
pub struct SchemaBrief {
    pub name: String,
    pub tables: Vec<TableBrief>,
}

/// A compact foreign-key edge: `child` references one or more `parents`.
#[derive(Debug, Clone, Serialize)]
pub struct RelationshipBrief {
    pub child: String,
    pub parents: Vec<String>,
}

/// Migration state, present only when a migrations directory resolves on disk.
#[derive(Debug, Clone, Serialize)]
pub struct MigrationsBrief {
    pub applied: usize,
    pub pending: usize,
    /// Pending versions, listed only when the pending set is small (<= 5).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pending_versions: Vec<String>,
}

/// A glance-level health note. Cheap catalog reads only; displayed, not scored.
#[derive(Debug, Clone, Serialize)]
pub struct HealthFlag {
    pub kind: String,
    pub detail: String,
}

/// Everything `brief` knows about the target, ready to render or serialize.
#[derive(Debug, Serialize)]
pub struct BriefResult {
    pub target: TargetInfo,
    pub server: ServerInfo,
    pub schemas: Vec<SchemaBrief>,
    pub relationships: Vec<RelationshipBrief>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrations: Option<MigrationsBrief>,
    /// Installed extensions, sorted by name (name=version pairs).
    pub extensions: Vec<(String, String)>,
    pub health: Vec<HealthFlag>,
}

/// Query schemas and their tables in one shot, grouped and ordered.
///
/// Excludes system schemas (`pg_*`, `information_schema`) and pgcrate's own
/// bookkeeping schema (`pgcrate`) — the latter is covered by the migrations
/// line and is pure noise in the headline. `reltuples` of -1 (never analyzed)
/// becomes `None` so it renders as `?` rather than a misleading -1.
async fn get_schemas(client: &Client) -> Result<Vec<SchemaBrief>> {
    let rows = client
        .query(
            r#"
            SELECT
                n.nspname                              AS schema,
                c.relname                              AS name,
                c.reltuples::bigint                    AS est_rows,
                pg_total_relation_size(c.oid)          AS size_bytes,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS size
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname NOT LIKE 'pg_%'
              AND n.nspname <> 'information_schema'
              AND n.nspname <> 'pgcrate'
            ORDER BY n.nspname,
                     pg_total_relation_size(c.oid) DESC,
                     c.relname
            "#,
            &[],
        )
        .await?;

    // Preserve insertion order per schema; BTreeMap keeps schemas sorted, and
    // the query already orders tables within each schema.
    let mut grouped: BTreeMap<String, Vec<TableBrief>> = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema");
        let est: i64 = row.get("est_rows");
        grouped.entry(schema.clone()).or_default().push(TableBrief {
            schema,
            name: row.get("name"),
            est_rows: if est < 0 { None } else { Some(est) },
            size: row.get("size"),
            size_bytes: row.get("size_bytes"),
        });
    }

    // Include schemas that exist but hold no tables — an empty `solitaire`
    // schema is itself orientation ("the schema is here, it's just empty").
    let schema_rows = client
        .query(
            r#"
            SELECT nspname AS schema
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
              AND nspname <> 'information_schema'
              AND nspname <> 'pgcrate'
            ORDER BY nspname
            "#,
            &[],
        )
        .await?;

    for row in schema_rows {
        let name: String = row.get("schema");
        grouped.entry(name).or_default();
    }

    Ok(grouped
        .into_iter()
        .map(|(name, tables)| SchemaBrief { name, tables })
        .collect())
}

/// Query foreign keys, collapsed to `child → {parents}` edges.
///
/// One row per FK constraint; multiple FKs from the same child collapse into a
/// single edge with a deduplicated, sorted parent set. Self-references are kept
/// (they are real orientation — a tree/graph table).
async fn get_relationships(client: &Client) -> Result<Vec<RelationshipBrief>> {
    let rows = client
        .query(
            r#"
            SELECT
                cn.nspname || '.' || cl.relname AS child,
                fn.nspname || '.' || fl.relname AS parent
            FROM pg_constraint con
            JOIN pg_class cl     ON con.conrelid = cl.oid
            JOIN pg_namespace cn ON cl.relnamespace = cn.oid
            JOIN pg_class fl     ON con.confrelid = fl.oid
            JOIN pg_namespace fn ON fl.relnamespace = fn.oid
            WHERE con.contype = 'f'
              AND cn.nspname NOT LIKE 'pg_%'
              AND cn.nspname <> 'information_schema'
            ORDER BY child, parent
            "#,
            &[],
        )
        .await?;

    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let child: String = row.get("child");
        let parent: String = row.get("parent");
        let parents = grouped.entry(child).or_default();
        if !parents.contains(&parent) {
            parents.push(parent);
        }
    }

    Ok(grouped
        .into_iter()
        .map(|(child, parents)| RelationshipBrief { child, parents })
        .collect())
}

/// Resolve migration state, or `None` when no migrations exist on disk.
///
/// Degrades silently: brief runs against arbitrary databases with zero project
/// context, so a missing `pgcrate.toml` or migrations directory is not an
/// error — it just means there's no migration line to print.
async fn get_migrations(client: &Client, config: &Config) -> Result<Option<MigrationsBrief>> {
    let dir = config.migrations_dir();
    let migrations = discover_migrations(Path::new(dir))?;
    if migrations.is_empty() {
        return Ok(None);
    }

    // Read applied versions without creating the schema_migrations table —
    // brief is read-only and must never mutate the target. A database with no
    // pgcrate schema simply reports zero applied.
    let applied: std::collections::HashSet<String> = client
        .query("SELECT version FROM pgcrate.schema_migrations", &[])
        .await
        .map(|rows| rows.iter().map(|r| r.get::<_, String>("version")).collect())
        .unwrap_or_default();

    let pending: Vec<String> = migrations
        .iter()
        .filter(|m| !applied.contains(&m.version))
        .map(|m| m.version.clone())
        .collect();

    let applied_count = migrations
        .iter()
        .filter(|m| applied.contains(&m.version))
        .count();

    // List pending versions only when the set is small enough to be useful
    // inline; beyond that the count alone is the signal.
    let pending_versions = if pending.len() <= 5 {
        pending.clone()
    } else {
        Vec::new()
    };

    Ok(Some(MigrationsBrief {
        applied: applied_count,
        pending: pending.len(),
        pending_versions,
    }))
}

/// Gather the cheap, at-a-glance health flags. Catalog reads only.
///
/// Sequences past `SEQ_WARN_PCT` of their max, and current-database XID age
/// past `XID_WARN_AGE`. Both are nearly free; anything requiring a table scan
/// or `pg_stat_statements` belongs in `dba triage`, not here.
async fn get_health(client: &Client) -> Result<Vec<HealthFlag>> {
    let mut flags = Vec::new();

    // Sequences approaching exhaustion.
    let seq_rows = client
        .query(
            r#"
            SELECT
                schemaname || '.' || sequencename AS name,
                round(100.0 * last_value / max_value, 1)::float8 AS pct
            FROM pg_sequences
            WHERE last_value IS NOT NULL
              AND max_value > 0
              AND increment_by > 0
              AND (100.0 * last_value / max_value) >= $1
            ORDER BY pct DESC
            "#,
            &[&SEQ_WARN_PCT],
        )
        .await
        .unwrap_or_default();

    for row in seq_rows {
        let name: String = row.get("name");
        let pct: f64 = row.get("pct");
        flags.push(HealthFlag {
            kind: "sequence".to_string(),
            detail: format!("{} at {:.1}% of max", name, pct),
        });
    }

    // Current-database transaction-ID age (wraparound risk).
    if let Ok(row) = client
        .query_one(
            "SELECT age(datfrozenxid)::bigint AS xid_age
             FROM pg_database WHERE datname = current_database()",
            &[],
        )
        .await
    {
        let age: i64 = row.get("xid_age");
        if age >= XID_WARN_AGE {
            let pct = 100.0 * age as f64 / 2_147_483_647.0;
            flags.push(HealthFlag {
                kind: "xid".to_string(),
                detail: format!("database XID age {} ({:.0}% to wraparound)", age, pct),
            });
        }
    }

    Ok(flags)
}

/// Run the full brief: one pass over the catalog, assembled into `BriefResult`.
pub async fn run_brief(
    client: &Client,
    connection_url: &str,
    read_only: bool,
    no_redact: bool,
    config: &Config,
) -> Result<BriefResult> {
    let target = get_target_info(client, connection_url, read_only, no_redact).await?;
    let server = get_server_info(client, no_redact).await?;
    let schemas = get_schemas(client).await?;
    let relationships = get_relationships(client).await?;
    let migrations = get_migrations(client, config).await?;
    let health = get_health(client).await?;

    let mut extensions: Vec<(String, String)> = get_extensions(client).await?.into_iter().collect();
    extensions.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(BriefResult {
        target,
        server,
        schemas,
        relationships,
        migrations,
        extensions,
        health,
    })
}

/// Format an estimated row count for display: `?` for never-analyzed tables,
/// thousands-grouped otherwise (`12,345`).
fn fmt_rows(est: Option<i64>) -> String {
    match est {
        None => "?".to_string(),
        Some(n) => group_thousands(n),
    }
}

/// Group an integer with thousands separators (`1234567` -> `1,234,567`).
fn group_thousands(n: i64) -> String {
    let neg = n < 0;
    let digits = n.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    let bytes = digits.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    if neg {
        format!("-{}", out)
    } else {
        out
    }
}

/// Print the brief in human-readable form (dense or pretty).
pub fn print_human(result: &BriefResult, density: Density) {
    if density.is_dense() {
        print_dense(result);
    } else {
        print_pretty(result);
    }
}

fn mode_str(read_only: bool) -> &'static str {
    if read_only {
        "read-only"
    } else {
        "read-write"
    }
}

fn print_dense(r: &BriefResult) {
    // Target — loud and first. The worst session hazard is being on the wrong
    // instance while everything looks fine, so this line leads.
    println!(
        "TARGET: {}@{}:{}/{} ({}) — pg {} {}",
        r.target.user,
        r.target.host,
        r.target.port,
        r.target.database,
        mode_str(r.target.readonly),
        r.server.version_major,
        if r.server.in_recovery {
            "replica"
        } else {
            "primary"
        },
    );

    // Schemas → tables → estimated rows. The headline section.
    let table_count: usize = r.schemas.iter().map(|s| s.tables.len()).sum();
    println!(
        "SCHEMAS ({}), {} table{}:",
        r.schemas.len(),
        table_count,
        if table_count == 1 { "" } else { "s" }
    );
    for schema in &r.schemas {
        if schema.tables.is_empty() {
            println!("  {} (empty)", schema.name);
            continue;
        }
        println!("  {} ({}):", schema.name, schema.tables.len());
        for t in schema.tables.iter().take(SCHEMA_TABLE_CAP) {
            println!("    {} ~{} rows, {}", t.name, fmt_rows(t.est_rows), t.size);
        }
        if schema.tables.len() > SCHEMA_TABLE_CAP {
            println!(
                "    … and {} more (largest shown; `--json` lists all)",
                schema.tables.len() - SCHEMA_TABLE_CAP
            );
        }
    }

    // Relationships — compact FK summary.
    if r.relationships.is_empty() {
        println!("RELATIONSHIPS: none");
    } else {
        println!("RELATIONSHIPS ({}):", r.relationships.len());
        for rel in &r.relationships {
            println!("  {} → {}", rel.child, rel.parents.join(", "));
        }
    }

    // Migrations — only when configured.
    if let Some(m) = &r.migrations {
        let mut line = format!("MIGRATIONS: applied {} / pending {}", m.applied, m.pending);
        if !m.pending_versions.is_empty() {
            line.push_str(&format!(" ({})", m.pending_versions.join(", ")));
        }
        println!("{}", line);
    }

    // Extensions + version (one line).
    if r.extensions.is_empty() {
        println!("EXTENSIONS (0)");
    } else {
        let list: Vec<String> = r
            .extensions
            .iter()
            .map(|(name, ver)| format!("{}={}", name, ver))
            .collect();
        println!("EXTENSIONS ({}): {}", r.extensions.len(), list.join(" "));
    }
    println!(
        "SERVER: pg {} ({})",
        r.server.version_major, r.server.version_num
    );

    // Health flags — displayed, not scored.
    if r.health.is_empty() {
        println!("HEALTH: no flags");
    } else {
        println!("HEALTH ({}):", r.health.len());
        for flag in &r.health {
            println!("  {}: {}", flag.kind, flag.detail);
        }
    }
}

fn print_pretty(r: &BriefResult) {
    println!("TARGET");
    println!("  Database: {}", r.target.database);
    println!("  Host:     {}:{}", r.target.host, r.target.port);
    println!("  User:     {}", r.target.user);
    println!("  Mode:     {}", mode_str(r.target.readonly));
    println!(
        "  Server:   pg {} ({}) {}",
        r.server.version_major,
        r.server.version_num,
        if r.server.in_recovery {
            "replica"
        } else {
            "primary"
        }
    );

    println!();
    let table_count: usize = r.schemas.iter().map(|s| s.tables.len()).sum();
    println!(
        "SCHEMAS ({}, {} table{})",
        r.schemas.len(),
        table_count,
        if table_count == 1 { "" } else { "s" }
    );
    for schema in &r.schemas {
        if schema.tables.is_empty() {
            println!("  {} (empty)", schema.name);
            continue;
        }
        println!("  {} ({})", schema.name, schema.tables.len());
        for t in schema.tables.iter().take(SCHEMA_TABLE_CAP) {
            println!(
                "    {:30} ~{:>12} rows  {:>10}",
                t.name,
                fmt_rows(t.est_rows),
                t.size
            );
        }
        if schema.tables.len() > SCHEMA_TABLE_CAP {
            println!(
                "    … and {} more (largest shown; `--json` lists all)",
                schema.tables.len() - SCHEMA_TABLE_CAP
            );
        }
    }

    println!();
    if r.relationships.is_empty() {
        println!("RELATIONSHIPS");
        println!("  (none)");
    } else {
        println!("RELATIONSHIPS ({})", r.relationships.len());
        for rel in &r.relationships {
            println!("  {} → {}", rel.child, rel.parents.join(", "));
        }
    }

    if let Some(m) = &r.migrations {
        println!();
        println!("MIGRATIONS");
        println!("  Applied: {}", m.applied);
        println!("  Pending: {}", m.pending);
        if !m.pending_versions.is_empty() {
            for v in &m.pending_versions {
                println!("    · {}", v);
            }
        }
    }

    println!();
    println!("EXTENSIONS ({})", r.extensions.len());
    if r.extensions.is_empty() {
        println!("  (none)");
    } else {
        for (name, ver) in &r.extensions {
            println!("  {} ({})", name, ver);
        }
    }

    println!();
    if r.health.is_empty() {
        println!("HEALTH");
        println!("  No flags. (Run `pgcrate dba triage` for a full health pass.)");
    } else {
        println!("HEALTH ({})", r.health.len());
        for flag in &r.health {
            println!("  {}: {}", flag.kind, flag.detail);
        }
    }
}

/// Print the brief as versioned JSON.
///
/// Schema id `pgcrate.brief`. Severity is always `Healthy` — brief reports
/// findings without scoring them, so health flags never change the envelope's
/// severity or the exit code.
pub fn print_json(
    result: &BriefResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{DiagnosticOutput, Severity};

    const SCHEMA_ID: &str = "pgcrate.brief";

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(SCHEMA_ID, result, Severity::Healthy, t),
        None => DiagnosticOutput::new(SCHEMA_ID, result, Severity::Healthy),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_rows_never_analyzed_is_question_mark() {
        assert_eq!(fmt_rows(None), "?");
    }

    #[test]
    fn fmt_rows_groups_thousands() {
        assert_eq!(fmt_rows(Some(0)), "0");
        assert_eq!(fmt_rows(Some(42)), "42");
        assert_eq!(fmt_rows(Some(1_000)), "1,000");
        assert_eq!(fmt_rows(Some(1_234_567)), "1,234,567");
    }

    #[test]
    fn group_thousands_handles_boundaries() {
        assert_eq!(group_thousands(0), "0");
        assert_eq!(group_thousands(999), "999");
        assert_eq!(group_thousands(1_000), "1,000");
        assert_eq!(group_thousands(10_000), "10,000");
        assert_eq!(group_thousands(100_000), "100,000");
        assert_eq!(group_thousands(1_000_000), "1,000,000");
    }
}
