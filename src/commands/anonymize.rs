//! Anonymize commands for pgcrate CLI.

use crate::config::{url_matches_production_patterns, Config};
use crate::sql::quote_ident;
use anyhow::{bail, Result};
use colored::Colorize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Write};
use tokio_postgres::Client;

use super::connect;

/// Setup anonymization: install functions
pub async fn anonymize_setup(
    database_url: &str,
    quiet: bool,
    verbose: bool,
) -> Result<(), anyhow::Error> {
    use crate::anonymize::ALL_FUNCTION_SQL;

    let client = connect(database_url).await?;

    // Ensure pgcrate schema exists
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS pgcrate")
        .await?;

    if !quiet {
        println!("Setting up anonymize...");
    }

    // Ensure pgcrypto extension exists (required for sha256)
    if verbose && !quiet {
        println!("  Ensuring pgcrypto extension");
    }
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        .await?;

    // Install functions
    for sql in ALL_FUNCTION_SQL {
        if verbose && !quiet {
            // Extract function name from CREATE FUNCTION statement
            let func_name = sql
                .lines()
                .find(|l| l.contains("CREATE OR REPLACE FUNCTION"))
                .and_then(|l| l.split_whitespace().nth(4))
                .unwrap_or("unknown");
            println!("  Installing function {}", func_name);
        }
        client.batch_execute(sql).await?;
    }

    if !quiet && !verbose {
        println!(
            "  Ensuring pgcrypto and {} functions",
            ALL_FUNCTION_SQL.len()
        );
    }

    if !quiet {
        println!();
        println!("{}", "Setup complete.".green());
        println!();
        println!("Next: configure your rules in pgcrate.anonymize.toml");
    }

    Ok(())
}

/// Dump anonymized data
#[allow(clippy::too_many_arguments)]
pub async fn anonymize_dump(
    database_url: &str,
    config: &Config,
    anonymize_config_path: Option<&std::path::Path>,
    seed_override: Option<&str>,
    output: Option<&std::path::Path>,
    dry_run: bool,
    quiet: bool,
    _verbose: bool,
) -> Result<(), anyhow::Error> {
    use crate::anonymize::{get_skipped_tables, AnonymizeRule};
    use crate::config::AnonymizeConfig;

    let client = connect(database_url).await?;

    // Check if anonymize is set up
    check_anonymize_setup(&client).await?;

    // Load config
    let anon_config = AnonymizeConfig::load(anonymize_config_path)?;

    // Resolve seed: CLI > Env > File
    let seed = seed_override
        .map(|s| s.to_string())
        .or_else(|| std::env::var("PGCRATE_ANONYMIZE_SEED").ok())
        .or_else(|| anon_config.seed.clone())
        .ok_or_else(|| {
            anyhow::anyhow!("No anonymization seed provided. Use --seed flag, PGCRATE_ANONYMIZE_SEED env var, or 'seed' in pgcrate.anonymize.toml")
        })?;

    // Convert config rules to anonymize::AnonymizeRule
    let mut rules = Vec::new();
    for rule in anon_config.rules {
        let (schema, table) = crate::anonymize::parse_table_name(&rule.table);
        if rule.skip {
            rules.push(AnonymizeRule::skip_table(&schema, &table));
        } else if let Some(columns) = rule.columns {
            for (col, strategy) in columns {
                crate::anonymize::validate_strategy(&strategy)?;
                rules.push(AnonymizeRule::column(&schema, &table, &col, &strategy));
            }
        }
    }

    // Warn about production patterns
    if url_matches_production_patterns(database_url, config) && !quiet {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns.".yellow()
        );
    }

    let skipped_tables = get_skipped_tables(&rules);

    // Get all tables to process (excluding system schemas and skipped tables)
    let tables = get_tables_for_dump(&client, &skipped_tables).await?;

    if dry_run {
        // Dry run mode - show what would happen
        print_dry_run_preview(&client, &tables, &rules, &seed, &skipped_tables, quiet).await?;
        return Ok(());
    }

    // Dump to file or stdout
    let is_stdout = output.is_none() || output == Some(std::path::Path::new("-"));
    let mut writer: Box<dyn Write> = if is_stdout {
        Box::new(io::stdout())
    } else {
        Box::new(std::fs::File::create(output.unwrap())?)
    };

    execute_anonymize_dump(&client, &tables, &rules, &seed, &mut *writer, quiet).await?;

    if !quiet && !is_stdout {
        println!();
        println!(
            "{}",
            format!("Anonymized dump saved: {}", output.unwrap().display()).green()
        );
    }

    Ok(())
}

/// Check if anonymize setup has been run (functions exist)
async fn check_anonymize_setup(client: &Client) -> Result<(), anyhow::Error> {
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
             WHERE n.nspname = 'pgcrate' AND p.proname = 'anon_fake_email'",
            &[],
        )
        .await?;

    if exists.is_none() {
        bail!("Anonymize functions not found.\nHint: Run `pgcrate anonymize setup` first.");
    }

    Ok(())
}

/// Get tables to dump, excluding system schemas and skipped tables
pub async fn get_tables_for_dump(
    client: &Client,
    skipped_tables: &HashSet<String>,
) -> Result<Vec<TableInfo>, anyhow::Error> {
    use crate::anonymize::is_excluded_schema;

    let rows = client
        .query(
            "SELECT table_schema, table_name
             FROM information_schema.tables
             WHERE table_type = 'BASE TABLE'
             ORDER BY table_schema, table_name",
            &[],
        )
        .await?;

    let mut tables = Vec::new();
    for row in rows {
        let schema: String = row.get("table_schema");
        let table: String = row.get("table_name");
        let qualified = format!("{}.{}", schema, table);

        if is_excluded_schema(&schema) {
            continue;
        }
        if skipped_tables.contains(&qualified) {
            continue;
        }

        tables.push(TableInfo {
            schema,
            name: table,
        });
    }

    Ok(tables)
}

#[derive(Debug)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
}

impl TableInfo {
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// Get columns for a table
pub async fn get_table_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get("column_name")).collect())
}

/// Get row count for a table
pub async fn get_row_count(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<i64, anyhow::Error> {
    let row = client
        .query_one(
            &format!(
                "SELECT COUNT(*) as count FROM {}.{}",
                quote_ident(schema),
                quote_ident(table)
            ),
            &[],
        )
        .await?;

    Ok(row.get("count"))
}

/// Print dry run preview
async fn print_dry_run_preview(
    client: &Client,
    tables: &[TableInfo],
    rules: &[crate::anonymize::AnonymizeRule],
    seed: &str,
    skipped_tables: &HashSet<String>,
    quiet: bool,
) -> Result<(), anyhow::Error> {
    if quiet {
        return Ok(());
    }

    println!("Anonymization Preview");
    println!("{}", "─".repeat(53));
    println!();

    // Show seed (masked)
    let masked_seed = if seed.len() > 10 {
        format!("{}... (hidden)", &seed[..8])
    } else {
        "*** (hidden)".to_string()
    };
    println!("Config:");
    println!("  Seed: {}", masked_seed);
    println!();

    // Build rule map for quick lookup
    let rule_map: HashMap<String, &crate::anonymize::AnonymizeRule> = rules
        .iter()
        .filter(|r| r.column_name.is_some())
        .map(|r| {
            (
                format!(
                    "{}.{}.{}",
                    r.table_schema,
                    r.table_name,
                    r.column_name.as_ref().unwrap()
                ),
                r,
            )
        })
        .collect();

    let mut total_rows: i64 = 0;
    let mut preserved_columns: Vec<String> = Vec::new();

    for table in tables {
        let row_count = get_row_count(client, &table.schema, &table.name).await?;
        total_rows += row_count;
        let columns = get_table_columns(client, &table.schema, &table.name).await?;

        println!(
            "Table: {} ({} rows)",
            table.qualified(),
            format_number(row_count)
        );

        for col in &columns {
            let key = format!("{}.{}.{}", table.schema, table.name, col);
            let strategy = rule_map
                .get(&key)
                .map(|r| r.strategy.as_str())
                .unwrap_or("preserve");

            if strategy == "preserve" {
                preserved_columns.push(key.clone());
            }

            println!("  {:<16} {}", col, strategy);
        }
        println!();
    }

    // Show skipped tables
    if !skipped_tables.is_empty() {
        println!("Skipped tables:");
        for t in skipped_tables {
            println!("  - {}", t);
        }
        println!();
    }

    println!(
        "Summary: {} tables ({} rows), {} skipped",
        tables.len(),
        format_number(total_rows),
        skipped_tables.len()
    );

    // Warn about preserved columns (potential PII leak)
    if !preserved_columns.is_empty() {
        println!();
        eprintln!(
            "{}",
            format!(
                "⚠️  WARNING: {} columns have no rules and will output REAL DATA.",
                preserved_columns.len()
            )
            .yellow()
        );
        eprintln!(
            "{}",
            "   Review the 'preserve' columns above. Add rules for any PII columns.".yellow()
        );
    }

    Ok(())
}

/// Format a number with commas
pub fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}

/// Internal execution engine for anonymization dump
pub async fn execute_anonymize_dump(
    client: &Client,
    tables: &[TableInfo],
    rules: &[crate::anonymize::AnonymizeRule],
    seed: &str,
    writer: &mut dyn Write,
    quiet: bool,
) -> Result<(), anyhow::Error> {
    use crate::anonymize::build_anonymized_select;
    use futures_util::StreamExt;

    // Sort tables by FK dependency order
    let ordered_tables = order_tables_by_fk(client, tables).await?;

    // Write header
    writeln!(writer, "-- pgcrate anonymized dump")?;
    writeln!(writer, "-- Generated: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(writer, "-- pgcrate version: {}", env!("CARGO_PKG_VERSION"))?;
    writeln!(writer, "--")?;
    writeln!(writer)?;

    if !quiet {
        eprintln!("Dumping anonymized data...");
    }

    for table in &ordered_tables {
        let columns = get_table_columns(client, &table.schema, &table.name).await?;
        let row_count = get_row_count(client, &table.schema, &table.name).await?;

        if !quiet {
            eprint!(
                "  Processing {} ({} rows)...",
                table.qualified(),
                format_number(row_count)
            );
            io::stderr().flush()?;
        }

        // Build the SELECT query
        let select_sql = build_anonymized_select(&table.schema, &table.name, &columns, rules, seed);

        // Write COPY header
        let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        writeln!(
            writer,
            "COPY {}.{} ({}) FROM stdin;",
            quote_ident(&table.schema),
            quote_ident(&table.name),
            col_list.join(", ")
        )?;

        // Execute COPY TO STDOUT with our SELECT
        let copy_sql = format!("COPY ({}) TO STDOUT", select_sql);

        // Use streaming for COPY
        let copy_stream = client.copy_out(&copy_sql).await?;
        tokio::pin!(copy_stream);
        while let Some(result) = copy_stream.next().await {
            let chunk = result?;
            writer.write_all(&chunk)?;
        }

        writeln!(writer, "\\.")?;
        writeln!(writer)?;

        if !quiet {
            eprintln!(" done");
        }
    }

    Ok(())
}

/// Order tables by foreign key dependencies (best effort)
pub async fn order_tables_by_fk(
    client: &Client,
    tables: &[TableInfo],
) -> Result<Vec<TableInfo>, anyhow::Error> {
    // Build set of tables we care about
    let table_set: HashSet<String> = tables.iter().map(|t| t.qualified()).collect();

    // Get FK relationships
    let fk_rows = client
        .query(
            "SELECT
                tc.table_schema AS from_schema,
                tc.table_name AS from_table,
                ccu.table_schema AS to_schema,
                ccu.table_name AS to_table
             FROM information_schema.table_constraints tc
             JOIN information_schema.constraint_column_usage ccu
                  ON tc.constraint_name = ccu.constraint_name
                  AND tc.table_schema = ccu.constraint_schema
             WHERE tc.constraint_type = 'FOREIGN KEY'",
            &[],
        )
        .await?;

    // Build adjacency list (from -> [to])
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    for table in tables {
        deps.insert(table.qualified(), Vec::new());
    }

    for row in &fk_rows {
        let from_schema: String = row.get("from_schema");
        let from_table: String = row.get("from_table");
        let to_schema: String = row.get("to_schema");
        let to_table: String = row.get("to_table");

        let from_qualified = format!("{}.{}", from_schema, from_table);
        let to_qualified = format!("{}.{}", to_schema, to_table);

        // Only consider FKs between tables we're dumping
        // For COPY FROM, parent tables (referenced) must come BEFORE child tables (referencing)
        // So child table (from) depends on parent table (to) - parent must be dumped first
        if table_set.contains(&from_qualified)
            && table_set.contains(&to_qualified)
            && from_qualified != to_qualified
        {
            // Self-references don't affect order
            // from depends on to (to must come first)
            deps.entry(to_qualified).or_default().push(from_qualified);
        }
    }

    // Topological sort using Kahn's algorithm
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for table in tables {
        in_degree.insert(table.qualified(), 0);
    }

    for targets in deps.values() {
        for target in targets {
            *in_degree.entry(target.clone()).or_default() += 1;
        }
    }

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(name, _)| name.clone())
        .collect();

    // Sort queue for determinism
    let mut queue_vec: Vec<String> = queue.drain(..).collect();
    queue_vec.sort();
    queue.extend(queue_vec);

    let mut result: Vec<String> = Vec::new();

    while let Some(table) = queue.pop_front() {
        result.push(table.clone());

        if let Some(dependents) = deps.get(&table) {
            for dep in dependents {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep.clone());
                    }
                }
            }
        }
    }

    // Check for cycles
    if result.len() < tables.len() {
        eprintln!(
            "{}",
            "⚠️  Warning: Circular FK dependencies detected, using alphabetical fallback.".yellow()
        );
        // Fall back to alphabetical
        result = tables.iter().map(|t| t.qualified()).collect();
        result.sort();
    }

    // Convert back to TableInfo
    let table_map: HashMap<String, &TableInfo> =
        tables.iter().map(|t| (t.qualified(), t)).collect();

    Ok(result
        .iter()
        .filter_map(|name| {
            table_map.get(name).map(|t| TableInfo {
                schema: t.schema.clone(),
                name: t.name.clone(),
            })
        })
        .collect())
}
