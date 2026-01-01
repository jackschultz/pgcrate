//! Seed commands for pgcrate CLI.

use anyhow::{Context, Result};
use bytes::Bytes;
use colored::Colorize;
use futures_util::pin_mut;
use std::path::Path;
use std::time::Instant;
use tokio_postgres::CopyInSink;

use crate::config::Config;
use crate::seed::{
    discover_seeds, parse_seed, ParsedCsvSeed, ParsedSeed, SeedFile, SeedSchema, SeedType,
};
use crate::sql::quote_ident;

use super::connect;

/// List available seed files
pub fn seed_list(config: &Config, quiet: bool) -> Result<()> {
    let seeds_dir = Path::new(config.seeds_dir());
    let seeds = discover_seeds(seeds_dir)?;

    if seeds.is_empty() {
        if !quiet {
            println!("No seeds found in {}", seeds_dir.display());
        }
        return Ok(());
    }

    if !quiet {
        println!("{}", "Available seeds:".bold());
        for seed in &seeds {
            let type_label = match seed.seed_type {
                SeedType::Csv => "csv",
                SeedType::Sql => "sql",
            };
            let schema_marker = if seed.schema_path.is_some() {
                " (with schema)"
            } else {
                ""
            };
            println!("  {} [{}]{}", seed.name, type_label, schema_marker);
        }
        println!("\n{} seed(s) found", seeds.len());
    }

    Ok(())
}

/// Validate seed files without loading
pub async fn seed_validate(
    database_url: &str,
    config: &Config,
    filter: Vec<String>,
    quiet: bool,
) -> Result<()> {
    let seeds_dir = Path::new(config.seeds_dir());
    let schema = config.seeds_schema();

    let all_seeds = discover_seeds(seeds_dir)?;

    if all_seeds.is_empty() {
        if !quiet {
            println!("No seeds found in {}", seeds_dir.display());
        }
        return Ok(());
    }

    // Filter seeds if specified
    let seeds: Vec<&SeedFile> = if filter.is_empty() {
        all_seeds.iter().collect()
    } else {
        all_seeds
            .iter()
            .filter(|s| filter.iter().any(|f| f == &s.name))
            .collect()
    };

    if seeds.is_empty() {
        if !quiet {
            println!("No matching seeds found");
        }
        return Ok(());
    }

    let mut has_errors = false;
    let mut has_warnings = false;

    if !quiet {
        println!("{}", "Validating seeds...".bold());
    }

    for seed_file in &seeds {
        let prefix = format!("  {}: ", seed_file.name);

        // Try to parse the seed
        match parse_seed(seed_file) {
            Ok(parsed) => {
                match &parsed {
                    ParsedSeed::Csv(csv) => {
                        if !quiet {
                            println!(
                                "{}✓ {} rows, {} columns",
                                prefix.green(),
                                csv.rows.len(),
                                csv.columns.len()
                            );
                        }

                        // Check for schema file
                        if seed_file.schema_path.is_some() {
                            if !quiet {
                                println!("    {} has schema file", "✓".green());
                            }
                        } else if !quiet {
                            println!("    {} no schema file (types inferred)", "⚠".yellow());
                            has_warnings = true;
                        }

                        // Show inferred types
                        if !quiet {
                            for col in &csv.columns {
                                let source = if seed_file.schema_path.is_some()
                                    && csv
                                        .schema
                                        .as_ref()
                                        .map(|s| s.columns.contains_key(&col.name))
                                        .unwrap_or(false)
                                {
                                    "schema"
                                } else {
                                    "inferred"
                                };
                                println!("    {} {} ({})", col.name, col.pg_type.dimmed(), source);
                            }
                        }
                    }
                    ParsedSeed::Sql(sql) => {
                        if !quiet {
                            println!("{}✓ SQL seed ({} bytes)", prefix.green(), sql.sql.len());
                        }
                    }
                }
            }
            Err(e) => {
                if !quiet {
                    println!("{}✗ {}", prefix.red(), e);
                }
                has_errors = true;
            }
        }
    }

    // Check database connection if URL provided
    if !database_url.is_empty() {
        if !quiet {
            println!("\n{}", "Checking database connection...".bold());
        }
        match connect(database_url).await {
            Ok(client) => {
                // Check if schema exists
                let row = client
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
                        &[&schema],
                    )
                    .await?;
                let exists: bool = row.get(0);
                if exists {
                    if !quiet {
                        println!("  {} schema '{}' exists", "✓".green(), schema);
                    }
                } else if !quiet {
                    println!(
                        "  {} schema '{}' does not exist (will be created)",
                        "⚠".yellow(),
                        schema
                    );
                }
            }
            Err(e) => {
                if !quiet {
                    println!("  {} connection failed: {}", "✗".red(), e);
                }
                has_errors = true;
            }
        }
    }

    if !quiet {
        println!();
        if has_errors {
            println!("{}", "Validation failed with errors.".red().bold());
        } else if has_warnings {
            println!("{}", "Validation passed with warnings.".yellow().bold());
        } else {
            println!("{}", "Validation passed.".green().bold());
        }
    }

    if has_errors {
        anyhow::bail!("Seed validation failed");
    }

    Ok(())
}

/// Compare seed files to database state
pub async fn seed_diff(
    database_url: &str,
    config: &Config,
    filter: Vec<String>,
    quiet: bool,
) -> Result<()> {
    let seeds_dir = Path::new(config.seeds_dir());
    let schema = config.seeds_schema();

    let all_seeds = discover_seeds(seeds_dir)?;

    if all_seeds.is_empty() {
        if !quiet {
            println!("No seeds found in {}", seeds_dir.display());
        }
        return Ok(());
    }

    // Filter seeds if specified
    let seeds: Vec<&SeedFile> = if filter.is_empty() {
        all_seeds.iter().collect()
    } else {
        all_seeds
            .iter()
            .filter(|s| filter.iter().any(|f| f == &s.name))
            .collect()
    };

    if seeds.is_empty() {
        if !quiet {
            println!("No matching seeds found");
        }
        return Ok(());
    }

    let client = connect(database_url).await?;

    if !quiet {
        println!("{}", "Comparing seeds to database...".bold());
    }

    for seed_file in &seeds {
        let table_name = format!("{}.{}", quote_ident(schema), quote_ident(&seed_file.name));
        let prefix = format!("  {}: ", seed_file.name);

        // Check if table exists
        let exists_row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                &[&schema, &seed_file.name],
            )
            .await?;
        let table_exists: bool = exists_row.get(0);

        if !table_exists {
            if !quiet {
                println!("{}table does not exist (would create)", prefix.yellow());
            }
            continue;
        }

        // Parse seed
        match parse_seed(seed_file) {
            Ok(ParsedSeed::Csv(csv_seed)) => {
                diff_csv_seed(&client, &csv_seed, &table_name, &prefix, quiet).await?;
            }
            Ok(ParsedSeed::Sql(_)) => {
                // SQL seed - can only show row count
                let count_sql = format!("SELECT COUNT(*) FROM {}", table_name);
                let count_row = client.query_one(&count_sql, &[]).await?;
                let db_count: i64 = count_row.get(0);
                if !quiet {
                    println!("{}SQL seed, {} rows in db", prefix.cyan(), db_count);
                }
            }
            Err(e) => {
                if !quiet {
                    println!("{}parse error: {}", prefix.red(), e);
                }
            }
        }
    }

    Ok(())
}

/// Compare a CSV seed to database and show row-level differences
async fn diff_csv_seed(
    client: &tokio_postgres::Client,
    csv_seed: &ParsedCsvSeed,
    table_name: &str,
    prefix: &str,
    quiet: bool,
) -> Result<()> {
    use std::collections::HashSet;

    if csv_seed.columns.is_empty() {
        if !quiet {
            println!("{}no columns in seed file", prefix.yellow());
        }
        return Ok(());
    }

    let file_count = csv_seed.rows.len();

    // Get database row count
    let count_sql = format!("SELECT COUNT(*) FROM {}", table_name);
    let count_row = client.query_one(&count_sql, &[]).await?;
    let db_count: i64 = count_row.get(0);

    if file_count == db_count as usize {
        if !quiet {
            println!("{}{} rows (in sync)", prefix.green(), file_count);
        }
        return Ok(());
    }

    // Determine key column: use first primary_key column from schema, or fall back to first column
    let key_col_name: &str = csv_seed
        .schema
        .as_ref()
        .and_then(|s| s.primary_key.as_ref())
        .and_then(|pk| pk.first())
        .map(|s| s.as_str())
        .unwrap_or(&csv_seed.columns[0].name);

    let key_col_idx = csv_seed
        .columns
        .iter()
        .position(|c| c.name == key_col_name)
        .unwrap_or(0);

    // Build set of keys from file
    let file_keys: HashSet<String> = csv_seed
        .rows
        .iter()
        .filter_map(|row| row.get(key_col_idx).and_then(|v| v.clone()))
        .collect();

    // Fetch keys from database
    let key_sql = format!(
        "SELECT {}::text FROM {}",
        quote_ident(key_col_name),
        table_name
    );
    let db_rows = client.query(&key_sql, &[]).await?;
    let db_keys: HashSet<String> = db_rows
        .iter()
        .filter_map(|row| row.get::<_, Option<String>>(0))
        .collect();

    // Find differences
    let missing_in_db: Vec<_> = file_keys.difference(&db_keys).collect();
    let extra_in_db: Vec<_> = db_keys.difference(&file_keys).collect();

    if !quiet {
        println!(
            "{}{} rows in file, {} in db",
            prefix.yellow(),
            file_count,
            db_count
        );

        const MAX_SHOW: usize = 5;

        if !missing_in_db.is_empty() {
            let show_count = missing_in_db.len().min(MAX_SHOW);
            for key in missing_in_db.iter().take(show_count) {
                println!(
                    "    {} {}={} (missing in db)",
                    "+".green(),
                    key_col_name,
                    key
                );
            }
            if missing_in_db.len() > MAX_SHOW {
                println!(
                    "    {} ...and {} more missing",
                    "+".green(),
                    missing_in_db.len() - MAX_SHOW
                );
            }
        }

        if !extra_in_db.is_empty() {
            let show_count = extra_in_db.len().min(MAX_SHOW);
            for key in extra_in_db.iter().take(show_count) {
                println!("    {} {}={} (extra in db)", "-".red(), key_col_name, key);
            }
            if extra_in_db.len() > MAX_SHOW {
                println!(
                    "    {} ...and {} more extra",
                    "-".red(),
                    extra_in_db.len() - MAX_SHOW
                );
            }
        }
    }

    Ok(())
}

/// Load seed data into database
pub async fn seed_run(
    database_url: &str,
    config: &Config,
    filter: Vec<String>,
    dry_run: bool,
    quiet: bool,
) -> Result<()> {
    let seeds_dir = Path::new(config.seeds_dir());
    let schema = config.seeds_schema();

    // Discover seeds
    let all_seeds = discover_seeds(seeds_dir)?;

    if all_seeds.is_empty() {
        if !quiet {
            println!("No seeds found in {}", seeds_dir.display());
        }
        return Ok(());
    }

    // Filter seeds if specified
    let seeds: Vec<SeedFile> = if filter.is_empty() {
        all_seeds
    } else {
        all_seeds
            .into_iter()
            .filter(|s| filter.iter().any(|f| f == &s.name))
            .collect()
    };

    if seeds.is_empty() {
        if !quiet {
            println!("No matching seeds found");
        }
        return Ok(());
    }

    // Parse all seeds first (to validate before making changes)
    let mut parsed_seeds: Vec<(SeedFile, ParsedSeed)> = Vec::new();
    for seed_file in seeds {
        let parsed =
            parse_seed(&seed_file).with_context(|| format!("parse seed: {}", seed_file.name))?;
        parsed_seeds.push((seed_file, parsed));
    }

    // Sort by dependencies (CSV seeds with FK refs come after their dependencies)
    let ordered_seeds = order_by_dependencies(&parsed_seeds);

    if dry_run {
        println!("{}", "Would load (in order):".bold());
        for (seed_file, parsed) in &ordered_seeds {
            let type_label = match seed_file.seed_type {
                SeedType::Csv => "csv",
                SeedType::Sql => "sql",
            };
            match parsed.row_count() {
                Some(count) => {
                    println!(
                        "  {} ({} rows) -> {}.{} [{}]",
                        parsed.name(),
                        count,
                        schema,
                        parsed.name(),
                        type_label
                    );
                }
                None => {
                    println!(
                        "  {} -> {}.{} [{}]",
                        parsed.name(),
                        schema,
                        parsed.name(),
                        type_label
                    );
                }
            }
        }
        return Ok(());
    }

    // Connect to database
    let client = connect(database_url).await?;

    // Create schema if needed
    let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(schema));
    client
        .batch_execute(&create_schema_sql)
        .await
        .context("create seeds schema")?;

    // Collect all table names for FK handling
    let csv_tables: Vec<String> = ordered_seeds
        .iter()
        .filter(|(sf, _)| sf.seed_type == SeedType::Csv)
        .map(|(_, p)| format!("{}.{}", quote_ident(schema), quote_ident(p.name())))
        .collect();

    // Disable FK constraints for CSV seed tables
    if !csv_tables.is_empty() {
        if !quiet {
            println!("{}", "Disabling foreign key constraints...".dimmed());
        }
        for table in &csv_tables {
            let disable_sql = format!("ALTER TABLE IF EXISTS {} DISABLE TRIGGER ALL", table);
            let _ = client.batch_execute(&disable_sql).await; // Ignore errors if table doesn't exist
        }
    }

    // Load each seed
    let start = Instant::now();
    let mut total_rows = 0;
    let mut loaded_count = 0;

    for (seed_file, parsed) in &ordered_seeds {
        if !quiet {
            print!("{} {}... ", "Loading".cyan(), parsed.name());
        }

        let load_start = Instant::now();
        let result = match parsed {
            ParsedSeed::Csv(csv) => load_csv_seed(&client, schema, csv).await,
            ParsedSeed::Sql(sql) => load_sql_seed(&client, schema, &sql.name, &sql.sql).await,
        };

        match result {
            Ok(rows) => {
                total_rows += rows;
                loaded_count += 1;
                let elapsed = load_start.elapsed();
                if !quiet {
                    if rows > 0 {
                        println!("{} rows ({:.2}s)", rows, elapsed.as_secs_f64());
                    } else {
                        println!("done ({:.2}s)", elapsed.as_secs_f64());
                    }
                }
            }
            Err(e) => {
                if !quiet {
                    println!("{}", "FAILED".red());
                }
                // Re-enable triggers before returning error
                for table in &csv_tables {
                    let enable_sql = format!("ALTER TABLE IF EXISTS {} ENABLE TRIGGER ALL", table);
                    let _ = client.batch_execute(&enable_sql).await;
                }
                return Err(e).with_context(|| format!("load seed: {}", seed_file.name));
            }
        }
    }

    // Re-enable FK constraints
    if !csv_tables.is_empty() {
        if !quiet {
            println!("{}", "Re-enabling foreign key constraints...".dimmed());
        }
        for table in &csv_tables {
            let enable_sql = format!("ALTER TABLE IF EXISTS {} ENABLE TRIGGER ALL", table);
            client
                .batch_execute(&enable_sql)
                .await
                .with_context(|| format!("re-enable triggers on {}", table))?;
        }
    }

    let elapsed = start.elapsed();
    if !quiet {
        println!(
            "\n{} {} seed(s) loaded ({} total rows) in {:.2}s",
            "Done.".green().bold(),
            loaded_count,
            total_rows,
            elapsed.as_secs_f64()
        );
    }

    Ok(())
}

/// Order seeds alphabetically for consistent loading order.
/// FK constraints are handled by disabling triggers during load.
fn order_by_dependencies(seeds: &[(SeedFile, ParsedSeed)]) -> Vec<(SeedFile, ParsedSeed)> {
    let mut result: Vec<_> = seeds
        .iter()
        .map(|(sf, p)| (sf.clone(), clone_parsed_seed(p)))
        .collect();
    result.sort_by(|(a, _), (b, _)| a.name.cmp(&b.name));
    result
}

fn clone_parsed_seed(p: &ParsedSeed) -> ParsedSeed {
    match p {
        ParsedSeed::Csv(csv) => ParsedSeed::Csv(ParsedCsvSeed {
            name: csv.name.clone(),
            columns: csv.columns.clone(),
            rows: csv.rows.clone(),
            schema: csv.schema.as_ref().map(|s| SeedSchema {
                columns: s.columns.clone(),
                primary_key: s.primary_key.clone(),
            }),
            csv_content: csv.csv_content.clone(),
        }),
        ParsedSeed::Sql(sql) => ParsedSeed::Sql(crate::seed::ParsedSqlSeed {
            name: sql.name.clone(),
            sql: sql.sql.clone(),
        }),
    }
}

/// Load a CSV seed using COPY protocol for performance
async fn load_csv_seed(
    client: &tokio_postgres::Client,
    schema: &str,
    seed: &ParsedCsvSeed,
) -> Result<usize> {
    let table_name = format!("{}.{}", quote_ident(schema), quote_ident(&seed.name));

    // Generate CREATE TABLE statement
    let columns_def: Vec<String> = seed
        .columns
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), c.pg_type))
        .collect();

    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
        table_name,
        columns_def.join(",\n  ")
    );

    client
        .batch_execute(&create_table_sql)
        .await
        .with_context(|| format!("create table: {}", table_name))?;

    // Truncate existing data
    let truncate_sql = format!("TRUNCATE {} CASCADE", table_name);
    client
        .batch_execute(&truncate_sql)
        .await
        .with_context(|| format!("truncate table: {}", table_name))?;

    if seed.rows.is_empty() {
        return Ok(0);
    }

    // Use COPY for bulk loading
    let col_names: Vec<String> = seed.columns.iter().map(|c| quote_ident(&c.name)).collect();
    let copy_sql = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')",
        table_name,
        col_names.join(", ")
    );

    // Build CSV data without header (COPY expects raw data)
    let mut csv_data = String::new();
    for row in &seed.rows {
        let values: Vec<String> = row
            .iter()
            .map(|v| match v {
                Some(s) => escape_csv_value(s),
                None => String::new(),
            })
            .collect();
        csv_data.push_str(&values.join(","));
        csv_data.push('\n');
    }

    // Execute COPY
    let sink: CopyInSink<Bytes> = client
        .copy_in(&copy_sql)
        .await
        .with_context(|| format!("start COPY for: {}", table_name))?;

    pin_mut!(sink);

    use futures_util::SinkExt;

    // Write CSV data to sink
    sink.send(Bytes::from(csv_data.into_bytes()))
        .await
        .with_context(|| format!("write data for: {}", table_name))?;

    let rows_copied = sink
        .finish()
        .await
        .with_context(|| format!("finish COPY for: {}", table_name))?;

    Ok(rows_copied as usize)
}

/// Escape a value for CSV format
fn escape_csv_value(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Load a SQL seed by executing it directly
async fn load_sql_seed(
    client: &tokio_postgres::Client,
    schema: &str,
    name: &str,
    sql: &str,
) -> Result<usize> {
    // Set search path to include seed schema
    let set_path_sql = format!("SET search_path TO {}, public", quote_ident(schema));
    client
        .batch_execute(&set_path_sql)
        .await
        .context("set search_path")?;

    // Execute the SQL seed
    client
        .batch_execute(sql)
        .await
        .with_context(|| format!("execute SQL seed: {}", name))?;

    // Reset search path
    client
        .batch_execute("RESET search_path")
        .await
        .context("reset search_path")?;

    // SQL seeds don't have a row count
    Ok(0)
}
