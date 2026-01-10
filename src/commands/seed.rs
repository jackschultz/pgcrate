//! Seed commands for pgcrate CLI.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use colored::Colorize;
use futures_util::pin_mut;
use std::path::Path;
use std::time::Instant;
use tokio_postgres::{Client, CopyInSink};

use crate::config::Config;
use crate::seed::{
    discover_seeds, parse_seed, ParsedCsvSeed, ParsedSeed, SeedFile, SeedSchema, SeedType,
};
use crate::sql::quote_ident;

use super::connect;

#[derive(Clone, Debug)]
struct TargetTable {
    schema: String,
    name: String,
}

#[derive(Clone, Debug)]
enum SeedSelector {
    Qualified { schema: String, table: String },
    Table { table: String },
}

fn parse_seed_selector(raw: &str) -> SeedSelector {
    match raw.split_once('.') {
        Some((schema, table)) => SeedSelector::Qualified {
            schema: schema.to_string(),
            table: table.to_string(),
        },
        None => SeedSelector::Table {
            table: raw.to_string(),
        },
    }
}

fn filter_seeds<'a>(all_seeds: &'a [SeedFile], filter: &[String]) -> Result<Vec<&'a SeedFile>> {
    if filter.is_empty() {
        return Ok(all_seeds.iter().collect());
    }

    let mut selected: Vec<&SeedFile> = Vec::new();

    for raw in filter {
        match parse_seed_selector(raw) {
            SeedSelector::Qualified { schema, table } => {
                let seed = all_seeds
                    .iter()
                    .find(|s| s.schema == schema && s.table == table)
                    .with_context(|| {
                        format!(
                            "No seed named '{}.{}' found in seeds directory",
                            schema, table
                        )
                    })?;
                selected.push(seed);
            }
            SeedSelector::Table { table } => {
                let matches: Vec<&SeedFile> =
                    all_seeds.iter().filter(|s| s.table == table).collect();
                match matches.len() {
                    0 => bail!("No seed named '{}' found in seeds directory", table),
                    1 => selected.push(matches[0]),
                    _ => {
                        let mut choices: Vec<String> =
                            matches.iter().map(|s| s.qualified_name()).collect();
                        choices.sort();
                        bail!(
                            "Seed '{}' exists in multiple schemas ({}). Use <schema>.{} to disambiguate.",
                            table,
                            choices.join(", "),
                            table
                        );
                    }
                }
            }
        }
    }

    // De-dup while preserving order
    let mut seen = std::collections::HashSet::new();
    selected.retain(|s| seen.insert(s.qualified_name()));
    Ok(selected)
}

async fn table_exists(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'
            )",
            &[&schema, &table],
        )
        .await
        .context("check table exists")?;
    Ok(row.get(0))
}

async fn require_table_exact(client: &Client, schema: &str, table: &str) -> Result<TargetTable> {
    if table_exists(client, schema, table).await? {
        return Ok(TargetTable {
            schema: schema.to_string(),
            name: table.to_string(),
        });
    }
    bail!(
        "Table '{}.{}' not found. Create it with a migration first.",
        schema,
        table
    );
}

fn print_no_seeds_hint(seeds_dir: &Path, quiet: bool) -> Result<()> {
    if quiet {
        return Ok(());
    }

    let mut root_seed_files = 0usize;
    let mut schema_dirs = 0usize;
    let mut schema_seed_files = 0usize;

    if seeds_dir.exists() {
        for entry in std::fs::read_dir(seeds_dir)
            .with_context(|| format!("read seeds directory: {}", seeds_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                schema_dirs += 1;
                for sub in std::fs::read_dir(&path)
                    .with_context(|| format!("read schema seed directory: {}", path.display()))?
                {
                    let sub = sub?;
                    let sub_path = sub.path();
                    if sub_path.is_dir() {
                        continue;
                    }
                    let ext = sub_path.extension().and_then(|e| e.to_str());
                    if matches!(ext, Some("csv") | Some("sql")) {
                        schema_seed_files += 1;
                    }
                }
                continue;
            }

            let ext = path.extension().and_then(|e| e.to_str());
            if matches!(ext, Some("csv") | Some("sql")) {
                root_seed_files += 1;
            }
        }
    }

    if !seeds_dir.exists() {
        println!("No seeds found in {}", seeds_dir.display());
        println!(
            "Expected seeds under `seeds/<schema>/<name>.csv` or `seeds/<schema>/<name>.sql`."
        );
        return Ok(());
    }

    println!("No seeds found in {}", seeds_dir.display());
    if root_seed_files > 0 {
        println!(
            "Found {} seed file(s) directly under `{}`. Seeds must be placed in schema subdirectories, e.g. `seeds/public/users.csv`.",
            root_seed_files,
            seeds_dir.display()
        );
    } else if schema_dirs == 0 {
        println!(
            "Expected seeds under `seeds/<schema>/<name>.csv` or `seeds/<schema>/<name>.sql`."
        );
    } else if schema_seed_files == 0 {
        println!(
            "Found {} schema director(ies), but no seed files. Expected `seeds/<schema>/<name>.csv` or `seeds/<schema>/<name>.sql`.",
            schema_dirs
        );
    } else {
        println!(
            "Expected seeds under `seeds/<schema>/<name>.csv` or `seeds/<schema>/<name>.sql`."
        );
    }

    Ok(())
}

/// List available seed files
pub fn seed_list(config: &Config, quiet: bool) -> Result<()> {
    let seeds_dir = Path::new(config.seeds_dir());
    let seeds = discover_seeds(seeds_dir)?;

    if seeds.is_empty() {
        print_no_seeds_hint(seeds_dir, quiet)?;
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
            println!(
                "  {} [{}]{}",
                seed.qualified_name(),
                type_label,
                schema_marker
            );
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

    let all_seeds = discover_seeds(seeds_dir)?;

    if all_seeds.is_empty() {
        print_no_seeds_hint(seeds_dir, quiet)?;
        return Ok(());
    }

    let seeds = filter_seeds(&all_seeds, &filter)?;

    if seeds.is_empty() {
        if !quiet {
            println!("No matching seeds found");
        }
        return Ok(());
    }

    let mut has_errors = false;
    let mut has_warnings = false;
    let client: Option<Client> = if !database_url.is_empty() {
        Some(connect(database_url).await?)
    } else {
        None
    };

    if !quiet {
        println!("{}", "Validating seeds...".bold());
    }

    for seed_file in &seeds {
        let prefix = format!("  {}: ", seed_file.qualified_name());

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
                                        .schema_def
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

                        // If a DB connection is available, validate target table existence.
                        if let Some(client) = client.as_ref() {
                            match require_table_exact(client, &csv.schema, &csv.table).await {
                                Ok(target) => {
                                    if !quiet {
                                        println!(
                                            "    {} target table {}.{} exists",
                                            "✓".green(),
                                            target.schema,
                                            target.name
                                        );
                                    }
                                }
                                Err(e) => {
                                    if !quiet {
                                        println!("    {} {}", "✗".red(), e);
                                    }
                                    has_errors = true;
                                }
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

    let all_seeds = discover_seeds(seeds_dir)?;

    if all_seeds.is_empty() {
        print_no_seeds_hint(seeds_dir, quiet)?;
        return Ok(());
    }

    let seeds = filter_seeds(&all_seeds, &filter)?;

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
        let prefix = format!("  {}: ", seed_file.qualified_name());

        let table_name = format!(
            "{}.{}",
            quote_ident(&seed_file.schema),
            quote_ident(&seed_file.table)
        );

        if !table_exists(&client, &seed_file.schema, &seed_file.table).await? {
            if !quiet {
                println!("{}table does not exist", prefix.yellow());
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
        .schema_def
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

    // Discover seeds
    let all_seeds = discover_seeds(seeds_dir)?;

    if all_seeds.is_empty() {
        print_no_seeds_hint(seeds_dir, quiet)?;
        return Ok(());
    }

    // Filter seeds if specified
    let seeds: Vec<SeedFile> = if filter.is_empty() {
        all_seeds
    } else {
        filter_seeds(&all_seeds, &filter)?
            .into_iter()
            .cloned()
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
        let parsed = parse_seed(&seed_file)
            .with_context(|| format!("parse seed: {}", seed_file.qualified_name()))?;
        parsed_seeds.push((seed_file, parsed));
    }

    // Sort by dependencies (CSV seeds with FK refs come after their dependencies)
    let ordered_seeds = order_by_dependencies(&parsed_seeds);

    if dry_run {
        let client: Option<Client> = if !database_url.is_empty() {
            Some(connect(database_url).await?)
        } else {
            None
        };
        println!("{}", "Would load (in order):".bold());
        for (seed_file, parsed) in &ordered_seeds {
            let type_label = match seed_file.seed_type {
                SeedType::Csv => "csv",
                SeedType::Sql => "sql",
            };
            let missing_note = if let Some(client) = client.as_ref() {
                if table_exists(client, parsed.schema(), parsed.table()).await? {
                    ""
                } else {
                    " -> <missing table>"
                }
            } else {
                ""
            };
            match parsed.row_count() {
                Some(count) => {
                    println!(
                        "  {} ({} rows) [{}]{}",
                        parsed.name(),
                        count,
                        type_label,
                        missing_note
                    );
                }
                None => {
                    println!("  {} [{}]{}", parsed.name(), type_label, missing_note);
                }
            }
        }
        return Ok(());
    }

    // Connect to database
    let client = connect(database_url).await?;

    // Resolve CSV seed targets up-front (and fail early with clear errors)
    let mut ordered_with_targets: Vec<(SeedFile, ParsedSeed, Option<TargetTable>)> = Vec::new();
    for (seed_file, parsed) in ordered_seeds {
        let target = match &parsed {
            ParsedSeed::Csv(_) => {
                Some(require_table_exact(&client, parsed.schema(), parsed.table()).await?)
            }
            ParsedSeed::Sql(_) => None,
        };
        ordered_with_targets.push((seed_file, parsed, target));
    }

    // Collect all table names for FK handling
    let mut csv_tables: Vec<String> = Vec::new();
    for (seed_file, parsed, target) in &ordered_with_targets {
        if seed_file.seed_type == SeedType::Csv {
            if let (ParsedSeed::Csv(_), Some(t)) = (parsed, target) {
                csv_tables.push(format!(
                    "{}.{}",
                    quote_ident(&t.schema),
                    quote_ident(&t.name)
                ));
            }
        }
    }
    csv_tables.sort();
    csv_tables.dedup();

    // Disable FK constraints for CSV seed tables
    if !csv_tables.is_empty() {
        if !quiet {
            println!("{}", "Disabling foreign key constraints...".dimmed());
        }
        for table in &csv_tables {
            let disable_sql = format!("ALTER TABLE IF EXISTS {} DISABLE TRIGGER ALL", table);
            let _ = client.batch_execute(&disable_sql).await; // Ignore errors if table doesn't exist
        }

        // Truncate once up-front so later seeds can't wipe earlier loaded tables via CASCADE.
        if !quiet {
            println!("{}", "Truncating seed tables...".dimmed());
        }
        let truncate_sql = format!("TRUNCATE {} CASCADE", csv_tables.join(", "));
        client
            .batch_execute(&truncate_sql)
            .await
            .with_context(|| format!("truncate seed tables: {}", csv_tables.join(", ")))?;
    }

    // Load each seed
    let start = Instant::now();
    let mut total_rows = 0;
    let mut loaded_count = 0;

    for (seed_file, parsed, target) in &ordered_with_targets {
        if !quiet {
            match (parsed, target) {
                (ParsedSeed::Csv(_), Some(t)) => {
                    print!(
                        "{} {} \u{2192} {}.{}... ",
                        "Loading".cyan(),
                        parsed.name(),
                        t.schema,
                        t.name
                    );
                }
                _ => {
                    print!("{} {}... ", "Loading".cyan(), parsed.name());
                }
            }
        }

        let load_start = Instant::now();
        let result = match parsed {
            ParsedSeed::Csv(csv) => {
                let t = target
                    .as_ref()
                    .context("missing target table for CSV seed")?;
                load_csv_seed(&client, t, csv).await
            }
            ParsedSeed::Sql(sql) => load_sql_seed(&client, &sql.name, &sql.sql).await,
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
                return Err(e)
                    .with_context(|| format!("load seed: {}", seed_file.qualified_name()));
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
    result.sort_by(|(a, _), (b, _)| {
        (a.schema.as_str(), a.table.as_str()).cmp(&(b.schema.as_str(), b.table.as_str()))
    });
    result
}

fn clone_parsed_seed(p: &ParsedSeed) -> ParsedSeed {
    match p {
        ParsedSeed::Csv(csv) => ParsedSeed::Csv(ParsedCsvSeed {
            schema: csv.schema.clone(),
            table: csv.table.clone(),
            name: csv.name.clone(),
            columns: csv.columns.clone(),
            rows: csv.rows.clone(),
            schema_def: csv.schema_def.as_ref().map(|s| SeedSchema {
                columns: s.columns.clone(),
                primary_key: s.primary_key.clone(),
            }),
            csv_content: csv.csv_content.clone(),
        }),
        ParsedSeed::Sql(sql) => ParsedSeed::Sql(crate::seed::ParsedSqlSeed {
            schema: sql.schema.clone(),
            table: sql.table.clone(),
            name: sql.name.clone(),
            sql: sql.sql.clone(),
        }),
    }
}

/// Load a CSV seed using COPY protocol for performance
async fn load_csv_seed(
    client: &tokio_postgres::Client,
    target: &TargetTable,
    seed: &ParsedCsvSeed,
) -> Result<usize> {
    let table_name = format!(
        "{}.{}",
        quote_ident(&target.schema),
        quote_ident(&target.name)
    );

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
async fn load_sql_seed(client: &tokio_postgres::Client, name: &str, sql: &str) -> Result<usize> {
    // Execute the SQL seed
    client
        .batch_execute(sql)
        .await
        .with_context(|| format!("execute SQL seed: {}", name))?;

    // SQL seeds don't have a row count
    Ok(0)
}
