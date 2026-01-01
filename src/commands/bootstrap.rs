//! Bootstrap command for pgcrate CLI.

use crate::config::Config;
use anyhow::{Context, Result};
use colored::Colorize;
use std::path::Path;
use std::process::{Command, Stdio};

use super::{anonymize_setup, db_create, up};

/// Bootstrap a new environment with anonymized data from a source
#[allow(clippy::too_many_arguments)]
pub async fn bootstrap(
    database_url: &str,
    from_url: &str,
    config: &Config,
    anonymize_config_path: Option<&Path>,
    quiet: bool,
    verbose: bool,
    dry_run: bool,
    yes: bool,
) -> Result<(), anyhow::Error> {
    if !yes && !dry_run {
        anyhow::bail!(
            "Bootstrap requires --yes flag to confirm. This will recreate the local database."
        );
    }

    if !quiet {
        println!("{}", "Starting bootstrap process...".bold().cyan());
    }

    if dry_run {
        println!(
            "  {} Would create/recreate local database",
            "[dry-run]".blue()
        );
        println!("  {} Would apply migrations", "[dry-run]".blue());
        println!(
            "  {} Would install anonymization functions",
            "[dry-run]".blue()
        );
        println!(
            "  {} Would stream anonymized data from {}",
            "[dry-run]".blue(),
            from_url
        );
        println!("\nRun with --yes to proceed.");
        return Ok(());
    }

    // 1. Create/Ensure local database
    if !quiet {
        println!("  1. Creating local database...");
    }
    db_create(database_url, None, config, quiet).await?;

    // 2. Apply migrations
    if !quiet {
        println!("  2. Applying migrations...");
    }
    up(database_url, config, quiet, verbose, false).await?;

    // 3. Ensure anonymize functions exist locally
    if !quiet {
        println!("  3. Installing anonymization helpers...");
    }
    anonymize_setup(database_url, quiet, verbose).await?;

    // 4. Stream anonymized data
    if !quiet {
        println!("  4. Streaming anonymized data from source...");
    }

    use super::anonymize::{execute_anonymize_dump, get_tables_for_dump};
    use super::connect;
    use crate::anonymize::{get_skipped_tables, parse_table_name, AnonymizeRule};
    use crate::config::AnonymizeConfig;

    // Connect to source
    let source_client = connect(from_url)
        .await
        .context("Failed to connect to source database")?;

    // Load anonymize config
    let anon_config = AnonymizeConfig::load(anonymize_config_path)?;

    // Resolve seed: Env > File
    let seed = std::env::var("PGCRATE_ANONYMIZE_SEED").ok()
        .or_else(|| anon_config.seed.clone())
        .ok_or_else(|| {
            anyhow::anyhow!("No anonymization seed provided. Use PGCRATE_ANONYMIZE_SEED env var, or 'seed' in pgcrate.anonymize.toml")
        })?;

    // Convert config rules to anonymize::AnonymizeRule
    let mut rules = Vec::new();
    for rule in anon_config.rules {
        let (schema, table) = parse_table_name(&rule.table);
        if rule.skip {
            rules.push(AnonymizeRule::skip_table(&schema, &table));
        } else if let Some(columns) = rule.columns {
            for (col, strategy) in columns {
                crate::anonymize::validate_strategy(&strategy)?;
                rules.push(AnonymizeRule::column(&schema, &table, &col, &strategy));
            }
        }
    }

    let skipped_tables = get_skipped_tables(&rules);
    let tables = get_tables_for_dump(&source_client, &skipped_tables).await?;

    // We'll use a child psql process for the local side to handle the SQL stream
    let psql_path = config.tool_path("psql");
    let mut child = Command::new(&psql_path)
        .arg(database_url)
        .arg("-q")
        .stdin(Stdio::piped())
        .spawn()
        .context("Failed to spawn psql process for data loading")?;

    let mut stdin = child.stdin.take().unwrap();

    // Stream from source via anonymizer into local psql
    execute_anonymize_dump(&source_client, &tables, &rules, &seed, &mut stdin, quiet).await?;

    // Close stdin and wait for psql to finish
    drop(stdin);
    let status = child.wait()?;

    if !status.success() {
        anyhow::bail!(
            "psql failed during data loading (exit code {})",
            status.code().unwrap_or(-1)
        );
    }

    if !quiet {
        println!(
            "\n{}",
            "Bootstrap complete. Environment is ready.".green().bold()
        );
    }

    Ok(())
}
