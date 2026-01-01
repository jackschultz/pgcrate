//! Migration commands for pgcrate CLI.

use crate::config::{url_matches_production_patterns, Config};
use crate::migrations::{discover_migrations, load_migrations, Migration};
use crate::output::{MigrationInfo, Output, StatusCounts, StatusResponse};
use anyhow::{bail, Result};
use chrono::Utc;
use colored::Colorize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use tokio_postgres::Client;

use super::{connect, get_applied_versions, run_migration, SCHEMA_MIGRATIONS_TABLE};

pub async fn up(
    database_url: &str,
    config: &Config,
    quiet: bool,
    verbose: bool,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    let client = connect(database_url).await?;

    // Ensure schema_migrations table exists
    client.batch_execute(SCHEMA_MIGRATIONS_TABLE).await?;

    let migrations = load_migrations(Path::new(config.migrations_dir()))?;
    let applied = get_applied_versions(&client).await?;

    let pending: Vec<_> = migrations
        .into_iter()
        .filter(|m| !applied.contains(&m.version))
        .collect();

    if pending.is_empty() {
        if !quiet {
            println!("{}", "No pending migrations".green());
        }
        return Ok(());
    }

    if !quiet {
        println!(
            "{}",
            format!("{} pending migration(s)", pending.len()).yellow()
        );
    }

    for migration in pending {
        if dry_run {
            if !quiet {
                println!(
                    "  {} {} {}",
                    "[dry-run]".blue(),
                    migration.version,
                    migration.name
                );
            }
            if verbose {
                println!("{}", migration.up_sql);
            }
        } else {
            if !quiet {
                print!("  {} {}...", migration.version, migration.name);
            }
            if verbose {
                println!("\n{}", migration.up_sql);
            }
            run_migration(&client, &migration).await?;
            if !quiet {
                println!(" {}", "done".green());
            }
        }
    }

    if !quiet {
        if dry_run {
            println!("{}", "\nDry run complete. No changes made.".blue());
        } else {
            println!("{}", "\nAll migrations applied.".green());
        }
    }

    Ok(())
}

pub async fn down(
    database_url: &str,
    config: &Config,
    quiet: bool,
    verbose: bool,
    steps: usize,
    yes: bool,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    // Check --yes flag first (before connecting)
    if !yes && !dry_run {
        bail!("Down migrations require --yes flag to confirm.");
    }

    let client = connect(database_url).await?;

    // Ensure schema_migrations table exists
    client.batch_execute(SCHEMA_MIGRATIONS_TABLE).await?;

    // Check DB environment flag (primary gate)
    let db_env = get_db_environment(&client).await?;
    if let Some(ref env) = db_env {
        if env == "prod" || env == "production" {
            bail!(
                "Database environment is 'prod'. Down migrations are disabled for production.\n\
                 This is set in pgcrate.settings and cannot be overridden from CLI."
            );
        }
    }

    // URL heuristics (secondary warning) - only if no DB environment is set
    if db_env.is_none() && url_matches_production_patterns(database_url, config) && !quiet {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns. Consider setting DB environment flag."
                .yellow()
        );
    }

    // Get applied migrations (most recent first for rollback)
    let applied = get_applied_versions(&client).await?;
    if applied.is_empty() {
        if !quiet {
            println!("{}", "No migrations to roll back".green());
        }
        return Ok(());
    }

    // Check if steps exceeds applied count
    if steps > applied.len() {
        bail!(
            "Requested {} steps but only {} migrations are applied.",
            steps,
            applied.len()
        );
    }

    // Load migration files from disk
    let migration_files = discover_migrations(Path::new(config.migrations_dir()))?;
    let file_map: std::collections::HashMap<String, Migration> = migration_files
        .into_iter()
        .map(|mf| (mf.version.clone(), mf))
        .collect();

    // Get the last N applied versions (in reverse order for rollback)
    let to_rollback: Vec<String> = applied.iter().rev().take(steps).cloned().collect();

    if !quiet {
        println!(
            "{}",
            format!("Rolling back {} migration(s)...", steps).yellow()
        );
    }

    // Execute each down migration
    for version in &to_rollback {
        let mf = file_map.get(version).ok_or_else(|| {
            anyhow::anyhow!("Migration {} is applied but not found on disk.", version)
        })?;
        let down_sql = mf.down_sql.as_ref();

        if dry_run {
            if !quiet {
                println!(
                    "  {} ↓ {}_{}{}",
                    "[dry-run]".blue(),
                    mf.version,
                    mf.name,
                    if down_sql.is_some() {
                        ""
                    } else {
                        " (no down section; rewind only)"
                    }
                );
            }
            if verbose {
                if let Some(sql) = down_sql {
                    println!("{}", sql);
                } else {
                    println!("{}", "[no down SQL to run]".dimmed());
                }
            }
        } else if let Some(sql) = down_sql {
            if !quiet {
                print!("  ↓ {}_{}...", mf.version, mf.name);
            }
            if verbose {
                println!("\n{}", sql);
            }

            client.execute("BEGIN", &[]).await?;

            match client.batch_execute(sql).await {
                Ok(()) => {
                    client
                        .execute(
                            "DELETE FROM pgcrate.schema_migrations WHERE version = $1",
                            &[&version],
                        )
                        .await?;
                    client.execute("COMMIT", &[]).await?;
                    if !quiet {
                        println!(" {}", "done".green());
                    }
                }
                Err(e) => {
                    client.execute("ROLLBACK", &[]).await?;
                    if !quiet {
                        println!(" {}", "failed".red());
                    }
                    bail!("Down migration failed for {}: {}", version, e);
                }
            }
        } else {
            if !quiet {
                print!(
                    "  ↓ {}_{} (no down section; rewinding)...",
                    mf.version, mf.name
                );
            }
            client.execute("BEGIN", &[]).await?;
            client
                .execute(
                    "DELETE FROM pgcrate.schema_migrations WHERE version = $1",
                    &[&version],
                )
                .await?;
            client.execute("COMMIT", &[]).await?;
            if !quiet {
                println!(" {}", "rewound".green());
            }
        }
    }

    if !quiet {
        if dry_run {
            println!("{}", "\nDry run complete. No changes made.".blue());
        } else {
            println!("{}", "\nRollback complete.".green());
        }
    }

    Ok(())
}

/// Get the database environment from pgcrate.settings table
async fn get_db_environment(client: &Client) -> Result<Option<String>, anyhow::Error> {
    // Check if settings table exists
    let table_exists = client
        .query_opt(
            "SELECT 1 FROM pg_catalog.pg_tables
             WHERE schemaname = 'pgcrate' AND tablename = 'settings'",
            &[],
        )
        .await?
        .is_some();

    if !table_exists {
        return Ok(None);
    }

    // Get environment value
    let row = client
        .query_opt(
            "SELECT value FROM pgcrate.settings WHERE key = 'environment'",
            &[],
        )
        .await?;

    Ok(row.map(|r| r.get("value")))
}

pub async fn status(
    database_url: &str,
    config: &Config,
    output: &Output,
) -> Result<(), anyhow::Error> {
    let client = connect(database_url).await?;

    // Ensure schema_migrations table exists
    client.batch_execute(SCHEMA_MIGRATIONS_TABLE).await?;

    let migrations_dir = config.migrations_dir();
    let migrations = discover_migrations(Path::new(migrations_dir))?;
    let applied = get_applied_versions(&client).await?;

    // Separate applied and pending migrations
    let (applied_migrations, pending_migrations): (Vec<_>, Vec<_>) = migrations
        .iter()
        .partition(|m| applied.contains(&m.version));

    // JSON mode: output structured data
    if output.is_json() {
        let response = StatusResponse {
            ok: true,
            applied: applied_migrations
                .iter()
                .map(|m| MigrationInfo {
                    version: m.version.clone(),
                    name: m.name.clone(),
                    has_down: m.down_sql.is_some(),
                })
                .collect(),
            pending: pending_migrations
                .iter()
                .map(|m| MigrationInfo {
                    version: m.version.clone(),
                    name: m.name.clone(),
                    has_down: m.down_sql.is_some(),
                })
                .collect(),
            counts: StatusCounts {
                applied: applied_migrations.len(),
                pending: pending_migrations.len(),
                total: migrations.len(),
            },
        };
        output.json(&response)?;
        return Ok(());
    }

    // Human mode
    if migrations.is_empty() {
        if !output.is_quiet() {
            println!(
                "{}",
                format!("No migrations found in {}/", migrations_dir).yellow()
            );
        }
        return Ok(());
    }

    if !output.is_quiet() {
        if !applied_migrations.is_empty() {
            println!("Applied migrations:");
            for mf in &applied_migrations {
                let down_status = if mf.down_sql.is_some() {
                    "down: yes".dimmed()
                } else {
                    "down: no".dimmed()
                };
                println!(
                    "  {} {}_{} ({})",
                    "✓".green(),
                    mf.version,
                    mf.name,
                    down_status
                );
            }
        }

        if !pending_migrations.is_empty() {
            if !applied_migrations.is_empty() {
                println!();
            }
            println!("Pending migrations:");
            for mf in &pending_migrations {
                let down_status = if mf.down_sql.is_some() {
                    "down: yes".dimmed()
                } else {
                    "down: no".dimmed()
                };
                println!(
                    "  {} {}_{} ({})",
                    "·".yellow(),
                    mf.version,
                    mf.name,
                    down_status
                );
            }
        }
    }

    Ok(())
}

pub fn new_migration(name: &str, config: &Config, with_down: bool) -> Result<(), anyhow::Error> {
    let dir = Path::new(config.migrations_dir());
    fs::create_dir_all(dir)?;

    let timestamp = Utc::now().format("%Y%m%d%H%M%S");
    let effective_with_down = with_down || config.default_with_down();

    let filename = format!("{}_{}.sql", timestamp, name);
    let path = dir.join(&filename);
    let down_hint = if effective_with_down {
        "-- down\n-- Add rollback SQL here (leave empty if irreversible)\n"
    } else {
        "-- down\n"
    };
    let contents = format!(
        "-- Migration: {}\n-- Created at: {}\n\n-- up\n-- Write your migration SQL here\n\n{}",
        name, timestamp, down_hint
    );
    fs::write(&path, contents)?;
    println!("Created: {}", path.display().to_string().green());

    Ok(())
}

#[allow(clippy::too_many_arguments)] // CLI handler - each arg maps to a CLI flag
pub async fn baseline(
    database_url: &str,
    config: &Config,
    quiet: bool,
    _verbose: bool, // Accepted for CLI consistency but baseline has no verbose output
    all: bool,
    version: Option<&str>,
    yes: bool,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    // Validate arguments
    if all && version.is_some() {
        bail!("Cannot specify both --all and a version prefix.");
    }
    if !yes && !dry_run {
        bail!("Baseline requires --yes flag to confirm.");
    }

    // Default to --all when neither --all nor version is specified
    let defaulting_to_all = !all && version.is_none();
    let all = all || version.is_none();

    if defaulting_to_all && !quiet {
        println!("{}", "No version specified, defaulting to --all".dimmed());
    }

    // Warn about production URL patterns
    if url_matches_production_patterns(database_url, config) && !quiet {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns. Proceeding with baseline.".yellow()
        );
    }

    let client = connect(database_url).await?;
    client.batch_execute(SCHEMA_MIGRATIONS_TABLE).await?;

    // Load migrations
    let migrations_dir = config.migrations_dir();
    let migrations = load_migrations(Path::new(migrations_dir))?;

    if migrations.is_empty() {
        if !quiet {
            println!(
                "{}",
                format!("No migrations found in {}/", migrations_dir).green()
            );
        }
        return Ok(());
    }

    // Select migrations to baseline
    let to_baseline: Vec<&Migration> = if all {
        migrations.iter().collect()
    } else {
        let prefix = version.unwrap(); // Safe: validated above
        let matched: Vec<_> = migrations
            .iter()
            .filter(|m| m.version.starts_with(prefix))
            .collect();

        if matched.is_empty() {
            let available: Vec<&str> = migrations.iter().map(|m| m.version.as_str()).collect();
            bail!(
                "No migrations found matching version prefix '{}'. Available versions:\n  {}",
                prefix,
                available.join("\n  ")
            );
        }
        matched
    };

    // Get already applied versions
    let applied = get_applied_versions(&client).await?;
    let applied_set: HashSet<&str> = applied.iter().map(|s| s.as_str()).collect();

    // Apply baseline
    let mut baselined = 0;
    let mut skipped = 0;

    if dry_run && !quiet {
        println!("{}", "Would baseline:".yellow());
    }

    for migration in &to_baseline {
        if applied_set.contains(migration.version.as_str()) {
            skipped += 1;
            if !quiet {
                println!(
                    "  {} {}_{} ({})",
                    "Skipped".yellow(),
                    migration.version,
                    migration.name,
                    "already applied".dimmed()
                );
            }
        } else {
            baselined += 1;
            if dry_run {
                if !quiet {
                    println!("  {}_{}", migration.version, migration.name);
                }
            } else {
                client
                    .execute(
                        "INSERT INTO pgcrate.schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING",
                        &[&migration.version],
                    )
                    .await?;
                if !quiet {
                    println!(
                        "  {} {}_{}",
                        "Baselined".green(),
                        migration.version,
                        migration.name
                    );
                }
            }
        }
    }

    // Summary
    if !quiet {
        if dry_run {
            println!(
                "\n{}",
                format!(
                    "Dry run complete. {} would be baselined, {} already applied.",
                    baselined, skipped
                )
                .blue()
            );
        } else if baselined > 0 {
            let msg = if skipped > 0 {
                format!("{} migration(s) baselined, {} skipped.", baselined, skipped)
            } else {
                format!("{} migration(s) baselined.", baselined)
            };
            println!("{}", msg.green());
        }
    }

    Ok(())
}
