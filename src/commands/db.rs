//! Database management commands for pgcrate CLI.

use crate::config::{parse_database_url, url_matches_production_patterns, Config};
use crate::sql::quote_ident;
use anyhow::{bail, Result};
use colored::Colorize;

use super::{connect, get_applied_versions, SCHEMA_MIGRATIONS_TABLE};

pub async fn db_create(
    database_url: &str,
    name_override: Option<&str>,
    config: &Config,
    quiet: bool,
) -> Result<(), anyhow::Error> {
    let parsed = parse_database_url(database_url)?;
    let db_name = name_override.unwrap_or(&parsed.database_name);

    // Warn about production patterns
    if url_matches_production_patterns(database_url, config) && !quiet {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns.".yellow()
        );
    }

    // Connect to admin database (postgres)
    let client = connect(&parsed.admin_url).await?;

    // Check if database already exists
    let exists = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1) AS exists",
            &[&db_name],
        )
        .await?;

    if exists.get::<_, bool>("exists") {
        if !quiet {
            println!(
                "{}",
                format!("Database '{}' already exists", db_name).yellow()
            );
        }
        return Ok(());
    }

    // Create database (can't use parameters for identifiers)
    let create_sql = format!("CREATE DATABASE {}", quote_ident(db_name));
    client.batch_execute(&create_sql).await?;

    if !quiet {
        println!("{}", format!("Created database '{}'", db_name).green());
    }

    Ok(())
}

pub async fn db_drop(
    database_url: &str,
    name_override: Option<&str>,
    config: &Config,
    quiet: bool,
    yes: bool,
) -> Result<(), anyhow::Error> {
    // Require --yes flag
    if !yes {
        bail!("Dropping a database requires --yes flag to confirm.");
    }

    let parsed = parse_database_url(database_url)?;
    let db_name = name_override.unwrap_or(&parsed.database_name);

    // Warn about production patterns
    if url_matches_production_patterns(database_url, config) && !quiet {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns.".yellow()
        );
    }

    // Connect to admin database (postgres)
    let client = connect(&parsed.admin_url).await?;

    // Check if database exists
    let exists = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1) AS exists",
            &[&db_name],
        )
        .await?;

    if !exists.get::<_, bool>("exists") {
        if !quiet {
            println!(
                "{}",
                format!("Database '{}' does not exist", db_name).yellow()
            );
        }
        return Ok(());
    }

    // Drop database (can't use parameters for identifiers)
    let drop_sql = format!("DROP DATABASE {}", quote_ident(db_name));
    client.batch_execute(&drop_sql).await?;

    if !quiet {
        println!("{}", format!("Dropped database '{}'", db_name).green());
    }

    Ok(())
}

pub async fn reset(
    database_url: &str,
    config: &Config,
    quiet: bool,
    verbose: bool,
    yes: bool,
    full: bool,
) -> Result<(), anyhow::Error> {
    // Require --yes flag
    if !yes {
        bail!("Reset requires --yes flag to confirm.");
    }

    // Warn about production patterns
    if url_matches_production_patterns(database_url, config) && !quiet {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns.".yellow()
        );
    }

    if full {
        // Full reset: drop DB, create DB, up
        if !quiet {
            println!(
                "{}",
                "Full reset: dropping and recreating database...".yellow()
            );
        }

        // Drop database
        db_drop(database_url, None, config, quiet, true).await?;

        // Create database
        db_create(database_url, None, config, quiet).await?;

        // Run migrations
        super::up(database_url, config, quiet, verbose, false).await?;
    } else {
        // Standard reset: down all, up
        if !quiet {
            println!("{}", "Reset: rolling back all migrations...".yellow());
        }

        // Get count of applied migrations
        let client = connect(database_url).await?;
        client.batch_execute(SCHEMA_MIGRATIONS_TABLE).await?;
        let applied = get_applied_versions(&client).await?;
        drop(client); // Close connection before running down

        if !applied.is_empty() {
            // Roll back all migrations
            super::down(
                database_url,
                config,
                quiet,
                verbose,
                applied.len(),
                true,  // yes
                false, // dry_run
            )
            .await?;
        }

        // Run migrations
        super::up(database_url, config, quiet, verbose, false).await?;
    }

    if !quiet {
        println!("{}", "\nReset complete.".green());
    }

    Ok(())
}
