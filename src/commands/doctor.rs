//! Doctor command for pgcrate CLI.

use crate::config::Config;
use crate::doctor::{mask_database_url, DoctorItem, DoctorReport};
use crate::migrations::discover_migrations;
use anyhow::{bail, Result};
use chrono::Utc;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio_postgres::Client;

use super::{connect, get_applied_versions};

pub async fn doctor(
    cli_database_url: Option<&str>,
    config_path: Option<&Path>,
    quiet: bool,
    json: bool,
    verbose: bool,
    strict: bool,
) -> Result<i32, anyhow::Error> {
    let generated_at = Utc::now().to_rfc3339();

    let (config, config_file) = match load_doctor_config(config_path) {
        Ok(v) => v,
        Err(e) => {
            let report = DoctorReport::fatal_config(generated_at, e.to_string());
            return emit_doctor_report(report, quiet, json, verbose, strict);
        }
    };
    let defaults_mode = matches!(config_file, DoctorConfigFile::MissingDefault);

    let Some(database_url) = config.get_database_url(cli_database_url) else {
        let report = DoctorReport::fatal_connection(
            generated_at,
            "DATABASE_URL not set. Use -d flag, set DATABASE_URL env var, or add to pgcrate.toml",
        );
        return emit_doctor_report(report, quiet, json, verbose, strict);
    };

    let client = match connect(&database_url).await {
        Ok(client) => client,
        Err(e) => {
            let report =
                DoctorReport::fatal_connection(generated_at, format!("Failed to connect: {}", e));
            return emit_doctor_report(report, quiet, json, verbose, strict);
        }
    };

    let mut report = DoctorReport::new(generated_at);
    report.connection.push(DoctorItem::pass(format!(
        "Connected to {}",
        mask_database_url(&database_url)
    )));

    add_config_checks(&config, &config_file, &mut report);
    add_schema_checks(&client, &mut report).await;
    add_migrations_checks(&client, &config, defaults_mode, &mut report).await;
    emit_doctor_report(report, quiet, json, verbose, strict)
}

#[derive(Debug, Clone)]
enum DoctorConfigFile {
    MissingDefault,
    Loaded(PathBuf),
}

fn emit_doctor_report(
    report: DoctorReport,
    quiet: bool,
    json: bool,
    verbose: bool,
    strict: bool,
) -> Result<i32, anyhow::Error> {
    let exit_code = report.exit_code(strict);

    if quiet {
        return Ok(exit_code);
    }

    if json {
        let payload = report.to_json(strict);
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(exit_code);
    }

    println!("{}", report.format_human(verbose));
    Ok(exit_code)
}

fn load_doctor_config(
    config_path: Option<&Path>,
) -> Result<(Config, DoctorConfigFile), anyhow::Error> {
    if let Some(path) = config_path {
        if !path.exists() {
            bail!("Config file not found: {}", path.display());
        }
        let config = Config::load(Some(path))?;
        return Ok((config, DoctorConfigFile::Loaded(path.to_path_buf())));
    }

    let default_path = Path::new("pgcrate.toml");
    if default_path.exists() {
        let config = Config::load(None)?;
        return Ok((config, DoctorConfigFile::Loaded(default_path.to_path_buf())));
    }

    Ok((Config::default(), DoctorConfigFile::MissingDefault))
}

fn add_config_checks(config: &Config, config_file: &DoctorConfigFile, report: &mut DoctorReport) {
    match config_file {
        DoctorConfigFile::Loaded(path) => report
            .config
            .push(DoctorItem::pass(format!("{} valid", path.display()))),
        DoctorConfigFile::MissingDefault => report
            .config
            .push(DoctorItem::warning("pgcrate.toml missing; using defaults")),
    }

    let migrations_dir = config.migrations_dir();
    if Path::new(migrations_dir).exists() {
        report
            .config
            .push(DoctorItem::pass("Migration directory exists"));
    } else {
        let message = format!("Migration directory missing: {}", migrations_dir);
        match config_file {
            DoctorConfigFile::MissingDefault => report.config.push(DoctorItem::warning(message)),
            DoctorConfigFile::Loaded(_) => report.config.push(DoctorItem::error(message)),
        }
    }
}

async fn add_schema_checks(client: &Client, report: &mut DoctorReport) {
    match client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'pgcrate')",
            &[],
        )
        .await
    {
        Ok(row) => {
            let exists: bool = row.get(0);
            if exists {
                report
                    .schema
                    .push(DoctorItem::pass("pgcrate schema exists"));
            } else {
                report
                    .schema
                    .push(DoctorItem::error("pgcrate schema missing"));
            }
        }
        Err(e) => report.schema.push(DoctorItem::error(format!(
            "Failed to check pgcrate schema: {}",
            e
        ))),
    }

    match client
        .query_one(
            "SELECT to_regclass('pgcrate.schema_migrations') IS NOT NULL",
            &[],
        )
        .await
    {
        Ok(row) => {
            let exists: bool = row.get(0);
            if exists {
                report
                    .schema
                    .push(DoctorItem::pass("schema_migrations table exists"));
            } else {
                report
                    .schema
                    .push(DoctorItem::error("schema_migrations table missing"));
            }
        }
        Err(e) => report.schema.push(DoctorItem::error(format!(
            "Failed to check schema_migrations: {}",
            e
        ))),
    }
}

async fn add_migrations_checks(
    client: &Client,
    config: &Config,
    defaults_mode: bool,
    report: &mut DoctorReport,
) {
    let schema_migrations_exists = match client
        .query_one(
            "SELECT to_regclass('pgcrate.schema_migrations') IS NOT NULL",
            &[],
        )
        .await
    {
        Ok(row) => row.get::<_, bool>(0),
        Err(e) => {
            report.migrations.push(DoctorItem::error(format!(
                "Failed to check schema_migrations table: {}",
                e
            )));
            return;
        }
    };

    if !schema_migrations_exists {
        report.migrations.push(DoctorItem::error(
            "Skipping migration checks: schema_migrations table missing",
        ));
        return;
    }

    let applied_versions = match get_applied_versions(client).await {
        Ok(v) => v,
        Err(e) => {
            report.migrations.push(DoctorItem::error(format!(
                "Failed to query applied migrations: {}",
                e
            )));
            return;
        }
    };

    report.migrations.push(DoctorItem::pass(format!(
        "{} migrations applied",
        applied_versions.len()
    )));

    let migrations_dir = config.migrations_dir();
    let migrations_path = Path::new(migrations_dir);
    if !migrations_path.exists() {
        let message = if defaults_mode {
            format!(
                "Skipping migration checks: migrations directory missing (defaults): {}",
                migrations_dir
            )
        } else {
            format!(
                "Skipping migration checks: migrations directory missing: {}",
                migrations_dir
            )
        };
        if defaults_mode {
            report.migrations.push(DoctorItem::warning(message));
        } else {
            report.migrations.push(DoctorItem::error(message));
        }
        return;
    }

    let migrations = match discover_migrations(migrations_path) {
        Ok(m) => m,
        Err(e) => {
            report
                .migrations
                .push(DoctorItem::error(format!("Invalid migration files: {}", e)));
            return;
        }
    };

    let applied_set: HashSet<String> = applied_versions.into_iter().collect();
    let file_versions: HashSet<String> = migrations.iter().map(|m| m.version.clone()).collect();

    let pending = migrations
        .iter()
        .filter(|m| !applied_set.contains(&m.version))
        .count();

    if pending > 0 {
        report.migrations.push(DoctorItem::warning(format!(
            "{} pending migration(s)",
            pending
        )));
    } else {
        report
            .migrations
            .push(DoctorItem::pass("0 pending migrations"));
    }

    let mut orphaned: Vec<String> = applied_set.difference(&file_versions).cloned().collect();
    orphaned.sort();

    if orphaned.is_empty() {
        report
            .migrations
            .push(DoctorItem::pass("No orphaned tracking rows"));
    } else {
        let preview = orphaned.iter().take(5).cloned().collect::<Vec<_>>();
        let suffix = if orphaned.len() > preview.len() {
            format!(" (+{} more)", orphaned.len() - preview.len())
        } else {
            String::new()
        };
        report.migrations.push(DoctorItem::error(format!(
            "{} orphaned tracking row(s): {}{}",
            orphaned.len(),
            preview.join(", "),
            suffix
        )));
    }
}
