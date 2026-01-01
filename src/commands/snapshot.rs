use crate::config::{parse_database_url, url_matches_production_patterns, Config};
use crate::snapshot::{
    self, check_pg_dump, check_pg_restore, check_psql, extract_host, get_pg_dump_version,
    should_warn_version_downgrade, snapshot_dir, snapshot_exists, snapshots_dir,
    validate_snapshot_name, SnapshotFormat, SnapshotMetadata,
};
use crate::sql::quote_ident;
use anyhow::{bail, Result};
use colored::Colorize;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use tokio::process::Command;
use tokio_postgres::Client;

use super::connect;

#[derive(Deserialize, Default, Debug)]
struct SnapshotProfilesFile {
    #[serde(default)]
    profiles: HashMap<String, crate::config::SnapshotProfile>,
}

#[derive(Deserialize, Default, Debug)]
struct SnapshotConfigFile {
    snapshot: Option<crate::config::SnapshotConfig>,
}

fn load_snapshot_profiles(path: &Path) -> Result<HashMap<String, crate::config::SnapshotProfile>> {
    let contents = fs::read_to_string(path)?;

    if let Ok(file) = toml::from_str::<SnapshotConfigFile>(&contents) {
        if let Some(snapshot) = file.snapshot {
            return Ok(snapshot.profiles);
        }
    }

    // Back-compat: old format used by earlier iterations/tests.
    if let Ok(legacy) = toml::from_str::<SnapshotProfilesFile>(&contents) {
        return Ok(legacy.profiles);
    }

    bail!(
        "Failed to parse {}.\nExpected either:\n  [snapshot.<profile>]\n  schemas = [\"...\"]\nOr legacy:\n  [profiles.<profile>]\n  schemas = [\"...\"]",
        path.display()
    );
}

/// Known harmless pg_restore warnings that can be safely suppressed
/// These typically occur when restoring from a newer Postgres version to an older one
fn is_harmless_restore_warning(line: &str) -> bool {
    // Settings that don't exist in older Postgres versions
    const HARMLESS_SETTINGS: &[&str] = &[
        "transaction_timeout",              // Postgres 17+
        "idle_session_timeout",             // Postgres 14+
        "client_connection_check_interval", // Postgres 14+
    ];

    // Check for "unrecognized configuration parameter" warnings
    if line.contains("unrecognized configuration parameter") {
        return HARMLESS_SETTINGS.iter().any(|s| line.contains(s));
    }

    // Filter out the "Command was: SET ..." follow-up lines for filtered warnings
    if line.trim().starts_with("Command was: SET") {
        return HARMLESS_SETTINGS
            .iter()
            .any(|s| line.contains(&format!("{} =", s)));
    }

    // Filter out "errors ignored on restore" summary - we'll show our own if needed
    if line.contains("errors ignored on restore") {
        return true;
    }

    false
}

/// Save current database state to a snapshot
#[allow(clippy::too_many_arguments)]
pub async fn snapshot_save(
    database_url: &str,
    name: &str,
    message: Option<&str>,
    profile_name: Option<&str>,
    snapshot_config_path: Option<&std::path::Path>,
    format_str: &str,
    no_owner: bool,
    no_privileges: bool,
    config: &Config,
    quiet: bool,
    verbose: bool,
    dry_run: bool,
) -> Result<()> {
    // Validate snapshot name
    validate_snapshot_name(name)?;

    // Parse and validate format
    let format: SnapshotFormat = format_str.parse()?;

    // Load snapshot profiles from file if available (CLI: --snapshot-config, default: ./pgcrate.snapshot.toml)
    let profiles_from_file: Option<HashMap<String, crate::config::SnapshotProfile>> =
        match snapshot_config_path {
            Some(p) => {
                if !p.exists() {
                    bail!("Snapshot profiles file not found: {}", p.display());
                }
                Some(load_snapshot_profiles(p)?)
            }
            None => {
                let default_path = Path::new("pgcrate.snapshot.toml");
                if default_path.exists() {
                    Some(load_snapshot_profiles(default_path)?)
                } else {
                    None
                }
            }
        };

    // Resolve profile from main config or snapshot profiles file.
    let effective_profile = if let Some(p) = profile_name {
        let from_main = config
            .snapshot
            .as_ref()
            .and_then(|s| s.profiles.get(p))
            .cloned();
        let from_file = profiles_from_file.as_ref().and_then(|s| s.get(p)).cloned();

        Some(
            from_main
                .or(from_file)
                .ok_or_else(|| anyhow::anyhow!("Snapshot profile \"{}\" not found in config", p))?,
        )
    } else {
        None
    };

    // Get snapshot directory override
    let snap_dir_override = Some(config.snapshot_dir());

    // Check if snapshot already exists (complete snapshot)
    if snapshot_exists(name, snap_dir_override) {
        bail!(
            "Snapshot \"{}\" already exists.\n\
             Hint: Delete it first with: pgcrate snapshot delete {}",
            name,
            name
        );
    }

    // Check if snapshot directory exists but is incomplete (from failed save)
    let snap_dir = snapshot_dir(name, snap_dir_override);
    if snap_dir.exists() {
        bail!(
            "Snapshot directory \"{}\" exists but is incomplete.\n\
             Hint: Delete it first with: pgcrate snapshot delete {}",
            name,
            name
        );
    }

    // Check pg_dump binary exists
    let pg_dump_path = config.tool_path("pg_dump");
    check_pg_dump(&pg_dump_path)?;

    // Warn about production patterns
    if url_matches_production_patterns(database_url, config) && !quiet {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns.".yellow()
        );
    }

    let parsed = parse_database_url(database_url)?;

    if !quiet {
        println!("Saving snapshot...");
        println!("  Database: {}", parsed.database_name);
        println!("  Format:   {}", format);
        if let Some(p) = profile_name {
            println!("  Profile:  {}", p);
        }
    }

    // Connect to database to get migration state and metadata
    let client = connect(database_url).await?;

    // Query applied migrations
    let (applied_count, latest_version) = get_migration_state(&client).await?;

    if !quiet {
        println!("  Migrations: {} applied", applied_count);
    }

    // Get PostgreSQL server version
    let pg_version = get_pg_version(&client).await.ok();

    // Get pg_dump version
    let pg_dump_version = get_pg_dump_version(&pg_dump_path).ok();

    // Get owner roles (for pre-flight checking on restore)
    let owner_roles = if !no_owner {
        get_owner_roles(&client).await.unwrap_or_default()
    } else {
        Vec::new()
    };

    // Extract source host
    let source_host = extract_host(database_url);

    // Determine dump filename based on format
    let dump_filename = format.dump_filename();
    let dump_path = snap_dir.join(dump_filename);

    // Build pg_dump command
    let mut cmd = Command::new(&pg_dump_path);

    match format {
        SnapshotFormat::Custom => {
            cmd.arg("--format=custom");
        }
        SnapshotFormat::Plain => {
            cmd.arg("--format=plain");
        }
    }

    cmd.arg("--file").arg(&dump_path);

    if no_owner {
        cmd.arg("--no-owner");
    }

    if no_privileges {
        cmd.arg("--no-acl");
    }

    if let Some(p) = effective_profile.as_ref() {
        if let Some(ref schemas) = p.schemas {
            for s in schemas {
                cmd.arg("-n").arg(s);
            }
        }
        if let Some(ref exclude_schemas) = p.exclude_schemas {
            for s in exclude_schemas {
                cmd.arg("-N").arg(s);
            }
        }
        if let Some(ref tables) = p.tables {
            for t in tables {
                cmd.arg("-t").arg(t);
            }
        }
        if let Some(ref exclude_tables) = p.exclude_tables {
            for t in exclude_tables {
                cmd.arg("-T").arg(t);
            }
        }
        if !p.data {
            cmd.arg("--schema-only");
        }
    }

    if verbose {
        cmd.arg("--verbose");
    }

    cmd.arg(database_url);

    if verbose && !quiet {
        let mut args_display = vec!["pg_dump".to_string(), format!("--format={}", format)];
        if no_owner {
            args_display.push("--no-owner".to_string());
        }
        if no_privileges {
            args_display.push("--no-acl".to_string());
        }
        if let Some(p) = profile_name {
            args_display.push(format!("--profile={}", p));
        }
        println!("\nRunning: {} ...", args_display.join(" "));
    }

    if dry_run {
        println!("\n{}", "Dry run: No snapshot was created.".blue());
        return Ok(());
    }

    // Create snapshot directory
    fs::create_dir_all(&snap_dir)?;

    let output = cmd.output().await?;

    if !output.status.success() {
        // Cleanup on failure
        let _ = fs::remove_dir_all(&snap_dir);
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("pg_dump failed:\n{}", stderr);
    }

    if verbose && !quiet {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stdout.is_empty() {
            println!("{}", stdout);
        }
        if !stderr.is_empty() {
            // pg_dump --verbose writes to stderr
            eprintln!("{}", stderr);
        }
    }

    // Get dump file size
    let size_bytes = match fs::metadata(&dump_path) {
        Ok(m) => m.len(),
        Err(e) => {
            let _ = fs::remove_dir_all(&snap_dir);
            bail!("Failed to get dump file size: {}", e);
        }
    };

    let metadata = SnapshotMetadata::new(
        name,
        &parsed.database_name,
        latest_version,
        applied_count,
        size_bytes,
        message.map(|s| s.to_string()),
        source_host,
        pg_version,
        pg_dump_version,
        owner_roles,
        format,
        !no_owner,
        !no_privileges,
        profile_name.map(|s| s.to_string()),
        effective_profile.as_ref().and_then(|p| p.schemas.clone()),
        effective_profile
            .as_ref()
            .and_then(|p| p.exclude_schemas.clone()),
        effective_profile.as_ref().and_then(|p| p.tables.clone()),
        effective_profile
            .as_ref()
            .and_then(|p| p.exclude_tables.clone()),
        effective_profile.as_ref().map(|p| p.data).unwrap_or(true),
    );

    if let Err(e) = metadata.save(&snap_dir) {
        let _ = fs::remove_dir_all(&snap_dir);
        bail!("Failed to save snapshot metadata: {}", e);
    }

    if let Err(e) = metadata.save(&snap_dir) {
        let _ = fs::remove_dir_all(&snap_dir);
        bail!("Failed to save snapshot metadata: {}", e);
    }

    if !quiet {
        println!(
            "\n{}",
            format!("Snapshot saved: {} ({})", name, metadata.format_size()).green()
        );
    }

    Ok(())
}

/// Get PostgreSQL server version
async fn get_pg_version(client: &Client) -> Result<String> {
    let row = client.query_one("SHOW server_version", &[]).await?;
    Ok(row.get(0))
}

/// Get roles that own objects in user schemas (for pre-flight checking)
async fn get_owner_roles(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            r#"
            SELECT DISTINCT r.rolname
            FROM pg_roles r
            JOIN pg_class c ON c.relowner = r.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND n.nspname NOT LIKE 'pg_%'
            ORDER BY r.rolname
            "#,
            &[],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Restore database from a snapshot
#[allow(clippy::too_many_arguments)]
pub async fn snapshot_restore(
    target_database_url: &str,
    name: &str,
    config: &Config,
    quiet: bool,
    verbose: bool,
    yes: bool,
    dry_run: bool,
    no_owner: bool,
) -> Result<()> {
    // Validate snapshot name (prevents path traversal)
    validate_snapshot_name(name)?;

    // Get snapshot directory override
    let snap_dir_override = Some(config.snapshot_dir());

    // Check if snapshot exists
    if !snapshot_exists(name, snap_dir_override) {
        let available = snapshot::available_snapshots(snap_dir_override);
        let hint = if available.is_empty() {
            "No snapshots available.".to_string()
        } else {
            format!("Available snapshots: {}", available.join(", "))
        };
        bail!("Snapshot \"{}\" not found.\n{}", name, hint);
    }

    // Load metadata
    let snap_dir = snapshot_dir(name, snap_dir_override);
    let metadata = SnapshotMetadata::load(&snap_dir)?;

    // Check appropriate restore tool exists based on format
    let pg_restore_path = config.tool_path("pg_restore");
    let psql_path = config.tool_path("psql");
    match metadata.format {
        SnapshotFormat::Custom => check_pg_restore(&pg_restore_path)?,
        SnapshotFormat::Plain => check_psql(&psql_path)?,
    }

    let parsed = parse_database_url(target_database_url)?;

    // Connect to target to check version and roles (for dry-run and pre-flight)
    let target_client = connect(target_database_url).await.ok();
    let target_pg_version = if let Some(ref client) = target_client {
        get_pg_version(client).await.ok()
    } else {
        None
    };

    // Check for version downgrade warning
    let version_warning = match (&metadata.pg_version, &target_pg_version) {
        (Some(snap_ver), Some(target_ver)) => {
            if should_warn_version_downgrade(snap_ver, target_ver) {
                Some(format!(
                    "Snapshot was created with PostgreSQL {}, target is {}.\n\
                     Downgrading may fail if the snapshot uses features not in PostgreSQL {}.",
                    snap_ver, target_ver, target_ver
                ))
            } else {
                None
            }
        }
        _ => None,
    };

    // Warn if --no-owner used with plain format that has ownership
    // psql cannot skip OWNER TO statements that are baked into the SQL file
    let plain_owner_warning =
        if no_owner && metadata.format == SnapshotFormat::Plain && metadata.include_owner {
            Some(
                "Warning: --no-owner has no effect for plain format snapshots.\n\
             Ownership statements are embedded in the SQL file and will be executed.\n\
             Hint: Save snapshots with --no-owner to exclude ownership, or use --format custom."
                    .to_string(),
            )
        } else {
            None
        };

    // Role pre-flight check (unless --no-owner)
    let missing_roles = if !no_owner && metadata.include_owner && !metadata.owner_roles.is_empty() {
        if let Some(ref client) = target_client {
            check_missing_roles(client, &metadata.owner_roles)
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Dry run: show what would happen and exit
    if dry_run {
        println!("Would restore snapshot: {}", name);
        println!();
        println!(
            "  Created:     {}",
            metadata.created_at.format("%Y-%m-%d %H:%M:%S")
        );
        println!("  Format:      {}", metadata.format);
        println!("  Size:        {}", metadata.format_size());
        println!("  Migrations:  {} applied", metadata.applied_migrations);
        if let Some(ref msg) = metadata.message {
            println!("  Message:     {}", msg);
        }
        println!();

        // Show version warning
        if let Some(ref warning) = version_warning {
            println!("{}", format!("Warning: {}", warning).yellow());
            println!();
        }

        // Show plain format --no-owner warning
        if let Some(ref warning) = plain_owner_warning {
            println!("{}", warning.yellow());
            println!();
        }

        // Show role pre-flight results (skip if --no-owner since we're ignoring roles)
        if !no_owner && !metadata.owner_roles.is_empty() && metadata.include_owner {
            println!("Owner roles referenced by snapshot:");
            for role in &metadata.owner_roles {
                if missing_roles.contains(role) {
                    println!("  {} {} (missing)", "✗".red(), role);
                } else {
                    println!("  {} {} (exists)", "✓".green(), role);
                }
            }
            println!();
        }

        if !missing_roles.is_empty() {
            println!(
                "{}",
                format!(
                    "Warning: {} role(s) missing. Use --no-owner to skip role assignment.",
                    missing_roles.len()
                )
                .yellow()
            );
            println!();
        }

        println!(
            "{}",
            format!(
                "This will DROP and recreate database: {}",
                parsed.database_name
            )
            .yellow()
        );
        println!("Run with --yes to proceed.");
        return Ok(());
    }

    // Role pre-flight error (blocking unless --no-owner)
    if !missing_roles.is_empty() && !no_owner {
        let missing_list = missing_roles.join(", ");
        bail!(
            "Role(s) \"{}\" do not exist on target database.\n\n\
             Options:\n\
             1. Create the missing role(s):\n\
                CREATE ROLE {} WITH LOGIN;\n\n\
             2. Restore without ownership:\n\
                pgcrate snapshot restore {} --no-owner --yes",
            missing_list,
            missing_roles.first().unwrap_or(&"<role>".to_string()),
            name
        );
    }

    // Require --yes flag
    if !yes {
        bail!(
            "Restoring a snapshot requires --yes flag to confirm.\n\
             This will DROP and recreate database: {}",
            parsed.database_name
        );
    }

    // Warn about production patterns
    if url_matches_production_patterns(target_database_url, config) {
        eprintln!(
            "{}",
            "⚠️  WARNING: URL matches production patterns. This is destructive!".yellow()
        );
    }

    // Show version warning (non-blocking)
    if let Some(ref warning) = version_warning {
        eprintln!("{}", format!("Warning: {}", warning).yellow());
        eprintln!();
    }

    // Show plain format --no-owner warning (non-blocking)
    if let Some(ref warning) = plain_owner_warning {
        eprintln!("{}", warning.yellow());
        eprintln!();
    }

    if !quiet {
        println!("Restoring snapshot: {}", name);
    }

    // Connect to admin database (postgres)
    let admin_client = connect(&parsed.admin_url).await?;

    // Terminate active connections to target database
    if !quiet {
        println!("  Terminating connections to {}...", parsed.database_name);
    }

    let terminated = terminate_connections(&admin_client, &parsed.database_name).await?;
    if verbose && !quiet && terminated > 0 {
        println!("    Terminated {} connection(s)", terminated);
    }

    // Drop existing database
    if !quiet {
        println!("  Dropping database {}...", parsed.database_name);
    }
    let drop_sql = format!(
        "DROP DATABASE IF EXISTS {}",
        quote_ident(&parsed.database_name)
    );
    admin_client.batch_execute(&drop_sql).await?;

    // Create fresh database
    if !quiet {
        println!("  Creating database {}...", parsed.database_name);
    }
    let create_sql = format!("CREATE DATABASE {}", quote_ident(&parsed.database_name));
    admin_client.batch_execute(&create_sql).await?;

    // Restore based on format
    if !quiet {
        println!("  Restoring data...");
    }

    let dump_path = snap_dir.join(metadata.format.dump_filename());
    if !dump_path.exists() {
        // Check for legacy dump.pgdump if format says custom but metadata might be old
        let legacy_path = snap_dir.join("dump.pgdump");
        if legacy_path.exists() {
            // Use legacy path
            restore_custom_format(
                &legacy_path,
                target_database_url,
                verbose,
                quiet,
                no_owner,
                &pg_restore_path,
            )
            .await?;
        } else {
            bail!(
                "Snapshot \"{}\" is incomplete: {} is missing.\n\
                 Delete it with: pgcrate snapshot delete {}",
                name,
                metadata.format.dump_filename(),
                name
            );
        }
    } else {
        match metadata.format {
            SnapshotFormat::Custom => {
                restore_custom_format(
                    &dump_path,
                    target_database_url,
                    verbose,
                    quiet,
                    no_owner,
                    &pg_restore_path,
                )
                .await?;
            }
            SnapshotFormat::Plain => {
                restore_plain_format(&dump_path, target_database_url, verbose, quiet, &psql_path)
                    .await?;
            }
        }
    }

    // Report success
    if !quiet {
        println!();
        println!("{}", "Snapshot restored successfully.".green());
        if let Some(ref version) = metadata.migration_version {
            println!(
                "Migration state: {} applied (version {})",
                metadata.applied_migrations, version
            );
        } else {
            println!("Migration state: {} applied", metadata.applied_migrations);
        }
    }

    Ok(())
}

/// Restore from custom format using pg_restore
async fn restore_custom_format(
    dump_path: &Path,
    database_url: &str,
    verbose: bool,
    quiet: bool,
    no_owner: bool,
    pg_restore_path: &str,
) -> Result<()> {
    let mut cmd = Command::new(pg_restore_path);
    cmd.arg("--dbname").arg(database_url);

    if no_owner {
        cmd.arg("--no-owner");
    }

    if verbose {
        cmd.arg("--verbose");
    }

    cmd.arg(dump_path);

    let output = cmd.output().await?;
    let stderr = String::from_utf8_lossy(&output.stderr);

    // pg_restore exits non-zero for warnings (e.g., "role does not exist") but still
    // restores successfully. Only fail on connection errors (exit code 1 with no data restored).
    // We detect this by checking if pg_restore couldn't even connect.
    if !output.status.success() {
        // Connection failures are fatal - pg_restore couldn't even start
        let is_connection_error = output.status.code() == Some(1)
            && (stderr.contains("could not connect")
                || stderr.contains("connection refused")
                || stderr.contains("no pg_hba.conf entry"));

        if is_connection_error {
            bail!(
                "pg_restore failed: could not connect to database\n{}",
                stderr
            );
        }

        // Non-connection errors: show as warnings (restore likely succeeded with caveats)
        // Filter out known harmless warnings from version mismatches
        let filtered_warnings: Vec<&str> = stderr
            .lines()
            .filter(|line| !is_harmless_restore_warning(line))
            .collect();

        if !quiet && !filtered_warnings.is_empty() {
            eprintln!("{}", "pg_restore completed with warnings:".yellow());
            for line in filtered_warnings {
                eprintln!("  {}", line);
            }
        }
    } else if verbose && !quiet && !stderr.is_empty() {
        eprintln!("{}", stderr);
    }

    Ok(())
}

/// Restore from plain format using psql
async fn restore_plain_format(
    dump_path: &Path,
    database_url: &str,
    verbose: bool,
    quiet: bool,
    psql_path: &str,
) -> Result<()> {
    let mut cmd = Command::new(psql_path);
    cmd.arg(database_url).arg("-f").arg(dump_path);

    if !verbose {
        cmd.arg("-q"); // Quiet mode unless verbose
    }

    let output = cmd.output().await?;
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "psql failed (exit code {}):\n{}",
            output.status.code().unwrap_or(-1),
            stderr
        );
    }

    if verbose && !quiet && !stderr.is_empty() {
        eprintln!("{}", stderr);
    }

    Ok(())
}

/// Check which roles from the list are missing on the target database
async fn check_missing_roles(client: &Client, roles: &[String]) -> Result<Vec<String>> {
    if roles.is_empty() {
        return Ok(Vec::new());
    }

    let existing: HashSet<String> = client
        .query(
            "SELECT rolname FROM pg_roles WHERE rolname = ANY($1)",
            &[&roles],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();

    Ok(roles
        .iter()
        .filter(|r| !existing.contains(*r))
        .cloned()
        .collect())
}

/// List all snapshots
pub fn snapshot_list(config: &Config, quiet: bool, json: bool) -> Result<()> {
    let snap_dir_override = Some(config.snapshot_dir());
    let snapshots = snapshot::list_snapshots(snap_dir_override)?;

    // JSON output
    if json {
        #[derive(serde::Serialize)]
        struct SnapshotListResponse {
            ok: bool,
            snapshots: Vec<SnapshotSummary>,
            total_size_bytes: u64,
        }
        #[derive(serde::Serialize)]
        struct SnapshotSummary {
            name: String,
            created_at: String,
            size_bytes: u64,
            applied_migrations: usize,
            message: Option<String>,
        }

        let total_size: u64 = snapshots.iter().map(|s| s.size_bytes).sum();
        let response = SnapshotListResponse {
            ok: true,
            snapshots: snapshots
                .iter()
                .map(|s| SnapshotSummary {
                    name: s.name.clone(),
                    created_at: s.created_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                    size_bytes: s.size_bytes,
                    applied_migrations: s.applied_migrations,
                    message: s.message.clone(),
                })
                .collect(),
            total_size_bytes: total_size,
        };
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    if snapshots.is_empty() {
        if !quiet {
            println!("No snapshots found.");
            println!();
            println!("Create one with: pgcrate snapshot save <name>");
        }
        return Ok(());
    }

    let snapshots_path = snapshots_dir(snap_dir_override);

    println!("Snapshots ({}/)", snapshots_path.display());
    println!();

    // Calculate column widths
    let name_width = snapshots
        .iter()
        .map(|s| s.name.len())
        .max()
        .unwrap_or(12)
        .max(12);
    let msg_width = 40;

    // Print header
    println!(
        "{:<name_width$}  {:<19}  {:>9}  {:>10}  MESSAGE",
        "NAME", "CREATED", "SIZE", "MIGRATIONS"
    );

    // Print rows
    for snap in &snapshots {
        let created = snap.created_at.format("%Y-%m-%d %H:%M:%S").to_string();
        let message = snap
            .message
            .as_ref()
            .map(|m| {
                if m.len() > msg_width {
                    format!("{}...", &m[..msg_width - 3])
                } else {
                    m.clone()
                }
            })
            .unwrap_or_default();

        println!(
            "{:<name_width$}  {:<19}  {:>9}  {:>10}  {}",
            snap.name,
            created,
            snap.format_size(),
            snap.applied_migrations,
            message
        );
    }

    // Print summary
    let total_size: u64 = snapshots.iter().map(|s| s.size_bytes).sum();
    println!();
    println!(
        "{} snapshot{}, {} total",
        snapshots.len(),
        if snapshots.len() == 1 { "" } else { "s" },
        snapshot::format_bytes(total_size)
    );

    Ok(())
}

/// Show detailed information about a snapshot
pub fn snapshot_info(name: &str, config: &Config, quiet: bool, json: bool) -> Result<()> {
    // Validate snapshot name (prevents path traversal)
    validate_snapshot_name(name)?;

    // Get snapshot directory override
    let snap_dir_override = Some(config.snapshot_dir());

    // Check if snapshot exists
    if !snapshot_exists(name, snap_dir_override) {
        let available = snapshot::available_snapshots(snap_dir_override);
        let hint = if available.is_empty() {
            "No snapshots available.".to_string()
        } else {
            format!("Available snapshots: {}", available.join(", "))
        };
        bail!("Snapshot \"{}\" not found.\n{}", name, hint);
    }

    // Load metadata
    let snap_dir = snapshot_dir(name, snap_dir_override);
    let metadata = SnapshotMetadata::load(&snap_dir)?;

    if json {
        // JSON output: print the full metadata
        let json_output = serde_json::to_string_pretty(&metadata)?;
        println!("{}", json_output);
        return Ok(());
    }

    // Human-readable output
    if !quiet {
        println!("Snapshot: {}", metadata.name);
        println!();

        // Basic info
        println!(
            "Created:     {}",
            metadata.created_at.format("%Y-%m-%d %H:%M:%S")
        );
        println!("Database:    {}", metadata.database);
        if let Some(ref host) = metadata.source_host {
            println!("Source:      {}", host);
        }
        println!("Format:      {}", metadata.format);
        println!("Size:        {}", metadata.format_size());
        if let Some(ref msg) = metadata.message {
            println!("Message:     {}", msg);
        }
        println!();

        // Versions (only show if available)
        let has_version_info = metadata.pg_version.is_some() || metadata.pg_dump_version.is_some();

        if has_version_info {
            println!("Versions:");
            if let Some(ref ver) = metadata.pg_version {
                println!("  PostgreSQL:  {}", ver);
            }
            if let Some(ref ver) = metadata.pg_dump_version {
                println!("  pg_dump:     {}", ver);
            }
            println!("  pgcrate:     {}", metadata.pgcrate_version);
            println!();
        }

        // Options
        println!("Options:");
        println!(
            "  Owner:       {}",
            if metadata.include_owner {
                "included"
            } else {
                "excluded"
            }
        );
        println!(
            "  Privileges:  {}",
            if metadata.include_privileges {
                "included"
            } else {
                "excluded"
            }
        );
        println!();

        // Owner roles (if any)
        if !metadata.owner_roles.is_empty() {
            println!("Owner Roles:");
            println!("  {}", metadata.owner_roles.join(", "));
            println!();
        }

        // Migration state
        println!("Migration State:");
        if let Some(ref version) = metadata.migration_version {
            println!("  Version:   {}", version);
        }
        println!("  Applied:   {} migrations", metadata.applied_migrations);
    }

    Ok(())
}

/// Delete a snapshot
pub fn snapshot_delete(name: &str, config: &Config, quiet: bool, yes: bool) -> Result<()> {
    // Validate snapshot name (prevents path traversal)
    validate_snapshot_name(name)?;

    // Get snapshot directory override
    let snap_dir_override = Some(config.snapshot_dir());

    // Check if snapshot directory exists (allow deleting incomplete snapshots)
    let snap_dir = snapshot_dir(name, snap_dir_override);
    if !snap_dir.exists() {
        let available = snapshot::available_snapshots(snap_dir_override);
        let hint = if available.is_empty() {
            "No snapshots available.".to_string()
        } else {
            format!("Available snapshots: {}", available.join(", "))
        };
        bail!("Snapshot \"{}\" not found.\n{}", name, hint);
    }

    // Try to load metadata (may not exist for incomplete snapshots)
    let metadata = SnapshotMetadata::load(&snap_dir).ok();
    let is_incomplete = metadata.is_none();

    // Confirm deletion unless --yes provided
    if !yes {
        let size_hint = metadata
            .as_ref()
            .map(|m| m.format_size())
            .unwrap_or_else(|| "incomplete".to_string());
        print!("Delete snapshot \"{}\" ({})? [y/N] ", name, size_hint);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            if !quiet {
                println!("Cancelled.");
            }
            return Ok(());
        }
    }

    // Delete snapshot directory
    fs::remove_dir_all(&snap_dir)?;

    if !quiet {
        if is_incomplete {
            println!("{}", "Incomplete snapshot deleted.".green());
        } else {
            println!("{}", "Snapshot deleted.".green());
        }
    }

    Ok(())
}

/// Terminate all connections to a database (except self)
async fn terminate_connections(client: &Client, dbname: &str) -> Result<usize> {
    let rows = client
        .execute(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = $1 AND pid <> pg_backend_pid()",
            &[&dbname],
        )
        .await?;

    Ok(rows as usize)
}

/// Get migration state (count and latest version)
async fn get_migration_state(client: &Client) -> Result<(usize, Option<String>)> {
    // Check if schema_migrations table exists
    let table_exists = client
        .query_one(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'pgcrate' AND table_name = 'schema_migrations'
            ) AS exists",
            &[],
        )
        .await?
        .get::<_, bool>("exists");

    if !table_exists {
        return Ok((0, None));
    }

    // Get count and latest version
    let row = client
        .query_one(
            "SELECT COUNT(*) AS count,
                    MAX(version) AS latest
             FROM pgcrate.schema_migrations",
            &[],
        )
        .await?;

    let count: i64 = row.get("count");
    let latest: Option<String> = row.get("latest");

    Ok((count as usize, latest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_harmless_restore_warning_transaction_timeout() {
        let line = "pg_restore: error: could not execute query: ERROR:  unrecognized configuration parameter \"transaction_timeout\"";
        assert!(is_harmless_restore_warning(line));
    }

    #[test]
    fn test_is_harmless_restore_warning_set_command() {
        let line = "Command was: SET transaction_timeout = 0;";
        assert!(is_harmless_restore_warning(line));
    }

    #[test]
    fn test_is_harmless_restore_warning_errors_ignored() {
        let line = "pg_restore: warning: errors ignored on restore: 1";
        assert!(is_harmless_restore_warning(line));
    }

    #[test]
    fn test_is_harmless_restore_warning_real_error() {
        let line =
            "pg_restore: error: could not execute query: ERROR:  relation \"users\" already exists";
        assert!(!is_harmless_restore_warning(line));
    }

    #[test]
    fn test_is_harmless_restore_warning_idle_session_timeout() {
        let line = "pg_restore: error: could not execute query: ERROR:  unrecognized configuration parameter \"idle_session_timeout\"";
        assert!(is_harmless_restore_warning(line));
    }
}
