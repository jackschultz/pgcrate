//! Schema commands for pgcrate CLI.
//!
//! Commands for working with database schemas: init, generate, diff, describe.

use crate::config::Config;
use crate::describe;
use crate::diff::{self, format_diff};
use crate::introspect::{self, GeneratedFile, IntrospectOptions, SplitMode};
use crate::output::{DiffResponse, DiffSummaryJson, Output};
use crate::sql::quote_ident;
use anyhow::{bail, Result};
use chrono::Utc;
use colored::Colorize;
use dialoguer::{Confirm, Input};
use std::fs;
use std::path::Path;

use super::connect;

/// Normalize a path: remove leading ./ and trailing /
fn normalize_path(path: &str) -> String {
    let mut p = path.trim().to_string();
    while p.starts_with("./") {
        p = p[2..].to_string();
    }
    while p.ends_with('/') {
        p.pop();
    }
    p
}

/// Configuration collected during init
struct InitConfig {
    migrations_dir: String,
    use_models: bool,
    models_dir: String,
    use_seeds: bool,
    seeds_dir: String,
    seeds_schema: String,
}

impl InitConfig {
    fn generate_toml(&self) -> String {
        let mut config = String::new();
        config.push_str("# pgcrate configuration\n");
        config.push_str("# See: https://github.com/jackschultz/pgcrate\n\n");

        config.push_str("[paths]\n");
        config.push_str(&format!("migrations = \"{}\"\n", self.migrations_dir));
        if self.use_models {
            config.push_str(&format!("models = \"{}\"\n", self.models_dir));
        }
        if self.use_seeds {
            config.push_str(&format!("seeds = \"{}\"\n", self.seeds_dir));
        }

        config.push_str("\n[defaults]\n");
        config.push_str("with_down = true\n");

        if self.use_seeds {
            config.push_str(&format!("\n[seeds]\nschema = \"{}\"\n", self.seeds_schema));
        }

        config
    }

    fn directories(&self) -> Vec<&str> {
        let mut dirs = vec![self.migrations_dir.as_str()];
        if self.use_models {
            dirs.push(self.models_dir.as_str());
        }
        if self.use_seeds {
            dirs.push(self.seeds_dir.as_str());
        }
        dirs
    }
}

#[allow(clippy::too_many_arguments)]
pub fn init(
    yes: bool,
    dry_run: bool,
    force: bool,
    quiet: bool,
    migrations_dir: &str,
    models: bool,
    models_dir: &str,
    seeds: bool,
    seeds_dir: &str,
    seeds_schema: &str,
) -> Result<()> {
    let config_path = Path::new("pgcrate.toml");

    // Check if config already exists
    if config_path.exists() && !force {
        bail!("pgcrate.toml already exists. Use --force to overwrite, or edit the file directly.");
    }

    // Collect configuration - either interactively or from flags
    let init_config = if yes {
        // Non-interactive mode: use flags directly
        InitConfig {
            migrations_dir: normalize_path(migrations_dir),
            use_models: models,
            models_dir: normalize_path(models_dir),
            use_seeds: seeds,
            seeds_dir: normalize_path(seeds_dir),
            seeds_schema: seeds_schema.to_string(),
        }
    } else {
        // Interactive mode
        collect_config_interactive(migrations_dir, models_dir, seeds_dir, seeds_schema)?
    };

    // Show what will be created
    if !quiet && !yes {
        println!("\n{}", "Will create:".bold());
        println!("  pgcrate.toml");
        for dir in init_config.directories() {
            println!("  {}/", dir);
        }
        println!();

        if !dry_run {
            let proceed = Confirm::new()
                .with_prompt("Proceed?")
                .default(true)
                .interact()?;

            if !proceed {
                println!("Aborted.");
                return Ok(());
            }
        }
    }

    // Dry run - just show what would be created
    if dry_run {
        if !quiet {
            println!("\n{}", "Would create pgcrate.toml:".bold());
            println!("{}", init_config.generate_toml());
            println!("{}", "Dry run complete. No files created.".yellow());
        }
        return Ok(());
    }

    // Create config file
    let toml_content = init_config.generate_toml();
    fs::write(config_path, &toml_content)?;
    if !quiet {
        println!("Created: {}", "pgcrate.toml".green());
    }

    // Create directories
    for dir in init_config.directories() {
        let dir_path = Path::new(dir);
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)?;
            if !quiet {
                println!("Created: {}/", dir.green());
            }
        }

        // Add .gitkeep if directory is empty
        let gitkeep = dir_path.join(".gitkeep");
        if !gitkeep.exists() {
            fs::write(&gitkeep, "")?;
        }
    }

    // Show next steps
    if !quiet {
        println!("\n{}", "Project initialized.".green().bold());
        println!("\nNext steps:");
        println!("  1. export DATABASE_URL=\"postgres://localhost/mydb\"");
        println!("  2. pgcrate db create");
        println!("  3. pgcrate new create_users");
    }

    Ok(())
}

fn collect_config_interactive(
    default_migrations: &str,
    default_models: &str,
    default_seeds: &str,
    default_seeds_schema: &str,
) -> Result<InitConfig> {
    // Migrations directory
    let migrations_dir: String = Input::new()
        .with_prompt("Migrations directory")
        .default(default_migrations.to_string())
        .interact_text()?;

    // Seeds (asked before models since more common)
    let use_seeds = Confirm::new()
        .with_prompt("Use seeds (reference data)?")
        .default(false)
        .interact()?;

    let (seeds_dir, seeds_schema) = if use_seeds {
        let dir: String = Input::new()
            .with_prompt("Seeds directory")
            .default(default_seeds.to_string())
            .interact_text()?;
        let schema: String = Input::new()
            .with_prompt("Seeds schema (created in database)")
            .default(default_seeds_schema.to_string())
            .interact_text()?;
        (dir, schema)
    } else {
        (default_seeds.to_string(), default_seeds_schema.to_string())
    };

    // Models
    let use_models = Confirm::new()
        .with_prompt("Use models (SQL transformations)?")
        .default(false)
        .interact()?;

    let models_dir = if use_models {
        Input::new()
            .with_prompt("Models directory")
            .default(default_models.to_string())
            .interact_text()?
    } else {
        default_models.to_string()
    };

    Ok(InitConfig {
        migrations_dir: normalize_path(&migrations_dir),
        use_models,
        models_dir: normalize_path(&models_dir),
        use_seeds,
        seeds_dir: normalize_path(&seeds_dir),
        seeds_schema,
    })
}

// =============================================================================
// Generate
// =============================================================================

#[allow(clippy::too_many_arguments)] // CLI handler - each arg maps to a CLI flag
pub async fn generate(
    database_url: &str,
    config: &Config,
    quiet: bool,
    split_by: Option<&str>,
    output: Option<&Path>,
    dry_run: bool,
    include_schemas: &[String],
    exclude_schemas: &[String],
) -> Result<(), anyhow::Error> {
    let client = connect(database_url).await?;

    // Build introspect options - CLI overrides config
    let introspect_options = IntrospectOptions {
        include_schemas: if include_schemas.is_empty() {
            config.generate_include_schemas()
        } else {
            include_schemas.to_vec()
        },
        exclude_schemas: if exclude_schemas.is_empty() {
            config.generate_exclude_schemas()
        } else {
            exclude_schemas.to_vec()
        },
    };

    // Determine split mode - CLI overrides config
    let split_mode = match split_by.or(config.generate_split_by()) {
        Some("schema") => SplitMode::Schema,
        Some("table") => SplitMode::Table,
        Some("none") | None => SplitMode::None,
        Some(other) => {
            bail!(
                "Invalid split_by mode '{}'. Expected: none, schema, or table",
                other
            );
        }
    };

    // Determine output directory - CLI overrides config
    let output_dir = output
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| config.generate_output().to_string());

    if !quiet {
        println!("Introspecting database schema...");
    }

    // Introspect the database
    let schema = introspect::introspect(&client, &introspect_options).await?;

    // Check if schema is empty
    if schema.tables.is_empty()
        && schema.views.is_empty()
        && schema.functions.is_empty()
        && schema.enums.is_empty()
    {
        if !quiet {
            println!(
                "{}",
                "No objects found in database (excluding system schemas).".yellow()
            );
        }
        return Ok(());
    }

    // Generate files
    let base_time = Utc::now();
    let files = introspect::generate_files(&schema, split_mode, base_time, database_url);

    if dry_run {
        print_dry_run_output(&files, &output_dir, quiet);
    } else {
        write_generated_files(&files, &output_dir, quiet)?;
    }

    Ok(())
}

fn print_dry_run_output(files: &[GeneratedFile], output_dir: &str, quiet: bool) {
    if quiet {
        return;
    }

    println!("\n{}", "Would create:".yellow());

    for file in files {
        let size = file.content.len();
        let size_str = if size >= 1024 {
            format!("~{} KB", size / 1024)
        } else {
            format!("~{} bytes", size)
        };

        // Build concise summary: focus on main objects, skip zero counts
        let s = &file.stats;
        let mut parts = Vec::new();

        if s.table_count > 0 {
            parts.push(format!("{} tables", s.table_count));
        }
        if s.view_count > 0 {
            parts.push(format!("{} views", s.view_count));
        }
        if s.function_count > 0 {
            parts.push(format!("{} functions", s.function_count));
        }

        let summary = if parts.is_empty() {
            size_str
        } else {
            format!("{}, {}", parts.join(", "), size_str)
        };

        println!("  {}/{} ({})", output_dir, file.filename, summary);
    }

    println!("\n{}", "Dry run complete. No files written.".blue());
}

fn write_generated_files(
    files: &[GeneratedFile],
    output_dir: &str,
    quiet: bool,
) -> Result<(), anyhow::Error> {
    let dir = Path::new(output_dir);

    // Create directory if it doesn't exist
    fs::create_dir_all(dir)?;

    // Check for file conflicts before writing anything
    let mut conflicts: Vec<String> = Vec::new();
    for file in files {
        let path = dir.join(&file.filename);
        if path.exists() {
            conflicts.push(file.filename.clone());
        }
    }

    if !conflicts.is_empty() {
        bail!(
            "File conflict: {} file(s) already exist in {}:\n  - {}\n\nHint: Delete existing files or use --output with a different directory.",
            conflicts.len(),
            output_dir,
            conflicts.join("\n  - ")
        );
    }

    // Write all files
    if !quiet {
        println!("\n{}", "Creating migration files:".green());
    }

    for file in files {
        let path = dir.join(&file.filename);
        fs::write(&path, &file.content)?;
        if !quiet {
            println!("  Created: {}", path.display().to_string().green());
        }
    }

    if !quiet {
        println!(
            "\n{}",
            format!("Generated {} migration file(s).", files.len()).green()
        );
    }

    Ok(())
}

// =============================================================================
// Diff
// =============================================================================

/// Extract database name from URL for display
fn extract_db_name(url: &str) -> String {
    // Handle various postgres URL formats:
    // - postgres://user:pass@host:port/dbname?params
    // - postgres:///dbname (Unix socket, no host)
    // - postgres://host/dbname

    // Find the path portion after the authority (host:port)
    // First, skip the scheme (postgres:// or postgresql://)
    let after_scheme = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .unwrap_or(url);

    // Find the database name - it's after the last '/' but before any '?'
    if let Some(slash_pos) = after_scheme.rfind('/') {
        let rest = &after_scheme[slash_pos + 1..];
        let db_name = rest.split('?').next().unwrap_or(rest);
        if !db_name.is_empty() {
            return db_name.to_string();
        }
    }

    // Fallback: return a safe placeholder (never expose full URL which may contain credentials)
    "<unknown-db>".to_string()
}

/// Compare two database schemas and report differences.
/// Returns exit code: 0 = identical, 1 = differs, 2 = error
pub async fn diff(
    from_url: &str,
    to_url: &str,
    output: &Output,
    include_schemas: &[String],
    exclude_schemas: &[String],
) -> Result<i32, anyhow::Error> {
    // Build introspect options
    let options = IntrospectOptions {
        include_schemas: include_schemas.to_vec(),
        exclude_schemas: exclude_schemas.to_vec(),
    };

    // Progress messages go to stderr in human mode, suppressed in JSON mode
    output.verbose(&"Connecting to source database...".dimmed().to_string());

    // Connect to both databases
    let from_client = connect(from_url).await?;

    output.verbose(&"Connecting to target database...".dimmed().to_string());

    let to_client = connect(to_url).await?;

    output.verbose(&"Introspecting schemas...".dimmed().to_string());

    // Introspect both databases
    let from_schema = introspect::introspect(&from_client, &options).await?;
    let to_schema = introspect::introspect(&to_client, &options).await?;

    // Compare schemas
    let schema_diff = diff::diff_schemas(&from_schema, &to_schema);

    // Determine exit code
    let exit_code = if schema_diff.is_empty() { 0 } else { 1 };

    // JSON mode: structured output to stdout
    if output.is_json() {
        let summary = schema_diff.summary();
        let from_label = extract_db_name(from_url);
        let to_label = extract_db_name(to_url);

        // Include formatted diff as text for convenience (without ANSI colors)
        let formatted = if schema_diff.is_empty() {
            None
        } else {
            // Strip ANSI codes by using a plain format
            Some(format_diff_plain(&schema_diff, &from_label, &to_label))
        };

        let response = DiffResponse {
            ok: true,
            identical: schema_diff.is_empty(),
            summary: DiffSummaryJson::from(&summary),
            formatted_diff: formatted,
        };
        output.json(&response)?;
        return Ok(exit_code);
    }

    // Human mode
    if output.is_quiet() {
        // Quiet mode: no output, just exit code
        return Ok(exit_code);
    }

    if schema_diff.is_empty() {
        println!("{}", "Schemas are identical.".green());
        return Ok(0);
    }

    // Format and print diff
    let from_label = extract_db_name(from_url);
    let to_label = extract_db_name(to_url);
    let formatted = format_diff(&schema_diff, &from_label, &to_label);
    println!("{}", formatted);

    Ok(exit_code)
}

/// Format diff without ANSI color codes (for JSON output)
fn format_diff_plain(diff: &diff::SchemaDiff, from_label: &str, to_label: &str) -> String {
    let mut output = Vec::new();

    output.push(format!("Comparing: {} → {}", from_label, to_label));
    output.push(String::new());
    output.push("Legend:".to_string());
    output.push("  + exists in TARGET (--to) only".to_string());
    output.push("  - exists in SOURCE (--from) only".to_string());
    output.push("  ~ exists in both but differs".to_string());

    // Tables
    if !diff.added_tables.is_empty()
        || !diff.removed_tables.is_empty()
        || !diff.modified_tables.is_empty()
    {
        output.push(String::new());
        output.push("Tables:".to_string());
        for table in &diff.added_tables {
            output.push(format!("  + {}.{}", table.schema, table.name));
        }
        for table in &diff.removed_tables {
            output.push(format!("  - {}.{}", table.schema, table.name));
        }
        for table in &diff.modified_tables {
            output.push(format!("  ~ {}.{} (differs)", table.schema, table.name));
        }
    }

    // Summary
    let summary = diff.summary();
    let mut summary_parts = Vec::new();
    if summary.tables > 0 {
        summary_parts.push(format!("{} tables", summary.tables));
    }
    if summary.columns > 0 {
        summary_parts.push(format!("{} columns", summary.columns));
    }
    if summary.indexes > 0 {
        summary_parts.push(format!("{} indexes", summary.indexes));
    }
    if summary.functions > 0 {
        summary_parts.push(format!("{} functions", summary.functions));
    }
    if summary.views > 0 {
        summary_parts.push(format!("{} views", summary.views));
    }

    if !summary_parts.is_empty() {
        output.push(String::new());
        output.push(format!("Summary: {} differ", summary_parts.join(", ")));
    }

    output.join("\n")
}

// =============================================================================
// Describe
// =============================================================================

pub async fn describe(
    database_url: &str,
    object: &str,
    dependents: bool,
    dependencies: bool,
    no_stats: bool,
    verbose: bool,
    output: &Output,
) -> Result<()> {
    let client = connect(database_url).await?;

    // Resolve the table name
    let resolved = describe::resolve_table(&client, object).await?;

    // Always run all queries to catch errors (even in quiet mode)
    let include_stats = !no_stats;
    let table_info = describe::describe_table(
        &client,
        &resolved.schema,
        &resolved.name,
        include_stats,
        verbose,
    )
    .await?;

    // Run dependents/dependencies queries if requested (for error detection even in quiet mode)
    let deps_output = if dependents {
        let deps = describe::get_dependents(&client, &resolved.schema, &resolved.name).await?;
        Some((
            "Direct Dependents:",
            deps.format(&resolved.schema, &resolved.name),
        ))
    } else if dependencies {
        let deps = describe::get_dependencies(&client, &resolved.schema, &resolved.name).await?;
        Some((
            "Direct Dependencies:",
            deps.format(&resolved.schema, &resolved.name),
        ))
    } else {
        None
    };

    // In quiet mode, skip all output (but we've already run queries above for error detection)
    if output.is_quiet() {
        return Ok(());
    }

    // Build and output the result
    let mut result = String::new();
    result.push('\n');
    result.push_str(&format!(
        "Table: {}.{}\n",
        quote_ident(&resolved.schema),
        quote_ident(&resolved.name)
    ));
    result.push_str(&"─".repeat(64));
    result.push('\n');
    result.push('\n');
    result.push_str(&table_info.format(verbose));

    // Append dependents/dependencies section if requested
    if let Some((header, formatted)) = deps_output {
        result.push('\n');
        result.push('\n');
        result.push_str(header);
        result.push('\n');
        result.push('\n');
        result.push_str(&formatted);
    }

    output.data(&result);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generated_config_is_valid_toml() {
        // Verify the generated config can be parsed as valid TOML
        let config = InitConfig {
            migrations_dir: "db/migrations".to_string(),
            use_models: false,
            models_dir: "models".to_string(),
            use_seeds: false,
            seeds_dir: "seeds".to_string(),
            seeds_schema: "seeds".to_string(),
        };
        let toml_str = config.generate_toml();
        let result: Result<toml::Value, _> = toml::from_str(&toml_str);
        assert!(
            result.is_ok(),
            "Generated config should be valid TOML: {}",
            toml_str
        );
    }

    #[test]
    fn test_generated_config_with_all_features() {
        let config = InitConfig {
            migrations_dir: "db/migrations".to_string(),
            use_models: true,
            models_dir: "models".to_string(),
            use_seeds: true,
            seeds_dir: "seeds".to_string(),
            seeds_schema: "seeds".to_string(),
        };
        let toml_str = config.generate_toml();
        let parsed: toml::Value = toml::from_str(&toml_str).unwrap();

        // Check paths section
        let paths = parsed.get("paths").expect("should have paths section");
        assert_eq!(
            paths.get("migrations").and_then(|v| v.as_str()),
            Some("db/migrations")
        );
        assert_eq!(paths.get("models").and_then(|v| v.as_str()), Some("models"));
        assert_eq!(paths.get("seeds").and_then(|v| v.as_str()), Some("seeds"));

        // Check seeds section
        let seeds = parsed.get("seeds").expect("should have seeds section");
        assert_eq!(seeds.get("schema").and_then(|v| v.as_str()), Some("seeds"));
    }

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("./db/migrations/"), "db/migrations");
        assert_eq!(normalize_path("db/migrations"), "db/migrations");
        assert_eq!(normalize_path("./models"), "models");
        assert_eq!(normalize_path("  seeds/  "), "seeds");
    }

    #[test]
    fn test_extract_db_name_standard_url() {
        assert_eq!(
            extract_db_name("postgres://user:pass@localhost:5432/mydb"),
            "mydb"
        );
    }

    #[test]
    fn test_extract_db_name_with_query_params() {
        assert_eq!(
            extract_db_name("postgres://localhost/mydb?sslmode=require"),
            "mydb"
        );
    }

    #[test]
    fn test_extract_db_name_unix_socket() {
        // postgres:///dbname uses Unix socket (no host)
        assert_eq!(extract_db_name("postgres:///mydb"), "mydb");
    }

    #[test]
    fn test_extract_db_name_postgresql_scheme() {
        assert_eq!(extract_db_name("postgresql://localhost/mydb"), "mydb");
    }

    #[test]
    fn test_extract_db_name_simple() {
        assert_eq!(extract_db_name("postgres://localhost/testdb"), "testdb");
    }

    #[test]
    fn test_extract_db_name_fallback() {
        // If we can't extract a name, return a safe placeholder (never expose credentials)
        assert_eq!(extract_db_name("invalid"), "<unknown-db>");
    }
}
