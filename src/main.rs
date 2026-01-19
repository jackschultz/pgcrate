use anyhow::{Context, Result};
use clap::{error::ErrorKind, Args, Parser, Subcommand};
use std::path::PathBuf;

mod anonymize;
mod commands;
mod config;
mod connection;
mod describe;
mod diagnostic;
mod diff;
mod doctor;
mod help;
mod introspect;
mod migrations;
mod model;
mod output;
mod redact;
mod seed;
mod snapshot;
mod sql;
mod suggest;
mod tips;
use config::Config;
use diagnostic::{DiagnosticSession, TimeoutConfig};
use output::{HelpResponse, JsonError, LlmHelpResponse, Output, VersionResponse};

/// Embedded LLM help content (compiled into binary)
const LLM_HELP: &str = include_str!("../llms.txt");

/// Version from Cargo.toml
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Parse CLI timeout options into a TimeoutConfig.
fn parse_timeout_config(cli: &Cli) -> Result<TimeoutConfig> {
    let connect_timeout = cli
        .connect_timeout
        .as_ref()
        .map(|s| diagnostic::parse_duration(s))
        .transpose()
        .context("Invalid --connect-timeout")?;

    let statement_timeout = cli
        .statement_timeout
        .as_ref()
        .map(|s| diagnostic::parse_duration(s))
        .transpose()
        .context("Invalid --statement-timeout")?;

    let lock_timeout = cli
        .lock_timeout
        .as_ref()
        .map(|s| diagnostic::parse_duration(s))
        .transpose()
        .context("Invalid --lock-timeout")?;

    Ok(TimeoutConfig::new(
        connect_timeout,
        statement_timeout,
        lock_timeout,
    ))
}

/// Whether the selected command supports JSON output mode.
/// Note: For commands with subcommands, JSON support can vary by subcommand.
fn json_supported(command: &Commands) -> bool {
    match command {
        Commands::Describe { .. } => true,
        Commands::Diff { .. } => true,
        Commands::Doctor { .. } => true,
        Commands::Triage => true,
        Commands::Locks { .. } => true,
        Commands::Xid { .. } => true,
        Commands::Sequences { .. } => true,
        Commands::Indexes { .. } => true,
        Commands::Sql { .. } => true,
        Commands::Snapshot { command } => matches!(
            command,
            SnapshotCommands::List | SnapshotCommands::Info { .. }
        ),
        Commands::Migrate { command } => matches!(command, MigrateCommands::Status),
        Commands::Model { command } => matches!(
            command,
            ModelCommands::Status { .. } | ModelCommands::Show { .. }
        ),
        Commands::Status => true,
        _ => false,
    }
}

#[derive(Parser)]
#[command(name = "pgcrate")]
#[command(version = VERSION)]
#[command(about = "Postgres migration tool", long_about = None)]
#[command(
    after_help = "For AI agents and LLMs: Use --help-llm for structured, detailed information suitable for programmatic usage."
)]
#[command(subcommand_required = true, arg_required_else_help = true)]
struct Cli {
    /// Show detailed help for AI agents and LLMs (structured output)
    #[arg(long = "help-llm", global = true)]
    help_llm: bool,

    /// Database URL (overrides DATABASE_URL env var and config file)
    #[arg(short = 'd', long = "database-url", global = true)]
    database_url: Option<String>,

    /// Named connection from pgcrate.toml [connections] section
    #[arg(short = 'C', long = "connection", global = true)]
    connection: Option<String>,

    /// Environment variable name containing DATABASE_URL (e.g., PROD_DATABASE_URL)
    #[arg(long = "env", global = true)]
    env_var: Option<String>,

    /// Confirm connection to primary database (required for role=primary connections)
    #[arg(long = "primary", global = true)]
    allow_primary: bool,

    /// Use read-write mode (default is read-only for diagnostic commands)
    #[arg(long = "read-write", global = true)]
    read_write: bool,

    /// Path to config file (default: ./pgcrate.toml)
    #[arg(long = "config", global = true)]
    config_path: Option<PathBuf>,

    /// Minimal output (errors only)
    #[arg(long, global = true)]
    quiet: bool,

    /// Show SQL being executed
    #[arg(long, global = true)]
    verbose: bool,

    /// Output as JSON instead of human-readable text
    #[arg(long, global = true)]
    json: bool,

    /// Path to anonymize rules file (default: ./pgcrate.anonymize.toml)
    #[arg(long, global = true)]
    anonymize_config: Option<PathBuf>,

    /// Path to snapshot profiles file (default: ./pgcrate.snapshot.toml)
    #[arg(long, global = true)]
    snapshot_config: Option<PathBuf>,

    // Timeout options for diagnostic commands
    /// Connection timeout (e.g., "5s", "500ms"). Default: 5s
    #[arg(long = "connect-timeout", global = true, value_name = "DURATION")]
    connect_timeout: Option<String>,

    /// Statement timeout (e.g., "30s", "1m"). Default: 30s
    #[arg(long = "statement-timeout", global = true, value_name = "DURATION")]
    statement_timeout: Option<String>,

    /// Lock timeout (e.g., "500ms", "1s"). Default: 500ms
    #[arg(long = "lock-timeout", global = true, value_name = "DURATION")]
    lock_timeout: Option<String>,

    /// Disable redaction of sensitive data in output (INSECURE)
    #[arg(long = "no-redact", global = true)]
    no_redact: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Migration commands (up, down, status, new, baseline)
    Migrate {
        #[command(subcommand)]
        command: MigrateCommands,
    },
    /// Model commands (run, compile)
    Model {
        #[command(subcommand)]
        command: ModelCommands,
    },

    /// Initialize a new pgcrate project
    Init {
        /// Accept all defaults without prompting
        #[arg(short = 'y', long)]
        yes: bool,
        /// Show what would be created without creating
        #[arg(long)]
        dry_run: bool,
        /// Overwrite existing pgcrate.toml
        #[arg(long)]
        force: bool,
        /// Minimal output
        #[arg(short = 'q', long)]
        quiet: bool,
        /// Migrations directory
        #[arg(long, default_value = "db/migrations")]
        migrations_dir: String,
        /// Enable models
        #[arg(long)]
        models: bool,
        /// Models directory (implies --models)
        #[arg(long, default_value = "models")]
        models_dir: String,
        /// Enable seeds
        #[arg(long)]
        seeds: bool,
        /// Seeds directory (implies --seeds)
        #[arg(long, default_value = "seeds")]
        seeds_dir: String,
    },
    /// Database management commands
    Db {
        #[command(subcommand)]
        command: DbCommands,
    },
    /// Reset database to clean state
    Reset {
        /// Confirm you want to reset the database
        #[arg(long)]
        yes: bool,
        /// Full reset: drop and recreate database (instead of just rolling back migrations)
        #[arg(long)]
        full: bool,
    },
    /// Generate migration files from existing database schema
    Generate {
        /// Split mode: "none" (single file), "schema", or "table"
        #[arg(long, value_name = "MODE")]
        split_by: Option<String>,
        /// Output directory (default: migrations directory)
        #[arg(long, short = 'o', value_name = "DIR")]
        output: Option<PathBuf>,
        /// Show what would be generated without writing files
        #[arg(long)]
        dry_run: bool,
        /// Include only these schemas (can be specified multiple times)
        #[arg(long = "schema", value_name = "SCHEMA")]
        schemas: Vec<String>,
        /// Exclude these schemas (can be specified multiple times)
        #[arg(long = "exclude-schema", value_name = "SCHEMA")]
        exclude_schemas: Vec<String>,
    },
    /// Compare two database schemas and show differences
    Diff {
        /// Source database URL (default: DATABASE_URL)
        #[arg(long)]
        from: Option<String>,
        /// Target database URL (required)
        #[arg(long)]
        to: String,
        /// Only compare these schemas (can be specified multiple times)
        #[arg(long = "schema", value_name = "SCHEMA")]
        schemas: Vec<String>,
        /// Exclude these schemas (can be specified multiple times)
        #[arg(
            long = "exclude-schema",
            value_name = "SCHEMA",
            conflicts_with = "schemas"
        )]
        exclude_schemas: Vec<String>,
    },
    /// Show detailed information about a table
    Describe {
        /// Table to describe (schema.name or just name)
        object: String,
        /// Show objects that depend on this table
        #[arg(long, conflicts_with = "dependencies")]
        dependents: bool,
        /// Show objects this table depends on
        #[arg(long, conflicts_with = "dependents")]
        dependencies: bool,
        /// Skip table statistics
        #[arg(long)]
        no_stats: bool,
    },
    /// One-command health check (connection, schema, migrations, seeds, config)
    Doctor {
        /// Treat warnings as errors (exit 1 on warnings)
        #[arg(long)]
        strict: bool,
    },
    /// Quick database health triage (locks, transactions, XID, sequences, connections)
    Triage,
    /// Inspect blocking locks and long transactions
    Locks {
        /// Show only blocking chains
        #[arg(long)]
        blocking: bool,
        /// Show transactions running longer than N minutes (default: 5)
        #[arg(long, value_name = "MINUTES")]
        long_tx: Option<u64>,
        /// Show idle-in-transaction sessions
        #[arg(long)]
        idle_in_tx: bool,
        /// Cancel query for PID (pg_cancel_backend)
        #[arg(long, value_name = "PID")]
        cancel: Option<i32>,
        /// Terminate connection for PID (pg_terminate_backend)
        #[arg(long, value_name = "PID")]
        kill: Option<i32>,
        /// Actually execute cancel/kill (default is dry-run)
        #[arg(long)]
        execute: bool,
    },
    /// Monitor transaction ID (XID) age to prevent wraparound
    Xid {
        /// Number of tables to show (default: 10)
        #[arg(long, default_value = "10")]
        tables: usize,
    },
    /// Monitor sequence exhaustion risk
    Sequences {
        /// Warning threshold percentage (default: 70)
        #[arg(long, value_name = "PCT")]
        warn: Option<i32>,
        /// Critical threshold percentage (default: 85)
        #[arg(long, value_name = "PCT")]
        crit: Option<i32>,
        /// Show all sequences, not just problematic ones
        #[arg(long)]
        all: bool,
    },
    /// Analyze missing, unused, and duplicate indexes
    Indexes {
        /// Number of missing index candidates to show (default: 10)
        #[arg(long, default_value = "10")]
        missing_limit: usize,
        /// Number of unused indexes to show (default: 20)
        #[arg(long, default_value = "20")]
        unused_limit: usize,
    },
    /// Run arbitrary SQL against the database (alias: query)
    #[command(alias = "query")]
    Sql {
        /// SQL to execute (can contain multiple statements). Reads from stdin if not provided.
        /// Use -c for psql compatibility: pgcrate sql -c "SELECT 1"
        #[arg(short = 'c', value_name = "SQL")]
        command: Option<String>,
        /// Allow write statements (INSERT/UPDATE/DELETE/DDL)
        #[arg(long)]
        allow_write: bool,
    },
    /// Save and restore database state
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommands,
    },
    /// Anonymize data for safe extraction
    Anonymize {
        #[command(subcommand)]
        command: AnonymizeCommands,
    },
    /// Load seed data from CSV or SQL files
    Seed {
        #[command(subcommand)]
        command: SeedCommands,
    },
    /// Bootstrap a new environment with anonymized data from a source
    Bootstrap {
        /// Source database URL to pull data from
        #[arg(long, required = true)]
        from: String,
        /// Show what would happen without making changes
        #[arg(long)]
        dry_run: bool,
        /// Confirm you want to bootstrap (recreates local database if it exists)
        #[arg(long)]
        yes: bool,
    },
    /// Inspect installed PostgreSQL extensions
    Extension {
        #[command(subcommand)]
        command: ExtensionCommands,
    },
    /// Inspect database roles and permissions
    Role {
        #[command(subcommand)]
        command: RoleCommands,
    },
    /// Show grants/permissions on database objects
    Grants {
        /// Table to show grants for (schema.table)
        object: Option<String>,
        /// Show all grants in a schema
        #[arg(long)]
        schema: Option<String>,
        /// Show what a specific role can access
        #[arg(long)]
        role: Option<String>,
    },

    /// Show migration status (alias for `migrate status`)
    Status,
}

#[derive(Subcommand)]
enum ExtensionCommands {
    /// List installed extensions
    List {
        /// Show available but not installed extensions
        #[arg(long)]
        available: bool,
    },
}

#[derive(Subcommand)]
enum RoleCommands {
    /// List all roles
    List {
        /// Show only login roles (users)
        #[arg(long)]
        users: bool,
        /// Show only non-login roles (groups)
        #[arg(long)]
        groups: bool,
    },
    /// Show detailed info about a role
    Describe {
        /// Role name
        name: String,
    },
}

#[derive(Subcommand)]
enum MigrateCommands {
    /// Run pending migrations
    Up {
        /// Accept defaults without prompting (no-op; `up` is non-interactive)
        #[arg(short = 'y', long)]
        yes: bool,
        /// Show what would run without running
        #[arg(long)]
        dry_run: bool,
    },
    /// Roll back applied migrations
    Down {
        /// Number of migrations to roll back (required)
        #[arg(long, required = true)]
        steps: usize,
        /// Confirm you want to run down migrations
        #[arg(long)]
        yes: bool,
        /// Show what would run without running
        #[arg(long)]
        dry_run: bool,
    },
    /// Show migration status
    Status,
    /// Create a new migration file
    New {
        /// Migration name (e.g., create_users)
        name: String,
        /// Accept defaults without prompting (no-op; `new` is non-interactive)
        #[arg(short = 'y', long)]
        yes: bool,
        /// Also create empty .down.sql file
        #[arg(long)]
        with_down: bool,
    },
    /// Mark migrations as applied without running them (for brownfield adoption)
    Baseline {
        /// Baseline all migration files
        #[arg(long, conflicts_with = "version")]
        all: bool,
        /// Baseline up to this version prefix (inclusive)
        #[arg(value_name = "VERSION")]
        version: Option<String>,
        /// Required confirmation flag
        #[arg(long)]
        yes: bool,
        /// Show what would be baselined without making changes
        #[arg(long)]
        dry_run: bool,
    },
}

/// Shared selection arguments for model commands
#[derive(Args, Clone)]
struct SelectionArgs {
    /// Select models by name or selector (can repeat). Examples:
    /// analytics.users, tag:daily, deps:analytics.orders, downstream:staging.raw, tree:analytics.orders
    #[arg(long, short = 's')]
    select: Vec<String>,

    /// Exclude models by name or selector (can repeat)
    #[arg(long, short = 'e')]
    exclude: Vec<String>,
}

#[derive(Subcommand)]
enum ModelCommands {
    /// Execute models in DAG order
    Run {
        /// Models to run (same as --select). Examples: analytics.users, tag:daily
        #[arg(value_name = "MODEL")]
        models: Vec<String>,
        #[command(flatten)]
        selection: SelectionArgs,
        /// Show execution plan without running
        #[arg(long)]
        dry_run: bool,
        /// Force full refresh for incremental models (drop and recreate)
        #[arg(long)]
        full_refresh: bool,
        /// Initialize models directory if missing
        #[arg(long)]
        init: bool,
        /// Accept defaults without prompting (for consistency; model run is non-interactive)
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Compile models to target/compiled/
    Compile {
        #[command(flatten)]
        selection: SelectionArgs,
        /// Initialize models directory if missing
        #[arg(long)]
        init: bool,
    },
    /// Run data tests defined in model headers
    Test {
        #[command(flatten)]
        selection: SelectionArgs,
        /// Initialize models directory if missing
        #[arg(long)]
        init: bool,
    },
    /// Generate markdown documentation
    Docs {
        #[command(flatten)]
        selection: SelectionArgs,
    },
    /// Show model dependency graph
    Graph {
        #[command(flatten)]
        selection: SelectionArgs,
        /// Output format: ascii (default), dot, json, mermaid
        #[arg(long, default_value = "ascii")]
        format: String,
    },
    /// Lint models for dependency and qualification issues
    Lint {
        #[command(subcommand)]
        command: LintCommands,
    },
    /// Run all lints (deps + qualify)
    Check {
        #[command(flatten)]
        selection: SelectionArgs,
    },
    /// Initialize models directory structure
    Init {
        /// Include example model
        #[arg(long)]
        example: bool,
        /// Accept defaults without prompting (for consistency; model init is non-interactive)
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Create a new model file
    New {
        /// Model id (schema.name)
        id: String,
        /// Materialization type: view, table, incremental
        #[arg(long, default_value = "view")]
        materialized: String,
        /// Skip prompts (e.g., overwrite confirmation)
        #[arg(short = 'y', long)]
        yes: bool,
        /// Overwrite existing file
        #[arg(long)]
        force: bool,
    },
    /// Show compiled SQL for a model (does not execute)
    Show {
        /// Model id (schema.name)
        id: String,
    },
    /// Show model sync status vs database
    Status {
        #[command(flatten)]
        selection: SelectionArgs,
    },
    /// Move/rename a model to a new schema or name
    Move {
        /// Source model id (schema.name)
        source: String,
        /// Destination model id (schema.name)
        dest: String,
        /// Drop existing database object if it exists
        #[arg(long)]
        drop_old: bool,
        /// Skip prompts
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum LintCommands {
    /// Check declared deps match inferred deps from SQL
    Deps {
        #[command(flatten)]
        selection: SelectionArgs,
        /// Auto-fix by rewriting deps line
        #[arg(long)]
        fix: bool,
    },
    /// Check for unqualified table references
    Qualify {
        #[command(flatten)]
        selection: SelectionArgs,
        /// Auto-fix by qualifying references
        #[arg(long)]
        fix: bool,
    },
}

#[derive(Subcommand)]
enum DbCommands {
    /// Create the database specified in DATABASE_URL
    Create {
        /// Database name (overrides DATABASE_URL)
        name: Option<String>,
    },
    /// Drop the database specified in DATABASE_URL
    Drop {
        /// Database name (overrides DATABASE_URL)
        name: Option<String>,
        /// Confirm you want to drop the database
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum SnapshotCommands {
    /// Save current database state to a snapshot
    Save {
        /// Snapshot name (alphanumeric, hyphens, underscores, max 64 chars)
        name: String,
        /// Description of the snapshot
        #[arg(short, long)]
        message: Option<String>,
        /// Snapshot profile to use (from pgcrate.snapshot.toml)
        #[arg(long)]
        profile: Option<String>,
        /// Dump format: 'custom' (default, binary) or 'plain' (readable SQL)
        #[arg(long)]
        format: Option<String>,
        /// Omit ownership (OWNER TO) statements from dump
        #[arg(long)]
        no_owner: bool,
        /// Omit privilege (GRANT/REVOKE) statements from dump
        #[arg(long)]
        no_privileges: bool,
        /// Show what would be saved without creating a snapshot
        #[arg(long)]
        dry_run: bool,
    },
    /// Restore database from a snapshot (destructive)
    Restore {
        /// Snapshot name to restore
        name: String,
        /// Required confirmation flag
        #[arg(long)]
        yes: bool,
        /// Show what would happen without making changes
        #[arg(long)]
        dry_run: bool,
        /// Target database URL (defaults to DATABASE_URL)
        #[arg(long)]
        to: Option<String>,
        /// Skip role pre-flight check and restore without ownership
        #[arg(long)]
        no_owner: bool,
    },
    /// List all snapshots
    List,
    /// Show detailed information about a snapshot
    Info {
        /// Snapshot name
        name: String,
    },
    /// Delete a snapshot
    Delete {
        /// Snapshot name to delete
        name: String,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum AnonymizeCommands {
    /// Install anonymization functions
    Setup,
    /// Dump anonymized data to file or stdout
    Dump {
        /// Anonymization seed (overrides env and file)
        #[arg(long)]
        seed: Option<String>,
        /// Output file path (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Preview what would be anonymized without writing output
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum SeedCommands {
    /// Load seed data into database
    Run {
        /// Specific seeds to run (`schema.table` or just `table` if unique)
        seeds: Vec<String>,
        /// Show what would be loaded without loading
        #[arg(long)]
        dry_run: bool,
    },
    /// List available seed files
    List,
    /// Validate seed files without loading
    Validate {
        /// Specific seeds to validate (`schema.table` or just `table` if unique)
        seeds: Vec<String>,
    },
    /// Compare seed files to database state
    Diff {
        /// Specific seeds to compare (`schema.table` or just `table` if unique)
        seeds: Vec<String>,
    },
}

#[tokio::main]
async fn main() {
    // Load .env file if present (before parsing CLI so env vars are available)
    let _ = dotenvy::dotenv();

    // Check for --json flag early (before full parsing) for error handling
    let json_mode = std::env::args().any(|arg| arg == "--json");

    // Handle --help-llm early (before clap parsing fails due to missing subcommand)
    // This is a special case because --help-llm should work without a subcommand
    if std::env::args().any(|arg| arg == "--help-llm") {
        if json_mode {
            LlmHelpResponse::new(LLM_HELP.to_string()).print();
        } else {
            print!("{}", LLM_HELP);
        }
        std::process::exit(0);
    }

    // Use try_parse to handle clap errors in JSON mode
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            // Handle meta UX flags (--help, --version) in JSON mode
            if json_mode {
                match e.kind() {
                    ErrorKind::DisplayHelp => {
                        // --help in JSON mode: return success payload
                        HelpResponse::new(e.to_string()).print();
                        std::process::exit(0);
                    }
                    ErrorKind::DisplayVersion => {
                        // --version in JSON mode: return success payload
                        VersionResponse::new(VERSION.to_string()).print();
                        std::process::exit(0);
                    }
                    _ => {
                        // Other errors in JSON mode: usage error
                        JsonError::new(e.to_string()).print();
                        std::process::exit(2);
                    }
                }
            } else {
                // Human mode: let clap print its formatted output
                e.exit();
            }
        }
    };

    let output = Output::new(cli.json, cli.quiet, cli.verbose);

    // Gate unsupported commands in JSON mode
    if cli.json && !json_supported(&cli.command) {
        JsonError::new("--json not supported for this command yet".to_string()).print();
        std::process::exit(1);
    }

    if let Err(e) = run(cli, &output).await {
        if json_mode {
            // JSON mode: output structured error to stdout
            // Only include details if source error is non-empty
            if let Some(model_err) = e.downcast_ref::<crate::model::ModelExecutionError>() {
                let payload = serde_json::json!({
                    "ok": false,
                    "model": model_err.model,
                    "error": model_err.error,
                    "sql": model_err.sql,
                    "hints": model_err.hints,
                    "suggestions": model_err.suggestions,
                });
                println!("{}", serde_json::to_string_pretty(&payload).unwrap());
            } else {
                // Use full error chain for details (same as human mode)
                let full_chain = format!("{e:#}");
                let json_err = JsonError::with_details(e.to_string(), full_chain);
                json_err.print();
            }
            std::process::exit(1);
        } else {
            // Human mode: error to stderr with full chain
            eprintln!("Error: {e:#}");
            std::process::exit(1);
        }
    }
}

async fn run(cli: Cli, output: &Output) -> Result<()> {
    match cli.command {
        Commands::Migrate { command } => {
            // Handle migrate subcommands
            match command {
                MigrateCommands::New {
                    name,
                    yes: _,
                    with_down,
                } => {
                    let config = Config::load(cli.config_path.as_deref())
                        .context("Failed to load configuration")?;
                    commands::new_migration(&name, &config, with_down)?;
                }
                MigrateCommands::Up { yes: _, dry_run } => {
                    let config = Config::load(cli.config_path.as_deref())
                        .context("Failed to load configuration")?;
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set")?;
                    commands::up(&database_url, &config, cli.quiet, cli.verbose, dry_run).await?;
                }
                MigrateCommands::Down {
                    steps,
                    yes,
                    dry_run,
                } => {
                    let config = Config::load(cli.config_path.as_deref())
                        .context("Failed to load configuration")?;
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set")?;
                    commands::down(
                        &database_url,
                        &config,
                        cli.quiet,
                        cli.verbose,
                        steps,
                        yes,
                        dry_run,
                    )
                    .await?;
                }
                MigrateCommands::Status => {
                    let config = Config::load(cli.config_path.as_deref())
                        .context("Failed to load configuration")?;
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set")?;
                    commands::status(&database_url, &config, output).await?;
                }
                MigrateCommands::Baseline {
                    all,
                    version,
                    yes,
                    dry_run,
                } => {
                    let config = Config::load(cli.config_path.as_deref())
                        .context("Failed to load configuration")?;
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set")?;
                    commands::baseline(
                        &database_url,
                        &config,
                        cli.quiet,
                        cli.verbose,
                        all,
                        version.as_deref(),
                        yes,
                        dry_run,
                    )
                    .await?;
                }
            }
        }
        Commands::Model { command } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let cwd = std::env::current_dir().context("get current directory")?;
            match command {
                ModelCommands::Compile { selection, init } => {
                    commands::model::compile(
                        &cwd,
                        &config,
                        &selection.select,
                        &selection.exclude,
                        init,
                        cli.quiet,
                    )?;
                }
                ModelCommands::Run {
                    models,
                    selection,
                    dry_run,
                    full_refresh,
                    init,
                    yes: _,
                } => {
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set")?;
                    // Merge positional models with --select flag
                    let mut select = selection.select.clone();
                    select.extend(models);
                    commands::model::run(
                        &cwd,
                        &config,
                        &database_url,
                        &select,
                        &selection.exclude,
                        dry_run,
                        full_refresh,
                        init,
                        cli.quiet,
                        cli.verbose,
                    )
                    .await?;
                }
                ModelCommands::Test { selection, init } => {
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set")?;
                    let exit_code = commands::model::test(
                        &cwd,
                        &config,
                        &database_url,
                        &selection.select,
                        &selection.exclude,
                        init,
                        cli.quiet,
                    )
                    .await?;
                    if exit_code != 0 {
                        std::process::exit(exit_code);
                    }
                }
                ModelCommands::Docs { selection } => {
                    commands::model::docs(
                        &cwd,
                        &config,
                        &selection.select,
                        &selection.exclude,
                        cli.quiet,
                    )?;
                }
                ModelCommands::Graph { selection, format } => {
                    commands::model::graph(
                        &cwd,
                        &config,
                        &selection.select,
                        &selection.exclude,
                        &format,
                        cli.quiet,
                    )?;
                }
                ModelCommands::Lint { command } => {
                    let exit_code = match command {
                        LintCommands::Deps { selection, fix } => commands::model::lint_deps(
                            &cwd,
                            &config,
                            &selection.select,
                            &selection.exclude,
                            fix,
                            cli.quiet,
                        )?,
                        LintCommands::Qualify { selection, fix } => commands::model::lint_qualify(
                            &cwd,
                            &config,
                            &selection.select,
                            &selection.exclude,
                            fix,
                            cli.quiet,
                        )?,
                    };
                    if exit_code != 0 {
                        std::process::exit(exit_code);
                    }
                }
                ModelCommands::Check { selection } => {
                    let exit_code = commands::model::check(
                        &cwd,
                        &config,
                        &selection.select,
                        &selection.exclude,
                        cli.quiet,
                    )?;
                    if exit_code != 0 {
                        std::process::exit(exit_code);
                    }
                }
                ModelCommands::Init { example, yes: _ } => {
                    commands::model::init(&cwd, &config, example, cli.quiet)?;
                }
                ModelCommands::New {
                    id,
                    materialized,
                    yes,
                    force,
                } => {
                    commands::model::new_model(
                        &cwd,
                        &config,
                        &id,
                        &materialized,
                        yes,
                        force,
                        cli.quiet,
                    )?;
                }
                ModelCommands::Show { id } => {
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .unwrap_or_default();
                    commands::model::show(&cwd, &config, &database_url, &id, cli.quiet, cli.json)
                        .await?;
                }
                ModelCommands::Status { selection } => {
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set")?;
                    let exit_code = commands::model::status(
                        &cwd,
                        &config,
                        &database_url,
                        &selection.select,
                        &selection.exclude,
                        cli.quiet,
                        cli.json,
                    )
                    .await?;
                    if exit_code != 0 {
                        std::process::exit(exit_code);
                    }
                }
                ModelCommands::Move {
                    source,
                    dest,
                    drop_old,
                    yes,
                } => {
                    let database_url = if drop_old {
                        Some(
                            config
                                .get_database_url(cli.database_url.as_deref())
                                .context("DATABASE_URL not set (required for --drop-old)")?,
                        )
                    } else {
                        config.get_database_url(cli.database_url.as_deref())
                    };
                    commands::model::move_model(
                        &cwd,
                        &config,
                        database_url.as_deref(),
                        &source,
                        &dest,
                        drop_old,
                        yes,
                        cli.quiet,
                    )
                    .await?;
                }
            }
        }
        Commands::Init {
            yes,
            dry_run,
            force,
            quiet,
            migrations_dir,
            models,
            models_dir,
            seeds,
            seeds_dir,
        } => {
            commands::init(
                yes,
                dry_run,
                force,
                quiet,
                &migrations_dir,
                models,
                &models_dir,
                seeds,
                &seeds_dir,
            )?;
        }
        Commands::Doctor { strict } => {
            let exit_code = commands::doctor(
                cli.database_url.as_deref(),
                cli.config_path.as_deref(),
                cli.quiet,
                cli.json,
                cli.verbose,
                strict,
            )
            .await?;
            if exit_code != 0 {
                std::process::exit(exit_code);
            }
        }
        Commands::Triage => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            // Use DiagnosticSession with timeout enforcement
            let timeout_config = parse_timeout_config(&cli)?;
            let session = DiagnosticSession::connect(&conn_result.url, timeout_config).await?;

            // Show effective timeouts unless quiet
            if !cli.quiet && !cli.json {
                eprintln!("pgcrate: timeouts: {}", session.effective_timeouts());
            }

            let results = commands::triage::run_triage(session.client()).await;

            if cli.json {
                commands::triage::print_json(&results)?;
            } else {
                commands::triage::print_human(&results, cli.quiet);
            }

            let exit_code = results.exit_code();
            if exit_code != 0 {
                std::process::exit(exit_code);
            }
        }
        Commands::Locks {
            blocking,
            long_tx,
            idle_in_tx,
            cancel,
            kill,
            execute,
        } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;

            // For cancel/kill operations, we need read-write access
            let needs_write = cancel.is_some() || kill.is_some();
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                needs_write || cli.read_write,
                cli.quiet,
            )?;

            // Use DiagnosticSession with timeout enforcement
            let timeout_config = parse_timeout_config(&cli)?;
            let session = DiagnosticSession::connect(&conn_result.url, timeout_config).await?;
            let client = session.client();

            // Show effective timeouts unless quiet
            if !cli.quiet && !cli.json {
                eprintln!("pgcrate: timeouts: {}", session.effective_timeouts());
            }

            // Handle cancel/kill operations (redact by default)
            let should_redact = !cli.no_redact;
            if cli.no_redact {
                eprintln!("pgcrate: WARNING: --no-redact disables credential redaction. Output may contain sensitive data.");
            }
            if let Some(pid) = cancel {
                commands::locks::cancel_query(&client, pid, execute, should_redact).await?;
                return Ok(());
            }
            if let Some(pid) = kill {
                commands::locks::terminate_connection(&client, pid, execute, should_redact).await?;
                return Ok(());
            }

            // Determine what to show (default: show blocking if nothing specified)
            let show_blocking = blocking || (long_tx.is_none() && !idle_in_tx);
            let show_long_tx = long_tx.is_some();
            let show_idle = idle_in_tx;

            let mut result = commands::locks::LocksResult {
                blocking_chains: vec![],
                long_transactions: vec![],
                idle_in_transaction: vec![],
            };

            if show_blocking {
                result.blocking_chains = commands::locks::get_blocking_chains(&client).await?;
            }
            if show_long_tx {
                let min_minutes = long_tx.unwrap_or(5);
                result.long_transactions =
                    commands::locks::get_long_transactions(&client, min_minutes).await?;
            }
            if show_idle {
                result.idle_in_transaction =
                    commands::locks::get_idle_in_transaction(&client).await?;
            }

            // Apply redaction unless explicitly disabled (warning already printed above)
            if should_redact {
                result.redact();
            }

            if cli.json {
                commands::locks::print_json(&result)?;
            } else {
                if show_blocking {
                    commands::locks::print_blocking_chains(&result.blocking_chains, cli.quiet);
                }
                if show_long_tx {
                    if show_blocking && !result.blocking_chains.is_empty() {
                        println!();
                    }
                    commands::locks::print_long_transactions(&result.long_transactions, cli.quiet);
                }
                if show_idle {
                    if (show_blocking && !result.blocking_chains.is_empty())
                        || (show_long_tx && !result.long_transactions.is_empty())
                    {
                        println!();
                    }
                    commands::locks::print_idle_in_transaction(
                        &result.idle_in_transaction,
                        cli.quiet,
                    );
                }
            }
        }
        Commands::Xid { tables } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            // Use DiagnosticSession with timeout enforcement
            let timeout_config = parse_timeout_config(&cli)?;
            let session = DiagnosticSession::connect(&conn_result.url, timeout_config).await?;

            // Show effective timeouts unless quiet
            if !cli.quiet && !cli.json {
                eprintln!("pgcrate: timeouts: {}", session.effective_timeouts());
            }

            let result = commands::xid::run_xid(session.client(), tables).await?;

            if cli.json {
                commands::xid::print_json(&result)?;
            } else {
                commands::xid::print_human(&result, cli.quiet);
            }

            // Exit with appropriate code
            match result.overall_status {
                commands::xid::XidStatus::Critical => std::process::exit(2),
                commands::xid::XidStatus::Warning => std::process::exit(1),
                commands::xid::XidStatus::Healthy => {}
            }
        }
        Commands::Sequences { warn, crit, all } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            // Use DiagnosticSession with timeout enforcement
            let timeout_config = parse_timeout_config(&cli)?;
            let session = DiagnosticSession::connect(&conn_result.url, timeout_config).await?;

            // Show effective timeouts unless quiet
            if !cli.quiet && !cli.json {
                eprintln!("pgcrate: timeouts: {}", session.effective_timeouts());
            }

            let result =
                commands::sequences::run_sequences(session.client(), warn, crit).await?;

            if cli.json {
                commands::sequences::print_json(&result)?;
            } else {
                commands::sequences::print_human(&result, cli.quiet, all);
            }

            // Exit with appropriate code
            match result.overall_status {
                commands::sequences::SeqStatus::Critical => std::process::exit(2),
                commands::sequences::SeqStatus::Warning => std::process::exit(1),
                commands::sequences::SeqStatus::Healthy => {}
            }
        }
        Commands::Indexes {
            missing_limit,
            unused_limit,
        } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            // Use DiagnosticSession with timeout enforcement
            let timeout_config = parse_timeout_config(&cli)?;
            let session = DiagnosticSession::connect(&conn_result.url, timeout_config).await?;

            // Show effective timeouts unless quiet
            if !cli.quiet && !cli.json {
                eprintln!("pgcrate: timeouts: {}", session.effective_timeouts());
            }

            let result =
                commands::indexes::run_indexes(session.client(), missing_limit, unused_limit)
                    .await?;

            if cli.json {
                commands::indexes::print_json(&result)?;
            } else {
                commands::indexes::print_human(&result, cli.verbose);
            }
        }
        Commands::Sql {
            command,
            allow_write,
        } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            // --allow-write implies --read-write (otherwise writes fail due to read-only URL)
            let effective_read_write = cli.read_write || allow_write;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                effective_read_write,
                cli.quiet,
            )?;
            commands::sql(
                &conn_result.url,
                command.as_deref(),
                allow_write,
                cli.quiet,
                cli.json,
            )
            .await?;
        }
        Commands::Db { command } => {
            // db commands need database URL but not config
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let database_url = config
                .get_database_url(cli.database_url.as_deref())
                .context("DATABASE_URL not set. Use -d flag, set DATABASE_URL env var, or add to pgcrate.toml")?;

            match command {
                DbCommands::Create { name } => {
                    commands::db_create(&database_url, name.as_deref(), &config, cli.quiet).await?;
                }
                DbCommands::Drop { name, yes } => {
                    commands::db_drop(&database_url, name.as_deref(), &config, cli.quiet, yes)
                        .await?;
                }
            }
        }
        Commands::Snapshot { command } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let database_url = config
                .get_database_url(cli.database_url.as_deref())
                .context("DATABASE_URL not set. Use -d flag, set DATABASE_URL env var, or add to pgcrate.toml")?;

            match command {
                SnapshotCommands::Save {
                    name,
                    message,
                    profile,
                    format,
                    no_owner,
                    no_privileges,
                    dry_run,
                } => {
                    let format_str = format
                        .as_deref()
                        .or_else(|| {
                            config
                                .snapshot
                                .as_ref()
                                .and_then(|s| s.default_format.as_deref())
                        })
                        .unwrap_or("custom");
                    commands::snapshot_save(
                        &database_url,
                        &name,
                        message.as_deref(),
                        profile.as_deref(),
                        cli.snapshot_config.as_deref(),
                        format_str,
                        no_owner,
                        no_privileges,
                        &config,
                        cli.quiet,
                        cli.verbose,
                        dry_run,
                    )
                    .await?;
                }
                SnapshotCommands::Restore {
                    name,
                    yes,
                    dry_run,
                    to,
                    no_owner,
                } => {
                    let target_url = to.as_deref().unwrap_or(&database_url);
                    commands::snapshot_restore(
                        target_url,
                        &name,
                        &config,
                        cli.quiet,
                        cli.verbose,
                        yes,
                        dry_run,
                        no_owner,
                    )
                    .await?;
                }
                SnapshotCommands::List => {
                    commands::snapshot_list(&config, cli.quiet, cli.json)?;
                }
                SnapshotCommands::Info { name } => {
                    commands::snapshot_info(&name, &config, cli.quiet, cli.json)?;
                }
                SnapshotCommands::Delete { name, yes } => {
                    commands::snapshot_delete(&name, &config, cli.quiet, yes)?;
                }
            }
        }
        Commands::Reset { yes, full } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let database_url = config
                .get_database_url(cli.database_url.as_deref())
                .context("DATABASE_URL not set. Use -d flag, set DATABASE_URL env var, or add to pgcrate.toml")?;

            commands::reset(&database_url, &config, cli.quiet, cli.verbose, yes, full).await?;
        }
        Commands::Anonymize { command } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let database_url = config
                .get_database_url(cli.database_url.as_deref())
                .context("DATABASE_URL not set. Use -d flag, set DATABASE_URL env var, or add to pgcrate.toml")?;

            match command {
                AnonymizeCommands::Setup => {
                    commands::anonymize_setup(&database_url, cli.quiet, cli.verbose).await?;
                }
                AnonymizeCommands::Dump {
                    seed,
                    output: out,
                    dry_run,
                } => {
                    commands::anonymize_dump(
                        &database_url,
                        &config,
                        cli.anonymize_config.as_deref(),
                        seed.as_deref(),
                        out.as_deref(),
                        dry_run,
                        cli.quiet,
                        cli.verbose,
                    )
                    .await?;
                }
            }
        }
        Commands::Seed { command } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;

            match command {
                SeedCommands::Run { seeds, dry_run } => {
                    // dry_run can run without a database connection, but if DATABASE_URL is
                    // available we use it to resolve target tables for clearer output.
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .unwrap_or_default();
                    if !dry_run && database_url.is_empty() {
                        anyhow::bail!("DATABASE_URL not set. Use -d flag, set DATABASE_URL env var, or add to pgcrate.toml");
                    }
                    commands::seed_run(&database_url, &config, seeds, dry_run, cli.quiet).await?;
                }
                SeedCommands::List => {
                    commands::seed_list(&config, cli.quiet)?;
                }
                SeedCommands::Validate { seeds } => {
                    // DATABASE_URL is optional for validate - it only validates files
                    // If provided, it also checks database connection and target tables
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .unwrap_or_default();
                    commands::seed_validate(&database_url, &config, seeds, cli.quiet).await?;
                }
                SeedCommands::Diff { seeds } => {
                    let database_url = config
                        .get_database_url(cli.database_url.as_deref())
                        .context("DATABASE_URL not set. Use -d flag, set DATABASE_URL env var, or add to pgcrate.toml")?;
                    commands::seed_diff(&database_url, &config, seeds, cli.quiet).await?;
                }
            }
        }
        Commands::Bootstrap { from, dry_run, yes } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let database_url = config
                .get_database_url(cli.database_url.as_deref())
                .context("DATABASE_URL not set. Use -d flag, set DATABASE_url env var, or add to pgcrate.toml")?;

            commands::bootstrap(
                &database_url,
                &from,
                &config,
                cli.anonymize_config.as_deref(),
                cli.quiet,
                cli.verbose,
                dry_run,
                yes,
            )
            .await?;
        }
        Commands::Extension { command } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            match command {
                ExtensionCommands::List { available } => {
                    commands::extension_list(&conn_result.url, available, cli.quiet).await?;
                }
            }
        }
        Commands::Role { command } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            match command {
                RoleCommands::List { users, groups } => {
                    commands::role_list(&conn_result.url, users, groups, cli.quiet).await?;
                }
                RoleCommands::Describe { name } => {
                    commands::role_describe(&conn_result.url, &name, cli.quiet).await?;
                }
            }
        }
        Commands::Grants {
            object,
            schema,
            role,
        } => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            commands::grants(
                &conn_result.url,
                object.as_deref(),
                schema.as_deref(),
                role.as_deref(),
                cli.quiet,
            )
            .await?;
        }
        Commands::Status => {
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;
            commands::status(&conn_result.url, &config, output).await?;
        }
        cmd => {
            // Load config file for other commands
            let config =
                Config::load(cli.config_path.as_deref()).context("Failed to load configuration")?;

            // Diagnostic commands use connection resolution
            let conn_result = connection::resolve_and_validate(
                &config,
                cli.database_url.as_deref(),
                cli.connection.as_deref(),
                cli.env_var.as_deref(),
                cli.allow_primary,
                cli.read_write,
                cli.quiet,
            )?;

            match cmd {
                Commands::Generate {
                    split_by,
                    output,
                    dry_run,
                    schemas,
                    exclude_schemas,
                } => {
                    commands::generate(
                        &conn_result.url,
                        &config,
                        cli.quiet,
                        split_by.as_deref(),
                        output.as_deref(),
                        dry_run,
                        &schemas,
                        &exclude_schemas,
                    )
                    .await?;
                }
                Commands::Diff {
                    from,
                    to,
                    schemas,
                    exclude_schemas,
                } => {
                    let exit_code = commands::diff(
                        from.as_deref().unwrap_or(&conn_result.url),
                        &to,
                        output,
                        &schemas,
                        &exclude_schemas,
                    )
                    .await?;
                    if exit_code != 0 {
                        std::process::exit(exit_code);
                    }
                }
                Commands::Describe {
                    object,
                    dependents,
                    dependencies,
                    no_stats,
                } => {
                    commands::describe(
                        &conn_result.url,
                        &object,
                        dependents,
                        dependencies,
                        no_stats,
                        cli.verbose,
                        output,
                    )
                    .await?;
                }
                Commands::Migrate { .. }
                | Commands::Model { .. }
                | Commands::Init { .. }
                | Commands::Doctor { .. }
                | Commands::Triage
                | Commands::Locks { .. }
                | Commands::Xid { .. }
                | Commands::Sequences { .. }
                | Commands::Indexes { .. }
                | Commands::Sql { .. }
                | Commands::Db { .. }
                | Commands::Snapshot { .. }
                | Commands::Reset { .. }
                | Commands::Anonymize { .. }
                | Commands::Seed { .. }
                | Commands::Bootstrap { .. }
                | Commands::Extension { .. }
                | Commands::Role { .. }
                | Commands::Grants { .. }
                | Commands::Status => unreachable!(),
            }
        }
    }

    Ok(())
}
