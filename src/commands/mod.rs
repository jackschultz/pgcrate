//! Command implementations for pgcrate CLI.
//!
//! Each submodule contains related command functions.

mod anonymize;
mod bootstrap;
mod db;
mod doctor;
mod extension;
pub mod locks;
mod migrations;
pub mod model;
mod role;
mod schema;
mod seed;
mod snapshot;
mod sql_cmd;
pub mod triage;

// Re-export snapshot commands from new module
pub use snapshot::{
    snapshot_delete, snapshot_info, snapshot_list, snapshot_restore, snapshot_save,
};

// Re-export anonymize commands from new module
pub use anonymize::{anonymize_dump, anonymize_setup};

// Re-export bootstrap command
pub use bootstrap::bootstrap;

// Re-export doctor command from new module
pub use doctor::doctor;

// Re-export migration commands from new module
pub use migrations::{baseline, down, new_migration, status, up};

// Re-export db commands from new module
pub use db::{db_create, db_drop, reset};

// Re-export schema commands from new module
pub use schema::{describe, diff, generate, init};

// Re-export seed commands from new module
pub use seed::{seed_diff, seed_list, seed_run, seed_validate};

// Re-export sql/query command
pub use sql_cmd::sql;

// Re-export extension commands from new module
pub use extension::extension_list;

// Re-export role and grants commands from new module
pub use role::{grants, role_describe, role_list};

// Shared utilities used by command modules
use crate::migrations::Migration;
use anyhow::Result;
use tokio_postgres::{Client, NoTls};

pub(crate) const SCHEMA_MIGRATIONS_TABLE: &str = r#"
CREATE SCHEMA IF NOT EXISTS pgcrate;
CREATE TABLE IF NOT EXISTS pgcrate.schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT now()
)
"#;

pub(crate) async fn connect(database_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

    // Spawn the connection handler
    tokio::spawn(async move {
        let _ = connection.await;
    });

    Ok(client)
}

pub(crate) async fn get_applied_versions(
    client: &Client,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT version FROM pgcrate.schema_migrations ORDER BY version",
            &[],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get("version")).collect())
}

pub(crate) async fn run_migration(client: &Client, migration: &Migration) -> Result<()> {
    // Run migration SQL
    client.batch_execute(&migration.up_sql).await?;

    // Record in schema_migrations
    client
        .execute(
            "INSERT INTO pgcrate.schema_migrations (version) VALUES ($1)",
            &[&migration.version],
        )
        .await?;

    Ok(())
}
