//! Extension commands for pgcrate CLI.

use anyhow::Result;
use colored::Colorize;

use super::connect;

/// List installed PostgreSQL extensions
pub async fn extension_list(database_url: &str, available: bool, quiet: bool) -> Result<()> {
    let client = connect(database_url).await?;

    if available {
        // Show available but not installed extensions
        let rows = client
            .query(
                r#"
                SELECT
                    name,
                    default_version,
                    comment
                FROM pg_available_extensions
                WHERE installed_version IS NULL
                ORDER BY name
                "#,
                &[],
            )
            .await?;

        if rows.is_empty() {
            if !quiet {
                println!("{}", "All available extensions are installed.".green());
            }
            return Ok(());
        }

        if !quiet {
            println!("Available extensions (not installed):\n");
            println!(
                "{:<24} {:<12} {}",
                "Name".bold(),
                "Version".bold(),
                "Description".bold()
            );
            println!("{}", "─".repeat(72));

            for row in &rows {
                let name: String = row.get("name");
                let version: Option<String> = row.get("default_version");
                let comment: Option<String> = row.get("comment");

                println!(
                    "{:<24} {:<12} {}",
                    name,
                    version.unwrap_or_default(),
                    comment.unwrap_or_default()
                );
            }

            println!("\n{} extension(s) available", rows.len());
        }
    } else {
        // Show installed extensions
        let rows = client
            .query(
                r#"
                SELECT
                    e.extname AS name,
                    e.extversion AS version,
                    n.nspname AS schema,
                    c.description
                FROM pg_extension e
                JOIN pg_namespace n ON e.extnamespace = n.oid
                LEFT JOIN pg_description c ON c.objoid = e.oid AND c.classoid = 'pg_extension'::regclass
                ORDER BY e.extname
                "#,
                &[],
            )
            .await?;

        if rows.is_empty() {
            if !quiet {
                println!("{}", "No extensions installed.".yellow());
            }
            return Ok(());
        }

        if !quiet {
            println!("Installed extensions:\n");
            println!(
                "{:<24} {:<12} {:<16} {}",
                "Name".bold(),
                "Version".bold(),
                "Schema".bold(),
                "Description".bold()
            );
            println!("{}", "─".repeat(80));

            for row in &rows {
                let name: String = row.get("name");
                let version: String = row.get("version");
                let schema: String = row.get("schema");
                let description: Option<String> = row.get("description");

                println!(
                    "{:<24} {:<12} {:<16} {}",
                    name,
                    version,
                    schema,
                    description.unwrap_or_default()
                );
            }

            println!("\n{} extension(s) installed", rows.len());
        }
    }

    Ok(())
}
