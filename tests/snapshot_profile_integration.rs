//! Integration tests for snapshot profiles.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string())
}

fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

fn run_pgcrate(args: &[&str], db_url: &str, workdir: &Path) -> std::process::Output {
    Command::new(pgcrate_binary())
        .args(args)
        .env("DATABASE_URL", db_url)
        .current_dir(workdir)
        .output()
        .expect("Failed to execute pgcrate")
}

fn run_psql(sql: &str, db_url: &str) {
    Command::new("psql")
        .args([db_url, "-c", sql])
        .output()
        .expect("Failed to execute psql");
}

fn can_connect(db_url: &str) -> bool {
    Command::new("psql")
        .args([db_url, "-c", "SELECT 1"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn create_test_db(db_url: &str, name: &str) -> String {
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, name))
        .unwrap_or_else(|| format!("{}/{}", db_url, name));
    run_psql(&format!("DROP DATABASE IF EXISTS {}", name), db_url);
    run_psql(&format!("CREATE DATABASE {}", name), db_url);
    test_url
}

#[test]
fn test_snapshot_save_with_profile() {
    let base_url = get_test_db_url();
    if !can_connect(&base_url) {
        return;
    }

    let test_db = create_test_db(&base_url, "pgcrate_snap_profile");
    let temp_dir = TempDir::new().unwrap();

    // Create multiple schemas and tables
    run_psql("CREATE SCHEMA app; CREATE SCHEMA analytics;", &test_db);
    run_psql(
        "CREATE TABLE app.users (id SERIAL); CREATE TABLE analytics.reports (id SERIAL);",
        &test_db,
    );

    // Create snapshot profile
    let toml_content = r#"
        [snapshot.app_only]
        schemas = ["app"]
    "#;
    fs::write(temp_dir.path().join("pgcrate.snapshot.toml"), toml_content).unwrap();

    // Save snapshot with profile (dry-run)
    let output = run_pgcrate(
        &[
            "snapshot",
            "save",
            "my-snap",
            "--profile",
            "app_only",
            "--dry-run",
        ],
        &test_db,
        temp_dir.path(),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        println!("pgcrate stdout: {}", stdout);
        println!("pgcrate stderr: {}", stderr);
    }
    assert!(output.status.success());
    assert!(stdout.contains("Profile:  app_only"));
    assert!(stdout.contains("Dry run"));

    run_psql("DROP DATABASE pgcrate_snap_profile", &base_url);
}
