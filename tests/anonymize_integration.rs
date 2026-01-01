//! Integration tests for `pgcrate anonymize` commands.
//!
//! These tests require a running PostgreSQL instance.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.
//!
//! Run with: cargo test --test anonymize_integration
//!
//! Note: Tests will be skipped if database is unavailable.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string())
}

/// Get the path to the compiled pgcrate binary
fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

/// Run pgcrate using the compiled binary
fn run_pgcrate(args: &[&str], db_url: &str, workdir: &Path) -> std::process::Output {
    Command::new(pgcrate_binary())
        .args(args)
        .env("DATABASE_URL", db_url)
        .current_dir(workdir)
        .output()
        .expect("Failed to execute pgcrate")
}

fn run_psql(sql: &str, db_url: &str) -> std::process::Output {
    Command::new("psql")
        .args([db_url, "-c", sql])
        .output()
        .expect("Failed to execute psql")
}

fn run_psql_query(sql: &str, db_url: &str) -> String {
    let output = Command::new("psql")
        .args([db_url, "-t", "-A", "-c", sql])
        .output()
        .expect("Failed to execute psql");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Check if database is accessible
fn can_connect(db_url: &str) -> bool {
    let output = Command::new("psql")
        .args([db_url, "-c", "SELECT 1"])
        .output();

    match output {
        Ok(o) => o.status.success(),
        Err(_) => false,
    }
}

/// Create a test database and return its URL
fn create_test_db(db_url: &str, name: &str) -> Option<String> {
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, name))
        .unwrap_or_else(|| format!("{}/{}", db_url, name));

    // Drop and create test database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", name), db_url);
    let create_result = run_psql(&format!("CREATE DATABASE {}", name), db_url);
    if !create_result.status.success() {
        return None;
    }
    Some(test_url)
}

/// Drop a test database
fn drop_test_db(db_url: &str, name: &str) {
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", name), db_url);
}

/// Setup test database with sample data
fn setup_test_data(db_url: &str) {
    // Create tables
    run_psql(
        r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            name TEXT NOT NULL,
            phone TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        "#,
        db_url,
    );

    run_psql(
        r#"
        INSERT INTO users (email, name, phone) VALUES
        ('john.doe@secret.com', 'John Doe', '555-1234'),
        ('jane.smith@private.org', 'Jane Smith', '555-5678')
        "#,
        db_url,
    );
}

#[test]
fn test_anonymize_setup_installs_functions() {
    let base_url = get_test_db_url();
    if !can_connect(&base_url) {
        return;
    }

    let test_db = create_test_db(&base_url, "pgcrate_anon_setup").unwrap();
    let temp_dir = TempDir::new().unwrap();

    let output = run_pgcrate(&["anonymize", "setup"], &test_db, temp_dir.path());
    assert!(output.status.success());

    // Verify functions exist
    let func_count = run_psql_query(
        "SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'pgcrate' AND p.proname LIKE 'anon%'",
        &test_db,
    );
    let count: i32 = func_count.parse().unwrap_or(0);
    assert!(count >= 6);

    drop_test_db(&base_url, "pgcrate_anon_setup");
}

#[test]
fn test_anonymize_dump_with_toml_config() {
    let base_url = get_test_db_url();
    if !can_connect(&base_url) {
        return;
    }

    let test_db = create_test_db(&base_url, "pgcrate_anon_toml").unwrap();
    let temp_dir = TempDir::new().unwrap();
    setup_test_data(&test_db);
    run_pgcrate(&["anonymize", "setup"], &test_db, temp_dir.path());

    // Create anonymize rules
    let toml_content = r#"
        seed = "test-seed"
        [[rules]]
        table = "public.users"
        columns = { email = "fake_email", name = "fake_name" }
    "#;
    fs::write(temp_dir.path().join("pgcrate.anonymize.toml"), toml_content).unwrap();

    // Run dump
    let dump_file = temp_dir.path().join("dump.sql");
    let output = run_pgcrate(
        &["anonymize", "dump", "--output", dump_file.to_str().unwrap()],
        &test_db,
        temp_dir.path(),
    );
    assert!(output.status.success());

    let content = fs::read_to_string(dump_file).unwrap();
    assert!(
        !content.contains("john.doe@secret.com"),
        "Should not contain original email"
    );
    assert!(
        !content.contains("John Doe"),
        "Should not contain original name"
    );
    assert!(
        content.contains("Smith")
            || content.contains("test.org")
            || content.contains("example.com"),
        "Should contain anonymized data fragments"
    );

    drop_test_db(&base_url, "pgcrate_anon_toml");
}

#[test]
fn test_anonymize_dump_requires_seed() {
    let base_url = get_test_db_url();
    if !can_connect(&base_url) {
        return;
    }

    let test_db = create_test_db(&base_url, "pgcrate_anon_no_seed").unwrap();
    let temp_dir = TempDir::new().unwrap();
    setup_test_data(&test_db);
    run_pgcrate(&["anonymize", "setup"], &test_db, temp_dir.path());

    // Create config WITHOUT seed
    let toml_content = r#"
        [[rules]]
        table = "public.users"
        columns = { email = "fake_email" }
    "#;
    fs::write(temp_dir.path().join("pgcrate.anonymize.toml"), toml_content).unwrap();

    let output = run_pgcrate(
        &["anonymize", "dump", "--dry-run"],
        &test_db,
        temp_dir.path(),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("No anonymization seed provided"));

    drop_test_db(&base_url, "pgcrate_anon_no_seed");
}

#[test]
fn test_anonymize_dump_with_cli_seed() {
    let base_url = get_test_db_url();
    if !can_connect(&base_url) {
        return;
    }

    let test_db = create_test_db(&base_url, "pgcrate_anon_cli_seed").unwrap();
    let temp_dir = TempDir::new().unwrap();
    setup_test_data(&test_db);
    run_pgcrate(&["anonymize", "setup"], &test_db, temp_dir.path());

    let toml_content = r#"
        [[rules]]
        table = "public.users"
        columns = { email = "fake_email" }
    "#;
    fs::write(temp_dir.path().join("pgcrate.anonymize.toml"), toml_content).unwrap();

    // Use --seed flag
    let output = run_pgcrate(
        &["anonymize", "dump", "--dry-run", "--seed", "cli-override"],
        &test_db,
        temp_dir.path(),
    );
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("cli-over..."));

    drop_test_db(&base_url, "pgcrate_anon_cli_seed");
}
