//! Integration tests for the `pgcrate extension` commands.
//!
//! These tests require a running PostgreSQL instance.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.

use std::env;
use std::process::Command;

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string())
}

/// Get the path to the compiled pgcrate binary
fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

/// Run pgcrate using the compiled binary
fn run_pgcrate(args: &[&str], db_url: &str) -> std::process::Output {
    Command::new(pgcrate_binary())
        .args(args)
        .env("DATABASE_URL", db_url)
        .output()
        .expect("Failed to execute pgcrate")
}

/// Test extension list shows at least plpgsql (always installed)
#[test]
fn test_extension_list_shows_plpgsql() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["extension", "list"], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "extension list should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("plpgsql"),
        "Should show plpgsql (always installed). stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Installed extensions:"),
        "Should have header. stdout: {}",
        stdout
    );
}

/// Test extension list --available shows available extensions
#[test]
fn test_extension_list_available() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["extension", "list", "--available"], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "extension list --available should succeed. stderr: {}",
        stderr
    );
    // pg_stat_statements is commonly available but not installed by default
    assert!(
        stdout.contains("Available extensions")
            || stdout.contains("All available extensions are installed"),
        "Should show available extensions or indicate all are installed. stdout: {}",
        stdout
    );
}

/// Test extension list with --quiet produces no output
#[test]
fn test_extension_list_quiet() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["extension", "list", "--quiet"], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "extension list --quiet should succeed"
    );
    assert!(
        stdout.is_empty(),
        "Quiet mode should produce no output. stdout: {}",
        stdout
    );
}
