//! Integration tests for `pgcrate snapshot` commands.
//!
//! These tests require a running PostgreSQL instance with pg_dump and pg_restore in PATH.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.
//!
//! Run with: cargo test --test snapshot_integration
//!
//! Note: Tests will be skipped if pg_dump version doesn't match the server.

use std::env;
use std::path::Path;
use std::process::Command;

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

/// Check if pg_dump is available
fn has_pg_dump() -> bool {
    Command::new("pg_dump")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Try a pg_dump operation and check if it works (handles version mismatch)
fn can_pg_dump(db_url: &str) -> bool {
    let output = Command::new("pg_dump")
        .args(["--format=custom", "--schema-only", db_url])
        .output();

    match output {
        Ok(o) => {
            if o.status.success() {
                return true;
            }
            let stderr = String::from_utf8_lossy(&o.stderr);
            // If there's a version mismatch, we should skip
            if stderr.contains("version mismatch") {
                eprintln!("Skipping test: pg_dump version mismatch");
                return false;
            }
            true // Let other errors be handled by the actual test
        }
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

/// Setup test database with schema and data
fn setup_test_data(db_url: &str) {
    let setup_sql = r#"
        CREATE SCHEMA IF NOT EXISTS pgcrate;
        CREATE TABLE IF NOT EXISTS pgcrate.schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT now()
        );
        INSERT INTO pgcrate.schema_migrations (version) VALUES ('20250101000000'), ('20250102000000');

        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        INSERT INTO users (name) VALUES ('Alice'), ('Bob');
    "#;
    run_psql(setup_sql, db_url);
}

/// Test snapshot save and list
#[test]
fn test_snapshot_save_and_list() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_save";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    // Check for pg_dump version compatibility
    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    // Create a temp directory for this test
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    // Setup test data
    setup_test_data(&test_url);

    // Save a snapshot
    let output = run_pgcrate(
        &["snapshot", "save", "test-snap", "-m", "Test snapshot"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "snapshot save should succeed. stdout: {}, stderr: {}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("Snapshot saved: test-snap"),
        "Should confirm save. stdout: {}",
        stdout
    );

    // Verify snapshot directory was created
    let snap_dir = workdir.join(".pgcrate/snapshots/test-snap");
    assert!(snap_dir.exists(), "Snapshot directory should exist");
    assert!(
        snap_dir.join("metadata.json").exists(),
        "metadata.json should exist"
    );
    assert!(
        snap_dir.join("dump.pgdump").exists(),
        "dump.pgdump should exist"
    );

    // List snapshots
    let output = run_pgcrate(&["snapshot", "list"], &test_url, workdir);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "snapshot list should succeed");
    assert!(
        stdout.contains("test-snap"),
        "List should include our snapshot. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Test snapshot"),
        "List should include message. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("1 snapshot"),
        "Should show count. stdout: {}",
        stdout
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test snapshot save/restore roundtrip
#[test]
fn test_snapshot_restore_roundtrip() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_restore";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    // Check for pg_dump version compatibility
    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    // Create a temp directory for this test
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    // Setup test data
    setup_test_data(&test_url);

    // Verify initial state
    let count = run_psql_query("SELECT COUNT(*) FROM users", &test_url);
    assert_eq!(count, "2", "Should have 2 users initially");

    // Save a snapshot
    let output = run_pgcrate(&["snapshot", "save", "restore-test"], &test_url, workdir);
    assert!(
        output.status.success(),
        "snapshot save should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Delete all data
    run_psql("DELETE FROM users", &test_url);
    let count = run_psql_query("SELECT COUNT(*) FROM users", &test_url);
    assert_eq!(count, "0", "Should have 0 users after delete");

    // Test dry-run first
    let output = run_pgcrate(
        &["snapshot", "restore", "restore-test", "--dry-run"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "dry-run should succeed");
    assert!(
        stdout.contains("Would restore snapshot"),
        "Should indicate dry-run. stdout: {}",
        stdout
    );

    // Verify data unchanged after dry-run
    let count = run_psql_query("SELECT COUNT(*) FROM users", &test_url);
    assert_eq!(count, "0", "Data should be unchanged after dry-run");

    // Restore the snapshot
    let output = run_pgcrate(
        &["snapshot", "restore", "restore-test", "--yes"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "snapshot restore should succeed. stdout: {}, stderr: {}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("Snapshot restored successfully"),
        "Should confirm restore. stdout: {}",
        stdout
    );

    // Verify data is back
    let count = run_psql_query("SELECT COUNT(*) FROM users", &test_url);
    assert_eq!(count, "2", "Should have 2 users after restore");

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test snapshot delete
#[test]
fn test_snapshot_delete() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_delete";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    // Check for pg_dump version compatibility
    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    // Create a temp directory for this test
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    // Setup test data
    setup_test_data(&test_url);

    // Save a snapshot
    let output = run_pgcrate(&["snapshot", "save", "delete-test"], &test_url, workdir);
    assert!(output.status.success(), "snapshot save should succeed");

    let snap_dir = workdir.join(".pgcrate/snapshots/delete-test");
    assert!(snap_dir.exists(), "Snapshot directory should exist");

    // Delete with --yes
    let output = run_pgcrate(
        &["snapshot", "delete", "delete-test", "--yes"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "snapshot delete should succeed");
    assert!(
        stdout.contains("Snapshot deleted"),
        "Should confirm deletion. stdout: {}",
        stdout
    );

    // Verify directory removed
    assert!(!snap_dir.exists(), "Snapshot directory should be removed");

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test error when snapshot already exists
#[test]
fn test_snapshot_save_already_exists() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_exists";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    // Check for pg_dump version compatibility
    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    setup_test_data(&test_url);

    // Save first snapshot
    let output = run_pgcrate(&["snapshot", "save", "dupe-test"], &test_url, workdir);
    assert!(output.status.success(), "First save should succeed");

    // Try to save again with same name
    let output = run_pgcrate(&["snapshot", "save", "dupe-test"], &test_url, workdir);

    assert!(
        !output.status.success(),
        "Second save should fail due to existing snapshot"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already exists"),
        "Error should mention snapshot exists. stderr: {}",
        stderr
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test error for invalid snapshot name
#[test]
fn test_snapshot_invalid_name() {
    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_invalid";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    // Try invalid name with spaces
    let output = run_pgcrate(&["snapshot", "save", "invalid name"], &test_url, workdir);

    assert!(!output.status.success(), "Should fail for invalid name");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid snapshot name") || stderr.contains("letters, numbers"),
        "Should explain name requirements. stderr: {}",
        stderr
    );

    // Try invalid name with special characters
    let output = run_pgcrate(&["snapshot", "save", "snap@shot!"], &test_url, workdir);

    assert!(
        !output.status.success(),
        "Should fail for name with special characters"
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test restore requires --yes flag
#[test]
fn test_snapshot_restore_requires_yes() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_yes";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    // Check for pg_dump version compatibility
    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    setup_test_data(&test_url);

    // Save snapshot
    let output = run_pgcrate(&["snapshot", "save", "yes-test"], &test_url, workdir);
    assert!(output.status.success(), "Save should succeed");

    // Try restore without --yes
    let output = run_pgcrate(&["snapshot", "restore", "yes-test"], &test_url, workdir);

    assert!(
        !output.status.success(),
        "Restore without --yes should fail"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--yes"),
        "Should mention --yes flag. stderr: {}",
        stderr
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test error for non-existent snapshot
#[test]
fn test_snapshot_not_found() {
    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_notfound";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    // Try to restore non-existent snapshot
    let output = run_pgcrate(
        &["snapshot", "restore", "nonexistent", "--yes"],
        &test_url,
        workdir,
    );

    assert!(
        !output.status.success(),
        "Should fail for non-existent snapshot"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "Should indicate snapshot not found. stderr: {}",
        stderr
    );

    // Try to delete non-existent snapshot
    let output = run_pgcrate(
        &["snapshot", "delete", "nonexistent", "--yes"],
        &test_url,
        workdir,
    );

    assert!(
        !output.status.success(),
        "Delete should fail for non-existent snapshot"
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test snapshot list with empty directory
#[test]
fn test_snapshot_list_empty() {
    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_empty";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    let output = run_pgcrate(&["snapshot", "list"], &test_url, workdir);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "List should succeed even when empty"
    );
    assert!(
        stdout.contains("No snapshots found"),
        "Should indicate empty. stdout: {}",
        stdout
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

// =============================================================================
// v0.10.1 Tests - Snapshot Advanced
// =============================================================================

/// Test snapshot info command
#[test]
fn test_snapshot_info() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_info";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    setup_test_data(&test_url);

    // Save a snapshot with message
    let output = run_pgcrate(
        &["snapshot", "save", "info-test", "-m", "Test for info"],
        &test_url,
        workdir,
    );
    assert!(output.status.success(), "Save should succeed");

    // Test info command
    let output = run_pgcrate(&["snapshot", "info", "info-test"], &test_url, workdir);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "Info should succeed");
    assert!(
        stdout.contains("Snapshot: info-test"),
        "Should show snapshot name. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Database:"),
        "Should show database. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Format:      custom"),
        "Should show format. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Test for info"),
        "Should show message. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Migration State:"),
        "Should show migration state. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Options:"),
        "Should show options. stdout: {}",
        stdout
    );

    // Test info --json
    let output = run_pgcrate(
        &["snapshot", "info", "info-test", "--json"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "Info --json should succeed");

    // Parse as JSON
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Should be valid JSON");
    assert_eq!(json["name"], "info-test");
    assert_eq!(json["format"], "custom");
    assert_eq!(json["include_owner"], true);
    assert_eq!(json["include_privileges"], true);

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test snapshot info for non-existent snapshot
#[test]
fn test_snapshot_info_not_found() {
    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_info_notfound";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    let output = run_pgcrate(&["snapshot", "info", "nonexistent"], &test_url, workdir);

    assert!(
        !output.status.success(),
        "Info should fail for non-existent"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "Should indicate not found. stderr: {}",
        stderr
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test plain format save and restore
#[test]
fn test_snapshot_plain_format() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_plain";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    setup_test_data(&test_url);

    // Save with plain format
    let output = run_pgcrate(
        &["snapshot", "save", "plain-test", "--format", "plain"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "Save with --format plain should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("Format:   plain"),
        "Should show plain format. stdout: {}",
        stdout
    );

    // Verify dump.sql exists instead of dump.pgdump
    let snap_dir = workdir.join(".pgcrate/snapshots/plain-test");
    assert!(
        snap_dir.join("dump.sql").exists(),
        "dump.sql should exist for plain format"
    );
    assert!(
        !snap_dir.join("dump.pgdump").exists(),
        "dump.pgdump should NOT exist for plain format"
    );

    // Verify info shows correct format
    let output = run_pgcrate(&["snapshot", "info", "plain-test"], &test_url, workdir);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Format:      plain"),
        "Info should show plain format. stdout: {}",
        stdout
    );

    // Delete data and restore
    run_psql("DELETE FROM users", &test_url);
    let count = run_psql_query("SELECT COUNT(*) FROM users", &test_url);
    assert_eq!(count, "0", "Should have 0 users after delete");

    let output = run_pgcrate(
        &["snapshot", "restore", "plain-test", "--yes"],
        &test_url,
        workdir,
    );

    assert!(
        output.status.success(),
        "Restore from plain format should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify data restored
    let count = run_psql_query("SELECT COUNT(*) FROM users", &test_url);
    assert_eq!(count, "2", "Should have 2 users after restore from plain");

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test invalid format
#[test]
fn test_snapshot_invalid_format() {
    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_invalid_format";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    let output = run_pgcrate(
        &["snapshot", "save", "test", "--format", "invalid"],
        &test_url,
        workdir,
    );

    assert!(!output.status.success(), "Invalid format should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid format") || stderr.contains("custom") && stderr.contains("plain"),
        "Should mention valid formats. stderr: {}",
        stderr
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test --no-owner flag on save
#[test]
fn test_snapshot_no_owner_save() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_no_owner";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    setup_test_data(&test_url);

    // Save with --no-owner
    let output = run_pgcrate(
        &["snapshot", "save", "no-owner-test", "--no-owner"],
        &test_url,
        workdir,
    );

    assert!(
        output.status.success(),
        "Save with --no-owner should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify metadata shows owner excluded
    let output = run_pgcrate(
        &["snapshot", "info", "no-owner-test", "--json"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Valid JSON");
    assert_eq!(
        json["include_owner"], false,
        "include_owner should be false"
    );
    assert_eq!(
        json["include_privileges"], true,
        "include_privileges should still be true"
    );

    // Save with both --no-owner and --no-privileges
    let output = run_pgcrate(
        &[
            "snapshot",
            "save",
            "no-both-test",
            "--no-owner",
            "--no-privileges",
        ],
        &test_url,
        workdir,
    );

    assert!(
        output.status.success(),
        "Save with both flags should succeed"
    );

    let output = run_pgcrate(
        &["snapshot", "info", "no-both-test", "--json"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Valid JSON");
    assert_eq!(json["include_owner"], false);
    assert_eq!(json["include_privileges"], false);

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test restore --to different database
#[test]
fn test_snapshot_restore_to_different_db() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let source_db = "pgcrate_snap_test_to_source";
    let target_db = "pgcrate_snap_test_to_target";

    let source_url = match create_test_db(&db_url, source_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create source database");
            return;
        }
    };

    if !can_pg_dump(&source_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", source_db), &db_url);
        return;
    }

    let target_url = match create_test_db(&db_url, target_db) {
        Some(url) => url,
        None => {
            let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", source_db), &db_url);
            eprintln!("Skipping test: could not create target database");
            return;
        }
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    // Setup data in source
    setup_test_data(&source_url);

    // Save snapshot from source
    let output = run_pgcrate(&["snapshot", "save", "to-test"], &source_url, workdir);
    assert!(output.status.success(), "Save should succeed");

    // Verify target is empty
    let count = run_psql_query(
        "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'",
        &target_url,
    );
    assert_eq!(count, "0", "Target should have no tables initially");

    // Restore to target database
    let output = run_pgcrate(
        &[
            "snapshot",
            "restore",
            "to-test",
            "--to",
            &target_url,
            "--yes",
        ],
        &source_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Restore --to should succeed. stdout: {}, stderr: {}",
        stdout,
        stderr
    );

    // Verify data in target
    let count = run_psql_query("SELECT COUNT(*) FROM users", &target_url);
    assert_eq!(count, "2", "Target should have 2 users after restore");

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", source_db), &db_url);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", target_db), &db_url);
}

/// Test snapshot dry-run shows v0.10.1 metadata
#[test]
fn test_snapshot_dry_run_shows_format() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_dryrun_format";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    setup_test_data(&test_url);

    // Save snapshot
    let output = run_pgcrate(&["snapshot", "save", "dryrun-test"], &test_url, workdir);
    assert!(output.status.success(), "Save should succeed");

    // Test dry-run shows format
    let output = run_pgcrate(
        &["snapshot", "restore", "dryrun-test", "--dry-run"],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "Dry-run should succeed");
    assert!(
        stdout.contains("Format:      custom"),
        "Dry-run should show format. stdout: {}",
        stdout
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test that --no-owner shows warning for plain format with ownership
#[test]
fn test_snapshot_plain_no_owner_warning() {
    if !has_pg_dump() {
        eprintln!("Skipping test: pg_dump not found");
        return;
    }

    let db_url = get_test_db_url();
    let test_db = "pgcrate_snap_test_plain_no_owner_warn";

    let test_url = match create_test_db(&db_url, test_db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: could not create test database");
            return;
        }
    };

    if !can_pg_dump(&test_url) {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let workdir = temp_dir.path();

    setup_test_data(&test_url);

    // Save plain format WITH ownership (no --no-owner flag)
    let output = run_pgcrate(
        &["snapshot", "save", "plain-with-owner", "--format", "plain"],
        &test_url,
        workdir,
    );
    assert!(output.status.success(), "Save should succeed");

    // Try to restore with --no-owner (should show warning in dry-run)
    let output = run_pgcrate(
        &[
            "snapshot",
            "restore",
            "plain-with-owner",
            "--dry-run",
            "--no-owner",
        ],
        &test_url,
        workdir,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "Dry-run should succeed");
    assert!(
        stdout.contains("--no-owner has no effect for plain format"),
        "Should warn about --no-owner limitation. stdout: {}",
        stdout
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}
