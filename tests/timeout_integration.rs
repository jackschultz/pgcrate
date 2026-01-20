//! Integration tests for diagnostic timeout behavior.
//!
//! Tests verify that:
//! - Default timeouts are applied
//! - CLI timeout flags override defaults
//! - Timeout information is shown in output
//! - Lock timeout prevents hanging on blocked queries

mod common;

use common::{TestDatabase, TestProject};

// ============================================================================
// Default timeout behavior
// ============================================================================

#[test]
fn test_triage_shows_timeout_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "triage"]);

    // Should show timeout info in stderr (unless quiet)
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timeouts:") || stderr.contains("connect="),
        "Should show timeout info: {}",
        stderr
    );
}

#[test]
fn test_triage_quiet_hides_timeout_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "triage", "--quiet"]);

    // Should NOT show timeout info when quiet
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("timeouts:"),
        "Should not show timeout info in quiet mode: {}",
        stderr
    );
}

#[test]
fn test_triage_json_hides_timeout_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "triage", "--json"]);

    // Should NOT show timeout info in stderr for JSON mode
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("timeouts:"),
        "Should not show timeout info in JSON mode: {}",
        stderr
    );
}

// ============================================================================
// Custom timeout flags
// ============================================================================

#[test]
fn test_custom_statement_timeout() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "triage", "--statement-timeout", "10s"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("statement=10000ms"),
        "Should show custom statement timeout: {}",
        stderr
    );
}

#[test]
fn test_custom_lock_timeout() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "triage", "--lock-timeout", "100ms"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("lock=100ms"),
        "Should show custom lock timeout: {}",
        stderr
    );
}

#[test]
fn test_custom_connect_timeout() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "triage", "--connect-timeout", "3s"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("connect=3000ms"),
        "Should show custom connect timeout: {}",
        stderr
    );
}

#[test]
fn test_all_timeout_flags_combined() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&[
        "triage",
        "--connect-timeout",
        "2s",
        "--statement-timeout",
        "15s",
        "--lock-timeout",
        "200ms",
    ]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("connect=2000ms"),
        "Should show custom connect timeout: {}",
        stderr
    );
    assert!(
        stderr.contains("statement=15000ms"),
        "Should show custom statement timeout: {}",
        stderr
    );
    assert!(
        stderr.contains("lock=200ms"),
        "Should show custom lock timeout: {}",
        stderr
    );
}

// ============================================================================
// Invalid timeout values
// ============================================================================

#[test]
fn test_invalid_timeout_format() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate(&["dba", "triage", "--statement-timeout", "invalid"]);

    assert!(
        !output.status.success(),
        "Should fail on invalid timeout format"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_lowercase().contains("invalid")
            || stderr.to_lowercase().contains("error")
            || stderr.to_lowercase().contains("timeout"),
        "Should report invalid timeout: {}",
        stderr
    );
}

// ============================================================================
// Timeout enforcement (lock_timeout)
// ============================================================================

#[test]
fn test_lock_timeout_prevents_hanging() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a table and lock it with psql
    db.run_sql_ok("CREATE TABLE lock_test (id serial PRIMARY KEY, value text)");

    // Start a transaction that holds an exclusive lock (in background)
    // We use psql in a way that holds the lock
    let lock_output = std::process::Command::new("psql")
        .args([
            db.url(),
            "-c",
            "BEGIN; LOCK TABLE lock_test IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(10);",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();

    if lock_output.is_err() {
        eprintln!("Skipping test: could not spawn psql for lock");
        return;
    }

    let mut lock_child = lock_output.unwrap();

    // Give it time to acquire the lock
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Now run triage with a short lock timeout - it should NOT hang
    let start = std::time::Instant::now();
    let output = project.run_pgcrate(&["dba", "triage", "--lock-timeout", "100ms"]);
    let elapsed = start.elapsed();

    // Clean up the background process
    let _ = lock_child.kill();
    let _ = lock_child.wait();

    // Should complete within a reasonable time (not 10 seconds!)
    assert!(
        elapsed.as_secs() < 5,
        "Triage should complete quickly with lock_timeout, took {:?}",
        elapsed
    );

    // The command may or may not succeed (depending on what triage queries)
    // but it should not hang
    let _ = output; // Just verify it completed
}

// ============================================================================
// Other diagnostic commands also use timeouts
// ============================================================================

#[test]
fn test_locks_shows_timeout_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "locks"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timeouts:") || stderr.contains("connect="),
        "locks should show timeout info: {}",
        stderr
    );
}

#[test]
fn test_sequences_shows_timeout_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "sequences"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timeouts:") || stderr.contains("connect="),
        "sequences should show timeout info: {}",
        stderr
    );
}

#[test]
fn test_indexes_shows_timeout_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "indexes"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timeouts:") || stderr.contains("connect="),
        "indexes should show timeout info: {}",
        stderr
    );
}

#[test]
fn test_xid_shows_timeout_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "xid"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timeouts:") || stderr.contains("connect="),
        "xid should show timeout info: {}",
        stderr
    );
}
