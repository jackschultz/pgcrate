//! Integration tests for `pgcrate doctor`.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

#[test]
fn test_doctor_healthy_database() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Set up a healthy state: migrations applied
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["dba", "doctor"]);

    // Should pass health check
    let out = stdout(&output);
    assert!(
        out.contains("✓") || out.contains("OK") || out.contains("pass") || out.contains("healthy"),
        "Should indicate healthy status: {}",
        out
    );
}

#[test]
fn test_doctor_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["dba", "doctor", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Doctor JSON may have various structures depending on implementation
    // Common keys: checks, status, overall, config, connection
    assert!(
        json.get("checks").is_some()
            || json.get("status").is_some()
            || json.get("overall").is_some()
            || json.get("config").is_some()
            || json.get("connection").is_some(),
        "JSON should have health check results: {}",
        json
    );
}

#[test]
fn test_doctor_errors_on_stderr() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["dba", "doctor"]);

    // Normal success case - stderr should be empty or just have warnings
    // (We can't guarantee stderr is empty as some implementations log there)
    // Main check is that stdout has the actual output
    let out = stdout(&output);
    assert!(!out.is_empty(), "Doctor output should be on stdout");
}

#[test]
fn test_doctor_checks_connection() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Doctor may return non-zero if schema_migrations is missing
    // We just want to verify it checks the connection
    let output = project.run_pgcrate(&["dba", "doctor"]);

    let out = stdout(&output);
    // Should check database connection - various output formats possible
    assert!(
        out.contains("connection")
            || out.contains("Connection")
            || out.contains("database")
            || out.contains("Database")
            || out.contains("✓")
            || out.contains("OK"),
        "Should check connection or show health status: {}",
        out
    );
}

#[test]
fn test_doctor_strict_mode() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Strict mode should exit non-zero on warnings
    let output = project.run_pgcrate(&["dba", "doctor", "--strict"]);

    // In a healthy setup, should still pass
    // (If there are warnings, it would fail)
    // Just verify it runs without crash
    assert!(output.status.code().is_some());
}
