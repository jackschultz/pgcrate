//! Integration tests for `pgcrate queries` command.
//!
//! Tests query analysis from pg_stat_statements.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

// ============================================================================
// queries - basic functionality
// ============================================================================

#[test]
fn test_queries_runs_without_error() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // queries may return 0 (healthy) even without pg_stat_statements
    // or may return non-zero if there are slow queries
    let output = project.run_pgcrate(&["queries"]);

    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "queries should return valid exit code (0=healthy, 1=warning, 2=critical)"
    );
}

#[test]
fn test_queries_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["queries", "--json"]);

    // Should succeed (extension may or may not be available)
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "queries --json should return valid exit code"
    );

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Schema versioning fields
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.queries"))
    );
    assert!(
        json.get("schema_version").is_some(),
        "JSON should have schema_version: {}",
        json
    );

    // Data fields (nested in data object)
    let data = json.get("data").expect("JSON should have data field");
    assert!(
        data.get("extension_available").is_some(),
        "JSON should have data.extension_available: {}",
        json
    );
    assert!(
        data.get("queries").is_some(),
        "JSON should have data.queries: {}",
        json
    );
    assert!(
        data.get("overall_status").is_some(),
        "JSON should have data.overall_status: {}",
        json
    );
}

#[test]
fn test_queries_sort_options() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Test different sort options
    for sort_by in &["total", "mean", "calls"] {
        let output = project.run_pgcrate(&["queries", "--by", sort_by]);
        assert!(
            output.status.code().unwrap_or(99) <= 2,
            "queries --by {} should succeed",
            sort_by
        );
    }
}

#[test]
fn test_queries_limit_option() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["queries", "--limit", "5", "--json"]);
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "queries --limit should succeed"
    );

    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");
    assert_eq!(
        data.get("limit"),
        Some(&serde_json::json!(5)),
        "JSON should reflect limit: {}",
        json
    );
}

#[test]
fn test_queries_all_option() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["queries", "--all", "--json"]);
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "queries --all should succeed"
    );

    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");
    assert_eq!(
        data.get("limit"),
        Some(&serde_json::json!(1000)),
        "JSON should reflect large limit for --all: {}",
        json
    );
}

#[test]
fn test_queries_invalid_sort_option() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["queries", "--by", "invalid"]);
    assert!(!output.status.success(), "queries --by invalid should fail");
}

#[test]
fn test_queries_human_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["queries"]);

    let out = stdout(&output);
    // Should show either query info or extension unavailable message
    assert!(
        out.contains("QUERIES")
            || out.contains("pg_stat_statements")
            || out.contains("No query statistics"),
        "Should show query info or extension message: {}",
        out
    );
}
