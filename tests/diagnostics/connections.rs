//! Integration tests for `pgcrate connections` command.
//!
//! Tests connection analysis vs max_connections.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

// ============================================================================
// connections - basic functionality
// ============================================================================

#[test]
fn test_connections_runs_without_error() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // connections should succeed - pg_stat_activity is always available
    let output = project.run_pgcrate(&["connections"]);

    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "connections should return valid exit code (0=healthy, 1=warning, 2=critical)"
    );
}

#[test]
fn test_connections_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["connections", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Schema versioning fields
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.connections"))
    );
    assert!(
        json.get("schema_version").is_some(),
        "JSON should have schema_version: {}",
        json
    );

    // Data fields (nested in data object)
    let data = json.get("data").expect("JSON should have data field");
    assert!(
        data.get("stats").is_some(),
        "JSON should have data.stats: {}",
        json
    );
    assert!(
        data.get("overall_status").is_some(),
        "JSON should have data.overall_status: {}",
        json
    );

    // Stats should have expected fields
    let stats = data.get("stats").expect("should have stats");
    assert!(
        stats.get("total").is_some(),
        "stats should have total: {}",
        json
    );
    assert!(
        stats.get("max_connections").is_some(),
        "stats should have max_connections: {}",
        json
    );
    assert!(
        stats.get("usage_pct").is_some(),
        "stats should have usage_pct: {}",
        json
    );
    assert!(
        stats.get("by_state").is_some(),
        "stats should have by_state: {}",
        json
    );
}

#[test]
fn test_connections_by_user_option() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["connections", "--by-user", "--json"]);

    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");
    assert!(
        data.get("by_user").is_some(),
        "JSON should have data.by_user when --by-user is specified: {}",
        json
    );
}

#[test]
fn test_connections_by_database_option() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["connections", "--by-database", "--json"]);

    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");
    assert!(
        data.get("by_database").is_some(),
        "JSON should have data.by_database when --by-database is specified: {}",
        json
    );
}

#[test]
fn test_connections_both_options() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["connections", "--by-user", "--by-database", "--json"]);

    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");
    assert!(
        data.get("by_user").is_some(),
        "JSON should have data.by_user: {}",
        json
    );
    assert!(
        data.get("by_database").is_some(),
        "JSON should have data.by_database: {}",
        json
    );
}

#[test]
fn test_connections_human_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["connections"]);

    let out = stdout(&output);
    // Should show connection summary
    assert!(
        out.contains("CONNECTIONS") || out.contains("connections"),
        "Should show connections header: {}",
        out
    );
    assert!(
        out.contains("max_connections") || out.contains("/") || out.contains("%"),
        "Should show connection usage: {}",
        out
    );
}

#[test]
fn test_connections_shows_states() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["connections"]);

    let out = stdout(&output);
    // Should show state breakdown
    assert!(
        out.contains("STATE") || out.contains("idle") || out.contains("active"),
        "Should show connection states: {}",
        out
    );
}

#[test]
fn test_connections_healthy_on_fresh_db() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // A test database should have very few connections (healthy)
    let output = project.run_pgcrate(&["connections", "--json"]);

    assert!(
        output.status.success(),
        "Fresh DB should have healthy connection count"
    );

    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");
    let status = data
        .get("overall_status")
        .expect("should have overall_status");
    assert_eq!(
        status,
        &serde_json::json!("healthy"),
        "Fresh DB should be healthy: {}",
        json
    );
}
