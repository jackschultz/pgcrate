//! Integration tests for new diagnostic commands:
//! - stats-age: Tables with stale statistics
//! - checkpoints: Checkpoint frequency and health
//! - autovacuum-progress: Currently running autovacuum
//! - config: PostgreSQL configuration review

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

// ============================================================================
// stats-age
// ============================================================================

#[test]
fn test_stats_age_runs() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // stats-age should run without error
    let output = project.run_pgcrate(&["dba", "stats-age"]);

    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "stats-age should return valid exit code"
    );
}

#[test]
fn test_stats_age_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "stats-age", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Check envelope
    assert!(json.get("schema_id").is_some(), "Should have schema_id");
    assert!(json.get("severity").is_some(), "Should have severity");

    // Check data structure
    let data = json.get("data").expect("Should have data field");
    assert!(
        data.get("tables").is_some(),
        "Should have data.tables: {}",
        json
    );
    assert!(
        data.get("overall_status").is_some(),
        "Should have data.overall_status: {}",
        json
    );
}

#[test]
fn test_stats_age_respects_threshold() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // With very low threshold (0.001 days = ~1.4 minutes), all tables should pass
    let output = project.run_pgcrate(&["dba", "stats-age", "--threshold", "0.001"]);

    // Should succeed (exit 0 = healthy)
    assert!(
        output.status.code().unwrap_or(99) <= 1,
        "Fresh tables should be healthy with low threshold"
    );
}

#[test]
fn test_stats_age_respects_limit() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create multiple tables for the test
    for i in 1..=5 {
        db.run_sql_ok(&format!(
            "CREATE TABLE stats_test_{} (id SERIAL PRIMARY KEY, data TEXT)",
            i
        ));
        // Insert some data so tables have rows
        db.run_sql_ok(&format!(
            "INSERT INTO stats_test_{} (data) VALUES ('test')",
            i
        ));
    }

    let output = project.run_pgcrate(&["dba", "stats-age", "--limit", "2", "--json"]);

    let out = stdout(&output);
    let json: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("Invalid JSON: {}\nOutput: {}", e, out));

    if let Some(data) = json.get("data") {
        if let Some(tables) = data.get("tables").and_then(|t| t.as_array()) {
            assert!(
                tables.len() <= 2,
                "Should respect --limit 2, got {} tables",
                tables.len()
            );
        }
    }
}

// ============================================================================
// checkpoints
// ============================================================================

#[test]
fn test_checkpoints_runs() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // checkpoints should run without error
    let output = project.run_pgcrate(&["dba", "checkpoints"]);

    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "checkpoints should return valid exit code"
    );
}

#[test]
fn test_checkpoints_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "checkpoints", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Check envelope
    assert!(json.get("schema_id").is_some(), "Should have schema_id");
    assert_eq!(
        json.get("schema_id").and_then(|s| s.as_str()),
        Some("pgcrate.diagnostics.checkpoints")
    );

    // Check data structure
    let data = json.get("data").expect("Should have data field");
    assert!(
        data.get("stats").is_some(),
        "Should have data.stats: {}",
        json
    );
    assert!(
        data.get("overall_status").is_some(),
        "Should have data.overall_status: {}",
        json
    );
}

#[test]
fn test_checkpoints_shows_metrics() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "checkpoints"]);

    let out = stdout(&output);

    // Should show checkpoint statistics
    assert!(
        out.contains("checkpoint") || out.contains("Checkpoint"),
        "Should show checkpoint info: {}",
        out
    );
}

// ============================================================================
// autovacuum-progress
// ============================================================================

#[test]
fn test_autovacuum_progress_runs() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // autovacuum-progress should run without error
    let output = project.run_pgcrate(&["dba", "autovacuum-progress"]);

    // This command always returns 0 (informational only)
    assert!(
        output.status.success(),
        "autovacuum-progress should succeed"
    );
}

#[test]
fn test_autovacuum_progress_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "autovacuum-progress", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Check envelope
    assert!(json.get("schema_id").is_some(), "Should have schema_id");
    assert_eq!(
        json.get("schema_id").and_then(|s| s.as_str()),
        Some("pgcrate.diagnostics.autovacuum_progress")
    );
    // Should always be healthy (informational)
    assert_eq!(
        json.get("severity").and_then(|s| s.as_str()),
        Some("healthy")
    );

    // Check data structure
    let data = json.get("data").expect("Should have data field");
    assert!(
        data.get("workers").is_some(),
        "Should have data.workers: {}",
        json
    );
    assert!(
        data.get("count").is_some(),
        "Should have data.count: {}",
        json
    );
}

#[test]
fn test_autovacuum_progress_empty_is_ok() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "autovacuum-progress"]);

    let out = stdout(&output);

    // When no autovacuum is running, should show friendly message
    assert!(
        out.contains("No autovacuum") || out.contains("autovacuum") || out.contains("running"),
        "Should show autovacuum status: {}",
        out
    );
}

// ============================================================================
// config
// ============================================================================

#[test]
fn test_config_runs() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // config should run without error
    let output = project.run_pgcrate(&["dba", "config"]);

    assert!(
        output.status.code().unwrap_or(99) <= 1,
        "config should return valid exit code (0 or 1 for suggestions)"
    );
}

#[test]
fn test_config_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "config", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Check envelope
    assert!(json.get("schema_id").is_some(), "Should have schema_id");
    assert_eq!(
        json.get("schema_id").and_then(|s| s.as_str()),
        Some("pgcrate.diagnostics.config")
    );

    // Should never be critical (always healthy or warning)
    let severity = json.get("severity").and_then(|s| s.as_str());
    assert!(
        severity == Some("healthy") || severity == Some("warning"),
        "config should never be critical, got: {:?}",
        severity
    );

    // Check data structure
    let data = json.get("data").expect("Should have data field");
    assert!(
        data.get("settings").is_some(),
        "Should have data.settings: {}",
        json
    );
    assert!(
        data.get("disclaimer").is_some(),
        "Should have data.disclaimer: {}",
        json
    );
    assert!(
        data.get("postgres_version").is_some(),
        "Should have data.postgres_version: {}",
        json
    );
}

#[test]
fn test_config_shows_settings() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "config"]);

    let out = stdout(&output);

    // Should show key settings
    assert!(
        out.contains("shared_buffers") || out.contains("memory") || out.contains("Memory"),
        "Should show memory settings: {}",
        out
    );
}

#[test]
fn test_config_includes_disclaimer() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "config"]);

    let out = stdout(&output);

    // Should include safety disclaimer
    assert!(
        out.contains("suggestion")
            || out.contains("Recommendation")
            || out.contains("test")
            || out.contains("Note"),
        "Should include disclaimer or note: {}",
        out
    );
}

#[test]
fn test_config_includes_requires_restart() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "config", "--json"]);

    let out = stdout(&output);
    let json: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("Invalid JSON: {}\nOutput: {}", e, out));

    let data = json.get("data").expect("Should have data field");
    let settings = data
        .get("settings")
        .and_then(|s| s.as_array())
        .expect("Should have settings array");

    // All settings should have requires_restart field
    for setting in settings {
        assert!(
            setting.get("requires_restart").is_some(),
            "Setting should have requires_restart field: {}",
            setting
        );
    }

    // shared_buffers should require restart (postmaster context)
    let shared_buffers = settings
        .iter()
        .find(|s| s.get("name").and_then(|n| n.as_str()) == Some("shared_buffers"));
    if let Some(sb) = shared_buffers {
        assert_eq!(
            sb.get("requires_restart").and_then(|r| r.as_bool()),
            Some(true),
            "shared_buffers should require restart"
        );
    }
}

// ============================================================================
// FK index detection (extended tests)
// ============================================================================

#[test]
fn test_fk_index_json_includes_fk_without_indexes() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create tables with FK but no index
    db.run_sql_ok(
        "CREATE TABLE fk_parent (
            id SERIAL PRIMARY KEY,
            name TEXT
        )",
    );
    db.run_sql_ok(
        "CREATE TABLE fk_child (
            id SERIAL PRIMARY KEY,
            parent_id INTEGER REFERENCES fk_parent(id),
            data TEXT
        )",
    );

    let output = project.run_pgcrate(&["dba", "indexes", "--json"]);

    let out = stdout(&output);
    let json: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("Invalid JSON: {}\nOutput: {}", e, out));

    let data = json.get("data").expect("Should have data field");
    assert!(
        data.get("fk_without_indexes").is_some(),
        "Should have data.fk_without_indexes: {}",
        json
    );
}

#[test]
fn test_fk_composite_key_detection() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create tables with composite FK but no matching index
    db.run_sql_ok(
        "CREATE TABLE composite_parent (
            tenant_id INTEGER,
            id INTEGER,
            name TEXT,
            PRIMARY KEY (tenant_id, id)
        )",
    );
    db.run_sql_ok(
        "CREATE TABLE composite_child (
            id SERIAL PRIMARY KEY,
            tenant_id INTEGER,
            parent_id INTEGER,
            data TEXT,
            FOREIGN KEY (tenant_id, parent_id) REFERENCES composite_parent(tenant_id, id)
        )",
    );
    // Note: No index on (tenant_id, parent_id) - should be flagged

    let output = project.run_pgcrate(&["dba", "indexes", "--json"]);

    let out = stdout(&output);
    let json: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("Invalid JSON: {}\nOutput: {}", e, out));

    let data = json.get("data").expect("Should have data field");
    let fk_list = data
        .get("fk_without_indexes")
        .and_then(|f| f.as_array())
        .expect("Should have fk_without_indexes array");

    // Should detect the composite FK without index
    let has_composite = fk_list
        .iter()
        .any(|fk| fk.to_string().contains("composite_child"));
    assert!(
        has_composite,
        "Should detect composite FK without index: {:?}",
        fk_list
    );
}

#[test]
fn test_fk_composite_key_with_index_not_flagged() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create tables with composite FK that HAS a matching index
    db.run_sql_ok(
        "CREATE TABLE comp_indexed_parent (
            tenant_id INTEGER,
            id INTEGER,
            name TEXT,
            PRIMARY KEY (tenant_id, id)
        )",
    );
    db.run_sql_ok(
        "CREATE TABLE comp_indexed_child (
            id SERIAL PRIMARY KEY,
            tenant_id INTEGER,
            parent_id INTEGER,
            data TEXT,
            FOREIGN KEY (tenant_id, parent_id) REFERENCES comp_indexed_parent(tenant_id, id)
        )",
    );
    // Add index covering the FK columns
    db.run_sql_ok(
        "CREATE INDEX comp_indexed_child_fk_idx ON comp_indexed_child(tenant_id, parent_id)",
    );

    let output = project.run_pgcrate(&["dba", "indexes", "--json"]);

    let out = stdout(&output);
    let json: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("Invalid JSON: {}\nOutput: {}", e, out));

    let data = json.get("data").expect("Should have data field");
    if let Some(fk_list) = data.get("fk_without_indexes").and_then(|f| f.as_array()) {
        let has_comp_indexed = fk_list
            .iter()
            .any(|fk| fk.to_string().contains("comp_indexed_child"));
        assert!(
            !has_comp_indexed,
            "FK with matching index should not be flagged: {:?}",
            fk_list
        );
    }
}
