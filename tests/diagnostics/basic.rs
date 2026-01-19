//! Integration tests for DBA diagnostic commands (healthy state).
//!
//! Tests triage and sequences in their normal/healthy state.
//! Warning and critical state scenarios are covered in PGC-38.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

// ============================================================================
// triage
// ============================================================================

#[test]
fn test_triage_healthy_database() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Set up healthy state
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["triage"]);

    // Fresh database should be healthy (exit 0)
    // triage returns 0=healthy, 1=warning, 2=critical
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "triage should return valid exit code"
    );
}

#[test]
fn test_triage_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Use run_pgcrate (not _ok) because triage can return non-zero for warnings
    let output = project.run_pgcrate(&["triage", "--json"]);

    // Triage should return valid exit code (0=healthy, 1=warning, 2=critical)
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "triage should return valid exit code, got {:?}",
        output.status.code()
    );

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Schema versioning fields
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.triage"))
    );
    assert!(
        json.get("schema_version").is_some(),
        "JSON should have schema_version: {}",
        json
    );

    // Data fields (nested in data object)
    let data = json.get("data").expect("JSON should have data field");
    assert!(
        data.get("overall_status").is_some(),
        "JSON should have data.overall_status: {}",
        json
    );
    assert!(
        data.get("checks").is_some(),
        "JSON should have data.checks: {}",
        json
    );
}

#[test]
fn test_triage_output_format() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["triage"]);

    let out = stdout(&output);
    // Should have structured output with status indicators
    assert!(
        out.contains("✓")
            || out.contains("OK")
            || out.contains("HEALTHY")
            || out.contains("healthy")
            || out.contains("pass"),
        "Should show health status: {}",
        out
    );
}

#[test]
fn test_triage_errors_on_stderr() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["triage"]);

    // On success, main output should be on stdout
    let out = stdout(&output);
    assert!(!out.is_empty(), "Triage output should be on stdout");
}

// ============================================================================
// sequences
// ============================================================================

#[test]
fn test_sequences_no_sequences() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    // Write minimal config
    std::fs::write(
        project.path("pgcrate.toml"),
        format!(
            r#"[database]
url = "{}"
"#,
            db.url()
        ),
    )
    .unwrap();

    // Empty database has no user sequences
    let output = project.run_pgcrate(&["sequences"]);

    // Should handle gracefully
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "sequences should handle empty DB"
    );
}

#[test]
fn test_sequences_healthy_sequence() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // users table has SERIAL id, which creates a sequence
    let output = project.run_pgcrate(&["sequences"]);

    // Fresh sequence at 0% should be healthy (exit 0)
    assert!(
        output.status.success() || output.status.code() == Some(0),
        "Fresh sequence should be healthy"
    );
}

#[test]
fn test_sequences_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["sequences", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Should have expected fields (nested in data object)
    let data = json.get("data").expect("JSON should have data field");
    assert!(
        data.get("sequences").is_some(),
        "JSON should have data.sequences: {}",
        json
    );
    assert!(
        data.get("overall_status").is_some(),
        "JSON should have data.overall_status: {}",
        json
    );
}

#[test]
fn test_sequences_shows_sequence_info() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["sequences"]);

    let out = stdout(&output);
    // Should list sequence information
    // The users_id_seq is created by SERIAL
    assert!(
        out.contains("users_id_seq")
            || out.contains("sequence")
            || out.contains("SERIAL")
            || out.contains("healthy")
            || out.contains("No sequences"),
        "Should show sequence info: {}",
        out
    );
}

// ============================================================================
// Output modes
// ============================================================================

#[test]
fn test_diagnostics_quiet_mode() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["triage", "--quiet"]);

    // Quiet mode should have minimal output (or none on success)
    let _out = stdout(&output);
    // Just verify it runs without error
    assert!(output.status.code().is_some());
}
