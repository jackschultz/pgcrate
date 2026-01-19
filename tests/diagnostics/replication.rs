//! Integration tests for replication diagnostic command.
//!
//! Note: These tests run against a standalone database without replicas,
//! so they test the "no replication" path and JSON structure.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

#[test]
fn test_replication_standalone_server() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

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

    // Standalone server should succeed with healthy status
    let output = project.run_pgcrate(&["replication"]);

    assert!(
        output.status.success(),
        "Standalone server should report healthy replication status"
    );
}

#[test]
fn test_replication_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

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

    let output = project.run_pgcrate_ok(&["replication", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Schema versioning fields
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.replication"))
    );
    assert!(json.get("schema_version").is_some());

    // Data fields
    let data = json.get("data").expect("JSON should have data field");
    assert!(data.get("server_role").is_some(), "Should have server_role");
    assert!(data.get("replicas").is_some(), "Should have replicas array");
    assert!(data.get("slots").is_some(), "Should have slots array");
    assert!(
        data.get("overall_status").is_some(),
        "Should have overall_status"
    );
}

#[test]
fn test_replication_detects_primary() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

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

    let output = project.run_pgcrate_ok(&["replication", "--json"]);

    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");

    // Standalone server should report as primary
    assert_eq!(
        data.get("server_role"),
        Some(&serde_json::json!("primary")),
        "Standalone server should report as primary"
    );
}

#[test]
fn test_replication_human_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

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

    let output = project.run_pgcrate(&["replication"]);
    let out = stdout(&output);

    // Should show role and status
    assert!(
        out.contains("PRIMARY") || out.contains("STANDBY"),
        "Should show server role: {}",
        out
    );
    assert!(
        out.contains("REPLICATION") || out.contains("replication"),
        "Should contain replication info: {}",
        out
    );
}
