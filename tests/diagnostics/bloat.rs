//! Integration tests for bloat diagnostic command.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

#[test]
fn test_bloat_empty_database() {
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

    let output = project.run_pgcrate(&["dba", "bloat"]);

    // Empty database should be healthy
    assert!(
        output.status.success(),
        "Empty database should have no bloat"
    );
}

#[test]
fn test_bloat_with_tables() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "bloat"]);

    // Fresh tables should have minimal bloat (healthy)
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "bloat should return valid exit code"
    );
}

#[test]
fn test_bloat_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["dba", "bloat", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Schema versioning fields
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.bloat"))
    );
    assert!(json.get("schema_version").is_some());

    // Data fields
    let data = json.get("data").expect("JSON should have data field");
    assert!(data.get("tables").is_some(), "Should have tables array");
    assert!(data.get("indexes").is_some(), "Should have indexes array");
    assert!(
        data.get("overall_status").is_some(),
        "Should have overall_status"
    );
    assert!(
        data.get("total_table_bloat_bytes").is_some(),
        "Should have total_table_bloat_bytes"
    );
    assert!(
        data.get("total_index_bloat_bytes").is_some(),
        "Should have total_index_bloat_bytes"
    );
}

#[test]
fn test_bloat_limit_option() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Test with custom limit
    let output = project.run_pgcrate(&["dba", "bloat", "--limit", "5"]);

    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "bloat with limit should work"
    );
}

#[test]
fn test_bloat_human_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["dba", "bloat"]);
    let out = stdout(&output);

    // Should show bloat summary
    assert!(
        out.contains("BLOAT") || out.contains("bloat") || out.contains("No significant bloat"),
        "Should show bloat information: {}",
        out
    );
}
