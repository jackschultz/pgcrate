//! Integration tests for WAL diagnostic command.
//!
//! Tests run against a standard PostgreSQL instance (typically with archiving disabled),
//! testing the basic WAL status path and JSON structure.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

#[test]
fn test_wal_basic() {
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

    // WAL command should succeed (may return warning for disabled archiving)
    let output = project.run_pgcrate(&["dba", "wal"]);

    // Exit code 0 (healthy) or 1 (warning) are both acceptable
    assert!(
        output.status.code() == Some(0) || output.status.code() == Some(1),
        "WAL command should return healthy or warning status, got: {:?}",
        output.status.code()
    );
}

#[test]
fn test_wal_json_structure() {
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

    let output = project.run_pgcrate(&["dba", "wal", "--json"]);

    // May be warning (exit code 1) if archiving is disabled
    assert!(
        output.status.code() == Some(0) || output.status.code() == Some(1),
        "WAL command should return healthy or warning status"
    );

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Schema versioning fields
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.wal"))
    );
    assert!(json.get("schema_version").is_some());
    assert!(json.get("tool_version").is_some());

    // Data fields
    let data = json.get("data").expect("JSON should have data field");
    assert!(data.get("wal_level").is_some(), "Should have wal_level");
    assert!(
        data.get("current_wal_lsn").is_some(),
        "Should have current_wal_lsn"
    );
    assert!(
        data.get("wal_segment_size_bytes").is_some(),
        "Should have wal_segment_size_bytes"
    );
    assert!(
        data.get("wal_directory").is_some(),
        "Should have wal_directory"
    );
    assert!(data.get("archiving").is_some(), "Should have archiving");
    assert!(
        data.get("generation_rate").is_some(),
        "Should have generation_rate"
    );
    assert!(data.get("issues").is_some(), "Should have issues array");
    assert!(
        data.get("overall_status").is_some(),
        "Should have overall_status"
    );
}

#[test]
fn test_wal_shows_wal_level() {
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

    let output = project.run_pgcrate(&["dba", "wal", "--json"]);
    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");

    // wal_level should be a valid PostgreSQL wal_level
    let wal_level = data
        .get("wal_level")
        .and_then(|v| v.as_str())
        .expect("wal_level should be a string");

    assert!(
        ["minimal", "replica", "logical"].contains(&wal_level),
        "wal_level should be minimal, replica, or logical, got: {}",
        wal_level
    );
}

#[test]
fn test_wal_archiving_structure() {
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

    let output = project.run_pgcrate(&["dba", "wal", "--json"]);
    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");
    let archiving = data.get("archiving").expect("Should have archiving object");

    // Check archiving fields
    assert!(
        archiving.get("enabled").is_some(),
        "archiving should have enabled field"
    );
    assert!(
        archiving.get("archive_mode").is_some(),
        "archiving should have archive_mode field"
    );
    assert!(
        archiving.get("failed_count").is_some(),
        "archiving should have failed_count field"
    );
}

#[test]
fn test_wal_human_output() {
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

    let output = project.run_pgcrate(&["dba", "wal"]);
    let out = stdout(&output);

    // Should show WAL health header
    assert!(
        out.contains("WAL HEALTH") || out.contains("WAL CONFIGURATION"),
        "Should show WAL sections: {}",
        out
    );

    // Should show wal_level
    assert!(
        out.contains("wal_level") || out.contains("replica") || out.contains("logical"),
        "Should show wal_level info: {}",
        out
    );

    // Should show archiving section
    assert!(
        out.contains("ARCHIVING") || out.contains("archive"),
        "Should show archiving info: {}",
        out
    );
}

#[test]
fn test_wal_segment_size_is_valid() {
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

    let output = project.run_pgcrate(&["dba", "wal", "--json"]);
    let json = parse_json(&output);
    let data = json.get("data").expect("JSON should have data field");

    let segment_size = data
        .get("wal_segment_size_bytes")
        .and_then(|v| v.as_i64())
        .expect("wal_segment_size_bytes should be an integer");

    // Default is 16MB, but can be configured between 1MB and 1GB
    assert!(
        (1_048_576..=1_073_741_824).contains(&segment_size),
        "wal_segment_size should be between 1MB and 1GB, got: {}",
        segment_size
    );
}
