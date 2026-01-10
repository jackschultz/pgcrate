//! Integration tests for model subcommands.
//!
//! These tests require a running PostgreSQL instance for `model show` on incremental models.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string())
}

fn create_temp_project_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = env::temp_dir().join(format!("pgcrate_model_{name}_{nanos}"));
    fs::create_dir_all(&dir).expect("Failed to create temp project dir");
    dir
}

#[test]
fn test_model_new_accepts_yes_and_creates_file() {
    let dir = create_temp_project_dir("new_yes");

    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .args(["model", "new", "analytics.user_stats", "-y"])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(out.status.success(), "model new should succeed");

    let path = dir.join("models/analytics/user_stats.sql");
    let contents = fs::read_to_string(&path).expect("model file should exist");
    assert!(
        contents.contains("-- materialized:"),
        "model scaffold should include materialized header"
    );
}

#[test]
fn test_model_new_incremental_scaffolds_watermark() {
    let dir = create_temp_project_dir("new_incremental");

    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .args([
            "model",
            "new",
            "analytics.daily_order_stats",
            "--materialized",
            "incremental",
            "-y",
        ])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(out.status.success(), "model new incremental should succeed");

    let path = dir.join("models/analytics/daily_order_stats.sql");
    let contents = fs::read_to_string(&path).expect("model file should exist");
    assert!(contents.contains("-- materialized: incremental"));
    assert!(contents.contains("-- unique_key:"));
    assert!(contents.contains("-- watermark:"));
}

/// Regression test for PostgreSQL error 42809: DROP TABLE IF EXISTS fails when object is a VIEW.
///
/// This test verifies that changing a model's materialization from VIEW to TABLE works correctly.
/// The bug was: `DROP TABLE IF EXISTS` doesn't silently succeed when a VIEW exists - it throws
/// error 42809 "wrong_object_type". The fix ensures we drop both VIEW and TABLE before creating.
#[test]
fn test_model_run_handles_view_to_table_change() {
    let dir = create_temp_project_dir("view_to_table");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let unique_name = format!("metrics_v2t_{}", nanos % 1_000_000);

    // Create pgcrate.toml config
    fs::write(
        dir.join("pgcrate.toml"),
        r#"[paths]
models = "models"
"#,
    )
    .expect("write config");

    // Create a model as VIEW initially (use public schema to avoid schema creation issues)
    fs::create_dir_all(dir.join("models/public")).expect("create models dir");
    fs::write(
        dir.join(format!("models/public/{}.sql", unique_name)),
        r#"-- materialized: view

SELECT 1 AS id, 'initial' AS status
"#,
    )
    .expect("write model file");

    let db_url = get_test_db_url();

    // Run as VIEW first
    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .env("DATABASE_URL", &db_url)
        .args(["model", "run"])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(
        out.status.success(),
        "model run (view) should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Now change to TABLE materialization (simulates user editing the file)
    fs::write(
        dir.join(format!("models/public/{}.sql", unique_name)),
        r#"-- materialized: table

SELECT 1 AS id, 'changed_to_table' AS status
"#,
    )
    .expect("rewrite model as table");

    // This would fail with error 42809 before the fix
    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .env("DATABASE_URL", &db_url)
        .args(["model", "run"])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(
        out.status.success(),
        "model run (view→table change) should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Complementary test: TABLE to VIEW change should also work
#[test]
fn test_model_run_handles_table_to_view_change() {
    let dir = create_temp_project_dir("table_to_view");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let unique_name = format!("stats_t2v_{}", nanos % 1_000_000);

    fs::write(
        dir.join("pgcrate.toml"),
        r#"[paths]
models = "models"
"#,
    )
    .expect("write config");

    fs::create_dir_all(dir.join("models/public")).expect("create models dir");
    fs::write(
        dir.join(format!("models/public/{}.sql", unique_name)),
        r#"-- materialized: table

SELECT 1 AS id
"#,
    )
    .expect("write model file");

    let db_url = get_test_db_url();

    // Run as TABLE first
    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .env("DATABASE_URL", &db_url)
        .args(["model", "run"])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(
        out.status.success(),
        "model run (table) should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Change to VIEW
    fs::write(
        dir.join(format!("models/public/{}.sql", unique_name)),
        r#"-- materialized: view

SELECT 1 AS id
"#,
    )
    .expect("rewrite model as view");

    // Run again - should handle the table→view transition
    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .env("DATABASE_URL", &db_url)
        .args(["model", "run"])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(
        out.status.success(),
        "model run (table→view change) should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn test_model_show_json_includes_merge_sql_for_incremental() {
    let dir = create_temp_project_dir("show_incremental");

    fs::create_dir_all(dir.join("models/analytics")).expect("create models dir");
    fs::write(
        dir.join("models/analytics/test.sql"),
        r#"-- materialized: incremental
-- unique_key: id

-- @base
SELECT 1 AS id
"#,
    )
    .expect("write model file");

    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .env("DATABASE_URL", get_test_db_url())
        .args(["--json", "model", "show", "analytics.test"])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(
        out.status.success(),
        "model show should succeed: stderr={} stdout={}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("valid json");

    assert_eq!(json["ok"], true);
    assert_eq!(json["materialized"], "incremental");
    assert!(json["sql"]["merge"]
        .as_str()
        .unwrap()
        .contains("MERGE INTO"));
    assert!(json["sql"]["create"]
        .as_str()
        .unwrap()
        .contains("CREATE TABLE"));
}
