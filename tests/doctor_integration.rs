//! Integration tests for `pgcrate doctor`.
//!
//! These tests require a running PostgreSQL instance and `psql`.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string())
}

fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

fn run_psql(sql: &str, db_url: &str) -> std::process::Output {
    Command::new("psql")
        .args([db_url, "-v", "ON_ERROR_STOP=1", "-c", sql])
        .output()
        .expect("Failed to execute psql")
}

fn with_db_name(base_url: &str, db_name: &str) -> String {
    base_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, db_name))
        .unwrap_or_else(|| format!("{}/{}", base_url, db_name))
}

fn create_temp_project_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = env::temp_dir().join(format!("pgcrate_doctor_{name}_{nanos}"));
    fs::create_dir_all(&dir).expect("Failed to create temp project dir");
    dir
}

fn unique_db_name(suffix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stamp = (nanos % 1_000_000_000_000u128) as u64;
    format!("pgcrate_doctor_{suffix}_{stamp}")
}

fn write_basic_config(dir: &Path) {
    fs::write(
        dir.join("pgcrate.toml"),
        r#"
[paths]
migrations = "migrations"
"#
        .trim_start(),
    )
    .expect("Failed to write pgcrate.toml");
    fs::create_dir_all(dir.join("migrations")).expect("Failed to create migrations dir");
}

fn run_doctor(dir: &Path, db_url: Option<&str>, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(pgcrate_binary());
    cmd.current_dir(dir).args(args);
    if let Some(url) = db_url {
        cmd.env("DATABASE_URL", url);
    } else {
        cmd.env_remove("DATABASE_URL");
    }
    cmd.output().expect("Failed to execute pgcrate")
}

fn setup_pgcrate_tables(db_url: &str) {
    let sql = r#"
CREATE SCHEMA IF NOT EXISTS pgcrate;
CREATE TABLE IF NOT EXISTS pgcrate.schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ DEFAULT now()
);
"#;
    let out = run_psql(sql, db_url);
    assert!(
        out.status.success(),
        "Failed to set up tables: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn create_test_db(db_url: &str, name: &str) -> Option<String> {
    let test_url = with_db_name(db_url, name);

    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", name), db_url);
    let create = run_psql(&format!("CREATE DATABASE {}", name), db_url);
    if !create.status.success() {
        eprintln!("Skipping test: could not create test database {}", name);
        return None;
    }

    Some(test_url)
}

fn drop_test_db(db_url: &str, name: &str) {
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", name), db_url);
}

#[test]
fn test_doctor_quiet_suppresses_all_output() {
    let project = create_temp_project_dir("quiet");
    write_basic_config(&project);

    let output = run_doctor(&project, None, &["--quiet", "doctor"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty(), "stdout should be empty");
    assert!(output.stderr.is_empty(), "stderr should be empty");

    let output = run_doctor(&project, None, &["--quiet", "--json", "doctor"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty(), "stdout should be empty");
    assert!(output.stderr.is_empty(), "stderr should be empty");

    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_json_fatal_connection_shape() {
    let project = create_temp_project_dir("json_fatal");
    write_basic_config(&project);

    let output = run_doctor(
        &project,
        // Parse-level failure (no network dependency) should still be treated as fatal connection.
        Some("postgres://localhost:abc/postgres"),
        &["--json", "doctor"],
    );

    assert_eq!(output.status.code(), Some(2));

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout should be valid JSON");

    assert_eq!(json["schema_version"], "0.8.0");
    assert_eq!(json["exit_code"], 2);
    assert!(json["generated_at"].as_str().unwrap().contains('T'));
    assert!(json["connection"].as_array().unwrap().len() >= 1);
    assert!(json["schema"].as_array().unwrap().is_empty());
    assert!(json["migrations"].as_array().unwrap().is_empty());
    assert!(json["config"].as_array().unwrap().is_empty());

    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_json_fatal_config_shape() {
    let project = create_temp_project_dir("json_fatal_config");

    // Invalid TOML should be treated as fatal config error (exit 2).
    fs::write(project.join("pgcrate.toml"), "not = [valid").unwrap();

    let output = run_doctor(&project, None, &["--json", "doctor"]);
    assert_eq!(output.status.code(), Some(2));

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout should be valid JSON");

    assert_eq!(json["schema_version"], "0.8.0");
    assert_eq!(json["exit_code"], 2);
    assert!(json["config"].as_array().unwrap().len() >= 1);
    assert!(json["connection"].as_array().unwrap().is_empty());
    assert!(json["schema"].as_array().unwrap().is_empty());
    assert!(json["migrations"].as_array().unwrap().is_empty());

    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_healthy_database_exit_0() {
    let db_url = get_test_db_url();
    let db_name = unique_db_name("healthy");
    let Some(test_url) = create_test_db(&db_url, &db_name) else {
        return;
    };

    let project = create_temp_project_dir("healthy");
    write_basic_config(&project);
    setup_pgcrate_tables(&test_url);

    let output = run_doctor(&project, Some(&test_url), &["doctor"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "doctor should succeed. stdout: {stdout}"
    );
    assert!(stdout.contains("pgcrate doctor"));

    let verbose = run_doctor(&project, Some(&test_url), &["--verbose", "doctor"]);
    let verbose_stdout = String::from_utf8_lossy(&verbose.stdout);
    assert!(verbose.status.success());
    assert!(
        verbose_stdout.contains("Connected to"),
        "verbose output should include passing checks"
    );

    drop_test_db(&db_url, &db_name);
    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_json_success_shape() {
    let db_url = get_test_db_url();
    let db_name = unique_db_name("json_success");
    let Some(test_url) = create_test_db(&db_url, &db_name) else {
        return;
    };

    let project = create_temp_project_dir("json_success");
    write_basic_config(&project);
    setup_pgcrate_tables(&test_url);

    let output = run_doctor(&project, Some(&test_url), &["--json", "doctor"]);
    assert!(output.status.success());

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout should be valid JSON");

    assert_eq!(json["schema_version"], "0.8.0");
    assert_eq!(json["exit_code"], 0);
    assert!(json["generated_at"].as_str().unwrap().contains('T'));
    assert!(json["connection"].as_array().unwrap().len() >= 1);
    assert!(json["summary"].is_object());

    drop_test_db(&db_url, &db_name);
    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_missing_tracking_tables_is_error() {
    let db_url = get_test_db_url();
    let db_name = unique_db_name("missing_tables");
    let Some(test_url) = create_test_db(&db_url, &db_name) else {
        return;
    };

    let project = create_temp_project_dir("missing_tables");
    write_basic_config(&project);

    // Only create schema, not tracking tables.
    let out = run_psql("CREATE SCHEMA IF NOT EXISTS pgcrate;", &test_url);
    assert!(out.status.success());

    let output = run_doctor(&project, Some(&test_url), &["doctor"]);
    assert_eq!(output.status.code(), Some(1));

    drop_test_db(&db_url, &db_name);
    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_pending_migrations_warns() {
    let db_url = get_test_db_url();
    let db_name = unique_db_name("pending");
    let Some(test_url) = create_test_db(&db_url, &db_name) else {
        return;
    };

    let project = create_temp_project_dir("pending");
    write_basic_config(&project);
    setup_pgcrate_tables(&test_url);

    fs::write(
        project.join("migrations/20250101120000_create_users.sql"),
        "-- up\nCREATE TABLE users (id int);\n\n-- down\nDROP TABLE users;\n",
    )
    .unwrap();

    let output = run_doctor(&project, Some(&test_url), &["doctor"]);
    assert!(
        output.status.success(),
        "warnings should not fail by default"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pending migration"), "stdout: {stdout}");

    let strict = run_doctor(&project, Some(&test_url), &["doctor", "--strict"]);
    assert_eq!(strict.status.code(), Some(1));

    drop_test_db(&db_url, &db_name);
    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_orphaned_tracking_rows_is_error() {
    let db_url = get_test_db_url();
    let db_name = unique_db_name("orphans");
    let Some(test_url) = create_test_db(&db_url, &db_name) else {
        return;
    };

    let project = create_temp_project_dir("orphans");
    write_basic_config(&project);
    setup_pgcrate_tables(&test_url);

    let out = run_psql(
        "INSERT INTO pgcrate.schema_migrations (version) VALUES ('20250101120000');",
        &test_url,
    );
    assert!(out.status.success());

    let output = run_doctor(&project, Some(&test_url), &["doctor"]);
    assert_eq!(output.status.code(), Some(1));

    drop_test_db(&db_url, &db_name);
    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_invalid_migration_files_is_error() {
    let db_url = get_test_db_url();
    let db_name = unique_db_name("invalid_migration");
    let Some(test_url) = create_test_db(&db_url, &db_name) else {
        return;
    };

    let project = create_temp_project_dir("invalid_migration");
    write_basic_config(&project);
    setup_pgcrate_tables(&test_url);

    fs::write(
        project.join("migrations/20250101120000_bad.sql"),
        "-- down\nDROP TABLE users;\n",
    )
    .unwrap();

    let output = run_doctor(&project, Some(&test_url), &["doctor"]);
    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Invalid migration files"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("20250101120000_bad.sql"),
        "stdout: {stdout}"
    );

    drop_test_db(&db_url, &db_name);
    let _ = fs::remove_dir_all(&project);
}

#[test]
fn test_doctor_missing_config_and_default_dirs_is_warning() {
    let db_url = get_test_db_url();
    let db_name = unique_db_name("missing_config_defaults");
    let Some(test_url) = create_test_db(&db_url, &db_name) else {
        return;
    };

    // Intentionally do NOT write pgcrate.toml and do NOT create db/migrations or db/seeds.
    let project = create_temp_project_dir("missing_config_defaults");
    setup_pgcrate_tables(&test_url);

    let output = run_doctor(&project, Some(&test_url), &["doctor"]);
    assert!(
        output.status.success(),
        "defaults-mode missing dirs should be warnings only"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pgcrate.toml missing"), "stdout: {stdout}");

    let strict = run_doctor(&project, Some(&test_url), &["doctor", "--strict"]);
    assert_eq!(strict.status.code(), Some(1));

    drop_test_db(&db_url, &db_name);
    let _ = fs::remove_dir_all(&project);
}
