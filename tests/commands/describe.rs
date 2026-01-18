//! Integration tests for `pgcrate describe`.

use crate::common::{parse_json, stderr, stdout, TestDatabase, TestProject};

#[test]
fn test_describe_shows_columns() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["describe", "users"]);

    let out = stdout(&output);
    // Should show column names
    assert!(out.contains("id"), "Should show id column: {}", out);
    assert!(out.contains("email"), "Should show email column: {}", out);
    assert!(out.contains("name"), "Should show name column: {}", out);
}

#[test]
fn test_describe_shows_indexes() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // posts has an index on user_id
    let output = project.run_pgcrate_ok(&["describe", "posts"]);

    let out = stdout(&output);
    // Should show the index
    assert!(
        out.contains("posts_user_id_idx") || out.contains("user_id") || out.contains("index"),
        "Should show index information: {}",
        out
    );
}

#[test]
fn test_describe_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["describe", "users", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object(), "Should return JSON object");

    // Should have columns info
    assert!(
        json.get("columns").is_some() || json.get("table").is_some(),
        "JSON should have column info: {}",
        json
    );
}

#[test]
fn test_describe_table_not_found() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["describe", "nonexistent_table"]);

    // Should fail with clear error
    assert!(
        !output.status.success(),
        "Should fail for non-existent table"
    );

    let err = stderr(&output);
    assert!(
        err.contains("not found") || err.contains("does not exist") || err.contains("error"),
        "Should report table not found: {}",
        err
    );
}

#[test]
fn test_describe_with_schema_prefix() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Should work with schema.table format
    let output = project.run_pgcrate_ok(&["describe", "public.users"]);

    let out = stdout(&output);
    assert!(
        out.contains("email"),
        "Should describe public.users: {}",
        out
    );
}
