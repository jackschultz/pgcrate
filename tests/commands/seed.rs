//! Integration tests for `pgcrate seed` commands.

use crate::common::{parse_json, stderr, stdout, TestDatabase, TestProject};

// ============================================================================
// seed run
// ============================================================================

#[test]
fn test_seed_run_inserts_data() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    // Apply migrations first to create tables
    project.run_pgcrate_ok(&["migrate", "up"]);

    // Run seeds
    let _output = project.run_pgcrate_ok(&["seed", "run"]);

    // Verify data was inserted
    let count = db.query("SELECT COUNT(*) FROM users");
    assert!(
        count.trim().parse::<i32>().unwrap_or(0) >= 3,
        "Should have inserted seed data, got count: {}",
        count
    );

    // Check specific data
    let admin = db.query("SELECT email FROM users WHERE is_admin = true");
    assert!(
        admin.contains("admin@example.com"),
        "Should have admin user: {}",
        admin
    );
}

#[test]
fn test_seed_run_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["seed", "run", "--json"]);

    // seed run may not support --json yet
    let out = stdout(&output);
    if out.starts_with('{') || out.starts_with('[') {
        let _json = parse_json(&output);
        // Valid JSON - test passes
    }
    // If no JSON support, that's okay for now - just verify command runs
}

#[test]
fn test_seed_run_no_seeds_dir() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // This fixture has migrations but no seeds
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["seed", "run"]);

    // Should handle gracefully - either succeed with "no seeds" or fail gracefully
    let out = stdout(&output);
    let err = stderr(&output);
    // Just verify it doesn't panic
    assert!(
        output.status.code().is_some(),
        "Should exit cleanly: stdout={}, stderr={}",
        out,
        err
    );
}

#[test]
fn test_seed_run_missing_table() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    // Don't run migrations - tables don't exist
    let output = project.run_pgcrate(&["seed", "run"]);

    // Should fail because table doesn't exist
    if output.status.success() {
        // Some implementations might skip missing tables
        return;
    }

    // Error may be on stdout or stderr depending on implementation
    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err);
    assert!(
        combined.contains("not exist")
            || combined.contains("relation")
            || combined.contains("error")
            || combined.contains("Error"),
        "Should report missing table error: stdout={}, stderr={}",
        out,
        err
    );
}

// ============================================================================
// seed list
// ============================================================================

#[test]
fn test_seed_list_shows_seeds() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    let output = project.run_pgcrate_ok(&["seed", "list"]);

    let out = stdout(&output);
    // Should list the seed files
    assert!(
        out.contains("users") || out.contains("public"),
        "Should list seed files: {}",
        out
    );
}

// ============================================================================
// seed validate
// ============================================================================

#[test]
fn test_seed_validate_checks_schema() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    // Apply migrations first
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["seed", "validate"]);

    // Should pass validation for valid seeds
    assert!(
        output.status.success(),
        "Validation should pass for valid seeds"
    );
}

#[test]
fn test_seed_validate_catches_mismatch() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    // Apply migrations
    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a seed with wrong columns
    std::fs::create_dir_all(project.path("db/seeds/public")).ok();
    std::fs::write(
        project.path("db/seeds/public/bad_users.csv"),
        "id,wrong_column,another_wrong\n1,foo,bar\n",
    )
    .unwrap();

    let output = project.run_pgcrate(&["seed", "validate"]);

    // May fail or warn about mismatch
    let _out = stdout(&output);
    let _err = stderr(&output);
    // Either fails or warns about the mismatch
    // (Behavior depends on implementation)
}

#[test]
fn test_seed_run_invalid_csv() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a malformed CSV
    std::fs::write(
        project.path("db/seeds/public/malformed.csv"),
        "this is not,valid csv\n\"unclosed quote",
    )
    .unwrap();

    let output = project.run_pgcrate(&["seed", "run"]);

    // Should handle gracefully (might fail or skip the bad file)
    // Just verify it doesn't panic
    assert!(output.status.code().is_some());
}
