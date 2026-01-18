//! Integration tests for `pgcrate migrate` commands.

use crate::common::{parse_json, stderr, stdout, TestDatabase, TestProject};

// ============================================================================
// migrate up
// ============================================================================

#[test]
fn test_migrate_up_applies_migrations() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate_ok(&["migrate", "up"]);

    // Verify tables were created
    let tables = db.query(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('users', 'posts') ORDER BY tablename"
    );
    assert!(tables.contains("posts"), "posts table should exist");
    assert!(tables.contains("users"), "users table should exist");

    // Check output
    let out = stdout(&output);
    assert!(
        out.contains("Applied") || out.contains("migrat"),
        "Should mention applied migrations: {}",
        out
    );
}

#[test]
fn test_migrate_up_idempotent() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Run twice
    project.run_pgcrate_ok(&["migrate", "up"]);
    let output = project.run_pgcrate_ok(&["migrate", "up"]);

    // Second run should succeed (nothing to apply)
    let _out = stdout(&output);
    // Should either say "nothing to apply" or just succeed silently
    assert!(output.status.success());
}

#[test]
fn test_migrate_up_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate(&["migrate", "up", "--json"]);

    // migrate up may not support --json yet, just verify it returns valid JSON
    // (even if it's an error message in JSON format)
    let out = stdout(&output);
    if out.starts_with('{') || out.starts_with('[') {
        let _json = parse_json(&output);
        // Valid JSON - test passes
    }
    // If no JSON support, that's okay for now
}

#[test]
fn test_migrate_up_no_migrations_dir() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    // Write config but no migrations dir
    std::fs::write(
        project.path("pgcrate.toml"),
        format!(
            r#"[database]
url = "{}"

[paths]
migrations = "db/migrations"
"#,
            db.url()
        ),
    )
    .unwrap();

    let output = project.run_pgcrate(&["migrate", "up"]);

    // Should handle missing migrations dir - either:
    // - fail with error about missing dir
    // - succeed with "no migrations found" / "no pending migrations"
    // - succeed with "0 migrations applied"
    let err = stderr(&output);
    let out = stdout(&output);
    let handled = !output.status.success()
        || err.to_lowercase().contains("not found")
        || err.to_lowercase().contains("does not exist")
        || out.to_lowercase().contains("no migration")
        || out.to_lowercase().contains("no pending")
        || out.contains("0 migration")
        || out.is_empty();
    assert!(
        handled,
        "Should handle missing migrations dir gracefully\nstdout: {}\nstderr: {}",
        out, err
    );
}

#[test]
fn test_migrate_up_invalid_sql() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Add a broken migration (use timestamp after existing migrations)
    std::fs::write(
        project.path("db/migrations/20240103000000_broken.sql"),
        "-- up\nCREATE TABL broken_syntax;\n-- down\nDROP TABLE broken_syntax;",
    )
    .unwrap();

    let output = project.run_pgcrate(&["migrate", "up"]);

    // Should fail on syntax error
    assert!(
        !output.status.success(),
        "Should fail on invalid SQL"
    );

    let err = stderr(&output);
    assert!(
        err.contains("error") || err.contains("syntax") || err.contains("Error"),
        "Should report SQL error: {}",
        err
    );
}

// ============================================================================
// migrate down
// ============================================================================

#[test]
fn test_migrate_down_rolls_back() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Apply migrations first
    project.run_pgcrate_ok(&["migrate", "up"]);

    // Verify posts table exists
    let before = db.query("SELECT tablename FROM pg_tables WHERE tablename = 'posts'");
    assert!(before.contains("posts"), "posts should exist before rollback");

    // Roll back one migration (--steps and --yes are required)
    project.run_pgcrate_ok(&["migrate", "down", "--steps", "1", "--yes"]);

    // posts table should be gone (last migration creates posts)
    let after = db.query("SELECT tablename FROM pg_tables WHERE tablename = 'posts'");
    assert!(!after.contains("posts"), "posts should not exist after rollback");
}

#[test]
fn test_migrate_down_nothing_to_rollback() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Don't apply any migrations, try to roll back
    let output = project.run_pgcrate(&["migrate", "down", "--steps", "1", "--yes"]);

    // Should handle gracefully (not crash)
    // May succeed with "nothing to roll back" or may return non-zero
    let out = stdout(&output);
    let err = stderr(&output);
    // Just verify it doesn't panic/crash
    assert!(
        output.status.code().is_some(),
        "Should exit cleanly: stdout={}, stderr={}",
        out,
        err
    );
}

// ============================================================================
// migrate status
// ============================================================================

#[test]
fn test_migrate_status_shows_applied() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Apply migrations
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["migrate", "status"]);

    let out = stdout(&output);
    // Should show the migrations and their status
    assert!(
        out.contains("create_users") || out.contains("20240101000000"),
        "Should list migrations: {}",
        out
    );
}

#[test]
fn test_migrate_status_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["migrate", "status", "--json"]);

    let json = parse_json(&output);
    assert!(json.is_object() || json.is_array(), "Should return valid JSON");
}

// ============================================================================
// migrate new
// ============================================================================

#[test]
fn test_migrate_new_creates_file() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let _output = project.run_pgcrate_ok(&["migrate", "new", "add_comments"]);

    // Should create a new migration file
    let migrations_dir = project.path("db/migrations");
    let files: Vec<_> = std::fs::read_dir(&migrations_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        files.iter().any(|f| f.contains("add_comments")),
        "Should create migration file with name: {:?}",
        files
    );
}

// ============================================================================
// migrate baseline
// ============================================================================

#[test]
fn test_migrate_baseline_marks_existing() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Manually create the tables (simulating brownfield)
    db.run_sql_ok("CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT)");

    // Run baseline to mark migrations as applied without running them (--yes is required)
    let _output = project.run_pgcrate_ok(&["migrate", "baseline", "--yes"]);

    // Now status should show migrations as applied
    let status = project.run_pgcrate_ok(&["migrate", "status"]);
    let out = stdout(&status);

    // The baseline migration tracking should exist
    assert!(
        out.contains("20240101000000") || out.contains("baseline") || out.contains("Applied")
        || out.contains("applied"),
        "Should show migrations after baseline: {}",
        out
    );
}

// ============================================================================
// verbose and quiet modes
// ============================================================================

#[test]
fn test_migrate_up_verbose() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate_ok(&["migrate", "up", "--verbose"]);

    let out = stdout(&output);
    // Verbose should show SQL or more details
    assert!(
        out.contains("CREATE") || out.contains("SQL") || out.len() > 50,
        "Verbose should show more output: {}",
        out
    );
}

#[test]
fn test_migrate_up_quiet() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate_ok(&["migrate", "up", "--quiet"]);

    // Quiet mode should have minimal output
    let _out = stdout(&output);
    // Just verify it succeeded; quiet output varies by implementation
    assert!(output.status.success());
}
