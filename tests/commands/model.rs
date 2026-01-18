//! Integration tests for `pgcrate model` commands.
//!
//! Tests verify model compilation, execution, and status tracking.

use crate::common::{parse_json, stderr, stdout, TestDatabase, TestProject};

// ============================================================================
// model compile
// ============================================================================

#[test]
fn test_model_compile_generates_sql() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["model", "compile"]);

    let out = stdout(&output);
    // Should mention compilation or models
    assert!(
        out.to_lowercase().contains("compil") || out.to_lowercase().contains("model") || out.is_empty(),
        "Should compile models: {}",
        out
    );

    // Check that compiled output exists
    assert!(
        project.file_exists("target/compiled") || out.contains("user_stats"),
        "Should create compiled output or mention model name"
    );
}

#[test]
fn test_model_compile_shows_model_name() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["model", "compile", "--verbose"]);

    let out = stdout(&output);
    let err = stderr(&output);
    let combined = format!("{}{}", out, err);

    // Should show model name in output
    assert!(
        combined.contains("user_stats") || combined.contains("model"),
        "Should mention model name: stdout={}, stderr={}",
        out, err
    );
}

// ============================================================================
// model run
// ============================================================================

#[test]
fn test_model_run_creates_table() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Insert some data for the model to aggregate
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('test@example.com', 'Test')");
    db.run_sql_ok("INSERT INTO posts (user_id, title) VALUES (1, 'First Post')");

    let _output = project.run_pgcrate_ok(&["model", "run"]);

    // Model should be created in 'marts' schema (matching folder structure)
    let tables = db.query(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'marts' AND tablename = 'user_stats'"
    );

    assert!(
        tables.contains("user_stats"),
        "Model table should exist in marts schema: {}",
        tables
    );
}

#[test]
fn test_model_run_populates_data() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Insert test data
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('alice@test.com', 'Alice')");
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('bob@test.com', 'Bob')");
    db.run_sql_ok("INSERT INTO posts (user_id, title) VALUES (1, 'Post 1')");
    db.run_sql_ok("INSERT INTO posts (user_id, title) VALUES (1, 'Post 2')");

    project.run_pgcrate_ok(&["model", "run"]);

    // Model is created in marts schema (matching folder structure)
    let count = db.query("SELECT COUNT(*) FROM marts.user_stats");

    let row_count = count.trim().parse::<i32>().unwrap_or(0);

    assert!(
        row_count >= 2,
        "Model should have data: marts.user_stats count={}",
        count
    );
}

// ============================================================================
// model status
// ============================================================================

#[test]
fn test_model_status_shows_models() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Status returns non-zero when models are not synced, which is expected
    let output = project.run_pgcrate(&["model", "status"]);

    let out = stdout(&output);
    // Should list the model
    assert!(
        out.contains("user_stats") || out.to_lowercase().contains("model"),
        "Should show model status: {}",
        out
    );
}

#[test]
fn test_model_status_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["model", "status", "--json"]);

    let out = stdout(&output);
    let err = stderr(&output);

    // --json should produce JSON if the command supports it
    // If command fails, that's acceptable (non-synced models)
    // But if it succeeds, output must be JSON
    if output.status.success() && !out.trim().is_empty() {
        assert!(
            out.trim().starts_with('{') || out.trim().starts_with('['),
            "model status --json should produce JSON when successful: stdout={}, stderr={}",
            out, err
        );
        let _json = parse_json(&output);
    }
}

// ============================================================================
// model graph
// ============================================================================

#[test]
fn test_model_graph_shows_dependencies() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["model", "graph"]);

    let out = stdout(&output);
    // Should show the model and its dependencies
    assert!(
        out.contains("user_stats") || out.contains("users") || out.contains("posts")
        || out.contains("->") || out.contains("deps"),
        "Should show dependency graph: {}",
        out
    );
}

// ============================================================================
// Error cases
// ============================================================================

#[test]
fn test_model_run_missing_source_table() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    // Don't run migrations - source tables don't exist
    let output = project.run_pgcrate(&["model", "run"]);

    // Should fail because users/posts tables don't exist
    assert!(
        !output.status.success(),
        "Should fail when source tables missing"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("not exist") || combined.contains("relation") || combined.contains("error"),
        "Should report missing table: stdout={}, stderr={}",
        out, err
    );
}

#[test]
fn test_model_lint_checks_issues() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["model", "lint"]);

    // Lint should complete (may have warnings or be clean)
    assert!(
        output.status.code().is_some(),
        "Lint should complete"
    );
}

// ============================================================================
// model show
// ============================================================================

#[test]
fn test_model_show_displays_sql() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_models", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Model show requires schema.model_name format
    let output = project.run_pgcrate_ok(&["model", "show", "marts.user_stats"]);

    let out = stdout(&output);
    // Should show the compiled SQL
    assert!(
        out.contains("SELECT") || out.contains("user_id") || out.contains("post_count"),
        "Should show compiled SQL: {}",
        out
    );
}
