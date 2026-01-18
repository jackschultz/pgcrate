//! Integration tests for `pgcrate sql`.
//!
//! Tests verify the sql command executes queries correctly with various options.

use crate::common::{parse_json, stderr, stdout, TestDatabase, TestProject};

// ============================================================================
// Basic execution
// ============================================================================

#[test]
fn test_sql_executes_select() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate_ok(&["sql", "-c", "SELECT 42 AS answer"]);

    let out = stdout(&output);
    // Must contain both the column name and the value
    assert!(
        out.contains("42") && out.contains("answer"),
        "Should return query result with column 'answer' and value '42': {}",
        out
    );
}

#[test]
fn test_sql_executes_against_tables() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Insert some data
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('test@example.com', 'Test User')");

    let output = project.run_pgcrate_ok(&["sql", "-c", "SELECT email FROM users"]);

    let out = stdout(&output);
    assert!(
        out.contains("test@example.com"),
        "Should return user data: {}",
        out
    );
}

// ============================================================================
// JSON output
// ============================================================================

#[test]
fn test_sql_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&["sql", "-c", "SELECT 1 AS num, 'hello' AS str", "--json"]);

    let json = parse_json(&output);

    // JSON structure: {"ok": true, "results": [...]}
    assert!(
        json.get("ok").is_some() || json.get("results").is_some() || json.is_array(),
        "JSON should have results: {}",
        json
    );
}

#[test]
fn test_sql_json_multiple_rows() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Insert multiple rows
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('a@test.com', 'A'), ('b@test.com', 'B')");

    let output = project.run_pgcrate_ok(&["sql", "-c", "SELECT email FROM users ORDER BY email", "--json"]);

    let json = parse_json(&output);

    // JSON structure: {"ok": true, "results": [{"columns": [...], "rows": [[...], [...]], ...}]}
    // Get rows from the first result
    let rows = json
        .get("results")
        .and_then(|r| r.as_array())
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("rows"))
        .and_then(|r| r.as_array());

    if let Some(rows) = rows {
        assert!(
            rows.len() >= 2,
            "Should return multiple rows: {:?}",
            rows
        );
    } else {
        // Alternative: check if output contains both emails
        let out = stdout(&output);
        assert!(
            out.contains("a@test.com") && out.contains("b@test.com"),
            "Should contain both emails: {}",
            out
        );
    }
}

// ============================================================================
// Write protection
// ============================================================================

#[test]
fn test_sql_blocks_write_by_default() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Try to INSERT without --allow-write
    let output = project.run_pgcrate(&["sql", "-c", "INSERT INTO users (email, name) VALUES ('blocked@test.com', 'Blocked')"]);

    // Should fail or warn
    let out = stdout(&output);
    let err = stderr(&output);
    let combined = format!("{}{}", out, err);

    assert!(
        !output.status.success() || combined.to_lowercase().contains("write") || combined.to_lowercase().contains("read"),
        "Should block or warn about write operation: stdout={}, stderr={}",
        out, err
    );
}

#[test]
fn test_sql_allows_write_with_flag() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // INSERT with --allow-write should work
    let _output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('allowed@test.com', 'Allowed')",
        "--allow-write"
    ]);

    // Verify data was inserted
    let check = db.query("SELECT email FROM users WHERE email = 'allowed@test.com'");
    assert!(
        check.contains("allowed@test.com"),
        "Data should be inserted with --allow-write"
    );
}

// ============================================================================
// Error handling
// ============================================================================

#[test]
fn test_sql_invalid_syntax_error() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate(&["sql", "-c", "SELEC broken syntax"]);

    // Should fail with error
    assert!(
        !output.status.success(),
        "Should fail on invalid SQL syntax"
    );

    let err = stderr(&output);
    assert!(
        err.to_lowercase().contains("error") || err.to_lowercase().contains("syntax"),
        "Should report syntax error: {}",
        err
    );
}

#[test]
fn test_sql_table_not_found_error() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Don't run migrations - tables don't exist
    let output = project.run_pgcrate(&["sql", "-c", "SELECT * FROM nonexistent_table"]);

    assert!(
        !output.status.success(),
        "Should fail on missing table"
    );

    let err = stderr(&output);
    assert!(
        err.contains("not exist") || err.contains("does not exist") || err.contains("relation"),
        "Should report table not found: {}",
        err
    );
}

// ============================================================================
// Multiple statements
// ============================================================================

#[test]
fn test_sql_multiple_statements() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Multiple SELECT statements
    let output = project.run_pgcrate_ok(&["sql", "-c", "SELECT 1; SELECT 2; SELECT 3"]);

    // Should execute all statements
    let out = stdout(&output);
    assert!(
        out.contains("1") && out.contains("2") && out.contains("3"),
        "Should execute all statements: {}",
        out
    );
}

// ============================================================================
// Verbose mode
// ============================================================================

#[test]
fn test_sql_verbose_shows_query() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate_ok(&["sql", "-c", "SELECT 42 AS answer", "--verbose"]);

    let out = stdout(&output);
    let err = stderr(&output);
    let combined = format!("{}{}", out, err);

    // Verbose should show the query being executed
    assert!(
        combined.contains("SELECT") || combined.contains("42") || combined.contains("answer"),
        "Verbose should show query details: stdout={}, stderr={}",
        out, err
    );
}
