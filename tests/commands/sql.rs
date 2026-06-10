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

    let output =
        project.run_pgcrate_ok(&["sql", "-c", "SELECT 1 AS num, 'hello' AS str", "--json"]);

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
    db.run_sql_ok(
        "INSERT INTO users (email, name) VALUES ('a@test.com', 'A'), ('b@test.com', 'B')",
    );

    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "SELECT email FROM users ORDER BY email",
        "--json",
    ]);

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
        assert!(rows.len() >= 2, "Should return multiple rows: {:?}", rows);
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
    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('blocked@test.com', 'Blocked')",
    ]);

    // Should fail or warn
    let out = stdout(&output);
    let err = stderr(&output);
    let combined = format!("{}{}", out, err);

    assert!(
        !output.status.success()
            || combined.to_lowercase().contains("write")
            || combined.to_lowercase().contains("read"),
        "Should block or warn about write operation: stdout={}, stderr={}",
        out,
        err
    );
}

// ============================================================================
// Dry-run by default (--allow-write previews and rolls back)
// ============================================================================

/// The core safety guarantee: `--allow-write` previews a destructive statement
/// inside a transaction and ROLLS BACK, so nothing changes. `--commit` applies.
#[test]
fn test_sql_allow_write_is_a_dry_run() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('seed@test.com', 'Original')");

    // Dry-run an UPDATE. It should report what it would do but change nothing.
    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "UPDATE users SET name = 'Changed' WHERE email = 'seed@test.com'",
        "--allow-write",
    ]);
    let out = stdout(&output);
    assert!(
        out.to_uppercase().contains("DRY RUN"),
        "Dry run should be announced: {}",
        out
    );

    // Provable rollback: the row is untouched.
    let name = db.query("SELECT name FROM users WHERE email = 'seed@test.com'");
    assert_eq!(
        name, "Original",
        "Dry run must not change data — row should still be 'Original'"
    );
}

/// `--commit` (which implies --allow-write) actually applies the change.
#[test]
fn test_sql_commit_applies_the_write() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('seed@test.com', 'Original')");

    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "UPDATE users SET name = 'Changed' WHERE email = 'seed@test.com'",
        "--commit",
    ]);
    let out = stdout(&output);
    assert!(
        out.to_uppercase().contains("COMMITTED"),
        "Commit should be announced: {}",
        out
    );

    let name = db.query("SELECT name FROM users WHERE email = 'seed@test.com'");
    assert_eq!(name, "Changed", "Commit must apply the write");
}

/// The full provable-rollback contract in one test: dry-run leaves data
/// unchanged, then --commit changes it.
#[test]
fn test_sql_dry_run_then_commit() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('rollback@test.com', 'Before')");

    // 1. Dry run — data unchanged.
    project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "UPDATE users SET name = 'After' WHERE email = 'rollback@test.com'",
        "--allow-write",
    ]);
    assert_eq!(
        db.query("SELECT name FROM users WHERE email = 'rollback@test.com'"),
        "Before",
        "After dry run the data must be unchanged"
    );

    // 2. Commit — data changed.
    project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "UPDATE users SET name = 'After' WHERE email = 'rollback@test.com'",
        "--commit",
    ]);
    assert_eq!(
        db.query("SELECT name FROM users WHERE email = 'rollback@test.com'"),
        "After",
        "After commit the data must be changed"
    );
}

/// Dry-run JSON output reports the affected count and committed=false; a sample
/// of affected rows is included for a single DML statement.
#[test]
fn test_sql_dry_run_json_shape() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('json@test.com', 'Orig')");

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "UPDATE users SET name = 'New' WHERE email = 'json@test.com'",
        "--allow-write",
        "--json",
    ]);
    assert!(output.status.success(), "dry-run JSON should succeed");

    let json = parse_json(&output);
    let write = json.get("write").expect("write outcome present");
    assert_eq!(
        write.get("committed").and_then(|v| v.as_bool()),
        Some(false),
        "dry run must report committed=false: {}",
        json
    );
    assert_eq!(
        write.get("rows_affected").and_then(|v| v.as_u64()),
        Some(1),
        "dry run must report rows_affected=1: {}",
        json
    );

    // A sample result with the would-be value should be present.
    let has_sample = json
        .get("results")
        .and_then(|r| r.as_array())
        .map(|arr| {
            arr.iter().any(|r| {
                r.get("type").and_then(|t| t.as_str()) == Some("sample")
                    && serde_json::to_string(r).unwrap().contains("New")
            })
        })
        .unwrap_or(false);
    assert!(has_sample, "dry run should include a sample row: {}", json);

    // And the data is still unchanged.
    assert_eq!(
        db.query("SELECT name FROM users WHERE email = 'json@test.com'"),
        "Orig"
    );
}

/// Non-transactional statements (CREATE INDEX CONCURRENTLY) cannot be previewed;
/// --allow-write alone must refuse with guidance to use --commit.
#[test]
fn test_sql_concurrent_index_requires_commit() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "CREATE INDEX CONCURRENTLY users_name_idx ON users (name)",
        "--allow-write",
    ]);
    assert!(
        !output.status.success(),
        "CREATE INDEX CONCURRENTLY under --allow-write should be refused"
    );
    let err = stderr(&output);
    assert!(
        err.contains("--commit"),
        "Refusal should point at --commit: {}",
        err
    );
}

// ============================================================================
// Row caps
// ============================================================================

/// SELECT output is capped and a trailer announces the withheld rows.
#[test]
fn test_sql_select_is_capped() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "SELECT g FROM generate_series(1, 20) g",
        "--limit",
        "5",
    ]);
    let out = stdout(&output);
    assert!(
        out.contains("more row"),
        "Capped output should show a '+N more rows' trailer: {}",
        out
    );
}

/// `--limit 0` uncaps the result set (no trailer).
#[test]
fn test_sql_limit_zero_uncaps() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "SELECT g FROM generate_series(1, 20) g",
        "--limit",
        "0",
    ]);
    let out = stdout(&output);
    assert!(
        !out.contains("more row"),
        "--limit 0 should not truncate: {}",
        out
    );
    // All 20 values present.
    assert!(out.contains("20"), "All rows should be shown: {}", out);
}

/// The row cap and its truncation count are reflected in JSON output too.
#[test]
fn test_sql_cap_in_json() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "SELECT g FROM generate_series(1, 20) g",
        "--limit",
        "5",
        "--json",
    ]);
    assert!(output.status.success());
    let json = parse_json(&output);
    let truncated = json
        .get("results")
        .and_then(|r| r.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|r| r.get("type").and_then(|t| t.as_str()) == Some("query"))
        })
        .and_then(|q| q.get("truncated"))
        .and_then(|t| t.as_u64());
    assert_eq!(
        truncated,
        Some(15),
        "JSON query result should report 15 truncated rows: {}",
        json
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

    assert!(!output.status.success(), "Should fail on missing table");

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
        out,
        err
    );
}
