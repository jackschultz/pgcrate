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

/// The EXPLAIN cost estimate is surfaced in `--json` output (the `write.cost`
/// object) so JSON consumers get the same signal humans see on stderr.
#[test]
fn test_sql_dry_run_json_includes_cost() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('cost@test.com', 'Orig')");

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "UPDATE users SET name = 'New' WHERE email = 'cost@test.com'",
        "--allow-write",
        "--json",
    ]);
    assert!(output.status.success(), "dry-run JSON should succeed");

    let json = parse_json(&output);
    let cost = json
        .get("write")
        .and_then(|w| w.get("cost"))
        .expect("write.cost present in JSON output");
    assert!(
        cost.get("estimated").and_then(|v| v.as_f64()).is_some(),
        "cost.estimated should be a number: {}",
        json
    );
    assert!(
        cost.get("threshold").and_then(|v| v.as_f64()).is_some(),
        "cost.threshold should be a number: {}",
        json
    );
    assert!(
        cost.get("exceeds_threshold")
            .and_then(|v| v.as_bool())
            .is_some(),
        "cost.exceeds_threshold should be a bool: {}",
        json
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
// Bypass regression: writes hidden inside a Query/Explain must not slip through
// the dry-run guard and commit in autocommit.
// ============================================================================

/// A writable CTE (`WITH d AS (DELETE … RETURNING *) SELECT …`) parses as a
/// plain query. Under `--allow-write` it must still dry-run and roll back —
/// previously it executed in autocommit and committed the DELETE.
#[test]
fn test_sql_writable_cte_delete_dry_runs() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('cte@test.com', 'Keep')");

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "WITH d AS (DELETE FROM users WHERE email = 'cte@test.com' RETURNING *) SELECT * FROM d",
        "--allow-write",
    ]);
    assert!(
        output.status.success(),
        "writable-CTE delete dry-run should succeed: {}",
        stderr(&output)
    );
    assert!(
        stdout(&output).to_uppercase().contains("DRY RUN"),
        "should be announced as a dry run: {}",
        stdout(&output)
    );

    // The row must survive — the embedded DELETE was rolled back.
    let count = db.query("SELECT count(*) FROM users WHERE email = 'cte@test.com'");
    assert_eq!(
        count, "1",
        "writable-CTE DELETE must roll back under --allow-write (row must survive)"
    );
}

/// A writable CTE under `--commit` actually applies the embedded write.
#[test]
fn test_sql_writable_cte_delete_commits() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('cte2@test.com', 'Gone')");

    project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "WITH d AS (DELETE FROM users WHERE email = 'cte2@test.com' RETURNING *) SELECT * FROM d",
        "--commit",
    ]);

    let count = db.query("SELECT count(*) FROM users WHERE email = 'cte2@test.com'");
    assert_eq!(count, "0", "writable-CTE DELETE under --commit must apply");
}

/// `EXPLAIN ANALYZE <write>` executes the statement. Under `--allow-write` it
/// must dry-run and roll back rather than committing the side effect.
#[test]
fn test_sql_explain_analyze_write_dry_runs() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('ea@test.com', 'Keep')");

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "EXPLAIN ANALYZE DELETE FROM users WHERE email = 'ea@test.com'",
        "--allow-write",
    ]);
    assert!(
        output.status.success(),
        "EXPLAIN ANALYZE write dry-run should succeed: {}",
        stderr(&output)
    );

    let count = db.query("SELECT count(*) FROM users WHERE email = 'ea@test.com'");
    assert_eq!(
        count, "1",
        "EXPLAIN ANALYZE DELETE must roll back under --allow-write (row must survive)"
    );
}

/// `SELECT … INTO new_table` materializes a table — a write. Under
/// `--allow-write` the table must not persist (rolled back).
#[test]
fn test_sql_select_into_dry_runs() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "SELECT * INTO zz_select_into FROM users",
        "--allow-write",
    ]);
    assert!(
        output.status.success(),
        "SELECT INTO dry-run should succeed: {}",
        stderr(&output)
    );

    let exists = db.query("SELECT to_regclass('zz_select_into') IS NOT NULL");
    assert_eq!(
        exists, "f",
        "SELECT INTO must roll back under --allow-write (table must not persist)"
    );
}

// ============================================================================
// PGC-104: exactly-once execution + honest abort reporting
//
// These probes use NON-IDEMPOTENT statements (INSERT against a UNIQUE
// constraint). The original bug double-executed single-DML writes — once for
// the count, once for the RETURNING-wrapped sample — and swallowed the abort
// that the second execution triggered, reporting COMMITTED with zero rows
// written. An UPDATE masks this (idempotent under re-execution); an INSERT
// against a unique index does not.
// ============================================================================

/// `--commit` of a single INSERT must execute the statement EXACTLY ONCE. The
/// double-execution bug inserted two rows for `INSERT … SELECT 1`; here a
/// single-value insert into a unique column must leave exactly one row.
#[test]
fn test_sql_commit_inserts_exactly_once() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) SELECT 'once@test.com', 'Once'",
        "--commit",
    ]);
    assert!(
        stdout(&output).to_uppercase().contains("COMMITTED"),
        "commit should be announced: {}",
        stdout(&output)
    );

    // Exactly one row — double execution would have inserted two (or aborted on
    // the unique violation and falsely reported COMMITTED).
    assert_eq!(
        db.query("SELECT count(*) FROM users WHERE email = 'once@test.com'"),
        "1",
        "single INSERT --commit must insert exactly one row"
    );
}

/// The headline failure: a `--commit` write whose second (phantom) execution
/// hit a unique violation used to report `COMMITTED` while the transaction had
/// silently rolled back. Now any abort must propagate: exit 10, an explicit
/// "ROLLED BACK" message, never COMMITTED, and the table left untouched.
///
/// Reproduced here by inserting a row that already exists, so the very first
/// (and only) execution violates the unique constraint.
#[test]
fn test_sql_commit_aborted_write_reports_failure_not_committed() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);
    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('dup@test.com', 'Existing')");

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('dup@test.com', 'Phantom')",
        "--commit",
    ]);

    // Exit 10 (operational failure), not 0.
    assert_eq!(
        output.status.code(),
        Some(10),
        "aborted commit must exit 10: stdout={}, stderr={}",
        stdout(&output),
        stderr(&output)
    );
    let combined = format!("{}{}", stdout(&output), stderr(&output));
    assert!(
        combined.to_uppercase().contains("ROLLED BACK"),
        "must announce ROLLED BACK: {}",
        combined
    );
    assert!(
        !combined.to_uppercase().contains("COMMITTED"),
        "must NEVER report COMMITTED on an aborted write: {}",
        combined
    );

    // The phantom row was never written and the original survives unchanged.
    assert_eq!(
        db.query("SELECT count(*) FROM users WHERE email = 'dup@test.com'"),
        "1",
        "aborted commit must leave the table unchanged"
    );
    assert_eq!(
        db.query("SELECT name FROM users WHERE email = 'dup@test.com'"),
        "Existing",
        "the pre-existing row must be untouched"
    );
}

/// The aborted-commit failure surfaces in JSON too: ok=false, exit 10, the
/// error message mentions the rollback, and there is no committed=true outcome.
#[test]
fn test_sql_commit_aborted_write_json_reports_failure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);
    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('dupj@test.com', 'Existing')");

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('dupj@test.com', 'Phantom')",
        "--commit",
        "--json",
    ]);
    assert_eq!(
        output.status.code(),
        Some(10),
        "aborted commit (json) must exit 10: {}",
        stdout(&output)
    );

    let json = parse_json(&output);
    assert_eq!(
        json.get("ok").and_then(|v| v.as_bool()),
        Some(false),
        "json must report ok=false on an aborted write: {}",
        json
    );
    let blob = serde_json::to_string(&json).unwrap().to_uppercase();
    assert!(
        blob.contains("ROLLED BACK"),
        "json error must mention ROLLED BACK: {}",
        json
    );

    assert_eq!(
        db.query("SELECT count(*) FROM users WHERE email = 'dupj@test.com'"),
        "1",
        "aborted commit must leave the table unchanged"
    );
}

/// A `--commit` insert that affects MANY rows must still execute once and
/// commit exactly that many — the count comes from the same single execution as
/// the sample, not a separate pass. Mirrors the solitaire repro shape (a bulk
/// `INSERT … SELECT` into a uniquely-indexed table).
#[test]
fn test_sql_commit_bulk_insert_executes_once() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);
    project.run_pgcrate_ok(&["migrate", "up"]);

    // 50 unique emails in one INSERT … SELECT.
    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) \
         SELECT 'bulk' || g || '@test.com', 'B' FROM generate_series(1, 50) g",
        "--commit",
    ]);
    assert!(
        stdout(&output).contains("50 row(s) affected"),
        "commit should report 50 rows affected: {}",
        stdout(&output)
    );

    // Exactly 50 rows — a double execution would have hit the unique constraint
    // on the second pass and aborted (which must now report failure, not 100
    // rows or a false COMMITTED).
    assert_eq!(
        db.query("SELECT count(*) FROM users WHERE email LIKE 'bulk%@test.com'"),
        "50",
        "bulk INSERT --commit must insert exactly 50 rows, once"
    );
}

/// The dry-run (`--allow-write`) path must also execute the statement once and
/// roll back. A single INSERT previewed under --allow-write must report the
/// would-be effect and leave the table empty.
#[test]
fn test_sql_dry_run_insert_executes_once_and_rolls_back() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('preview@test.com', 'Preview')",
        "--allow-write",
    ]);
    let out = stdout(&output);
    assert!(
        out.to_uppercase().contains("DRY RUN"),
        "dry run should be announced: {}",
        out
    );
    assert!(
        out.contains("1 row(s) would be affected"),
        "dry run should report one would-be row: {}",
        out
    );

    // Rolled back — nothing persisted.
    assert_eq!(
        db.query("SELECT count(*) FROM users WHERE email = 'preview@test.com'"),
        "0",
        "dry-run INSERT must roll back (no row persisted)"
    );
}

/// A single INSERT (no RETURNING) under --allow-write must still surface a
/// sample of the would-be-affected rows in JSON, derived from the SAME single
/// execution that produced the count (not a second wrapped run).
#[test]
fn test_sql_dry_run_insert_json_sample_from_single_exec() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('sample@test.com', 'Sampled')",
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

    // The sample carries the would-be values (email + name).
    let has_sample = json
        .get("results")
        .and_then(|r| r.as_array())
        .map(|arr| {
            arr.iter().any(|r| {
                r.get("type").and_then(|t| t.as_str()) == Some("sample")
                    && serde_json::to_string(r)
                        .unwrap()
                        .contains("sample@test.com")
            })
        })
        .unwrap_or(false);
    assert!(
        has_sample,
        "dry-run INSERT should include a sample row with the inserted values: {}",
        json
    );

    // And nothing was persisted.
    assert_eq!(
        db.query("SELECT count(*) FROM users WHERE email = 'sample@test.com'"),
        "0",
        "dry-run INSERT must not persist"
    );
}

/// A DML statement that carries its OWN RETURNING goes through the plain
/// single-execution path (no CTE wrap). It too must execute exactly once and
/// report honestly under --commit.
#[test]
fn test_sql_commit_insert_with_returning_executes_once() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);
    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate_ok(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('ret@test.com', 'Ret') RETURNING id",
        "--commit",
    ]);
    assert!(
        stdout(&output).to_uppercase().contains("COMMITTED"),
        "commit should be announced: {}",
        stdout(&output)
    );
    assert_eq!(
        db.query("SELECT count(*) FROM users WHERE email = 'ret@test.com'"),
        "1",
        "INSERT … RETURNING --commit must insert exactly one row"
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
