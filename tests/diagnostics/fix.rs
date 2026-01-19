//! Integration tests for fix commands.
//!
//! Tests fix sequence, fix index, and fix vacuum commands
//! including dry-run mode, gate checks, and safety blocks.

use crate::common::{parse_json, stdout, TestDatabase, TestProject};

// ============================================================================
// fix sequence
// ============================================================================

#[test]
fn test_fix_sequence_dry_run_shows_sql() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a test sequence
    db.run_sql_ok("CREATE SEQUENCE test_seq AS integer;");

    // Dry run should show SQL without executing
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "sequence",
        "public.test_seq",
        "--upgrade-to",
        "bigint",
        "--dry-run",
    ]);

    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("DRY RUN"), "Should indicate dry run mode");
    assert!(
        out.contains("ALTER SEQUENCE"),
        "Should show the SQL: {}",
        out
    );
}

#[test]
fn test_fix_sequence_requires_gates() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("CREATE SEQUENCE test_seq AS integer;");

    // Without --read-write and --primary, should fail
    let output = project.run_pgcrate(&[
        "fix",
        "sequence",
        "public.test_seq",
        "--upgrade-to",
        "bigint",
        "--dry-run",
    ]);

    assert!(!output.status.success(), "Should fail without gate flags");
}

#[test]
fn test_fix_sequence_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("CREATE SEQUENCE test_seq AS integer;");

    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "--json",
        "fix",
        "sequence",
        "public.test_seq",
        "--upgrade-to",
        "bigint",
        "--dry-run",
    ]);

    assert!(output.status.success());
    let json = parse_json(&output);
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.fix.sequence"))
    );

    let data = json.get("data").expect("Should have data field");
    assert_eq!(data.get("executed"), Some(&serde_json::json!(false)));
    assert_eq!(data.get("success"), Some(&serde_json::json!(true)));
    assert!(data.get("sql").is_some(), "Should have sql field");
}

#[test]
fn test_fix_sequence_blocks_downgrade() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a bigint sequence
    db.run_sql_ok("CREATE SEQUENCE test_seq AS bigint;");

    // Trying to downgrade to integer should fail
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "sequence",
        "public.test_seq",
        "--upgrade-to",
        "integer",
        "--dry-run",
    ]);

    assert!(
        !output.status.success(),
        "Should fail when trying to downgrade"
    );
}

// ============================================================================
// fix index
// ============================================================================

#[test]
fn test_fix_index_drop_dry_run() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a table and unused index
    db.run_sql_ok("CREATE TABLE test_table (id serial PRIMARY KEY, name text);");
    db.run_sql_ok("CREATE INDEX idx_test_name ON test_table(name);");

    // Dry run should show SQL
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "index",
        "--drop",
        "public.idx_test_name",
        "--dry-run",
    ]);

    assert!(
        output.status.success(),
        "fix index failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let out = stdout(&output);
    assert!(out.contains("DRY RUN"), "Should indicate dry run mode");
    assert!(
        out.contains("DROP INDEX CONCURRENTLY"),
        "Should show concurrent drop SQL: {}",
        out
    );
}

#[test]
fn test_fix_index_blocks_primary_key() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a table with primary key
    db.run_sql_ok("CREATE TABLE test_table (id serial PRIMARY KEY);");

    // Get the primary key index name
    let output = db.run_sql_ok(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'test_table' AND indexdef LIKE '%PRIMARY%' LIMIT 1;",
    );
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let index_name = stdout_str
        .lines()
        .skip(2)
        .next()
        .map(|s| s.trim())
        .unwrap_or("test_table_pkey");

    // Trying to drop primary key index should fail
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "index",
        "--drop",
        &format!("public.{}", index_name),
        "--yes",
    ]);

    assert!(
        !output.status.success(),
        "Should fail when trying to drop primary key index"
    );
}

#[test]
fn test_fix_index_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    db.run_sql_ok("CREATE TABLE test_table (id serial PRIMARY KEY, name text);");
    db.run_sql_ok("CREATE INDEX idx_test_name ON test_table(name);");

    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "--json",
        "fix",
        "index",
        "--drop",
        "public.idx_test_name",
        "--dry-run",
    ]);

    assert!(output.status.success());
    let json = parse_json(&output);
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.fix.index"))
    );

    let data = json.get("data").expect("Should have data field");
    assert_eq!(data.get("executed"), Some(&serde_json::json!(false)));
}

// ============================================================================
// fix vacuum
// ============================================================================

#[test]
fn test_fix_vacuum_dry_run() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a table
    db.run_sql_ok("CREATE TABLE test_table (id serial PRIMARY KEY, name text);");
    db.run_sql_ok("INSERT INTO test_table (name) VALUES ('test');");

    // Dry run should show SQL
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "vacuum",
        "public.test_table",
        "--dry-run",
    ]);

    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("DRY RUN"), "Should indicate dry run mode");
    assert!(out.contains("VACUUM"), "Should show VACUUM SQL: {}", out);
}

#[test]
fn test_fix_vacuum_full_requires_yes() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("CREATE TABLE test_table (id serial PRIMARY KEY);");

    // VACUUM FULL without --yes should fail
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "vacuum",
        "public.test_table",
        "--full",
    ]);

    assert!(
        !output.status.success(),
        "VACUUM FULL should require --yes flag"
    );
}

#[test]
fn test_fix_vacuum_json_output() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("CREATE TABLE test_table (id serial PRIMARY KEY, name text);");

    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "--json",
        "fix",
        "vacuum",
        "public.test_table",
        "--dry-run",
    ]);

    assert!(output.status.success());
    let json = parse_json(&output);
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.fix.vacuum"))
    );

    let data = json.get("data").expect("Should have data field");
    assert_eq!(data.get("executed"), Some(&serde_json::json!(false)));
}

// ============================================================================
// vacuum diagnostic
// ============================================================================

#[test]
fn test_vacuum_diagnostic_json() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a table with some data
    db.run_sql_ok("CREATE TABLE test_table (id serial PRIMARY KEY, name text);");
    db.run_sql_ok("INSERT INTO test_table (name) SELECT 'test' FROM generate_series(1, 100);");

    let output = project.run_pgcrate_ok(&["vacuum", "--json"]);

    let json = parse_json(&output);
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.vacuum"))
    );

    let data = json.get("data").expect("Should have data field");
    assert!(data.get("tables").is_some(), "Should have tables field");
}

// ============================================================================
// triage --include-fixes
// ============================================================================

#[test]
fn test_triage_include_fixes_json() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    let output = project.run_pgcrate(&["triage", "--include-fixes", "--json"]);

    // Should succeed (exit code 0, 1, or 2 depending on state)
    assert!(
        output.status.code().unwrap_or(99) <= 2,
        "triage should return valid exit code"
    );

    let json = parse_json(&output);
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));

    let data = json.get("data").expect("Should have data field");
    // With --include-fixes, actions should be present (even if empty)
    assert!(
        data.get("actions").is_some(),
        "Should have actions field with --include-fixes: {}",
        json
    );
}

// ============================================================================
// Execution tests (actually run fixes)
// ============================================================================

#[test]
fn test_fix_sequence_executes_upgrade() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create an integer sequence
    db.run_sql_ok("CREATE SEQUENCE exec_test_seq AS integer;");

    // Verify it's integer before fix
    let before =
        db.run_sql_ok("SELECT data_type FROM pg_sequences WHERE sequencename = 'exec_test_seq';");
    let before_type = String::from_utf8_lossy(&before.stdout);
    assert!(
        before_type.contains("integer"),
        "Sequence should be integer before fix: {}",
        before_type
    );

    // Execute the upgrade with --yes
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "sequence",
        "public.exec_test_seq",
        "--upgrade-to",
        "bigint",
        "--yes",
    ]);

    assert!(
        output.status.success(),
        "Fix should succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let out = stdout(&output);
    assert!(out.contains("SUCCESS"), "Should indicate success: {}", out);

    // Verify it's now bigint
    let after =
        db.run_sql_ok("SELECT data_type FROM pg_sequences WHERE sequencename = 'exec_test_seq';");
    let after_type = String::from_utf8_lossy(&after.stdout);
    assert!(
        after_type.contains("bigint"),
        "Sequence should be bigint after fix: {}",
        after_type
    );
}

// ============================================================================
// Special identifier tests
// ============================================================================

#[test]
fn test_fix_sequence_with_reserved_word_schema() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a schema named "user" (reserved word)
    db.run_sql_ok("CREATE SCHEMA \"user\";");
    db.run_sql_ok("CREATE SEQUENCE \"user\".test_seq AS integer;");

    // Dry run should work with quoted identifier
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "sequence",
        "user.test_seq",
        "--upgrade-to",
        "bigint",
        "--dry-run",
    ]);

    assert!(
        output.status.success(),
        "Should handle reserved word schema: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let out = stdout(&output);
    // The SQL should have properly quoted the "user" schema
    assert!(
        out.contains("\"user\""),
        "Should quote reserved word 'user': {}",
        out
    );
}

#[test]
fn test_fix_vacuum_with_special_table_name() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create a table with a name that needs quoting
    db.run_sql_ok("CREATE TABLE \"My-Table\" (id serial PRIMARY KEY);");
    db.run_sql_ok("INSERT INTO \"My-Table\" DEFAULT VALUES;");

    // Dry run should work with special characters
    let output = project.run_pgcrate(&[
        "--read-write",
        "--primary",
        "fix",
        "vacuum",
        "public.My-Table",
        "--dry-run",
    ]);

    assert!(
        output.status.success(),
        "Should handle special table name: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let out = stdout(&output);
    // The SQL should have properly quoted the table name
    assert!(
        out.contains("\"My-Table\""),
        "Should quote special table name: {}",
        out
    );
}
