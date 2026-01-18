//! Sequence exhaustion scenario tests.
//!
//! Tests verify that `pgcrate sequences` correctly detects warning and critical
//! states based on sequence capacity usage.
//!
//! Exit codes:
//! - 0: All sequences healthy (below warning threshold)
//! - 1: At least one sequence at warning level (>= warn%, < crit%)
//! - 2: At least one sequence at critical level (>= crit%)

use crate::common::{stdout, TestDatabase, TestProject};

/// Create a sequence and set it to a specific percentage of capacity.
///
/// Uses SMALLINT (max 32767) for fast testing - we don't need to actually
/// consume billions of values to test percentage calculations.
fn create_sequence_at_percentage(db: &TestDatabase, name: &str, percentage: u8) {
    // SMALLINT max is 32767
    let max_val: i64 = 32767;
    let restart_at = ((max_val as f64) * (percentage as f64 / 100.0)) as i64;

    // Create sequence as SMALLINT
    db.run_sql_ok(&format!(
        "CREATE SEQUENCE {} AS SMALLINT MINVALUE 1 MAXVALUE {}",
        name, max_val
    ));

    // Set to target percentage
    if restart_at > 1 {
        db.run_sql_ok(&format!("ALTER SEQUENCE {} RESTART WITH {}", name, restart_at));
        // Advance to register the value
        db.run_sql_ok(&format!("SELECT nextval('{}')", name));
    }
}

// ============================================================================
// Healthy state (below warning)
// ============================================================================

#[test]
fn test_sequences_healthy_at_50_percent() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create sequence at 50% (below default 70% warning)
    create_sequence_at_percentage(&db, "test_seq_50", 50);

    let output = project.run_pgcrate(&["sequences", "--all"]);

    // Should exit 0 (healthy)
    assert_eq!(
        output.status.code(),
        Some(0),
        "50% should be healthy (exit 0), got {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_sequences_healthy_at_69_percent() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // 69% is just below default 70% warning threshold
    create_sequence_at_percentage(&db, "test_seq_69", 69);

    let output = project.run_pgcrate(&["sequences", "--all"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "69% should be healthy (exit 0)"
    );
}

// ============================================================================
// Warning state (>= warn%, < crit%)
// ============================================================================

#[test]
fn test_sequences_warning_at_70_percent() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // 70% is exactly at default warning threshold
    create_sequence_at_percentage(&db, "test_seq_70", 70);

    let output = project.run_pgcrate(&["sequences"]);

    assert_eq!(
        output.status.code(),
        Some(1),
        "70% should be warning (exit 1), got {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_sequences_warning_at_84_percent() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // 84% is just below default 85% critical threshold
    create_sequence_at_percentage(&db, "test_seq_84", 84);

    let output = project.run_pgcrate(&["sequences"]);

    assert_eq!(
        output.status.code(),
        Some(1),
        "84% should be warning (exit 1)"
    );
}

// ============================================================================
// Critical state (>= crit%)
// ============================================================================

#[test]
fn test_sequences_critical_at_85_percent() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // 85% is exactly at default critical threshold
    create_sequence_at_percentage(&db, "test_seq_85", 85);

    let output = project.run_pgcrate(&["sequences"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "85% should be critical (exit 2), got {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_sequences_critical_at_99_percent() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // 99% is critically exhausted
    create_sequence_at_percentage(&db, "test_seq_99", 99);

    let output = project.run_pgcrate(&["sequences"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "99% should be critical (exit 2)"
    );
}

// ============================================================================
// Custom thresholds
// ============================================================================

#[test]
fn test_sequences_custom_thresholds() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create at 50%
    create_sequence_at_percentage(&db, "test_seq_custom", 50);

    // With --warn 40, 50% should be warning
    let output = project.run_pgcrate(&["sequences", "--warn", "40", "--crit", "60"]);

    assert_eq!(
        output.status.code(),
        Some(1),
        "50% with --warn 40 should be warning (exit 1)"
    );

    // With --warn 40 --crit 45, 50% should be critical
    let output = project.run_pgcrate(&["sequences", "--warn", "40", "--crit", "45"]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "50% with --crit 45 should be critical (exit 2)"
    );
}

// ============================================================================
// JSON output
// ============================================================================

#[test]
fn test_sequences_warning_json_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    create_sequence_at_percentage(&db, "test_seq_json", 75);

    let output = project.run_pgcrate(&["sequences", "--json"]);

    let out = stdout(&output);
    let json: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("Invalid JSON: {}\nOutput: {}", e, out));

    // Should have sequences array
    assert!(
        json.get("sequences").is_some(),
        "JSON should have sequences: {}",
        json
    );

    // Should have overall_status
    assert!(
        json.get("overall_status").is_some(),
        "JSON should have overall_status: {}",
        json
    );

    // Overall status should be warning
    let status = json.get("overall_status").and_then(|s| s.as_str());
    assert!(
        status == Some("warning") || status == Some("Warning"),
        "overall_status should be warning: {}",
        json
    );
}

// ============================================================================
// Output content verification
// ============================================================================

#[test]
fn test_sequences_output_shows_percentage() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    project.run_pgcrate_ok(&["migrate", "up"]);

    create_sequence_at_percentage(&db, "test_seq_pct", 75);

    let output = project.run_pgcrate(&["sequences", "--all"]);

    let out = stdout(&output);

    // Should show the sequence name
    assert!(
        out.contains("test_seq_pct"),
        "Output should show sequence name: {}",
        out
    );

    // Should show percentage (roughly 75%)
    assert!(
        out.contains("7") && out.contains("%"),
        "Output should show percentage: {}",
        out
    );
}
