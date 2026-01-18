//! Connection management tests.
//!
//! Tests verify that pgcrate correctly handles various connection options:
//! - Named connections from config (-C)
//! - Direct URL override (-d, --database-url)
//! - Environment variable fallback (DATABASE_URL)
//! - Error handling for invalid/unreachable connections

use std::process::Command;
use crate::common::{stderr, stdout, TestDatabase, TestProject};

// ============================================================================
// Named connections (-C)
// ============================================================================

#[test]
fn test_connection_named_from_config() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Add a named connection to config
    let config = format!(
        r#"[database]
url = "{}"

[paths]
migrations = "db/migrations"

[connections.test_conn]
url = "{}"
"#,
        db.url(),
        db.url()
    );
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // Use named connection - doctor may return non-zero for other issues,
    // but connection should succeed
    let output = project.run_pgcrate(&["doctor", "-C", "test_conn"]);

    let out = stdout(&output);
    // Verify connection succeeded (even if other checks failed)
    assert!(
        out.contains("Connection") && out.contains("OK"),
        "Should connect using named connection: {}",
        out
    );
}

#[test]
fn test_connection_named_not_found() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Try to use non-existent named connection
    let output = project.run_pgcrate(&["doctor", "-C", "nonexistent"]);

    assert!(
        !output.status.success(),
        "Should fail for non-existent connection"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("not found")
            || combined.contains("unknown")
            || combined.contains("nonexistent")
            || combined.contains("connection"),
        "Should report connection not found: stdout={}, stderr={}",
        out, err
    );
}

// ============================================================================
// Direct URL override (-d)
// ============================================================================

#[test]
fn test_connection_url_override() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    // Create config with wrong URL, but override with correct one
    let config = r#"[database]
url = "postgres://wrong:wrong@localhost:5432/nonexistent"
"#;
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // Override with correct URL via -d
    // Doctor may return non-zero for other issues, but connection should work
    let output = project.run_pgcrate(&["doctor", "-d", db.url()]);

    let out = stdout(&output);
    // Verify connection succeeded
    assert!(
        out.contains("Connection") && out.contains("OK"),
        "Should connect using URL override: {}",
        out
    );
}

#[test]
fn test_connection_database_url_flag() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    // Minimal config without database URL
    let config = "[paths]\nmigrations = \"db/migrations\"\n";
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // Provide URL via --database-url flag
    // Doctor may return non-zero for other issues, but connection should work
    let output = project.run_pgcrate(&["doctor", "--database-url", db.url()]);

    let out = stdout(&output);
    // Verify connection succeeded
    assert!(
        out.contains("Connection") && out.contains("OK"),
        "Should connect using --database-url: {}",
        out
    );
}

// ============================================================================
// Environment variable fallback (DATABASE_URL)
// ============================================================================

#[test]
fn test_connection_env_var_default() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    // Config without URL - pgcrate should fall back to DATABASE_URL env var
    let config = "[paths]\nmigrations = \"db/migrations\"\n";
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // Run with DATABASE_URL env var set (standard fallback)
    let output = Command::new(env!("CARGO_BIN_EXE_pgcrate"))
        .args(["doctor"])
        .current_dir(project.dir.path())
        .env_clear()
        .env("DATABASE_URL", db.url())
        .env("HOME", project.dir.path())
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .output()
        .expect("Failed to execute pgcrate");

    let out = String::from_utf8_lossy(&output.stdout);

    // Verify connection succeeded via DATABASE_URL env var
    assert!(
        out.contains("Connection") && out.contains("OK"),
        "Should connect using DATABASE_URL env var: stdout={}, stderr={}",
        out,
        String::from_utf8_lossy(&output.stderr)
    );
}

// ============================================================================
// Error handling
// ============================================================================

#[test]
fn test_connection_invalid_url_format() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    let config = "[paths]\nmigrations = \"db/migrations\"\n";
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // Completely invalid URL
    let output = project.run_pgcrate(&["doctor", "-d", "not-a-valid-url"]);

    assert!(
        !output.status.success(),
        "Should fail on invalid URL format"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("invalid") || combined.contains("error") || combined.contains("url"),
        "Should report URL error: {}",
        combined
    );
}

#[test]
fn test_connection_wrong_password() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    let config = "[paths]\nmigrations = \"db/migrations\"\n";
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // URL with wrong password
    let bad_url = format!(
        "postgres://postgres:wrongpassword@localhost:5432/{}",
        &db.name
    );

    let output = project.run_pgcrate(&["doctor", "-d", &bad_url]);

    // Should fail with auth error
    assert!(
        !output.status.success(),
        "Should fail on wrong password"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("password")
            || combined.contains("authentication")
            || combined.contains("auth")
            || combined.contains("denied")
            || combined.contains("error"),
        "Should report auth error: {}",
        combined
    );
}

#[test]
fn test_connection_host_unreachable() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    let config = "[paths]\nmigrations = \"db/migrations\"\n";
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // URL with unreachable host (use localhost but wrong port)
    let bad_url = "postgres://postgres:postgres@localhost:59999/test";

    let output = project.run_pgcrate(&["doctor", "-d", bad_url]);

    assert!(
        !output.status.success(),
        "Should fail on unreachable host"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("connect")
            || combined.contains("refused")
            || combined.contains("timeout")
            || combined.contains("error")
            || combined.contains("failed"),
        "Should report connection error: {}",
        combined
    );
}

#[test]
fn test_connection_database_not_found() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    let config = "[paths]\nmigrations = \"db/migrations\"\n";
    std::fs::write(project.path("pgcrate.toml"), config).unwrap();

    // URL with non-existent database
    let bad_url = "postgres://postgres:postgres@localhost:5432/nonexistent_db_12345";

    let output = project.run_pgcrate(&["doctor", "-d", bad_url]);

    assert!(
        !output.status.success(),
        "Should fail on non-existent database"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("not exist")
            || combined.contains("does not exist")
            || combined.contains("database")
            || combined.contains("error"),
        "Should report database not found: {}",
        combined
    );
}

// ============================================================================
// Config precedence
// ============================================================================

#[test]
fn test_connection_flag_overrides_config() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let db2 = TestDatabase::new(); // Second database
    let project = TestProject::from_fixture("with_migrations", &db);

    // Run migrate up on db2 using -d override
    let _output = project.run_pgcrate_ok(&["migrate", "up", "-d", db2.url()]);

    // Verify tables are in db2, not db
    let tables_db2 = db2.query(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users'"
    );
    let tables_db = db.query(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users'"
    );

    assert!(
        tables_db2.contains("users"),
        "Should create tables in overridden database"
    );
    assert!(
        !tables_db.contains("users"),
        "Should NOT create tables in config database"
    );
}
