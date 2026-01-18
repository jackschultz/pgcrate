//! Permission error tests.
//!
//! Tests verify that pgcrate provides clear error messages when
//! operations fail due to insufficient database permissions.

use crate::common::{stderr, stdout, TestDatabase, TestProject};
use std::sync::atomic::{AtomicU32, Ordering};

static USER_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Create a read-only user and return a connection URL for that user.
/// Username is unique per test to avoid parallelism hazards.
fn create_readonly_user(db: &TestDatabase) -> Option<String> {
    let count = USER_COUNTER.fetch_add(1, Ordering::SeqCst);
    let username = format!("ro_{}_{}", std::process::id(), count);
    let password = "readonly_test_pass";

    // Create user
    let result = db.run_sql(&format!(
        "CREATE USER {} WITH PASSWORD '{}'",
        username, password
    ));
    if !result.status.success() {
        // May not have permission to create users
        return None;
    }

    // Grant connect
    db.run_sql_ok(&format!(
        "GRANT CONNECT ON DATABASE {} TO {}",
        db.name, username
    ));

    // Grant usage on public schema
    db.run_sql_ok(&format!("GRANT USAGE ON SCHEMA public TO {}", username));

    // Grant SELECT on all tables
    db.run_sql_ok(&format!(
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}",
        username
    ));

    // Build connection URL
    // Parse existing URL and replace credentials
    let url = db.url();
    let readonly_url = build_url_with_credentials(url, &username, password);

    Some(readonly_url)
}

/// Build a connection URL with different credentials.
fn build_url_with_credentials(base_url: &str, user: &str, password: &str) -> String {
    // postgres://user:pass@host:port/db -> postgres://newuser:newpass@host:port/db
    if let Some(at_idx) = base_url.find('@') {
        let scheme_end = base_url.find("://").map(|i| i + 3).unwrap_or(0);
        let host_and_rest = &base_url[at_idx..];
        format!(
            "{}{}:{}{}",
            &base_url[..scheme_end],
            user,
            password,
            host_and_rest
        )
    } else {
        // No credentials in URL, insert them
        if let Some(scheme_end) = base_url.find("://") {
            let rest = &base_url[scheme_end + 3..];
            format!(
                "{}://{}:{}@{}",
                &base_url[..scheme_end],
                user,
                password,
                rest
            )
        } else {
            panic!("Invalid database URL format: {}", base_url);
        }
    }
}

/// Cleanup read-only user after test.
fn cleanup_readonly_user(db: &TestDatabase, username: &str) {
    // Revoke privileges and drop user
    let _ = db.run_sql(&format!(
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {}",
        username
    ));
    let _ = db.run_sql(&format!("REVOKE USAGE ON SCHEMA public FROM {}", username));
    let _ = db.run_sql(&format!(
        "REVOKE CONNECT ON DATABASE {} FROM {}",
        db.name, username
    ));
    let _ = db.run_sql(&format!("DROP USER IF EXISTS {}", username));
}

// ============================================================================
// Read-only user permission tests
// ============================================================================

#[test]
fn test_migrate_readonly_user_permission_denied() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Create read-only user
    let readonly_url = match create_readonly_user(&db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: cannot create read-only user");
            return;
        }
    };

    let username = readonly_url
        .split("://")
        .nth(1)
        .and_then(|s| s.split(':').next())
        .unwrap_or("unknown");

    // Try to migrate with read-only user
    let output = project.run_pgcrate(&["migrate", "up", "-d", &readonly_url]);

    // Should fail with permission error
    assert!(
        !output.status.success(),
        "Should fail when read-only user tries to migrate"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("permission")
            || combined.contains("denied")
            || combined.contains("privilege")
            || combined.contains("error"),
        "Should report permission error: {}",
        combined
    );

    // Cleanup
    cleanup_readonly_user(&db, username);
}

#[test]
fn test_seed_readonly_user_permission_denied() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_seeds", &db);

    // First migrate with admin to create tables
    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create read-only user
    let readonly_url = match create_readonly_user(&db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: cannot create read-only user");
            return;
        }
    };

    let username = readonly_url
        .split("://")
        .nth(1)
        .and_then(|s| s.split(':').next())
        .unwrap_or("unknown");

    // Try to seed with read-only user
    let output = project.run_pgcrate(&["seed", "run", "-d", &readonly_url]);

    // Should fail with permission error
    assert!(
        !output.status.success(),
        "Should fail when read-only user tries to seed"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("permission")
            || combined.contains("denied")
            || combined.contains("privilege")
            || combined.contains("error"),
        "Should report permission error: {}",
        combined
    );

    // Cleanup
    cleanup_readonly_user(&db, username);
}

#[test]
fn test_sql_write_readonly_user_permission_denied() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // First migrate with admin to create tables
    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create read-only user
    let readonly_url = match create_readonly_user(&db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: cannot create read-only user");
            return;
        }
    };

    let username = readonly_url
        .split("://")
        .nth(1)
        .and_then(|s| s.split(':').next())
        .unwrap_or("unknown");

    // Try to INSERT with read-only user
    let output = project.run_pgcrate(&[
        "sql",
        "-c",
        "INSERT INTO users (email, name) VALUES ('test@test.com', 'Test')",
        "--allow-write",
        "-d",
        &readonly_url,
    ]);

    // Should fail with permission error
    assert!(
        !output.status.success(),
        "Should fail when read-only user tries to write"
    );

    let err = stderr(&output);
    let out = stdout(&output);
    let combined = format!("{}{}", out, err).to_lowercase();

    assert!(
        combined.contains("permission")
            || combined.contains("denied")
            || combined.contains("privilege")
            || combined.contains("error"),
        "Should report permission error: {}",
        combined
    );

    // Cleanup
    cleanup_readonly_user(&db, username);
}

#[test]
fn test_readonly_user_can_read() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // First migrate and insert data with admin
    project.run_pgcrate_ok(&["migrate", "up"]);
    db.run_sql_ok("INSERT INTO users (email, name) VALUES ('test@example.com', 'Test User')");

    // Create read-only user
    let readonly_url = match create_readonly_user(&db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: cannot create read-only user");
            return;
        }
    };

    let username = readonly_url
        .split("://")
        .nth(1)
        .and_then(|s| s.split(':').next())
        .unwrap_or("unknown");

    // Read-only user should be able to SELECT
    let output =
        project.run_pgcrate_ok(&["sql", "-c", "SELECT email FROM users", "-d", &readonly_url]);

    let out = stdout(&output);
    assert!(
        out.contains("test@example.com"),
        "Read-only user should be able to read: {}",
        out
    );

    // Cleanup
    cleanup_readonly_user(&db, username);
}

#[test]
fn test_readonly_user_can_run_diagnostics() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // First migrate with admin
    project.run_pgcrate_ok(&["migrate", "up"]);

    // Create read-only user
    let readonly_url = match create_readonly_user(&db) {
        Some(url) => url,
        None => {
            eprintln!("Skipping test: cannot create read-only user");
            return;
        }
    };

    let username = readonly_url
        .split("://")
        .nth(1)
        .and_then(|s| s.split(':').next())
        .unwrap_or("unknown");

    // Diagnostic commands should work with read-only user
    let output = project.run_pgcrate(&["sequences", "-d", &readonly_url]);

    // Should succeed (diagnostics are read-only)
    // Note: May return non-zero for warnings, but shouldn't fail with permission error
    let err = stderr(&output);
    let combined_lower = err.to_lowercase();

    assert!(
        !combined_lower.contains("permission denied") && !combined_lower.contains("privilege"),
        "Diagnostics should work for read-only user: {}",
        err
    );

    // Cleanup
    cleanup_readonly_user(&db, username);
}
