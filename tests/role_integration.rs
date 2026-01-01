//! Integration tests for the `pgcrate role` and `pgcrate grants` commands.

use std::env;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string())
}

fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

fn run_pgcrate(args: &[&str], db_url: &str) -> std::process::Output {
    Command::new(pgcrate_binary())
        .args(args)
        .env("DATABASE_URL", db_url)
        .output()
        .expect("Failed to execute pgcrate")
}

fn run_psql(sql: &str, db_url: &str) -> std::process::Output {
    Command::new("psql")
        .args([db_url, "-c", sql])
        .output()
        .expect("Failed to execute psql")
}

fn unique_name(base: &str) -> String {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("{}_{pid}_{id}", base)
}

/// Test role list shows postgres role
#[test]
fn test_role_list_shows_postgres() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["role", "list"], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "role list should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("postgres"),
        "Should show postgres role. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("superuser"),
        "Should show superuser attribute. stdout: {}",
        stdout
    );
}

/// Test role list --users filter
#[test]
fn test_role_list_users_filter() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["role", "list", "--users"], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "role list --users should succeed");
    assert!(
        stdout.contains("Login roles (users):") || stdout.contains("login"),
        "Should filter to login roles. stdout: {}",
        stdout
    );
}

/// Test role describe
#[test]
fn test_role_describe_postgres() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["role", "describe", "postgres"], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "role describe should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("Role: postgres"),
        "Should show role name. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Superuser:") && stdout.contains("yes"),
        "Should show superuser attribute. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Owned objects:"),
        "Should show owned objects section. stdout: {}",
        stdout
    );
}

/// Test role describe with nonexistent role
#[test]
fn test_role_describe_not_found() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["role", "describe", "nonexistent_role_xyz"], &db_url);

    assert!(
        !output.status.success(),
        "role describe should fail for nonexistent role"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "Should indicate role not found. stderr: {}",
        stderr
    );
}

/// Test grants on a table
#[test]
fn test_grants_on_table() {
    let db_url = get_test_db_url();

    // Create test table and role
    let role_name = unique_name("test_role");
    let table_name = unique_name("test_table");

    let setup_sql = format!(
        r#"
        DROP ROLE IF EXISTS {role_name};
        CREATE ROLE {role_name};
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (id int);
        GRANT SELECT ON {table_name} TO {role_name};
        "#
    );
    let setup = run_psql(&setup_sql, &db_url);
    assert!(setup.status.success(), "Setup should succeed");

    // Test grants command
    let output = run_pgcrate(&["grants", &format!("public.{}", table_name)], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "grants should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains(&role_name),
        "Should show the role. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("SELECT"),
        "Should show SELECT privilege. stdout: {}",
        stdout
    );

    // Cleanup
    let cleanup = format!("DROP TABLE IF EXISTS {table_name}; DROP ROLE IF EXISTS {role_name};");
    let _ = run_psql(&cleanup, &db_url);
}

/// Test grants --role
#[test]
fn test_grants_for_role() {
    let db_url = get_test_db_url();

    let role_name = unique_name("test_role");
    let table_name = unique_name("test_table");

    let setup_sql = format!(
        r#"
        DROP ROLE IF EXISTS {role_name};
        CREATE ROLE {role_name};
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (id int);
        GRANT SELECT, INSERT ON {table_name} TO {role_name};
        "#
    );
    let setup = run_psql(&setup_sql, &db_url);
    assert!(setup.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["grants", "--role", &role_name], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "grants --role should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains(&table_name),
        "Should show the table. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("SELECT") && stdout.contains("INSERT"),
        "Should show privileges. stdout: {}",
        stdout
    );

    // Cleanup
    let cleanup = format!("DROP TABLE IF EXISTS {table_name}; DROP ROLE IF EXISTS {role_name};");
    let _ = run_psql(&cleanup, &db_url);
}

/// Test grants --schema
#[test]
fn test_grants_for_schema() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["grants", "--schema", "public"], &db_url);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "grants --schema should succeed");
    assert!(
        stdout.contains("Grants in schema: public"),
        "Should show schema grants header. stdout: {}",
        stdout
    );
}

/// Test grants requires exactly one argument
#[test]
fn test_grants_requires_argument() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["grants"], &db_url);

    assert!(
        !output.status.success(),
        "grants without argument should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Specify one of"),
        "Should indicate which arguments are needed. stderr: {}",
        stderr
    );
}

/// Test grants with nonexistent table
#[test]
fn test_grants_table_not_found() {
    let db_url = get_test_db_url();
    let output = run_pgcrate(&["grants", "public.nonexistent_table_xyz"], &db_url);

    assert!(
        !output.status.success(),
        "grants should fail for nonexistent table"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "Should indicate table not found. stderr: {}",
        stderr
    );
}
