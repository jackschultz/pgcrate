//! Integration tests for the `pgcrate describe` command.
//!
//! These tests require a running PostgreSQL instance.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.
//!
//! Run with: cargo test --test describe_integration
//!
//! Tests use the compiled binary (CARGO_BIN_EXE_pgcrate) instead of `cargo run`
//! for faster and more reliable execution.

use std::env;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

/// Unique counter for generating parallel-safe database names
static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/postgres".to_string())
}

/// Get the path to the compiled pgcrate binary
fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

/// Run pgcrate using the compiled binary (not cargo run)
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

/// Generate a unique database name for parallel-safe testing
fn unique_db_name(base: &str) -> String {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("{}_{pid}_{id}", base)
}

/// Create test database and return test URL
/// Uses unique naming to support parallel test execution
fn setup_test_db(base_name: &str) -> Option<String> {
    let test_db = unique_db_name(base_name);
    let db_url = get_test_db_url();
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, test_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, test_db));

    let _ = run_psql(&format!("DROP DATABASE IF EXISTS \"{}\"", test_db), &db_url);
    let create_result = run_psql(&format!("CREATE DATABASE \"{}\"", test_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return None;
    }
    Some(test_url)
}

/// Cleanup test database
fn cleanup_test_db(test_url: &str) {
    let db_url = get_test_db_url();
    // Extract database name from URL
    if let Some(db_name) = test_url.rsplit('/').next() {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS \"{}\"", db_name), &db_url);
    }
}

/// Test basic describe output with columns, indexes, constraints
#[test]
fn test_describe_basic_table() {
    let test_db = "pgcrate_describe_test_basic";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE INDEX idx_users_name ON users(name);
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.users"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("Table: \"public\".\"users\""),
        "Should show table name. stdout: {}",
        stdout
    );
    assert!(stdout.contains("Columns:"), "Should have Columns section");
    assert!(stdout.contains("id"), "Should show id column");
    assert!(stdout.contains("email"), "Should show email column");
    assert!(stdout.contains("Indexes:"), "Should have Indexes section");
    assert!(
        stdout.contains("idx_users_name"),
        "Should show custom index"
    );
    assert!(
        stdout.contains("Constraints:"),
        "Should have Constraints section"
    );
    assert!(stdout.contains("PRIMARY KEY"), "Should show primary key");
    assert!(
        stdout.contains("Stats:"),
        "Should have Stats section by default"
    );

    cleanup_test_db(&test_url);
}

/// Test describe with --no-stats flag
#[test]
fn test_describe_no_stats() {
    let test_db = "pgcrate_describe_test_nostats";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = "CREATE TABLE items (id SERIAL PRIMARY KEY);";
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.items", "--no-stats"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    assert!(
        !stdout.contains("Stats:"),
        "Should NOT have Stats section with --no-stats"
    );
    assert!(
        stdout.contains("Columns:"),
        "Should still have Columns section"
    );

    cleanup_test_db(&test_url);
}

/// Test describe with --verbose flag
#[test]
fn test_describe_verbose() {
    let test_db = "pgcrate_describe_test_verbose";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = "CREATE TABLE items (id SERIAL PRIMARY KEY);";
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.items", "--verbose"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("Details:"),
        "Should have Details section with --verbose"
    );
    assert!(stdout.contains("Owner:"), "Should show owner");
    assert!(stdout.contains("Type:"), "Should show table type");

    cleanup_test_db(&test_url);
}

/// Test describe with table not found
#[test]
fn test_describe_table_not_found() {
    let test_db = "pgcrate_describe_test_notfound";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let output = run_pgcrate(&["inspect", "table", "public.nonexistent"], &test_url);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "describe should fail for non-existent table"
    );
    assert!(
        stderr.contains("not found") || stderr.contains("Table"),
        "Should report table not found. stderr: {}",
        stderr
    );

    cleanup_test_db(&test_url);
}

/// Test describe with ambiguous unqualified name
#[test]
fn test_describe_ambiguous_name() {
    let test_db = "pgcrate_describe_test_ambiguous";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE public.items (id SERIAL PRIMARY KEY);
        CREATE SCHEMA app;
        CREATE TABLE app.items (id SERIAL PRIMARY KEY);
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "items"], &test_url);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "describe should fail for ambiguous name"
    );
    assert!(
        stderr.contains("multiple schemas") || stderr.contains("Hint:"),
        "Should report ambiguity with hint. stderr: {}",
        stderr
    );

    cleanup_test_db(&test_url);
}

/// Test describe with --dependents flag
#[test]
fn test_describe_dependents() {
    let test_db = "pgcrate_describe_test_dependents";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            amount NUMERIC
        );
        CREATE VIEW user_summary AS SELECT id, name FROM users;
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.users", "--dependents"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe --dependents should succeed. stderr: {}",
        stderr
    );
    // Base describe output should still be shown (augments, not replaces)
    assert!(
        stdout.contains("Columns:"),
        "Should have Columns section (base describe)"
    );
    assert!(
        stdout.contains("Stats:"),
        "Should have Stats section in dependents mode. stdout: {}",
        stdout
    );
    // Dependents-specific sections
    assert!(
        stdout.contains("Direct Dependents:"),
        "Should have Dependents header"
    );
    assert!(stdout.contains("Foreign Keys"), "Should have FK section");
    assert!(
        stdout.contains("orders") && stdout.contains("user_id"),
        "Should show orders FK"
    );
    assert!(stdout.contains("Views:"), "Should have Views section");
    assert!(
        stdout.contains("user_summary"),
        "Should show dependent view"
    );

    cleanup_test_db(&test_url);
}

/// Test describe with --dependencies flag
#[test]
fn test_describe_dependencies() {
    let test_db = "pgcrate_describe_test_dependencies";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE teams (id SERIAL PRIMARY KEY, name TEXT);
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            team_id INTEGER REFERENCES teams(id),
            name TEXT
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.users", "--dependencies"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe --dependencies should succeed. stderr: {}",
        stderr
    );
    // Base describe output should still be shown (augments, not replaces)
    assert!(
        stdout.contains("Columns:"),
        "Should have Columns section (base describe)"
    );
    assert!(
        stdout.contains("Stats:"),
        "Should have Stats section in dependencies mode. stdout: {}",
        stdout
    );
    // Dependencies-specific sections
    assert!(
        stdout.contains("Direct Dependencies:"),
        "Should have Dependencies header"
    );
    assert!(
        stdout.contains("Foreign Keys (this table references)"),
        "Should have FK section"
    );
    assert!(
        stdout.contains("team_id") && stdout.contains("teams"),
        "Should show FK to teams"
    );

    cleanup_test_db(&test_url);
}

/// Test that --dependents and --dependencies are mutually exclusive
#[test]
fn test_describe_exclusive_flags() {
    let test_db = "pgcrate_describe_test_exclusive";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = "CREATE TABLE items (id SERIAL PRIMARY KEY);";
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(
        &["inspect", "table", "public.items", "--dependents", "--dependencies"],
        &test_url,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "describe should fail with both flags"
    );
    assert!(
        stderr.contains("cannot be used with") || stderr.contains("conflict"),
        "Should report flag conflict. stderr: {}",
        stderr
    );

    cleanup_test_db(&test_url);
}

/// Test describe with composite foreign key
#[test]
fn test_describe_composite_fk() {
    let test_db = "pgcrate_describe_test_composite";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE parent (
            a INTEGER NOT NULL,
            b INTEGER NOT NULL,
            data TEXT,
            PRIMARY KEY (a, b)
        );
        CREATE TABLE child (
            id SERIAL PRIMARY KEY,
            ref_a INTEGER,
            ref_b INTEGER,
            FOREIGN KEY (ref_a, ref_b) REFERENCES parent(a, b)
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Check dependents of parent (should show child's composite FK)
    let output = run_pgcrate(&["inspect", "table", "public.parent", "--dependents"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    // Composite FK should show grouped columns
    assert!(
        stdout.contains("ref_a") && stdout.contains("ref_b"),
        "Should show both FK columns. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe with user-defined enum type
#[test]
fn test_describe_with_enum_type() {
    let test_db = "pgcrate_describe_test_enum";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TYPE status AS ENUM ('pending', 'active', 'done');
        CREATE TABLE tasks (
            id SERIAL PRIMARY KEY,
            status status DEFAULT 'pending'
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.tasks", "--dependencies"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    assert!(stdout.contains("Types:"), "Should have Types section");
    assert!(
        stdout.contains("status") && stdout.contains("enum"),
        "Should show enum type. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe with trigger function dependency
#[test]
fn test_describe_with_trigger_function() {
    let test_db = "pgcrate_describe_test_trigger";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE items (
            id SERIAL PRIMARY KEY,
            updated_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER update_items_timestamp
            BEFORE UPDATE ON items
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Check dependencies (should show trigger function)
    let output = run_pgcrate(&["inspect", "table", "public.items", "--dependencies"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("Triggers (functions called):"),
        "Should have trigger functions section"
    );
    assert!(
        stdout.contains("set_updated_at"),
        "Should show trigger function. stdout: {}",
        stdout
    );

    // Also check basic describe shows the trigger
    let output2 = run_pgcrate(&["inspect", "table", "public.items"], &test_url);
    let stdout2 = String::from_utf8_lossy(&output2.stdout);
    assert!(
        stdout2.contains("Triggers:"),
        "Should have Triggers section"
    );
    assert!(
        stdout2.contains("update_items_timestamp"),
        "Should show trigger name"
    );

    cleanup_test_db(&test_url);
}

/// Test describe with partitioned table
#[test]
fn test_describe_partitioned_table() {
    let test_db = "pgcrate_describe_test_partition";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE events (
            id SERIAL,
            event_date DATE NOT NULL,
            data TEXT,
            PRIMARY KEY (id, event_date)
        ) PARTITION BY RANGE (event_date);

        CREATE TABLE events_2024 PARTITION OF events
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.events", "--verbose"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed for partitioned table. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("partitioned table"),
        "Should indicate partitioned table type. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe shows partitioned table stats caveat
#[test]
fn test_describe_partitioned_table_stats_caveat() {
    let test_db = "pgcrate_describe_test_partition_stats";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE events (
            id SERIAL,
            event_date DATE NOT NULL,
            PRIMARY KEY (id, event_date)
        ) PARTITION BY RANGE (event_date);

        CREATE TABLE events_2024 PARTITION OF events
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.events"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("parent table only") || stdout.contains("partitions not included"),
        "Should show partition size caveat. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe with materialized view as dependent
#[test]
fn test_describe_materialized_view_dependent() {
    let test_db = "pgcrate_describe_test_matview";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC
        );

        CREATE MATERIALIZED VIEW product_summary AS
            SELECT id, name, price FROM products WHERE price > 0;
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.products", "--dependents"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe --dependents should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("product_summary") && stdout.contains("materialized"),
        "Should show materialized view as dependent. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe with enum array column type dependency
#[test]
fn test_describe_enum_array_dependency() {
    let test_db = "pgcrate_describe_test_enum_array";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TYPE priority AS ENUM ('low', 'medium', 'high');
        CREATE TABLE tickets (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            tags priority[] DEFAULT '{}'
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.tickets", "--dependencies"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe --dependencies should succeed. stderr: {}",
        stderr
    );
    assert!(stdout.contains("Types:"), "Should have Types section");
    assert!(
        stdout.contains("priority") && stdout.contains("enum"),
        "Should show enum type from array column. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe with cross-schema composite foreign key
#[test]
fn test_describe_cross_schema_composite_fk() {
    let test_db = "pgcrate_describe_test_cross_fk";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE SCHEMA inventory;
        CREATE TABLE inventory.products (
            store_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            name TEXT,
            PRIMARY KEY (store_id, product_id)
        );

        CREATE TABLE public.orders (
            id SERIAL PRIMARY KEY,
            store_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            FOREIGN KEY (store_id, product_id) REFERENCES inventory.products(store_id, product_id)
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Check dependents of inventory.products
    let output = run_pgcrate(
        &["inspect", "table", "inventory.products", "--dependents"],
        &test_url,
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );
    // Should show cross-schema FK with both columns in correct order
    assert!(
        stdout.contains("public") && stdout.contains("orders"),
        "Should show FK from public.orders. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("store_id") && stdout.contains("product_id"),
        "Should show both FK columns. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe shows row-level security policies
#[test]
fn test_describe_rls_policies() {
    let test_db = "pgcrate_describe_test_rls";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE tenants (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE documents (
            id SERIAL PRIMARY KEY,
            tenant_id UUID REFERENCES tenants(id),
            content TEXT
        );

        ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
        ALTER TABLE documents FORCE ROW LEVEL SECURITY;

        CREATE POLICY tenant_isolation ON documents
            FOR ALL
            USING (tenant_id = current_setting('app.tenant_id')::uuid);

        CREATE POLICY admin_read ON documents
            FOR SELECT
            TO postgres
            USING (true);
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.documents"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );

    // Check RLS is shown as enabled and forced
    assert!(
        stdout.contains("Row-Level Security: ENABLED (forced)"),
        "Should show RLS enabled and forced. stdout: {}",
        stdout
    );

    // Check policies are listed
    assert!(
        stdout.contains("Policies:"),
        "Should have Policies section. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("tenant_isolation")
            && stdout.contains("PERMISSIVE")
            && stdout.contains("ALL"),
        "Should show tenant_isolation policy. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("admin_read") && stdout.contains("SELECT"),
        "Should show admin_read policy. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("[postgres]"),
        "Should show role restriction for admin_read. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("Using:") && stdout.contains("tenant_id"),
        "Should show USING expression. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}

/// Test describe does not show RLS section for tables without RLS
#[test]
fn test_describe_no_rls_section_when_disabled() {
    let test_db = "pgcrate_describe_test_no_rls";
    let Some(test_url) = setup_test_db(test_db) else {
        return;
    };

    let setup_sql = r#"
        CREATE TABLE simple_table (
            id SERIAL PRIMARY KEY,
            data TEXT
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    let output = run_pgcrate(&["inspect", "table", "public.simple_table"], &test_url);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "describe should succeed. stderr: {}",
        stderr
    );

    // Should NOT show RLS section
    assert!(
        !stdout.contains("Row-Level Security"),
        "Should not show RLS section for table without RLS. stdout: {}",
        stdout
    );

    cleanup_test_db(&test_url);
}
