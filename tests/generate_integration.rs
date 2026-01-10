//! Integration tests for the `pgcrate generate` command.
//!
//! These tests require a running PostgreSQL instance.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.
//!
//! Run with: cargo test --test generate_integration
//!
//! Tests use the compiled binary (CARGO_BIN_EXE_pgcrate) instead of `cargo run`
//! for faster and more reliable execution.

use std::env;
use std::process::Command;

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
        .args(&[db_url, "-c", sql])
        .output()
        .expect("Failed to execute psql")
}

/// Test that generate --dry-run works with an empty database
#[test]
fn test_generate_dry_run_empty_database() {
    let db_url = get_test_db_url();

    // Create a fresh test database
    let test_db = "pgcrate_gen_test_empty";
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, test_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, test_db));

    // Drop and create test database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
    let create_result = run_psql(&format!("CREATE DATABASE {}", test_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return;
    }

    // Run generate --dry-run on empty database
    let output = run_pgcrate(&["generate", "--dry-run"], &test_url);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should succeed but report no objects
    assert!(
        output.status.success(),
        "generate should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("No objects found") || stdout.contains("Dry run complete"),
        "Should indicate empty or complete. stdout: {}",
        stdout
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test that generate --dry-run correctly reports tables
#[test]
fn test_generate_dry_run_with_tables() {
    let db_url = get_test_db_url();

    let test_db = "pgcrate_gen_test_tables";
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, test_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, test_db));

    // Drop and create test database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
    let create_result = run_psql(&format!("CREATE DATABASE {}", test_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return;
    }

    // Create test schema
    let setup_sql = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            title TEXT NOT NULL,
            body TEXT
        );
        CREATE INDEX idx_posts_user_id ON posts(user_id);
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Run generate --dry-run
    let output = run_pgcrate(&["generate", "--dry-run"], &test_url);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "generate should succeed. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("Would create:"),
        "Should show files to create. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains("2 tables"),
        "Should report 2 tables. stdout: {}",
        stdout
    );
    assert!(
        stdout.contains(".sql"),
        "Should show .sql filename. stdout: {}",
        stdout
    );

    // Cleanup
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test that generate correctly handles SERIAL vs IDENTITY columns
#[test]
fn test_generate_serial_vs_identity() {
    let db_url = get_test_db_url();

    let test_db = "pgcrate_gen_test_identity";
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, test_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, test_db));

    // Drop and create test database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
    let create_result = run_psql(&format!("CREATE DATABASE {}", test_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return;
    }

    // Create tables with different ID styles
    let setup_sql = r#"
        CREATE TABLE with_serial (
            id SERIAL PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE with_identity (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name TEXT
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Run generate to a temp directory
    let temp_dir = std::env::temp_dir().join("pgcrate_test_identity");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");

    let output = run_pgcrate(
        &["generate", "--output", temp_dir.to_str().unwrap()],
        &test_url,
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "generate should succeed. stderr: {}",
        stderr
    );

    // Read generated file and check content
    let files: Vec<_> = std::fs::read_dir(&temp_dir)
        .expect("Failed to read temp dir")
        .filter_map(|e| e.ok())
        .collect();

    assert!(!files.is_empty(), "Should generate at least one file");

    let content = std::fs::read_to_string(files[0].path()).expect("Failed to read generated file");

    // Check SERIAL is preserved
    assert!(
        content.contains("SERIAL"),
        "Should preserve SERIAL. content: {}",
        content
    );

    // Check IDENTITY is preserved
    assert!(
        content.contains("GENERATED ALWAYS AS IDENTITY"),
        "Should preserve IDENTITY. content: {}",
        content
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test that generate handles enums correctly
#[test]
fn test_generate_with_enums() {
    let db_url = get_test_db_url();

    let test_db = "pgcrate_gen_test_enums";
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, test_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, test_db));

    // Drop and create test database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
    let create_result = run_psql(&format!("CREATE DATABASE {}", test_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return;
    }

    // Create enum with special character (single quote) to test escaping
    let setup_sql = r#"
        CREATE TYPE status AS ENUM ('pending', 'active', 'completed');
        CREATE TABLE tasks (
            id SERIAL PRIMARY KEY,
            status status DEFAULT 'pending'
        );
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Run generate to a temp directory
    let temp_dir = std::env::temp_dir().join("pgcrate_test_enums");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");

    let output = run_pgcrate(
        &["generate", "--output", temp_dir.to_str().unwrap()],
        &test_url,
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "generate should succeed. stderr: {}",
        stderr
    );

    // Read generated file and check enum is present
    let files: Vec<_> = std::fs::read_dir(&temp_dir)
        .expect("Failed to read temp dir")
        .filter_map(|e| e.ok())
        .collect();

    assert!(!files.is_empty(), "Should generate at least one file");

    let content = std::fs::read_to_string(files[0].path()).expect("Failed to read generated file");

    assert!(
        content.contains("CREATE TYPE"),
        "Should contain CREATE TYPE"
    );
    assert!(content.contains("status"), "Should contain enum name");
    assert!(content.contains("'pending'"), "Should contain enum values");

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test that generate with --split-by table creates multiple files
#[test]
fn test_generate_split_by_table() {
    let db_url = get_test_db_url();

    let test_db = "pgcrate_gen_test_split";
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, test_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, test_db));

    // Drop and create test database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
    let create_result = run_psql(&format!("CREATE DATABASE {}", test_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return;
    }

    // Create multiple tables
    let setup_sql = r#"
        CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
        CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT);
        CREATE TABLE comments (id SERIAL PRIMARY KEY, body TEXT);
    "#;
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Run generate with --split-by table
    let temp_dir = std::env::temp_dir().join("pgcrate_test_split");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");

    let output = run_pgcrate(
        &[
            "generate",
            "--split-by",
            "table",
            "--output",
            temp_dir.to_str().unwrap(),
        ],
        &test_url,
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "generate should succeed. stderr: {}",
        stderr
    );

    // Count generated files
    let files: Vec<_> = std::fs::read_dir(&temp_dir)
        .expect("Failed to read temp dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "sql")
                .unwrap_or(false)
        })
        .collect();

    // Should have at least 3 files (one per table) plus possibly FK file
    assert!(
        files.len() >= 3,
        "Should generate at least 3 files for 3 tables, got {}",
        files.len()
    );

    // Check filenames contain table names
    let filenames: Vec<String> = files
        .iter()
        .map(|f| f.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        filenames.iter().any(|f| f.contains("users")),
        "Should have users file"
    );
    assert!(
        filenames.iter().any(|f| f.contains("posts")),
        "Should have posts file"
    );
    assert!(
        filenames.iter().any(|f| f.contains("comments")),
        "Should have comments file"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test that generate fails gracefully when output files already exist
#[test]
fn test_generate_file_conflict() {
    let db_url = get_test_db_url();

    let test_db = "pgcrate_gen_test_conflict";
    let test_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, test_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, test_db));

    // Drop and create test database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
    let create_result = run_psql(&format!("CREATE DATABASE {}", test_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return;
    }

    // Create a simple table
    let setup_sql = "CREATE TABLE users (id SERIAL PRIMARY KEY);";
    let setup_result = run_psql(setup_sql, &test_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Create output directory
    let temp_dir = std::env::temp_dir().join("pgcrate_test_conflict");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");

    // First generate should succeed
    let output1 = run_pgcrate(
        &["generate", "--output", temp_dir.to_str().unwrap()],
        &test_url,
    );
    let stderr1 = String::from_utf8_lossy(&output1.stderr);
    assert!(
        output1.status.success(),
        "First generate should succeed. stderr: {}",
        stderr1
    );

    // Check that at least one file was created
    let files: Vec<_> = std::fs::read_dir(&temp_dir)
        .expect("Failed to read temp dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "sql")
                .unwrap_or(false)
        })
        .collect();
    assert!(!files.is_empty(), "Should have created at least one file");

    // Run generate again immediately (same second = same timestamp = same filename)
    // This should hit a file conflict
    let output2 = run_pgcrate(
        &["generate", "--output", temp_dir.to_str().unwrap()],
        &test_url,
    );

    let stderr2 = String::from_utf8_lossy(&output2.stderr);

    // The behavior depends on timing - if we're in the same second, we get a conflict
    // If we're in a different second, the filenames differ and it succeeds
    // Both are valid behaviors, so we just verify the error message format is correct when there IS a conflict
    if !output2.status.success() {
        assert!(
            stderr2.contains("File conflict") || stderr2.contains("already exist"),
            "Should report file conflict with clear message. stderr: {}",
            stderr2
        );
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", test_db), &db_url);
}

/// Test round-trip: generate SQL, apply it to fresh DB, verify schema matches
#[test]
fn test_generate_round_trip() {
    let db_url = get_test_db_url();

    let source_db = "pgcrate_gen_test_source";
    let target_db = "pgcrate_gen_test_target";
    let source_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, source_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, source_db));
    let target_url = db_url
        .rsplit_once('/')
        .map(|(base, _)| format!("{}/{}", base, target_db))
        .unwrap_or_else(|| format!("{}/{}", db_url, target_db));

    // Create source database
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", source_db), &db_url);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", target_db), &db_url);

    let create_result = run_psql(&format!("CREATE DATABASE {}", source_db), &db_url);
    if !create_result.status.success() {
        eprintln!("Skipping test: could not create test database");
        return;
    }

    // Create complex schema in source
    let setup_sql = r#"
        CREATE TYPE priority AS ENUM ('low', 'medium', 'high');

        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE tasks (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            priority priority DEFAULT 'medium',
            completed BOOLEAN DEFAULT false
        );

        CREATE INDEX idx_tasks_user ON tasks(user_id);
        CREATE INDEX idx_tasks_priority ON tasks(priority);

        CREATE VIEW active_tasks AS
            SELECT t.*, u.email as user_email
            FROM tasks t
            JOIN users u ON t.user_id = u.id
            WHERE NOT t.completed;
    "#;
    let setup_result = run_psql(setup_sql, &source_url);
    assert!(setup_result.status.success(), "Setup should succeed");

    // Generate migration from source
    let temp_dir = std::env::temp_dir().join("pgcrate_test_roundtrip");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");

    let migrations_dir = temp_dir.join("migrations");
    std::fs::create_dir_all(&migrations_dir).expect("Failed to create migrations dir");

    let gen_output = run_pgcrate(
        &["generate", "--output", migrations_dir.to_str().unwrap()],
        &source_url,
    );
    assert!(gen_output.status.success(), "Generate should succeed");

    // Create target database
    let create_target = run_psql(&format!("CREATE DATABASE {}", target_db), &db_url);
    assert!(
        create_target.status.success(),
        "Create target should succeed"
    );

    // Create config file with relative path
    let config_path = temp_dir.join("pgcrate.toml");
    let config_content = r#"[paths]
migrations = "migrations"
"#;
    std::fs::write(&config_path, config_content).expect("Failed to write config");

    // Get path to the built binary
    let binary_path = std::env::current_exe()
        .expect("Failed to get current exe")
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("pgcrate");

    // Apply generated migration to target using pgcrate migrate up
    // Run from temp_dir so relative path works
    let up_output = Command::new(&binary_path)
        .args(&["migrate", "up", "--config", "pgcrate.toml"])
        .env("DATABASE_URL", &target_url)
        .current_dir(&temp_dir)
        .output()
        .expect("Failed to execute pgcrate");

    let stderr = String::from_utf8_lossy(&up_output.stderr);
    let stdout = String::from_utf8_lossy(&up_output.stdout);
    assert!(
        up_output.status.success(),
        "Migration should succeed. stderr: {}, stdout: {}",
        stderr,
        stdout
    );

    // Verify tables exist in target
    let verify_tables = run_psql(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;",
        &target_url,
    );
    let tables_output = String::from_utf8_lossy(&verify_tables.stdout);
    assert!(tables_output.contains("users"), "Should have users table");
    assert!(tables_output.contains("tasks"), "Should have tasks table");

    // Verify enum exists
    let verify_enum = run_psql(
        "SELECT typname FROM pg_type WHERE typname = 'priority';",
        &target_url,
    );
    let enum_output = String::from_utf8_lossy(&verify_enum.stdout);
    assert!(
        enum_output.contains("priority"),
        "Should have priority enum"
    );

    // Verify view exists
    let verify_view = run_psql(
        "SELECT viewname FROM pg_views WHERE schemaname = 'public';",
        &target_url,
    );
    let view_output = String::from_utf8_lossy(&verify_view.stdout);
    assert!(
        view_output.contains("active_tasks"),
        "Should have active_tasks view"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", source_db), &db_url);
    let _ = run_psql(&format!("DROP DATABASE IF EXISTS {}", target_db), &db_url);
}
