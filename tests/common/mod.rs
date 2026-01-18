//! Common test infrastructure for pgcrate integration tests.
//!
//! Provides:
//! - TestDatabase: Per-test database isolation with automatic cleanup
//! - TestProject: Temp directory with pgcrate config and fixtures
//! - Output assertion helpers

use std::path::Path;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

/// A test database that is created fresh for each test and cleaned up on drop.
pub struct TestDatabase {
    pub name: String,
    pub url: String,
    admin_url: String,
}

impl TestDatabase {
    /// Create a new test database.
    /// Requires TEST_DATABASE_URL env var or defaults to postgres://postgres:postgres@localhost:5432/postgres
    pub fn new() -> Self {
        let base_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".into());

        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("pgcrate_test_{}_{}", std::process::id(), count);

        // Use psql to create the database (simpler than async postgres client)
        let admin_db = Self::extract_database(&base_url).unwrap_or("postgres");
        let admin_url_for_commands = Self::replace_database(&base_url, admin_db);

        // Terminate existing connections and drop if exists
        let _ = Command::new("psql")
            .args([
                &admin_url_for_commands,
                "-c",
                &format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
                    name
                ),
            ])
            .output();

        let _ = Command::new("psql")
            .args([
                &admin_url_for_commands,
                "-c",
                &format!("DROP DATABASE IF EXISTS {}", name),
            ])
            .output();

        // Create the test database
        let output = Command::new("psql")
            .args([
                &admin_url_for_commands,
                "-c",
                &format!("CREATE DATABASE {}", name),
            ])
            .output()
            .expect("Failed to create test database");

        if !output.status.success() {
            panic!(
                "Failed to create test database {}:\n{}",
                name,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let url = Self::replace_database(&base_url, &name);

        Self {
            name,
            url,
            admin_url: admin_url_for_commands,
        }
    }

    /// Get the database URL for this test database
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Run SQL against this test database
    pub fn run_sql(&self, sql: &str) -> Output {
        Command::new("psql")
            .args([&self.url, "-c", sql])
            .output()
            .expect("Failed to run SQL")
    }

    /// Run SQL and assert success
    pub fn run_sql_ok(&self, sql: &str) -> Output {
        let output = self.run_sql(sql);
        assert!(
            output.status.success(),
            "SQL failed: {}\nstderr: {}",
            sql,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    /// Query and return stdout
    pub fn query(&self, sql: &str) -> String {
        let output = Command::new("psql")
            .args([&self.url, "-t", "-c", sql])
            .output()
            .expect("Failed to run query");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn extract_database(url: &str) -> Option<&str> {
        // Extract database name from postgres://user:pass@host:port/database
        url.rsplit('/').next()?.split('?').next()
    }

    fn replace_database(url: &str, new_db: &str) -> String {
        if let Some(idx) = url.rfind('/') {
            let base = &url[..idx];
            // Handle query params if present
            let query_start = url[idx + 1..].find('?');
            if let Some(q) = query_start {
                format!("{}/{}?{}", base, new_db, &url[idx + 1 + q + 1..])
            } else {
                format!("{}/{}", base, new_db)
            }
        } else {
            format!("{}/{}", url, new_db)
        }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Best-effort cleanup - errors are intentionally ignored since:
        // 1. Cleanup failures shouldn't fail tests
        // 2. Leftover test databases are harmless and can be manually cleaned
        // 3. In CI, the container is destroyed anyway
        let name = self.name.clone();
        let admin_url = self.admin_url.clone();

        std::thread::spawn(move || {
            // Terminate any remaining connections to allow DROP
            let _ = Command::new("psql")
                .args([
                    &admin_url,
                    "-c",
                    &format!(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
                        name
                    ),
                ])
                .output();

            // Brief delay for connections to close
            std::thread::sleep(Duration::from_millis(50));

            // Drop the test database
            let _ = Command::new("psql")
                .args([&admin_url, "-c", &format!("DROP DATABASE IF EXISTS {}", name)])
                .output();
        })
        .join()
        .ok();
    }
}

/// A test project with isolated directory and pgcrate config.
pub struct TestProject {
    pub dir: tempfile::TempDir,
    pub db_url: String,
}

impl TestProject {
    /// Create a project from a fixture in tests/fixtures/projects/
    pub fn from_fixture(fixture_name: &str, db: &TestDatabase) -> Self {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/projects")
            .join(fixture_name);

        if fixture_path.exists() {
            copy_dir_all(&fixture_path, dir.path()).expect("Failed to copy fixture");
        }

        // Write pgcrate.toml with test database URL
        let config = format!(
            r#"[database]
url = "{}"

[paths]
migrations = "db/migrations"
seeds = "db/seeds"
"#,
            db.url()
        );

        std::fs::write(dir.path().join("pgcrate.toml"), config)
            .expect("Failed to write pgcrate.toml");

        Self {
            dir,
            db_url: db.url().to_string(),
        }
    }

    /// Create an empty project (for init testing)
    pub fn empty(db: &TestDatabase) -> Self {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        Self {
            dir,
            db_url: db.url().to_string(),
        }
    }

    /// Run pgcrate command with isolated environment
    pub fn run_pgcrate(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_pgcrate"))
            .args(args)
            .current_dir(self.dir.path())
            // Isolate environment
            .env_clear()
            .env("DATABASE_URL", &self.db_url)
            .env("HOME", self.dir.path())
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            // Prevent interactive prompts
            .env("PGCRATE_NON_INTERACTIVE", "1")
            .output()
            .expect("Failed to execute pgcrate")
    }

    /// Run pgcrate and assert success
    pub fn run_pgcrate_ok(&self, args: &[&str]) -> Output {
        let output = self.run_pgcrate(args);
        assert!(
            output.status.success(),
            "pgcrate {:?} failed (exit {:?}):\nstdout: {}\nstderr: {}",
            args,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    /// Run pgcrate and assert failure with specific exit code
    #[allow(dead_code)]
    pub fn run_pgcrate_fails(&self, args: &[&str], expected_code: i32) -> Output {
        let output = self.run_pgcrate(args);
        assert_eq!(
            output.status.code(),
            Some(expected_code),
            "pgcrate {:?} expected exit {} but got {:?}:\nstdout: {}\nstderr: {}",
            args,
            expected_code,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    /// Get path to a file in the project
    pub fn path(&self, relative: &str) -> std::path::PathBuf {
        self.dir.path().join(relative)
    }

    /// Check if a file exists in the project
    pub fn file_exists(&self, relative: &str) -> bool {
        self.path(relative).exists()
    }

    /// Read a file from the project
    pub fn read_file(&self, relative: &str) -> String {
        std::fs::read_to_string(self.path(relative))
            .unwrap_or_else(|_| panic!("Failed to read {}", relative))
    }
}

fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dst_path = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&entry.path(), &dst_path)?;
        } else {
            std::fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}

// ============================================================================
// Output assertion helpers
// ============================================================================

/// Parse JSON output and return the value
pub fn parse_json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("Invalid JSON output:\n{}\nError: {}", stdout, e)
    })
}

/// Assert stdout contains a substring
#[allow(dead_code)]
pub fn assert_stdout_contains(output: &Output, expected: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(expected),
        "Expected stdout to contain '{}':\n{}",
        expected,
        stdout
    );
}

/// Assert stderr contains a substring
#[allow(dead_code)]
pub fn assert_stderr_contains(output: &Output, expected: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(expected),
        "Expected stderr to contain '{}':\n{}",
        expected,
        stderr
    );
}

/// Assert stderr is empty (no errors)
#[allow(dead_code)]
pub fn assert_stderr_empty(output: &Output) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.is_empty(), "Expected empty stderr, got:\n{}", stderr);
}

/// Get stdout as string
pub fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Get stderr as string
pub fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

// ============================================================================
// Test skip helper
// ============================================================================

/// Check if we can connect to the test database
pub fn can_connect_to_db() -> bool {
    let db_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".into());

    Command::new("psql")
        .args([&db_url, "-c", "SELECT 1"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Skip test if database is not available
#[macro_export]
macro_rules! skip_if_no_db {
    () => {
        if !$crate::common::can_connect_to_db() {
            eprintln!("Skipping test: cannot connect to database");
            return;
        }
    };
}
