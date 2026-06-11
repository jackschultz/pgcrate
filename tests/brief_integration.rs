//! Integration tests for `pgcrate brief` (PGC-100).
//!
//! brief is the orientation command an agent runs first, every session. These
//! tests prove the headline guarantee: dropped on an unfamiliar database, brief
//! alone surfaces schemas (including non-public ones), tables with estimated
//! rows, foreign-key relationships, and at-a-glance facts — sub-second, catalog
//! only. The multi-schema fixture exists specifically to catch the original
//! dogfooding bug: `public`-only views hiding data that lives in another schema.
//!
//! These tests require a running PostgreSQL instance.
//! Set TEST_DATABASE_URL or use the default postgres://postgres:postgres@localhost:5432/postgres.
//!
//! Run with: cargo test --test brief_integration

use std::env;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

fn get_test_db_url() -> String {
    env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_string())
}

fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

/// Run pgcrate via the compiled binary. stdout is a pipe here, so without an
/// override the binary picks the dense format — exactly the agent-capture case.
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

fn unique_db_name(base: &str) -> String {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("{}_{pid}_{id}", base)
}

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

fn cleanup_test_db(test_url: &str) {
    let db_url = get_test_db_url();
    if let Some(db_name) = test_url.rsplit('/').next() {
        let _ = run_psql(&format!("DROP DATABASE IF EXISTS \"{}\"", db_name), &db_url);
    }
}

fn out(o: &std::process::Output) -> String {
    String::from_utf8_lossy(&o.stdout).to_string()
}

/// A schema outside `public` with related tables and an FK — the shape that
/// caused the original session's wild-goose chase.
const MULTI_SCHEMA_SETUP: &str = r#"
    CREATE SCHEMA app;
    CREATE TABLE app.users (
        id   SERIAL PRIMARY KEY,
        email TEXT NOT NULL UNIQUE
    );
    CREATE TABLE app.orders (
        id      SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES app.users(id),
        total   NUMERIC(12,2) NOT NULL DEFAULT 0
    );
    INSERT INTO app.users (email)
        SELECT 'u' || g || '@example.com' FROM generate_series(1, 25) g;
    INSERT INTO app.orders (user_id, total)
        SELECT (g % 25) + 1, g FROM generate_series(1, 100) g;
    -- Force reltuples to be populated so estimated rows are real, not '?'.
    ANALYZE app.users;
    ANALYZE app.orders;
"#;

// ===========================================================================
// Headline: schemas, tables, estimated rows
// ===========================================================================

#[test]
fn brief_surfaces_a_non_public_schema_and_its_tables() {
    let Some(test_url) = setup_test_db("pgcrate_brief_multi") else {
        return;
    };
    assert!(
        run_psql(MULTI_SCHEMA_SETUP, &test_url).status.success(),
        "multi-schema setup failed"
    );

    let stdout = out(&run_pgcrate(&["brief"], &test_url));

    // The headline section names the non-public schema and both its tables.
    assert!(
        stdout.contains("SCHEMAS"),
        "brief leads with a SCHEMAS section: {stdout}"
    );
    assert!(
        stdout.contains("app"),
        "brief surfaces the non-public schema 'app': {stdout}"
    );
    assert!(
        stdout.contains("users") && stdout.contains("orders"),
        "brief lists tables in the non-public schema: {stdout}"
    );
    // Estimated rows are present (ANALYZE ran, so these are real, not '?').
    assert!(
        stdout.contains("~25 rows") && stdout.contains("~100 rows"),
        "brief shows estimated row counts per table: {stdout}"
    );

    cleanup_test_db(&test_url);
}

#[test]
fn brief_renders_never_analyzed_tables_as_question_mark_not_minus_one() {
    let Some(test_url) = setup_test_db("pgcrate_brief_unanalyzed") else {
        return;
    };
    // A fresh table with no ANALYZE has reltuples = -1 on modern PG.
    let setup = "CREATE TABLE fresh (id SERIAL PRIMARY KEY, note TEXT);";
    assert!(run_psql(setup, &test_url).status.success(), "setup failed");

    let stdout = out(&run_pgcrate(&["brief"], &test_url));

    assert!(
        stdout.contains("fresh ~? rows"),
        "never-analyzed table shows '?' rows: {stdout}"
    );
    assert!(
        !stdout.contains("-1 rows"),
        "brief must never print the raw -1 reltuples value: {stdout}"
    );

    cleanup_test_db(&test_url);
}

// ===========================================================================
// Relationships
// ===========================================================================

#[test]
fn brief_summarizes_foreign_key_relationships() {
    let Some(test_url) = setup_test_db("pgcrate_brief_fk") else {
        return;
    };
    assert!(
        run_psql(MULTI_SCHEMA_SETUP, &test_url).status.success(),
        "setup failed"
    );

    let stdout = out(&run_pgcrate(&["brief"], &test_url));

    assert!(
        stdout.contains("RELATIONSHIPS"),
        "brief has a RELATIONSHIPS section: {stdout}"
    );
    // The compact edge form: child → parent.
    assert!(
        stdout.contains("app.orders → app.users"),
        "brief shows the FK edge orders → users: {stdout}"
    );

    cleanup_test_db(&test_url);
}

// ===========================================================================
// Density: dense default vs pretty override
// ===========================================================================

#[test]
fn brief_piped_defaults_to_dense() {
    let Some(test_url) = setup_test_db("pgcrate_brief_dense") else {
        return;
    };
    assert!(
        run_psql(MULTI_SCHEMA_SETUP, &test_url).status.success(),
        "setup failed"
    );

    let dense = out(&run_pgcrate(&["brief"], &test_url));
    // Dense folds the target onto one line beginning "TARGET:".
    assert!(
        dense.contains("TARGET: ") && dense.contains('@'),
        "piped brief is dense (single-line TARGET): {dense}"
    );
    // The padded multi-line "  Host:" form must not appear when dense.
    assert!(
        !dense.contains("  Host:"),
        "dense brief drops the padded Host: line: {dense}"
    );

    cleanup_test_db(&test_url);
}

#[test]
fn brief_pretty_and_dense_carry_the_same_facts() {
    let Some(test_url) = setup_test_db("pgcrate_brief_parity") else {
        return;
    };
    assert!(
        run_psql(MULTI_SCHEMA_SETUP, &test_url).status.success(),
        "setup failed"
    );

    let dense = out(&run_pgcrate(&["brief", "--dense"], &test_url));
    let pretty = out(&run_pgcrate(&["brief", "--pretty"], &test_url));

    // Both forms report every section — density changes layout, not facts.
    for section in ["TARGET", "SCHEMAS", "RELATIONSHIPS", "EXTENSIONS", "HEALTH"] {
        assert!(dense.contains(section), "dense missing {section}: {dense}");
        assert!(
            pretty.contains(section),
            "pretty missing {section}: {pretty}"
        );
    }
    // Both name the non-public schema and its tables.
    for fact in ["app", "users", "orders"] {
        assert!(dense.contains(fact), "dense missing {fact}");
        assert!(pretty.contains(fact), "pretty missing {fact}");
    }

    cleanup_test_db(&test_url);
}

// ===========================================================================
// JSON
// ===========================================================================

#[test]
fn brief_json_has_brief_schema_and_full_structure() {
    let Some(test_url) = setup_test_db("pgcrate_brief_json") else {
        return;
    };
    assert!(
        run_psql(MULTI_SCHEMA_SETUP, &test_url).status.success(),
        "setup failed"
    );

    // Use run_pgcrate (not _ok): health flags are valid states, never an error,
    // but we still want the raw output regardless of exit code.
    let output = run_pgcrate(&["brief", "--json"], &test_url);
    let stdout = out(&output);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("brief --json must be valid JSON");

    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.brief")),
        "schema_id is pgcrate.brief: {stdout}"
    );
    // Brief is informational — severity never escalates past healthy.
    assert_eq!(json.get("severity"), Some(&serde_json::json!("healthy")));

    let data = json.get("data").expect("data payload present");
    // Target echo.
    assert!(data.get("target").and_then(|t| t.get("database")).is_some());
    // The non-public schema appears in the schemas array.
    let schemas = data
        .get("schemas")
        .and_then(|s| s.as_array())
        .expect("schemas array");
    let app = schemas
        .iter()
        .find(|s| s.get("name") == Some(&serde_json::json!("app")))
        .expect("app schema present in JSON");
    let tables = app.get("tables").and_then(|t| t.as_array()).unwrap();
    assert_eq!(tables.len(), 2, "app has two tables in JSON");
    // Relationships are structured child/parents.
    let rels = data
        .get("relationships")
        .and_then(|r| r.as_array())
        .expect("relationships array");
    assert!(
        rels.iter()
            .any(|r| r.get("child") == Some(&serde_json::json!("app.orders"))),
        "FK edge present in JSON: {stdout}"
    );

    cleanup_test_db(&test_url);
}

#[test]
fn brief_json_is_unaffected_by_density_flags() {
    let db_url = get_test_db_url();
    // --json wins over --dense/--pretty and emits the structured envelope.
    let stdout = out(&run_pgcrate(&["brief", "--json", "--dense"], &db_url));
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .expect("brief --json must be valid JSON regardless of density");
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.brief"))
    );
}

// ===========================================================================
// Migrations degrade silently with no project context
// ===========================================================================

#[test]
fn brief_omits_migrations_when_no_project_context() {
    let Some(test_url) = setup_test_db("pgcrate_brief_nomig") else {
        return;
    };
    let setup = "CREATE TABLE thing (id SERIAL PRIMARY KEY);";
    assert!(run_psql(setup, &test_url).status.success(), "setup failed");

    // No pgcrate.toml, no migrations dir on the test's cwd → no migrations line.
    // (The binary's cwd is the crate root, which has no db/migrations either.)
    let stdout = out(&run_pgcrate(&["brief"], &test_url));
    assert!(
        !stdout.contains("MIGRATIONS"),
        "brief must omit the migrations section with no project context: {stdout}"
    );

    cleanup_test_db(&test_url);
}

// ===========================================================================
// Exit code: informational, always 0 regardless of findings
// ===========================================================================

#[test]
fn brief_exits_zero_informational() {
    let Some(test_url) = setup_test_db("pgcrate_brief_exit") else {
        return;
    };
    let setup = "CREATE TABLE thing (id SERIAL PRIMARY KEY);";
    assert!(run_psql(setup, &test_url).status.success(), "setup failed");

    let output = run_pgcrate(&["brief"], &test_url);
    assert_eq!(
        output.status.code(),
        Some(0),
        "brief is orientation, not triage — it exits 0: {}",
        out(&output)
    );

    cleanup_test_db(&test_url);
}
