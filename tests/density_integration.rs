//! Integration tests for output density (PGC-102).
//!
//! Verifies the TTY-aware readable-output behavior end to end: a piped
//! invocation (which every test is, since stdout is a pipe) defaults to the
//! dense format, `--pretty` restores the decorated form, `--dense` is honored
//! on top of detection, and `--json` is untouched by any of it. The core
//! guarantee under test is *zero information loss* between the two forms.
//!
//! These tests require a running PostgreSQL instance.
//! Set TEST_DATABASE_URL or use the default postgres://localhost/postgres.
//!
//! Run with: cargo test --test density_integration

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

// ===========================================================================
// context
// ===========================================================================

#[test]
fn context_piped_defaults_to_dense() {
    let db_url = get_test_db_url();
    let stdout = out(&run_pgcrate(&["context"], &db_url));

    // Dense folds the connection onto a single line: "CONNECTION: user@host…".
    assert!(
        stdout.contains("CONNECTION: ") && stdout.contains('@'),
        "piped context should be dense (single-line CONNECTION): {stdout}"
    );
    // The padded, multi-line "  Host:" form must NOT appear when dense.
    assert!(
        !stdout.contains("  Host:"),
        "dense context drops the padded Host: line: {stdout}"
    );
    // Information is still present.
    assert!(stdout.contains("SERVER:"), "dense keeps SERVER section");
    assert!(stdout.contains("PRIVILEGES:"), "dense keeps PRIVILEGES");
}

#[test]
fn context_pretty_override_restores_decorated_form() {
    let db_url = get_test_db_url();
    let stdout = out(&run_pgcrate(&["context", "--pretty"], &db_url));

    // --pretty forces the multi-line padded layout even though stdout is piped.
    assert!(
        stdout.contains("CONNECTION:\n") || stdout.contains("  Host:"),
        "--pretty restores the multi-line form: {stdout}"
    );
    assert!(stdout.contains("  Host:"), "pretty shows the Host: field");
    assert!(stdout.contains("  Port:"), "pretty shows the Port: field");
}

#[test]
fn context_dense_and_pretty_carry_same_information() {
    let db_url = get_test_db_url();
    let dense = out(&run_pgcrate(&["context", "--dense"], &db_url));
    let pretty = out(&run_pgcrate(&["context", "--pretty"], &db_url));

    // Both forms must report every section — density changes layout, not facts.
    for section in ["CONNECTION", "SERVER", "EXTENSIONS", "PRIVILEGES"] {
        assert!(dense.contains(section), "dense missing {section}: {dense}");
        assert!(
            pretty.contains(section),
            "pretty missing {section}: {pretty}"
        );
    }
    // Both must name the superuser privilege state (yes/no in dense, ✓/✗ pretty).
    assert!(
        dense.contains("superuser="),
        "dense reports superuser state"
    );
    assert!(
        pretty.contains("Superuser:"),
        "pretty reports superuser state"
    );
}

#[test]
fn context_json_is_unaffected_by_density_flags() {
    let db_url = get_test_db_url();
    // --json wins over --dense/--pretty and emits the structured envelope.
    let stdout = out(&run_pgcrate(&["context", "--json", "--dense"], &db_url));
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .expect("context --json must be valid JSON regardless of density");
    assert_eq!(json.get("ok"), Some(&serde_json::json!(true)));
    assert_eq!(
        json.get("schema_id"),
        Some(&serde_json::json!("pgcrate.diagnostics.context"))
    );
}

// ===========================================================================
// inspect table
// ===========================================================================

#[test]
fn inspect_table_dense_drops_decoration_keeps_data() {
    let Some(test_url) = setup_test_db("pgcrate_density_inspect") else {
        return;
    };
    let setup = r#"
        CREATE TABLE accounts (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            balance NUMERIC(12,2) NOT NULL DEFAULT 0
        );
        CREATE INDEX idx_accounts_email ON accounts(email);
    "#;
    assert!(run_psql(setup, &test_url).status.success(), "setup failed");

    let dense = out(&run_pgcrate(
        &["inspect", "table", "public.accounts"],
        &test_url,
    ));

    // Section labels and data survive the dense form.
    assert!(dense.contains("Table: \"public\".\"accounts\""));
    assert!(dense.contains("Columns:"), "dense keeps Columns label");
    assert!(
        dense.contains("email text NOT NULL"),
        "dense single-spaces cells"
    );
    assert!(
        dense.contains("Constraints:"),
        "dense keeps Constraints label"
    );

    // Decoration the agent doesn't need is gone: the ── rule and empty sections.
    assert!(!dense.contains('─'), "dense drops the box rule: {dense}");
    assert!(
        !dense.contains("Triggers:"),
        "dense omits the empty Triggers section: {dense}"
    );

    // The same table under --pretty keeps the rule and the empty section.
    let pretty = out(&run_pgcrate(
        &["inspect", "table", "public.accounts", "--pretty"],
        &test_url,
    ));
    assert!(pretty.contains('─'), "pretty keeps the box rule");
    assert!(
        pretty.contains("Triggers:"),
        "pretty keeps the empty Triggers section"
    );

    cleanup_test_db(&test_url);
}

// ===========================================================================
// dba triage
// ===========================================================================

#[test]
fn triage_dense_drops_padding_and_emoji() {
    let db_url = get_test_db_url();
    let stdout = out(&run_pgcrate(&["dba", "triage"], &db_url));

    // Dense triage is one "LABEL: summary — status" line per check, no emoji.
    assert!(
        stdout.contains("BLOCKING LOCKS:"),
        "dense triage labels each check: {stdout}"
    );
    assert!(
        stdout.contains("healthy") || stdout.contains("WARNING") || stdout.contains("CRITICAL"),
        "dense triage carries the status word: {stdout}"
    );
    assert!(
        !stdout.contains('✓'),
        "dense triage drops the status emoji: {stdout}"
    );
}

// ===========================================================================
// capabilities
// ===========================================================================

#[test]
fn capabilities_dense_drops_glyphs_keeps_ids() {
    let db_url = get_test_db_url();
    let stdout = out(&run_pgcrate(&["capabilities"], &db_url));

    assert!(stdout.contains("CAPABILITIES:"), "header present");
    assert!(
        stdout.contains("diagnostics.triage available"),
        "dense lists capability id + status word: {stdout}"
    );
    assert!(stdout.contains("SUMMARY:"), "summary line present");
    // No glyph markers in dense mode.
    assert!(
        !stdout.contains('✓') && !stdout.contains('✗') && !stdout.contains('⚠'),
        "dense capabilities drop status glyphs: {stdout}"
    );
}
