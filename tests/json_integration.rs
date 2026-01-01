//! Integration tests for JSON output mode.
//!
//! Tests --json flag behavior for status, diff, and error handling.
//!
//! Guarantees tested:
//! - JSON mode outputs only JSON to stdout (no human text)
//! - JSON mode outputs JSON errors to stdout, not stderr
//! - Exit code 2 for clap/usage errors in JSON mode
//! - Exit code 1 for application errors in JSON mode
//! - Unsupported commands in JSON mode return JSON error
//! - JSON error.details omitted when not present (not empty string)
//! - No ANSI escape codes in JSON output
//!
//! Tests use the compiled binary (CARGO_BIN_EXE_pgcrate) instead of `cargo run`
//! for faster and more reliable execution.

use std::process::Command;

/// Get the path to the compiled pgcrate binary
fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

/// Run pgcrate with args and return output
fn run_pgcrate(args: &[&str]) -> std::process::Output {
    Command::new(pgcrate_binary())
        .args(args)
        .output()
        .expect("Failed to execute pgcrate")
}

/// Run pgcrate with args, removing DATABASE_URL from environment
fn run_pgcrate_no_db(args: &[&str]) -> std::process::Output {
    Command::new(pgcrate_binary())
        .args(args)
        .env_remove("DATABASE_URL")
        .output()
        .expect("Failed to execute pgcrate")
}

/// Parse JSON from stdout, panicking with helpful message on failure
fn parse_json(output: &std::process::Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to parse JSON: {}\nstdout: {}\nstderr: {}",
            e,
            stdout,
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn test_json_error_missing_database_url() {
    // When DATABASE_URL is not set and no -d flag, should get JSON error
    let output = run_pgcrate_no_db(&["--json", "status"]);

    // Should exit with code 1 (application error)
    assert!(!output.status.success(), "Should fail without DATABASE_URL");
    assert_eq!(
        output.status.code(),
        Some(1),
        "Should exit with code 1 for app errors"
    );

    // stdout should contain JSON error
    let json = parse_json(&output);
    assert_eq!(json["ok"], false);
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("DATABASE_URL"));
}

#[test]
fn test_json_error_schema() {
    // Verify the JSON error schema shape
    let output = run_pgcrate_no_db(&["--json", "status"]);

    let json = parse_json(&output);

    // Required fields
    assert!(json.get("ok").is_some(), "Missing 'ok' field");
    assert!(json.get("error").is_some(), "Missing 'error' field");
    assert!(
        json["error"].get("message").is_some(),
        "Missing 'error.message' field"
    );

    // ok must be false for errors
    assert_eq!(json["ok"], false);

    // message must be a non-empty string
    let message = json["error"]["message"].as_str().unwrap();
    assert!(!message.is_empty(), "Error message should not be empty");
}

#[test]
fn test_json_error_details_omitted_when_empty() {
    // When there's no source error, details should be omitted (not an empty string)
    let output = run_pgcrate_no_db(&["--json", "status"]);

    let json = parse_json(&output);

    // details field should either be absent or contain a non-empty string
    if let Some(details) = json["error"].get("details") {
        let details_str = details
            .as_str()
            .expect("details should be a string if present");
        assert!(
            !details_str.is_empty(),
            "details should not be an empty string"
        );
    }
    // If details is absent, that's correct behavior
}

#[test]
fn test_json_flag_in_help() {
    // Verify --json flag is documented in help
    let output = run_pgcrate(&["--help"]);
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--json"), "Help should mention --json flag");
    assert!(stdout.contains("JSON"), "Help should describe JSON output");
}

#[test]
fn test_json_no_ansi_codes() {
    // JSON output should not contain ANSI escape codes
    let output = run_pgcrate_no_db(&["--json", "status"]);

    let stdout = String::from_utf8_lossy(&output.stdout);

    // ANSI escape sequences start with \x1b[ or \033[
    assert!(
        !stdout.contains('\x1b'),
        "JSON output should not contain ANSI escape codes"
    );
}

#[test]
fn test_human_mode_error_to_stderr() {
    // In human mode, errors should go to stderr, not stdout
    let output = Command::new("cargo")
        .args(&["run", "-q", "--"])
        .args(&["status"]) // No --json
        .env_remove("DATABASE_URL")
        .output()
        .expect("Failed to execute pgcrate");

    assert!(!output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Error should be in stderr
    assert!(
        stderr.contains("Error") || stderr.contains("DATABASE_URL"),
        "Human mode error should be in stderr"
    );

    // stdout should be empty or minimal
    assert!(
        stdout.trim().is_empty() || !stdout.contains("Error"),
        "Human mode error should not be in stdout"
    );
}

#[test]
fn test_json_mode_error_to_stdout() {
    // In JSON mode, errors should go to stdout as JSON
    let output = run_pgcrate_no_db(&["--json", "status"]);

    assert!(!output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // JSON error should be in stdout
    assert!(
        stdout.contains("\"ok\"") && stdout.contains("\"error\""),
        "JSON mode error should be JSON in stdout. Got stdout: {}, stderr: {}",
        stdout,
        stderr
    );

    // stderr should not contain "Error:" (the human-readable error format)
    assert!(
        !stderr.contains("Error:"),
        "JSON mode should not print 'Error:' to stderr. stderr: {}",
        stderr
    );
}

// =============================================================================
// Usage/Clap Error Tests
// =============================================================================

#[test]
fn test_json_usage_error_missing_required_arg() {
    // When --json is set and a required arg is missing, should get JSON error with exit 2
    // `down` requires --steps
    let output = run_pgcrate_no_db(&["--json", "down"]);

    // Should exit with code 2 (usage error)
    assert_eq!(
        output.status.code(),
        Some(2),
        "Should exit with code 2 for usage errors"
    );

    // stdout should contain JSON error
    let json = parse_json(&output);
    assert_eq!(json["ok"], false);
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("--steps"));
}

#[test]
fn test_json_usage_error_invalid_subcommand() {
    // When --json is set and an invalid subcommand is given
    let output = run_pgcrate_no_db(&["--json", "notacommand"]);

    // Should exit with code 2 (usage error)
    assert_eq!(
        output.status.code(),
        Some(2),
        "Should exit with code 2 for usage errors"
    );

    // stdout should contain JSON error
    let json = parse_json(&output);
    assert_eq!(json["ok"], false);
}

#[test]
fn test_human_usage_error_to_stderr() {
    // In human mode, usage errors should go to stderr (via clap)
    let output = Command::new("cargo")
        .args(&["run", "-q", "--"])
        .args(&["down"]) // Missing --steps, no --json
        .env_remove("DATABASE_URL")
        .output()
        .expect("Failed to execute pgcrate");

    // Should fail
    assert!(!output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Clap error should be in stderr
    assert!(
        stderr.contains("--steps") || stderr.contains("required"),
        "Usage error should be in stderr: {}",
        stderr
    );

    // stdout should not contain the error
    assert!(
        !stdout.contains("--steps"),
        "Usage error should not be in stdout"
    );
}

// =============================================================================
// Unsupported Command Tests
// =============================================================================

#[test]
fn test_json_unsupported_command_returns_json_error() {
    // When --json is set with an unsupported command, should get JSON error
    // `up` is not yet supported for JSON output
    let output = run_pgcrate_no_db(&["--json", "up"]);

    // Should exit with code 1
    assert_eq!(
        output.status.code(),
        Some(1),
        "Should exit with code 1 for unsupported command"
    );

    // stdout should contain JSON error
    let json = parse_json(&output);
    assert_eq!(json["ok"], false);

    let message = json["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("--json not supported") && message.contains("up"),
        "Error should mention --json not supported for 'up': {}",
        message
    );
}

#[test]
fn test_json_unsupported_command_no_human_output() {
    // Unsupported command in JSON mode should not emit any human-readable output
    let output = run_pgcrate_no_db(&["--json", "up"]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Verify stdout is valid JSON
    let _json = parse_json(&output);

    // Verify no human-readable error text in stdout
    assert!(
        !stdout.contains("Error:"),
        "Should not have human error prefix in stdout"
    );

    // Verify stderr doesn't have our app's error format
    assert!(
        !stderr.contains("Error:"),
        "Should not have human error in stderr"
    );
}

// =============================================================================
// Meta UX Flag Tests (--help, --version, --help-llm)
// =============================================================================

#[test]
fn test_json_help_returns_success() {
    // --json --help should return JSON success with help text
    let output = run_pgcrate(&["--json", "--help"]);

    // Should exit with code 0 (success)
    assert!(output.status.success(), "Should succeed with --help");
    assert_eq!(output.status.code(), Some(0), "Should exit with code 0");

    // stdout should contain JSON success
    let json = parse_json(&output);
    assert_eq!(json["ok"], true, "ok should be true");
    assert!(json.get("help").is_some(), "Should have 'help' field");

    let help_text = json["help"].as_str().unwrap();
    assert!(help_text.contains("pgcrate"), "Help should mention pgcrate");
    assert!(help_text.contains("COMMAND"), "Help should list commands");
}

#[test]
fn test_json_version_returns_success() {
    // --json --version should return JSON success with version
    let output = run_pgcrate(&["--json", "--version"]);

    // Should exit with code 0 (success)
    assert!(output.status.success(), "Should succeed with --version");
    assert_eq!(output.status.code(), Some(0), "Should exit with code 0");

    // stdout should contain JSON success
    let json = parse_json(&output);
    assert_eq!(json["ok"], true, "ok should be true");
    assert!(json.get("version").is_some(), "Should have 'version' field");

    let version = json["version"].as_str().unwrap();
    assert!(!version.is_empty(), "Version should not be empty");
    // Version should match semver pattern (at minimum)
    assert!(
        version.contains('.'),
        "Version should contain dots (semver)"
    );
}

#[test]
fn test_json_help_llm_returns_success() {
    // --json --help-llm should return JSON success with llm_help content
    let output = run_pgcrate(&["--json", "--help-llm"]);

    // Should exit with code 0 (success)
    assert!(output.status.success(), "Should succeed with --help-llm");
    assert_eq!(output.status.code(), Some(0), "Should exit with code 0");

    // stdout should contain JSON success
    let json = parse_json(&output);
    assert_eq!(json["ok"], true, "ok should be true");
    assert!(
        json.get("llm_help").is_some(),
        "Should have 'llm_help' field"
    );

    let llm_help = json["llm_help"].as_str().unwrap();
    assert!(
        llm_help.contains("pgcrate"),
        "LLM help should mention pgcrate"
    );
    assert!(
        llm_help.contains("## OVERVIEW"),
        "LLM help should have sections"
    );
    assert!(
        llm_help.contains("JSON OUTPUT MODE"),
        "LLM help should document JSON mode"
    );
}
