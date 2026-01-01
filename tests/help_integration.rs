//! Tests use the compiled binary (CARGO_BIN_EXE_pgcrate) instead of `cargo run`
//! for faster and more reliable execution.

use std::process::Command;

/// Get the path to the compiled pgcrate binary
fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

#[test]
fn test_help_llm_flag() {
    let output = Command::new(pgcrate_binary())
        .args(&["--help-llm"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Test that it contains key sections
    assert!(stdout.contains("pgcrate - Postgres Migration Tool"));
    assert!(stdout.contains("## OVERVIEW"));
    assert!(stdout.contains("## COMMANDS"));
    assert!(stdout.contains("## CONFIGURATION"));
    assert!(stdout.contains("Migration Commands"));
    assert!(stdout.contains("pgcrate new create_users"));
    assert!(stdout.contains("DATABASE_URL"));

    // Test that it doesn't contain clap's default help format
    assert!(!stdout.contains("Usage: pgcrate"));
    assert!(!stdout.contains("[OPTIONS]"));
}

#[test]
fn test_regular_help_still_works() {
    let output = Command::new(pgcrate_binary())
        .args(&["--help"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Test that regular help format is preserved
    assert!(stdout.contains("Usage: pgcrate"));
    assert!(stdout.contains("[OPTIONS]"));
    assert!(stdout.contains("Commands:"));
    assert!(stdout.contains("up"));
    assert!(stdout.contains("down"));
    assert!(stdout.contains("status"));
    assert!(stdout.contains("new"));

    // Test that it mentions the LLM help
    assert!(stdout.contains("--help-llm"));
    assert!(stdout.contains("AI agents and LLMs"));
}

#[test]
fn test_help_llm_with_subcommand() {
    let output = Command::new(pgcrate_binary())
        .args(&["--help-llm", "up"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should still show LLM help even with subcommand
    assert!(stdout.contains("pgcrate - Postgres Migration Tool"));
    assert!(stdout.contains("## OVERVIEW"));

    // Should not show normal help or try to run the command
    assert!(!stdout.contains("Usage: pgcrate"));
}
