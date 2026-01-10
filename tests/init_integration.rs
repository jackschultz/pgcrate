//! Integration tests for `pgcrate init`.
//!
//! Tests use the compiled binary (CARGO_BIN_EXE_pgcrate) instead of `cargo run`.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn pgcrate_binary() -> String {
    env!("CARGO_BIN_EXE_pgcrate").to_string()
}

fn create_temp_project_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = env::temp_dir().join(format!("pgcrate_init_{name}_{nanos}"));
    fs::create_dir_all(&dir).expect("Failed to create temp project dir");
    dir
}

#[test]
fn test_init_models_prints_models_next_steps() {
    let dir = create_temp_project_dir("models_next_steps");

    let out = Command::new(pgcrate_binary())
        .current_dir(&dir)
        .args(["init", "-y", "--models"])
        .output()
        .expect("Failed to execute pgcrate");

    assert!(out.status.success(), "init should succeed");
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(stdout.contains("Models:"), "Should print Models section");
    assert!(
        stdout.contains("pgcrate model new"),
        "Should mention model new"
    );
    assert!(
        stdout.contains("pgcrate model run"),
        "Should mention model run"
    );
    assert!(
        stdout.contains("pgcrate model test"),
        "Should mention model test"
    );
}
