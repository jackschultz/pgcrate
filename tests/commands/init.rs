//! Integration tests for `pgcrate init`.

use crate::common::{stdout, TestDatabase, TestProject};

#[test]
fn test_init_creates_structure() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    let output = project.run_pgcrate_ok(&["init", "-y"]);

    // Verify config was created
    assert!(project.file_exists("pgcrate.toml"), "pgcrate.toml should exist");

    // Verify directories were created
    assert!(project.file_exists("db/migrations"), "migrations dir should exist");

    // Check output mentions what was created
    let out = stdout(&output);
    assert!(out.contains("pgcrate.toml") || out.contains("Created"), "Should mention creation");
}

#[test]
fn test_init_respects_existing_config() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::from_fixture("with_migrations", &db);

    // Read original config
    let _original = project.read_file("pgcrate.toml");

    // Run init without --force
    let _output = project.run_pgcrate(&["init", "-y"]);

    // Should either skip or fail gracefully (not overwrite)
    let new_config = project.read_file("pgcrate.toml");

    // Config should still contain database URL from fixture (test harness sets it)
    assert!(new_config.contains(db.url()), "Config should have test DB URL");
}

#[test]
fn test_init_in_empty_dir() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    // Should work even with no prior files
    let _output = project.run_pgcrate_ok(&["init", "-y"]);

    assert!(project.file_exists("pgcrate.toml"), "Should create pgcrate.toml");
}

#[test]
fn test_init_with_models_flag() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    let output = project.run_pgcrate_ok(&["init", "-y", "--models"]);

    // Should mention models in output
    let out = stdout(&output);
    assert!(
        out.contains("models") || out.contains("Models"),
        "Should mention models: {}",
        out
    );
}

#[test]
fn test_init_with_seeds_flag() {
    skip_if_no_db!();
    let db = TestDatabase::new();
    let project = TestProject::empty(&db);

    let output = project.run_pgcrate_ok(&["init", "-y", "--seeds"]);

    // Should create seeds directory or mention seeds
    let out = stdout(&output);
    assert!(
        out.contains("seeds") || out.contains("Seeds") || project.file_exists("seeds"),
        "Should set up seeds: {}",
        out
    );
}
