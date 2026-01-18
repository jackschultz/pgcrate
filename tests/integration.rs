//! Integration tests for pgcrate.
//!
//! These tests require a running PostgreSQL instance and verify pgcrate commands
//! work correctly against a real database.
//!
//! ## Running Locally
//!
//! ```bash
//! # Default: connects to postgres://postgres:postgres@localhost:5432/postgres
//! cargo test --test integration
//!
//! # Custom database URL
//! TEST_DATABASE_URL=postgres://user:pass@host:5432/db cargo test --test integration
//! ```
//!
//! ## CI Environment
//!
//! In CI, set `TEST_DATABASE_URL` to point to the PostgreSQL service container.
//! Tests create isolated databases per-test and clean them up automatically.
//!
//! ## Test Organization
//!
//! **Commands:**
//! - `commands/init.rs` - project initialization
//! - `commands/migrate.rs` - migration commands (up, down, status, new, baseline)
//! - `commands/seed.rs` - seed data commands (run, list, validate)
//! - `commands/describe.rs` - table introspection
//! - `commands/doctor.rs` - health checks
//! - `commands/sql.rs` - arbitrary SQL execution
//! - `commands/model.rs` - model compile, run, status, graph
//!
//! **Diagnostics:**
//! - `diagnostics/basic.rs` - triage, sequences (healthy state)
//! - `diagnostics/sequences_scenarios.rs` - sequence warning/critical thresholds
//! - `diagnostics/indexes.rs` - duplicate, missing FK index detection

#[macro_use]
mod common;
mod commands;
mod diagnostics;
