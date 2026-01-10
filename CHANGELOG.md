# Changelog

## v0.2.0

Agent-tested improvements based on real-world usage feedback.

### Features

- **Incremental models**: Watermark directive for efficient updates on large tables
- **Two-section models**: `@base` and `@incremental` sections for aggregation patterns
- **Model move**: Rename and relocate models with `pgcrate model move`
- **PostgreSQL 9.5-16 support**: INSERT ON CONFLICT for incremental models on older PG versions

### CLI Improvements

- `pgcrate status`: Top-level command for quick migration status
- `pgcrate sql`: New command for running queries (replaces `query`, works without `-c`)
- `pgcrate model new`: Scaffold new model files
- `pgcrate model show`: Display compiled SQL for a model
- `pgcrate describe --json`: JSON output support
- `pgcrate model run --verbose`: Show SQL being executed
- `-y` flag consistency across all commands
- Better error messages with full error chain

### Seeds

- New directory layout: `seeds/<schema>/<table>.csv`
- Load CSV data into existing tables
- Truncate tables once per seed run

### Documentation

- Improved `--help-llm` with examples and workflow guides
- Schema auto-creation documentation
- PostgreSQL version requirements

## v0.1.0

Initial release.
