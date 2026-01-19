# Changelog

## Unreleased

**Phase 1: JSON Contracts Foundation**

### Breaking Changes

- JSON output now nests command-specific data under a `data` field instead of flattening at the top level
- Added new envelope fields: `tool_version`, `generated_at`, `severity`
- Consumers must update to read from `response.data` instead of directly from `response`

### New Commands

- **`pgcrate context`**: Connection context, server info, extensions, and privileges
- **`pgcrate capabilities`**: Permission-aware feature discovery

### JSON Envelope v2.0.0

All diagnostic commands now use a consistent envelope:

```json
{
  "ok": true,
  "schema_id": "pgcrate.diagnostics.triage",
  "schema_version": "2.0.0",
  "tool_version": "0.3.0",
  "generated_at": "2026-01-19T12:00:00Z",
  "severity": "healthy",
  "warnings": [],
  "errors": [],
  "data": { ... }
}
```

### Reason Codes

- Added 27 stable reason codes across 3 categories (operational, policy, capability)
- Skipped checks now use `reason_code` from the ReasonCode enum for automation
- Error responses use the same envelope structure as success responses

### Improvements

- `data_directory` in context output now gated behind `--no-redact` flag
- SQLSTATE 57014 correctly disambiguates `statement_timeout` vs `query_cancelled`
- `indexes` command returns `severity: warning` when findings exist (was always `healthy`)
- JSON schemas use `$ref` composition with `envelope.schema.json` for consistency

## v0.3.0

Production-safe diagnostic commands with safety rails.

### Safety Rails

- **Timeout enforcement**: `--connect-timeout`, `--statement-timeout`, `--lock-timeout` flags on all diagnostic commands
- **Lock timeout default**: 500ms prevents diagnostics from blocking production queries
- **Read-only by default**: Diagnostic commands don't modify data
- **Ctrl+C handling**: Cleanly cancels in-flight queries
- **Query redaction**: Sensitive values hidden in `--verbose` output

### JSON Output

- **Schema versioning**: All JSON output includes `schema_id` and `schema_version`
- **Timeouts in output**: JSON includes effective timeout configuration
- **Consistent envelope**: `{ ok, schema_id, schema_version, timeouts, data }`
- **Lowercase enums**: Status values are `healthy`, `warning`, `critical`

### Triage Improvements

- **Structured actions**: `next_actions` array with suggested commands
- **Graceful degradation**: `skipped_checks` with `reason_code` when permissions insufficient
- **Better summaries**: Overall status reflects worst finding

### Exit Codes

- **0** = healthy
- **1** = warning
- **2** = critical
- **10+** = operational failure (connection error, permission denied, etc.)

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
