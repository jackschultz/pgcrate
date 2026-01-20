# Changelog

## v0.4.0

**Command Structure Refactor (PGC-51)**

Restructured CLI to reduce top-level command clutter and provide clearer organization.

### Breaking Changes

All DBA diagnostic commands are now under `pgcrate dba`:
- `pgcrate triage` → `pgcrate dba triage`
- `pgcrate locks` → `pgcrate dba locks`
- `pgcrate sequences` → `pgcrate dba sequences`
- `pgcrate xid` → `pgcrate dba xid`
- `pgcrate indexes` → `pgcrate dba indexes`
- `pgcrate vacuum` → `pgcrate dba vacuum`
- `pgcrate bloat` → `pgcrate dba bloat`
- `pgcrate replication` → `pgcrate dba replication`
- `pgcrate queries` → `pgcrate dba queries`
- `pgcrate connections` → `pgcrate dba connections`
- `pgcrate doctor` → `pgcrate dba doctor`
- `pgcrate fix` → `pgcrate dba fix`

Schema inspection commands are now under `pgcrate inspect`:
- `pgcrate describe` → `pgcrate inspect table`
- `pgcrate diff` → `pgcrate inspect diff`
- `pgcrate extension` → `pgcrate inspect extensions`
- `pgcrate role` → `pgcrate inspect roles`
- `pgcrate grants` → `pgcrate inspect grants`

### New Features

- **`pgcrate dba`** (no subcommand) runs triage by default
- Top-level `--help` now shows ~16 commands instead of ~35
- Clear separation: developer commands (migrate, model, seed) vs DBA commands vs inspection

### New Command Structure

```
pgcrate
├── migrate      # Schema migrations
├── model        # SQL transformations
├── seed         # Test data
├── generate     # Generate migrations from DB
├── status       # Migration status
├── dba          # DBA diagnostics and remediation
│   ├── triage   # Quick health overview (default)
│   ├── locks    # Blocking locks
│   ├── sequences# Sequence exhaustion
│   ├── xid      # Transaction wraparound
│   ├── indexes  # Index health
│   ├── vacuum   # Dead tuple diagnostic
│   ├── bloat    # Table/index bloat
│   ├── replication # Streaming replication
│   ├── queries  # Slow queries
│   ├── connections # Connection usage
│   ├── doctor   # Project health
│   └── fix      # Remediation commands
│       ├── sequence
│       ├── index
│       └── vacuum
├── inspect      # Schema inspection
│   ├── table    # Table details
│   ├── diff     # Schema comparison
│   ├── extensions # List extensions
│   ├── roles    # List/describe roles
│   └── grants   # Show permissions
├── context      # Connection info
├── capabilities # Permission discovery
├── sql          # Run queries
├── snapshot     # Save/restore state
├── anonymize    # Data anonymization
├── bootstrap    # Bootstrap environment
├── db           # Database management
└── reset        # Reset to clean state
```

---

## v0.3.0

**Phase 3a: Query Performance Diagnostics**

Complete the "why is prod slow?" workflow with query and connection analysis.

### New Commands

- **`pgcrate queries`**: Top queries from pg_stat_statements
  - Sort by total time, mean time, or call count (`--by total|mean|calls`)
  - Cache hit ratio per query
  - Status thresholds: warning >1s mean, critical >5s mean
  - Graceful degradation when pg_stat_statements not installed
  - Full JSON support with `pgcrate.diagnostics.queries` schema

- **`pgcrate connections`**: Connection usage analysis
  - Usage vs max_connections with percentage
  - Breakdown by state (active, idle, idle in transaction)
  - Group by user, database, or application (`--by-user`, `--by-database`, `--by-application`)
  - Status thresholds: warning >75%, critical >90%
  - Full JSON support with `pgcrate.diagnostics.connections` schema

### Capabilities

- `diagnostics.queries` - Available when pg_stat_statements extension is installed
- `diagnostics.connections` - Always available (uses pg_stat_activity)

---

**Phase 3b: Bloat + Replication Diagnostics**

### New Commands

- **`pgcrate bloat`**: Estimate table and index bloat
  - Statistical index bloat estimation (ioguix-style, works without extensions)
  - Table bloat from dead tuple ratios (pg_stat_user_tables)
  - Recommendations for VACUUM FULL and REINDEX when critical
  - `--limit` option to control number of results
  - Full JSON support with `pgcrate.diagnostics.bloat` schema

- **`pgcrate replication`**: Monitor streaming replication health
  - Server role detection (primary vs standby)
  - Replica lag monitoring (write, flush, replay lag)
  - Replication slot status and WAL retention
  - WAL receiver info (standby only)
  - Warning: replay_lag >30s or inactive slot retaining >1GB
  - Critical: replay_lag >5min or wal_status='lost'
  - Full JSON support with `pgcrate.diagnostics.replication` schema

### Bug Fixes

- Fix UTF-8 string slicing in `bloat` and `sequences` display (prevents panic on non-ASCII names)
- Fix XID age type overflow in triage (i32 → i64 for databases with high XID age)
- Fix triage sequences percentage calculation to match `sequences` command (consistent rounding)
- Add better error context for XID command on empty databases

### UX Improvements

- Add `migration` as alias for `migrate` command (reduces confusion)
- Add `create` as alias for `migrate new` command

---

**Phase 2a: Fix Commands**

Complete the diagnose→fix→verify loop with safe remediation commands.

### New Commands

- **`pgcrate vacuum`**: Table bloat and vacuum health diagnostic
- **`pgcrate fix sequence`**: Upgrade sequence types (smallint→integer→bigint) to prevent exhaustion
- **`pgcrate fix index --drop`**: Safely drop unused/duplicate indexes with `DROP INDEX CONCURRENTLY`
- **`pgcrate fix vacuum`**: Run VACUUM on tables (regular, freeze, full, analyze)

### Fix Command Features

- **Gate system**: `--read-write`, `--primary`, `--yes` flags required based on operation risk
- **Dry-run mode**: All fix commands support `--dry-run` to preview SQL
- **SQL preview**: Fix actions include exact SQL that will be executed
- **Evidence collection**: Detailed context about the issue being fixed
- **Safety checks**: Block dangerous operations (e.g., cannot drop primary key index)
- **Verification**: `--verify` flag runs post-fix validation

### Triage Enhancements

- **`--include-fixes` flag**: Returns structured fix actions for detected issues
- **StructuredAction format**: Each action includes command, args, risk level, gates, evidence, and verify steps

### Index Evidence Improvements

- Added `stats_since` and `stats_age_days` for confidence in usage statistics
- Added `backing_constraint` field for indexes backing constraints
- Added `is_replica_identity` field for logical replication safety

---

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
