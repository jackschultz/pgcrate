# PGC-90: DBA Diagnostic Enhancements (v0.6.0)

> Expand DBA diagnostics with FK index detection, stats age, checkpoints, autovacuum progress, and config review.

**Project:** pgcrate-dba-diagnostics
**Branch:** `feature/dba-enhancements-v060`
**Effort:** M (3 days)
**Priority:** High

---

## Overview

Five additions to complete the DBA diagnostic suite:

| # | Addition | Type | Effort |
|---|----------|------|--------|
| 1 | FK index detection | Enhance `dba indexes` | S |
| 2 | `dba stats-age` | New command | S |
| 3 | `dba checkpoints` | New command | S-M |
| 4 | `dba autovacuum-progress` | New command | S |
| 5 | `dba config` | New command | M |

---

## 1. FK Index Detection (enhance `dba indexes`)

### Problem

Foreign keys without supporting indexes cause:
- Slow DELETEs (full table scan to check references)
- Slow JOINs on FK columns
- Lock contention during cascading operations

### Solution

Add fourth category to `dba indexes` output: **Foreign Keys Without Indexes**.

### Output

```
FOREIGN KEYS WITHOUT INDEXES:
  ⚠ orders.user_id → users.id (orders: ~1.2M rows)
  ⚠ order_items.order_id → orders.id (order_items: ~5.4M rows)
```

### SQL Query

```sql
WITH fk_columns AS (
    SELECT
        c.conname AS constraint_name,
        n.nspname AS schema_name,
        t.relname AS table_name,
        a.attname AS column_name,
        nf.nspname AS ref_schema,
        tf.relname AS ref_table,
        af.attname AS ref_column
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_class tf ON tf.oid = c.confrelid
    JOIN pg_namespace nf ON nf.oid = tf.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
    JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = c.confkey[1]
    WHERE c.contype = 'f'
),
indexed_columns AS (
    SELECT
        n.nspname AS schema_name,
        t.relname AS table_name,
        a.attname AS column_name,
        i.relname AS index_name
    FROM pg_index ix
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ix.indkey[0]
)
SELECT
    fk.schema_name,
    fk.table_name,
    fk.column_name,
    fk.ref_schema,
    fk.ref_table,
    fk.ref_column,
    fk.constraint_name,
    ic.index_name,
    COALESCE(s.n_live_tup, 0) AS table_rows
FROM fk_columns fk
LEFT JOIN indexed_columns ic USING (schema_name, table_name, column_name)
LEFT JOIN pg_stat_user_tables s ON s.schemaname = fk.schema_name AND s.relname = fk.table_name
WHERE ic.index_name IS NULL  -- Only show missing
ORDER BY s.n_live_tup DESC NULLS LAST;
```

### Status Thresholds

- Critical: No index, table > 100K rows
- Warning: No index, table > 10K rows
- Info: No index, table < 10K rows

### Struct

```rust
#[derive(Debug, Clone, Serialize)]
pub struct FkWithoutIndex {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub ref_schema: String,
    pub ref_table: String,
    pub ref_column: String,
    pub constraint_name: String,
    pub table_rows: i64,
    pub status: IndexStatus,
}
```

---

## 2. `dba stats-age` - Statistics Freshness

### Problem

Stale table statistics lead to poor query plans. PostgreSQL's planner relies on statistics to estimate row counts and choose join strategies.

### Usage

```bash
pgcrate dba stats-age                # Tables with oldest statistics
pgcrate dba stats-age --threshold 7d # Warn if stats > 7 days old
pgcrate dba stats-age --json
```

### Output

```
STATISTICS AGE
==============

Tables with oldest statistics:
  ✗ public.events          last analyzed: 45 days ago (CRITICAL)
  ⚠ public.orders          last analyzed: 12 days ago
  ⚠ public.users           last analyzed: 8 days ago
  ✓ public.products        last analyzed: 2 days ago

Recommendation: Run ANALYZE on tables with stale statistics
  ANALYZE public.events;
  ANALYZE public.orders;
```

### SQL Query

```sql
SELECT
    schemaname,
    relname,
    n_live_tup AS row_estimate,
    last_analyze,
    last_autoanalyze,
    GREATEST(last_analyze, last_autoanalyze) AS last_stats_update,
    EXTRACT(EPOCH FROM (now() - GREATEST(last_analyze, last_autoanalyze))) / 86400 AS days_since_analyze
FROM pg_stat_user_tables
WHERE n_live_tup > 1000  -- Only tables with meaningful data
ORDER BY GREATEST(last_analyze, last_autoanalyze) ASC NULLS FIRST
LIMIT 20;
```

### Status Thresholds

- Critical: > 30 days since analyze (or never analyzed)
- Warning: > 7 days since analyze
- Healthy: < 7 days

### Struct

```rust
#[derive(Debug, Clone, Serialize)]
pub struct TableStatsAge {
    pub schema: String,
    pub table: String,
    pub row_estimate: i64,
    pub last_analyze: Option<String>,
    pub last_autoanalyze: Option<String>,
    pub days_since_analyze: Option<f64>,
    pub status: StatsStatus,
}
```

---

## 3. `dba checkpoints` - Checkpoint Analysis

### Problem

Frequent checkpoints indicate:
- `checkpoint_timeout` too low
- `max_wal_size` too small
- Heavy write workload overwhelming WAL

### Usage

```bash
pgcrate dba checkpoints              # Checkpoint statistics
pgcrate dba checkpoints --json
```

### Output

```
CHECKPOINT ANALYSIS
===================

Statistics since: 2026-01-15 10:00:00 (6 days ago)

Checkpoint Frequency:
  Total checkpoints:     142
  Timed (scheduled):     120 (85%)
  Requested (forced):    22 (15%)
  Avg interval:          ~61 minutes

Write Performance:
  Buffers written:       12.4 GB (checkpoints)
  Buffers written:       892 MB (bgwriter)
  Buffers written:       2.1 GB (backends) ⚠

⚠ Warnings:
  - 15% of checkpoints are forced (requested) - consider increasing max_wal_size
  - Backends writing 2.1 GB directly - bgwriter may need tuning
```

### SQL Query

```sql
SELECT
    checkpoints_timed,
    checkpoints_req,
    checkpoint_write_time,
    checkpoint_sync_time,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend,
    maxwritten_clean,
    stats_reset
FROM pg_stat_bgwriter;
```

### Status Thresholds

- Critical: > 50% checkpoints requested (forced)
- Warning: > 20% checkpoints requested, or backends writing > 10% of buffers
- Healthy: Mostly timed checkpoints, bgwriter handling writes

### Struct

```rust
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointStats {
    pub checkpoints_timed: i64,
    pub checkpoints_requested: i64,
    pub requested_pct: f64,
    pub checkpoint_write_time_ms: f64,
    pub checkpoint_sync_time_ms: f64,
    pub buffers_checkpoint: i64,
    pub buffers_bgwriter: i64,
    pub buffers_backend: i64,
    pub backend_write_pct: f64,
    pub stats_since: Option<String>,
    pub status: CheckpointStatus,
}
```

---

## 4. `dba autovacuum-progress` - Running Autovacuum

### Problem

Users want to know if autovacuum is currently running and on which tables.

### Usage

```bash
pgcrate dba autovacuum-progress      # Currently running autovacuum
pgcrate dba autovacuum-progress --json
```

### Output

```
AUTOVACUUM IN PROGRESS
======================

Currently running: 2 autovacuum workers

  public.events
    Phase: scanning heap
    Progress: 45% (heap_blks_scanned: 450,000 / 1,000,000)
    Dead tuples: 125,000 collected
    Started: 3 minutes ago

  public.audit_log
    Phase: vacuuming indexes
    Progress: 78%
    Started: 8 minutes ago

No autovacuum running: ✓ (all tables healthy)
```

### SQL Query

```sql
SELECT
    p.pid,
    p.datname,
    p.relid::regclass AS table_name,
    p.phase,
    p.heap_blks_total,
    p.heap_blks_scanned,
    p.heap_blks_vacuumed,
    p.index_vacuum_count,
    p.max_dead_tuples,
    p.num_dead_tuples,
    a.query_start,
    EXTRACT(EPOCH FROM (now() - a.query_start)) AS running_seconds
FROM pg_stat_progress_vacuum p
JOIN pg_stat_activity a ON a.pid = p.pid
ORDER BY a.query_start;
```

### Status

- No status thresholds (informational command)
- Just shows what's currently running

### Struct

```rust
#[derive(Debug, Clone, Serialize)]
pub struct AutovacuumProgress {
    pub pid: i32,
    pub database: String,
    pub table: String,
    pub phase: String,
    pub heap_blks_total: i64,
    pub heap_blks_scanned: i64,
    pub heap_blks_vacuumed: i64,
    pub progress_pct: f64,
    pub dead_tuples_collected: i64,
    pub running_seconds: f64,
}
```

---

## 5. `dba config` - Configuration Review

### Problem

Users want to know if their PostgreSQL settings are reasonable.

### Usage

```bash
pgcrate dba config                   # Review key settings
pgcrate dba config --json
```

### Output

```
CONFIGURATION REVIEW
====================

Memory Settings:
  shared_buffers:        128 MB    ⚠ Low (recommend: 2-4 GB for 16 GB RAM)
  effective_cache_size:  4 GB      ✓ Reasonable
  work_mem:              4 MB      ✓ Default (adjust per workload)
  maintenance_work_mem:  64 MB     ⚠ Low for large tables

Connection Settings:
  max_connections:       100       ✓ Default

WAL Settings:
  wal_buffers:           4 MB      ✓ Auto-tuned
  checkpoint_timeout:    5 min     ✓ Default
  max_wal_size:          1 GB      ⚠ May cause frequent checkpoints

System Info:
  PostgreSQL version:    15.4
  System RAM:            (not available from SQL)

⚠ Recommendations are starting points, not guarantees.
   Optimal settings depend on workload (OLTP vs OLAP vs mixed).
   Test changes in non-production first.
```

### SQL Query

```sql
SELECT name, setting, unit, context, short_desc
FROM pg_settings
WHERE name IN (
    'shared_buffers',
    'effective_cache_size',
    'work_mem',
    'maintenance_work_mem',
    'max_connections',
    'wal_buffers',
    'checkpoint_timeout',
    'max_wal_size',
    'min_wal_size',
    'random_page_cost',
    'effective_io_concurrency',
    'max_worker_processes',
    'max_parallel_workers',
    'max_parallel_workers_per_gather'
);
```

### Recommendations Logic

| Setting | Heuristic |
|---------|-----------|
| shared_buffers | 25% of RAM, max 8GB (diminishing returns) |
| effective_cache_size | 50-75% of RAM |
| work_mem | RAM / max_connections / 4 (rough starting point) |
| maintenance_work_mem | 256MB-1GB depending on RAM |
| max_wal_size | 2-4GB for busy systems |

### Caveats

- Add prominent disclaimer about context-dependent recommendations
- Mark as "suggestions" not "requirements"
- Don't make this a blocking/critical status

### Struct

```rust
#[derive(Debug, Clone, Serialize)]
pub struct ConfigSetting {
    pub name: String,
    pub current_value: String,
    pub unit: Option<String>,
    pub recommended_value: Option<String>,
    pub recommendation: Option<String>,
    pub status: ConfigStatus,  // Ok, Suggestion, Warning (never Critical)
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigResult {
    pub settings: Vec<ConfigSetting>,
    pub postgres_version: String,
    pub disclaimer: String,  // Always include
}
```

---

## Implementation Plan

### Files to Create

```
src/commands/
├── stats_age.rs          # NEW
├── checkpoints.rs        # NEW
├── autovacuum_progress.rs # NEW
├── config.rs             # NEW
```

### Files to Modify

```
src/commands/indexes.rs   # Add FK detection
src/commands/mod.rs       # Export new modules
src/main.rs               # Wire up CLI
schemas/                  # Add JSON schemas
llms.txt                  # Update docs
```

### Task Breakdown

| ID | Task | Effort |
|----|------|--------|
| 1 | FK index detection in `indexes.rs` | S |
| 2 | `dba stats-age` command | S |
| 3 | `dba checkpoints` command | S |
| 4 | `dba autovacuum-progress` command | S |
| 5 | `dba config` command | M |
| 6 | Integration tests | S |
| 7 | Update llms.txt and docs | S |

---

## Success Criteria

- [ ] `pgcrate dba indexes` shows FK columns without indexes
- [ ] `pgcrate dba stats-age` shows tables with stale statistics
- [ ] `pgcrate dba checkpoints` shows checkpoint health
- [ ] `pgcrate dba autovacuum-progress` shows running autovacuum
- [ ] `pgcrate dba config` shows settings with recommendations
- [ ] All commands support `--json`
- [ ] Integration tests pass
- [ ] llms.txt updated

---

## Notes

- All commands are read-only, safe for production
- Keep `dba config` recommendations conservative
- Include disclaimers where appropriate
