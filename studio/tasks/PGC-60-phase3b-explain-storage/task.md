# Phase 3b: Query Explain + Storage Diagnostics

> Complete the "why is prod slow?" workflow with `explain`, add "am I running out of disk?" with `storage`.

**Project:** pgcrate-dba-diagnostics
**Location:** `/Users/jackschultz/workspace/dev/pgcrate-studio/pgcrate-dba-diagnostics`
**Status:** Planning

---

## Executive Summary

Two high-value additions that complete key workflows:

1. **`pgcrate dba explain`** - Query plan analysis with recommendations
   - Completes: "Why is prod slow?" → `locks → queries → explain`
   - Safe: EXPLAIN only, not EXPLAIN ANALYZE by default

2. **`pgcrate dba storage`** - Disk usage analysis
   - Completes: "Am I about to have an outage?" → adds disk pressure signal
   - Common outage cause, high signal, read-only

---

## Deliverable 1: `pgcrate dba explain`

### Usage

```bash
pgcrate dba explain "SELECT * FROM users WHERE email = 'foo@bar.com'"
pgcrate dba explain "SELECT * FROM users WHERE email = 'foo@bar.com'" --analyze  # Actually run (careful!)
pgcrate dba explain --file query.sql
pgcrate dba explain "..." --json
```

### What It Shows

**Human output:**
```
Query Plan Analysis
═══════════════════

Plan:
  Seq Scan on users  (cost=0.00..25.00 rows=1 width=100)
    Filter: (email = 'foo@bar.com'::text)

⚠ Potential Issues:
  • Sequential scan on large table (users: ~100K rows)
  • Filter on non-indexed column: email

💡 Recommendations:
  • Consider index: CREATE INDEX idx_users_email ON users(email);
  • Estimated improvement: Seq Scan → Index Scan
```

### Key Design Decisions

1. **EXPLAIN only by default** - Safe, read-only
2. **EXPLAIN ANALYZE requires `--analyze` flag** - Actually executes the query
3. **Basic recommendations only** - Don't try to be a full query optimizer
   - Sequential scan on large table → suggest index
   - Missing index on filter/join column
   - High cost estimates
4. **No query modification** - Just analyze, don't rewrite

### Recommendations Engine (Simple)

| Pattern | Detection | Recommendation |
|---------|-----------|----------------|
| Seq Scan on large table | `Seq Scan` + estimated rows > 10K | Suggest index on filter columns |
| Missing index on WHERE | Filter on non-indexed column | `CREATE INDEX` suggestion |
| Nested loop on large sets | Nested Loop + high row estimates | Note potential performance issue |
| Sort without index | Sort node + no index | Suggest index for ORDER BY |

**Important:** Keep recommendations conservative. Better to miss some than give bad advice.

### JSON Schema

```json
{
  "ok": true,
  "schema_id": "pgcrate.diagnostics.explain",
  "schema_version": "1.0.0",
  "data": {
    "query": "SELECT * FROM users WHERE email = $1",
    "plan_text": "Seq Scan on users...",
    "plan_json": { /* raw EXPLAIN JSON */ },
    "analyzed": false,
    "issues": [
      {
        "type": "seq_scan_large_table",
        "severity": "warning",
        "message": "Sequential scan on table with ~100K rows",
        "table": "users",
        "suggestion": "Consider index on filter columns"
      }
    ],
    "recommendations": [
      {
        "type": "create_index",
        "sql": "CREATE INDEX idx_users_email ON users(email);",
        "rationale": "Filter on non-indexed column: email"
      }
    ],
    "stats": {
      "estimated_cost": 25.00,
      "estimated_rows": 1,
      "actual_time_ms": null,  // Only if --analyze
      "actual_rows": null
    }
  }
}
```

---

## Deliverable 2: `pgcrate dba storage`

### Usage

```bash
pgcrate dba storage                     # Overview of all objects
pgcrate dba storage --top 20            # Top 20 largest objects
pgcrate dba storage public.events       # Deep-dive on specific table
pgcrate dba storage --growth            # Show recent growth (requires pg_stat_user_tables)
pgcrate dba storage --json
```

### What It Shows

**Human output (overview):**
```
Storage Overview
════════════════

Database Size: 15.2 GB

Top Tables by Size:
  ✓ public.events           8.2 GB  (54%)  [12M rows]
  ✓ public.audit_log        3.1 GB  (20%)  [45M rows]
  ✓ public.users            1.2 GB  (8%)   [2M rows]
  ⚠ public.sessions         890 MB  (6%)   [500K rows] - High bloat estimate

Top Indexes by Size:
  ✓ events_pkey             1.8 GB
  ✓ idx_events_created      1.2 GB
  ⚠ idx_audit_user_id       450 MB - Unused (0 scans)

TOAST Tables:
  public.events             2.1 GB (in-table TOAST)

Temp Files (current):
  None active

Tablespaces:
  pg_default                14.8 GB
  pg_global                 400 MB
```

**Human output (table deep-dive):**
```
Storage: public.events
══════════════════════

Table Size:        8.2 GB
  - Main:          6.1 GB
  - TOAST:         2.1 GB
  - Indexes:       3.0 GB (total)

Row Count:         12,450,000
Row Size (avg):    ~520 bytes

Indexes:
  events_pkey              1.8 GB  [btree, unique]
  idx_events_created       1.2 GB  [btree]

Bloat Estimate:    ~5% (heuristic)

Recent Growth:
  Last vacuum:     2 hours ago
  Dead tuples:     ~45,000 (0.4%)
  Inserts/day:     ~150,000 (estimated)
```

### Key Metrics

| Metric | Source | Notes |
|--------|--------|-------|
| Table size | `pg_total_relation_size()` | Includes indexes, TOAST |
| Table-only size | `pg_relation_size()` | Main heap only |
| Index sizes | `pg_indexes_size()` | All indexes |
| TOAST size | `pg_relation_size(reltoastrelid)` | Out-of-line storage |
| Row count | `pg_stat_user_tables.n_live_tup` | Estimate |
| Bloat | Heuristic or pgstattuple | See below |
| Dead tuples | `pg_stat_user_tables.n_dead_tup` | Vacuum needed? |
| Temp files | `pg_stat_database.temp_bytes` | Current session temp usage |
| Tablespaces | `pg_tablespace` | If multiple |

### Bloat Estimation

Two modes:
1. **Heuristic (default)** - Uses statistical estimates, always available
2. **Accurate (with pgstattuple)** - If extension installed, use `pgstattuple()` for precise measurement

### JSON Schema

```json
{
  "ok": true,
  "schema_id": "pgcrate.diagnostics.storage",
  "schema_version": "1.0.0",
  "data": {
    "database_size_bytes": 16321847296,
    "tables": [
      {
        "schema": "public",
        "name": "events",
        "total_bytes": 8800000000,
        "table_bytes": 6500000000,
        "toast_bytes": 2100000000,
        "index_bytes": 3200000000,
        "row_count": 12450000,
        "dead_tuples": 45000,
        "bloat_estimate_pct": 5.0,
        "bloat_method": "heuristic",
        "last_vacuum": "2026-01-20T10:00:00Z",
        "last_analyze": "2026-01-20T10:00:00Z",
        "status": "healthy"
      }
    ],
    "indexes": [
      {
        "schema": "public",
        "name": "events_pkey",
        "table": "events",
        "size_bytes": 1900000000,
        "type": "btree",
        "is_unique": true,
        "is_primary": true,
        "scans": 45000000
      }
    ],
    "tablespaces": [
      {
        "name": "pg_default",
        "size_bytes": 15800000000,
        "location": null
      }
    ],
    "temp_bytes": 0,
    "overall_status": "healthy"
  }
}
```

---

## Implementation Plan

### Task Breakdown

| ID | Task | Effort | Dependencies |
|----|------|--------|--------------|
| PGC-61 | `pgcrate dba explain` core | M | None |
| PGC-62 | Explain recommendations engine | M | PGC-61 |
| PGC-63 | `pgcrate dba storage` overview | M | None |
| PGC-64 | Storage table deep-dive | S | PGC-63 |
| PGC-65 | Integration tests | S | PGC-61-64 |
| PGC-66 | Update REFERENCE.md / llms.txt | S | PGC-61-64 |

### File Structure

```
src/commands/
├── explain.rs       # NEW
├── storage.rs       # NEW
└── mod.rs           # Add: pub mod explain; pub mod storage;
```

### CLI Wiring

```rust
// In DbaCommands enum
Explain {
    /// SQL query to explain
    query: Option<String>,
    /// Read query from file
    #[arg(long)]
    file: Option<PathBuf>,
    /// Actually execute with EXPLAIN ANALYZE (careful!)
    #[arg(long)]
    analyze: bool,
},
Storage {
    /// Specific table to analyze (schema.table)
    table: Option<String>,
    /// Number of top objects to show
    #[arg(long, default_value = "10")]
    top: usize,
    /// Show growth statistics
    #[arg(long)]
    growth: bool,
},
```

---

## SQL Queries

### explain - Get query plan

```sql
-- Basic EXPLAIN (safe, read-only)
EXPLAIN (FORMAT JSON, VERBOSE) $query;

-- With ANALYZE (actually runs the query!)
EXPLAIN (ANALYZE, FORMAT JSON, VERBOSE, BUFFERS) $query;
```

### storage - Overview

```sql
-- Database size
SELECT pg_database_size(current_database());

-- Table sizes
SELECT
    schemaname,
    relname,
    pg_total_relation_size(relid) as total_bytes,
    pg_relation_size(relid) as table_bytes,
    pg_indexes_size(relid) as index_bytes,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_analyze
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT $1;

-- Index sizes and usage
SELECT
    schemaname,
    indexrelname,
    relname as table_name,
    pg_relation_size(indexrelid) as size_bytes,
    idx_scan
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC
LIMIT $1;

-- TOAST sizes
SELECT
    c.relname as table_name,
    pg_relation_size(c.reltoastrelid) as toast_bytes
FROM pg_class c
WHERE c.reltoastrelid != 0
  AND c.relkind = 'r'
ORDER BY pg_relation_size(c.reltoastrelid) DESC;

-- Tablespaces
SELECT
    spcname,
    pg_tablespace_size(oid) as size_bytes,
    pg_tablespace_location(oid) as location
FROM pg_tablespace;
```

---

## Status Thresholds

### explain
- No status levels (it's analysis, not health check)
- Issues have severity: info, warning, critical

### storage
- **Critical:** Table bloat > 50%, temp files > 10GB, tablespace > 90% full
- **Warning:** Table bloat > 20%, unused large indexes, high dead tuple ratio
- **Healthy:** Normal operation

---

## Open Questions

1. **Historical metrics storage** - Should we add optional SQLite to track storage trends over time? (Deferred - can add later)

2. **pg_stat_monitor vs pg_stat_statements** - For explain recommendations, should we check if query appears in pg_stat_statements for real-world stats?

3. **Tablespace alerts** - How do we know if a tablespace is "almost full"? Need filesystem info which Postgres doesn't expose.

---

## Success Criteria

- [ ] `pgcrate dba explain "SELECT..."` shows plan with basic recommendations
- [ ] `pgcrate dba explain --analyze` works with explicit flag
- [ ] `pgcrate dba storage` shows top tables/indexes by size
- [ ] `pgcrate dba storage public.events` shows table deep-dive
- [ ] JSON output matches schemas
- [ ] Integration tests pass
- [ ] REFERENCE.md and llms.txt updated

---

## References

- [ROADMAP.md](../../ROADMAP.md) - Phase 3 definition
- [FUTURE.md](../../FUTURE.md) - storage command spec
- PostgreSQL EXPLAIN docs: https://www.postgresql.org/docs/current/sql-explain.html
