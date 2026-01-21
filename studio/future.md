# Future: pgcrate Roadmap

Ideas and planned features, prioritized by impact and effort.

---

## DBA Diagnostics (v0.6.0)

### High Priority

#### `dba fk-indexes` - Foreign Keys Without Indexes
**Effort:** M (1 day)
**Impact:** High - most common "hidden" performance issue

Foreign keys without supporting indexes cause:
- Slow DELETE operations (full table scan to check references)
- Slow joins on FK columns
- Lock contention during cascading deletes

```bash
pgcrate dba fk-indexes              # List FKs missing indexes
pgcrate dba fk-indexes --json       # JSON output
pgcrate dba fix index --create ...  # Future: auto-create missing indexes
```

**Implementation:**
- Query pg_constraint + pg_index to find FKs without matching indexes
- Show: table, column, referenced table, estimated row count
- Status: warning if table has >10k rows, critical if >100k

---

### Medium Priority

#### `dba long-queries` - Long-Running Queries
**Effort:** S (few hours)
**Impact:** Medium - catches stuck queries

```bash
pgcrate dba long-queries             # Queries running > 60s (default)
pgcrate dba long-queries --threshold 30s
pgcrate dba long-queries --json
```

**Implementation:**
- Query pg_stat_activity WHERE state = 'active' AND now() - query_start > threshold
- Show: pid, duration, query (truncated), user, database
- Include pg_cancel_backend() suggestion for stuck queries

---

#### `dba idle-txn` - Idle in Transaction
**Effort:** S (few hours)
**Impact:** Medium - common cause of lock contention

```bash
pgcrate dba idle-txn                 # Connections idle in transaction > 60s
pgcrate dba idle-txn --threshold 30s
pgcrate dba idle-txn --json
```

**Implementation:**
- Query pg_stat_activity WHERE state = 'idle in transaction'
- Show: pid, duration, last query, user
- Critical if > 5 minutes (likely forgotten transaction)

---

#### `dba stats-age` - Statistics Freshness
**Effort:** S (few hours)
**Impact:** Medium - stale stats = bad query plans

```bash
pgcrate dba stats-age                # Tables with oldest statistics
pgcrate dba stats-age --threshold 7d # Warn if stats > 7 days old
pgcrate dba stats-age --json
```

**Implementation:**
- Query pg_stat_user_tables for last_analyze, last_autoanalyze
- Show: table, last analyzed, row estimate vs actual (if available)
- Recommend ANALYZE for stale tables

---

#### `dba autovacuum` - Autovacuum Health
**Effort:** S (few hours)
**Impact:** Medium - is autovacuum keeping up?

```bash
pgcrate dba autovacuum               # Autovacuum status and lagging tables
pgcrate dba autovacuum --json
```

**Implementation:**
- Check pg_stat_progress_vacuum for running autovacuum
- Check pg_stat_user_tables for tables approaching autovacuum threshold
- Show: tables with high dead tuple ratio, last vacuum time
- Warning if dead_tup_ratio > 10%, critical if > 20%

---

### Lower Priority

#### `dba checkpoints` - Checkpoint Analysis
**Effort:** M (1 day)
**Impact:** Low-Medium - tuning for write-heavy workloads

```bash
pgcrate dba checkpoints              # Checkpoint frequency and spread
pgcrate dba checkpoints --json
```

**Implementation:**
- Query pg_stat_bgwriter for checkpoint stats
- Calculate checkpoint frequency, buffers written
- Warn if checkpoints too frequent (< 5 min apart)

---

#### `dba config` - Configuration Review
**Effort:** M (1 day)
**Impact:** Medium - but subjective, hard to get right

```bash
pgcrate dba config                   # Compare settings to recommendations
pgcrate dba config --json
```

**Implementation:**
- Check shared_buffers, work_mem, effective_cache_size, etc.
- Compare to system RAM and connection count
- Recommendations based on PGTune-style heuristics
- Caution: recommendations are starting points, not guarantees

---

## Developer Workflow (v0.7.0+)

### `inspect diff` - Schema Comparison
Compare schemas between two databases.

### `anonymize` - Data Anonymization
Export data with PII redacted based on TOML rules.

### `snapshot` - Database Snapshots
Save/restore database state with selective profiles.

### `bootstrap` - Environment Setup
Full environment setup from production with anonymized data.

---

## Fix Command Extensions

### `dba fix index --create`
Auto-create missing indexes for foreign keys.

### `dba fix autovacuum`
Adjust autovacuum settings for specific tables.

### `dba fix config`
Apply recommended configuration changes (dangerous - requires careful review).

---

## Notes

- DBA diagnostics should be quick to run (< 5s on typical databases)
- All commands support `--json` for automation
- Exit codes: 0 = healthy, 1 = warning, 2 = critical
- Gate flags (`--read-write`, `--primary`) required for any modifications
