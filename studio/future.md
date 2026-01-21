# Future: pgcrate Roadmap

Ideas and planned features, prioritized by impact and effort.

**Last reviewed:** 2026-01-21

---

## v0.6.0: FK Index Detection

### `dba indexes` Enhancement - Foreign Keys Without Indexes
**Status:** Next up (PGC-90)
**Effort:** S-M (1 day)
**Impact:** High - most common hidden performance issue

Add FK index detection to existing `dba indexes` command. Foreign keys without supporting indexes cause:
- Slow DELETE operations (full table scan to check references)
- Slow JOINs on FK columns
- Lock contention during cascading deletes

See: `studio/tasks/PGC-90-fk-indexes/task.md`

---

## v0.6.x: Additional DBA Enhancements

### `dba stats-age` - Statistics Freshness
**Effort:** S (few hours)
**Impact:** Medium - stale stats = bad query plans

```bash
pgcrate dba stats-age                # Tables with oldest statistics
pgcrate dba stats-age --threshold 7d # Warn if stats > 7 days old
pgcrate dba stats-age --json
```

Shows when table statistics were last updated. Stale statistics lead to poor query plans.

---

### `dba autovacuum-progress` - Running Autovacuum
**Effort:** S (few hours)
**Impact:** Low-Medium - monitoring insight

```bash
pgcrate dba autovacuum-progress      # Currently running autovacuum operations
pgcrate dba autovacuum-progress --json
```

Shows pg_stat_progress_vacuum for in-flight autovacuum operations.

Note: `dba vacuum` already shows last_vacuum/last_autovacuum times and dead tuple ratios.

---

### `dba checkpoints` - Checkpoint Analysis
**Effort:** M (1 day)
**Impact:** Low-Medium - tuning for write-heavy workloads

```bash
pgcrate dba checkpoints              # Checkpoint frequency and spread
pgcrate dba checkpoints --json
```

Analyze pg_stat_bgwriter for checkpoint stats. Warn if checkpoints too frequent.

---

### `dba config` - Configuration Review
**Effort:** M (1 day)
**Impact:** Medium - but subjective, hard to get right

```bash
pgcrate dba config                   # Compare settings to recommendations
pgcrate dba config --json
```

Compare shared_buffers, work_mem, etc. to PGTune-style heuristics.
Caution: recommendations are starting points, not guarantees.

---

## Already Covered (removed from roadmap)

These were originally planned but already exist:

| Proposed | Covered By |
|----------|------------|
| `dba long-queries` | `dba locks --long-tx` |
| `dba idle-txn` | `dba locks --idle-in-tx` + `dba connections` |
| `dba autovacuum` | `dba vacuum` (shows last_autovacuum, dead tuples) |

---

## v0.7.0+: Developer Workflow

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
Natural follow-up to PGC-90.

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
