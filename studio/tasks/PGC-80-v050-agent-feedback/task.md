# PGC-80: v0.5.0 Agent Feedback Fixes

> Fixes identified from agentest scenario runs before v0.5.0 release.

**Branch:** `feature/dba-fix-bloat` (same branch, pre-release)
**Source:** Agent feedback from diagnostics/slow-database and diagnostics/query-optimization scenarios

---

## Context

Ran pgcrate diagnostic scenarios with sonnet model. Initial runs scored 6/7 due to:
1. Index recommendations for columns that already have indexes
2. Agents tried `dba cache` command that doesn't exist
3. Exit code 1 for warnings breaks CI pipelines expecting 0 for non-errors

After fixing filter parsing and skill docs, both scenarios hit 7/7. These remaining items are enhancements identified from feedback.

---

## Tasks

### PGC-80a: Check existing indexes before recommending

**Source:** Run #19 feedback
> "recommendation: CREATE INDEX idx_orders_status... but idx_orders_status already exists"

**Problem:** `dba explain --include-actions` recommends indexes without checking if one already exists on that column.

**Solution:** Before adding a CreateIndex recommendation, query `pg_indexes` to check if an index exists on the same table/column combination.

**Implementation:**
```rust
// In explain.rs, before pushing CreateIndex recommendation
async fn index_exists_on_column(client: &Client, schema: &str, table: &str, column: &str) -> bool {
    // Check pg_indexes for existing index on this column
}
```

**Effort:** S (small)

---

### PGC-80b: Add `dba cache` command

**Source:** Run #11 feedback
> Agent tried `pgcrate dba cache` - "unrecognized subcommand"

**Problem:** Agents expect a cache hit ratio command. Currently this info is buried in `dba queries` output per-query.

**Solution:** Add `pgcrate dba cache` showing database-wide and per-table cache hit ratios from `pg_statio_user_tables`.

**Usage:**
```bash
pgcrate dba cache                # Database-wide + top tables by cache misses
pgcrate dba cache --json         # JSON output
```

**Key metrics:**
- Database-wide cache hit ratio (from pg_stat_database)
- Per-table cache hit ratio (heap_blks_hit / (heap_blks_hit + heap_blks_read))
- Tables with lowest cache hit ratios (potential memory pressure)

**Thresholds:**
- Critical: < 90% hit ratio
- Warning: < 95% hit ratio
- Healthy: >= 95%

**Effort:** S (small)

---

### PGC-80c: Fix exit codes for JSON mode

**Source:** Run #15 feedback
> "Exit code 1 on successful query analysis with warnings... breaks CI pipelines"

**Problem:** When `--json` is used and `ok: true` but severity is "warning", exit code is 1. This breaks CI that checks `$?`.

**Current behavior:**
- Exit 0: healthy
- Exit 1: warning
- Exit 2: critical

**Proposed behavior with `--json`:**
- Exit 0: healthy OR warning (ok: true)
- Exit 1: critical OR error (ok: false)

**Rationale:** JSON consumers parse the response; exit code should indicate "did it work" not "are there warnings". Warnings are informational, not failures.

**Implementation:** In diagnostic output handlers, when `--json` flag is set, return 0 for warning severity.

**Effort:** S (small)

---

## Success Criteria

- [ ] `dba explain` doesn't recommend indexes that already exist
- [ ] `dba cache` shows cache hit ratios with status thresholds
- [ ] `--json` mode returns exit 0 for warnings
- [ ] All existing tests pass
- [ ] Re-run agentest scenarios to validate

---

## PM Notes

These are polish items from real agent usage. None are blockers for v0.5.0 release, but they improve the experience significantly:

1. **PGC-80a** prevents confusing "create duplicate index" suggestions
2. **PGC-80b** fills a gap agents naturally expected
3. **PGC-80c** makes pgcrate CI-friendly

All are small effort. Recommend including in v0.5.0 before release.
