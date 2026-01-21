# PGC-90: Foreign Key Index Detection

> Add FK index detection to `dba indexes` command - the most common hidden performance issue.

**Project:** pgcrate-dba-diagnostics
**Branch:** `feature/fk-index-detection`
**Effort:** S-M (1 day)
**Priority:** High

---

## Problem

Foreign keys without supporting indexes cause:
1. **Slow DELETEs** - PostgreSQL scans child tables to check references
2. **Slow JOINs** - No index to accelerate FK column lookups
3. **Lock contention** - Long scans hold locks during cascading operations

This is the most common "hidden" performance issue in PostgreSQL. Tables grow, deletes get slow, and the FK index is the culprit.

**Example:** Parent table `users`, child table `orders` with `orders.user_id REFERENCES users(id)`.
- Without index on `orders.user_id`: DELETE FROM users takes full table scan of orders
- With index: instant lookup

---

## Solution

Enhance `pgcrate dba indexes` to include a fourth category: **Foreign Keys Without Indexes**.

### Current Output
```
MISSING INDEXES (tables with high sequential scans):
  ...

UNUSED INDEXES:
  ...

DUPLICATE INDEXES:
  ...
```

### New Output
```
MISSING INDEXES (tables with high sequential scans):
  ...

FOREIGN KEYS WITHOUT INDEXES:
  ⚠ orders.user_id → users.id (orders: ~1.2M rows)
  ⚠ order_items.order_id → orders.id (order_items: ~5.4M rows)
  ✓ payments.user_id → users.id (has index: idx_payments_user_id)

UNUSED INDEXES:
  ...

DUPLICATE INDEXES:
  ...
```

---

## Implementation

### 1. Add struct for FK index info

```rust
#[derive(Debug, Clone, Serialize)]
pub struct ForeignKeyIndex {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub references_schema: String,
    pub references_table: String,
    pub references_column: String,
    pub constraint_name: String,
    pub has_index: bool,
    pub index_name: Option<String>,
    pub table_rows: i64,
    pub status: IndexStatus,  // Reuse existing enum
}
```

### 2. SQL Query

```sql
WITH fk_columns AS (
    SELECT
        c.conname AS constraint_name,
        c.conrelid::regclass AS table_name,
        a.attname AS column_name,
        c.confrelid::regclass AS references_table,
        af.attname AS references_column,
        n.nspname AS schema_name,
        nf.nspname AS references_schema
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = ANY(c.confkey)
    JOIN pg_namespace n ON n.oid = c.connamespace
    JOIN pg_class cl ON cl.oid = c.confrelid
    JOIN pg_namespace nf ON nf.oid = cl.relnamespace
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
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE ix.indkey[0] = a.attnum  -- First column of index matches
)
SELECT
    fk.schema_name,
    fk.table_name::text,
    fk.column_name,
    fk.references_schema,
    fk.references_table::text,
    fk.references_column,
    fk.constraint_name,
    ic.index_name,
    COALESCE(s.n_live_tup, 0) AS table_rows
FROM fk_columns fk
LEFT JOIN indexed_columns ic
    ON ic.schema_name = fk.schema_name
    AND ic.table_name = fk.table_name::text
    AND ic.column_name = fk.column_name
LEFT JOIN pg_stat_user_tables s
    ON s.schemaname = fk.schema_name
    AND s.relname = fk.table_name::text
ORDER BY
    CASE WHEN ic.index_name IS NULL THEN 0 ELSE 1 END,  -- Missing first
    s.n_live_tup DESC NULLS LAST;
```

### 3. Status Thresholds

| Condition | Status |
|-----------|--------|
| No index, table > 100K rows | Critical |
| No index, table > 10K rows | Warning |
| No index, table < 10K rows | Info (show but don't warn) |
| Has index | Healthy (only show in verbose) |

### 4. JSON Schema Addition

Add to `IndexesResult`:

```rust
pub struct IndexesResult {
    pub missing_candidates: Vec<MissingIndexCandidate>,
    pub fk_without_indexes: Vec<ForeignKeyIndex>,  // NEW
    pub unused: Vec<UnusedIndex>,
    pub duplicate: Vec<DuplicateIndexSet>,
    pub overall_status: IndexStatus,
}
```

### 5. Triage Integration

Update `dba triage` to include FK index check in summary:
- Add to `next_actions` if critical FK indexes found
- Suggest: `pgcrate dba indexes --verbose`

---

## Tasks

| ID | Task | Effort |
|----|------|--------|
| 1 | Add `ForeignKeyIndex` struct and query | S |
| 2 | Integrate into `run_indexes()` | S |
| 3 | Update human output formatting | S |
| 4 | Update JSON schema | S |
| 5 | Add to triage summary | S |
| 6 | Integration tests | S |
| 7 | Update llms.txt | S |

---

## Files to Modify

```
src/commands/indexes.rs    # Main implementation
src/commands/triage.rs     # Add FK check to triage
schemas/indexes.schema.json # Update JSON schema
llms.txt                   # Update documentation
```

---

## Success Criteria

- [ ] `pgcrate dba indexes` shows FK columns without indexes
- [ ] Large tables (>100K rows) with missing FK indexes show as critical
- [ ] `pgcrate dba indexes --json` includes `fk_without_indexes` array
- [ ] `pgcrate dba triage` mentions FK index issues if critical
- [ ] Integration tests cover FK index detection
- [ ] Verified against real database with known missing FK indexes

---

## Future: `dba fix index --create`

After this task, we can add:
```bash
pgcrate dba fix index --create orders.user_id --dry-run
pgcrate dba fix index --create orders.user_id --yes --read-write --primary
```

This completes the diagnose→fix loop for FK indexes.

---

## Notes

- This is the #1 "hidden" performance issue I see in production PostgreSQL
- Agents doing "why is prod slow?" will benefit immediately
- Query is read-only, safe for production
- Index creation (fix command) is separate scope
