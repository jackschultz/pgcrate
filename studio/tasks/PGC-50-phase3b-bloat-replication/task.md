# Phase 3b/4: Bloat + Replication Diagnostics

**Branch:** `feature/phase3b-bloat-replication`
**Location:** `/Users/jackschultz/workspace/dev/pgcrate-studio/pgcrate-dba-diagnostics`

---

## Summary

Add `pgcrate bloat` and `pgcrate replication` to complete production diagnostics. Defer `pgcrate pooler` (architecture mismatch - requires separate endpoint).

---

## Commands

### `pgcrate bloat`

```bash
pgcrate bloat                     # All tables + indexes
pgcrate bloat --table schema.tbl  # Specific table
pgcrate bloat --schema app        # All in schema
pgcrate bloat --limit 20          # Top N by bloat
pgcrate bloat --json              # JSON output
```

**Metrics:**
- Table bloat: dead tuples ratio, pgstattuple if available
- Index bloat: statistical estimation (ioguix-style)
- Bloat bytes and percentage

**Thresholds:**
- Warning: >20% bloat
- Critical: >50% bloat

**Capability:** `diagnostics.bloat` - always available (statistical), enhanced with pgstattuple

### `pgcrate replication`

```bash
pgcrate replication               # Full overview
pgcrate replication --slots-only  # Just slots
pgcrate replication --json        # JSON output
```

**Metrics:**
- Server role (primary/standby via pg_is_in_recovery())
- Replica lag (write_lag, flush_lag, replay_lag, byte lag)
- Slot status (active, wal_status, retained bytes)
- WAL receiver status (standby only)

**Thresholds:**
- Warning: replay_lag >30s OR inactive slot retaining >1GB
- Critical: replay_lag >5min OR wal_status='lost'

**Capability:** `diagnostics.replication` - requires pg_stat_replication SELECT

---

## Task Breakdown

### PGC-50a: `pgcrate bloat`

1. Create `src/commands/bloat.rs`
2. Statistical index bloat query (no extension)
3. Table bloat from pg_stat_user_tables + optional pgstattuple
4. Human + JSON output
5. Wire up in main.rs, capabilities.rs, output.rs
6. Integration tests

### PGC-50b: `pgcrate replication`

1. Create `src/commands/replication.rs`
2. Server role detection
3. Query pg_stat_replication, pg_replication_slots, pg_stat_wal_receiver
4. Human + JSON output
5. Wire up
6. Integration tests (limited - hard to test without replica)

---

## SQL Queries

### Bloat - Index Statistical Estimation

```sql
WITH index_stats AS (
  SELECT
    schemaname,
    tablename,
    indexname,
    pg_relation_size(indexrelid) as index_size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
  FROM pg_stat_user_indexes
  JOIN pg_index ON indexrelid = pg_stat_user_indexes.indexrelid
  WHERE NOT indisunique  -- unique indexes don't bloat the same way
),
index_bloat AS (
  SELECT
    nspname AS schema,
    tblname AS table,
    idxname AS index,
    bs*(relpages)::bigint AS real_size,
    bs*(relpages-est_pages)::bigint AS bloat_bytes,
    100 * (relpages-est_pages)::float / relpages AS bloat_pct
  FROM (
    SELECT
      coalesce(1 + ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0) AS est_pages,
      bs, nspname, tblname, idxname, relpages
    FROM (
      SELECT
        maxalign, bs, nspname, tblname, idxname, reltuples, relpages,
        pagehdr, pageopqdata,
        (index_tuple_hdr_bm + maxalign - CASE WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign ELSE index_tuple_hdr_bm%maxalign END + nulldatawidth + maxalign - CASE WHEN nulldatawidth%maxalign = 0 THEN maxalign ELSE nulldatawidth%maxalign END)::float AS nulldatahdrwidth
      FROM (
        SELECT
          n.nspname, ct.relname AS tblname, ci.relname AS idxname,
          ci.reltuples, ci.relpages,
          current_setting('block_size')::int AS bs,
          CASE WHEN version() ~ 'mingw32|64-bit' THEN 8 ELSE 4 END AS maxalign,
          24 AS pagehdr, 16 AS pageopqdata,
          CASE WHEN max(coalesce(s.null_frac,0)) = 0 THEN 2 ELSE 2 + (32 + 8 - 1) / 8 END AS index_tuple_hdr_bm,
          sum((1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth
        FROM pg_index i
        JOIN pg_class ct ON ct.oid = i.indrelid
        JOIN pg_class ci ON ci.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = ct.relnamespace
        JOIN pg_stats s ON s.schemaname = n.nspname AND s.tablename = ct.relname AND s.attname = ANY(ARRAY(SELECT a.attname FROM pg_attribute a WHERE a.attrelid = ct.oid AND a.attnum = ANY(i.indkey)))
        WHERE NOT i.indisunique
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY 1,2,3,4,5,6,7,8,9
      ) sub1
    ) sub2
  ) sub3
  WHERE relpages > 0
)
SELECT * FROM index_bloat WHERE bloat_pct > 0 ORDER BY bloat_bytes DESC;
```

### Bloat - Table (pg_stat_user_tables)

```sql
SELECT
  schemaname,
  relname,
  pg_total_relation_size(relid) as total_bytes,
  pg_table_size(relid) as table_bytes,
  n_live_tup,
  n_dead_tup,
  CASE WHEN n_live_tup > 0
    THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 1)
    ELSE 0
  END as dead_tuple_pct,
  last_vacuum,
  last_autovacuum
FROM pg_stat_user_tables
WHERE n_dead_tup > 0
ORDER BY n_dead_tup DESC;
```

### Replication - Primary

```sql
SELECT
  application_name,
  client_addr::text,
  state,
  sync_state,
  sent_lsn::text,
  write_lsn::text,
  flush_lsn::text,
  replay_lsn::text,
  write_lag::text,
  flush_lag::text,
  replay_lag::text,
  pg_wal_lsn_diff(sent_lsn, replay_lsn) as lag_bytes
FROM pg_stat_replication;
```

### Replication - Slots

```sql
SELECT
  slot_name,
  slot_type,
  database,
  active,
  wal_status,
  pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as retained_bytes
FROM pg_replication_slots;
```

### Replication - Standby

```sql
SELECT
  status,
  sender_host,
  sender_port,
  slot_name,
  pg_wal_lsn_diff(latest_end_lsn, received_lsn) as lag_bytes
FROM pg_stat_wal_receiver;
```

---

## Success Criteria

- [ ] `pgcrate bloat` shows table and index bloat estimates
- [ ] `pgcrate bloat --json` matches schema patterns
- [ ] Graceful degradation without pgstattuple
- [ ] `pgcrate replication` detects primary vs standby
- [ ] `pgcrate replication` shows replica lag and slot status
- [ ] `pgcrate replication` handles "no replication" gracefully
- [ ] Capabilities updated for both commands
- [ ] All tests pass
