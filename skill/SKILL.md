---
name: pgcrate
description: >-
  Use when working with a PostgreSQL database: running queries, inspecting
  schema, writing or applying migrations, loading seed/test data, or
  diagnosing health and performance ("why is the DB slow", locks, bloat,
  sequence exhaustion, slow queries). pgcrate is a single binary that wraps
  psql-class work with guardrails — reads are read-only and capped, writes
  preview themselves (dry-run + rollback) before they commit, plus timeouts,
  structured JSON, and semantic exit codes so the work is safe to run unsupervised.
---

# pgcrate: the Postgres interface for agents

`pgcrate` is a CLI you reach for instead of raw `psql` when touching Postgres.
You are the user; the human supervising you is the customer. pgcrate exists so
that work is trustworthy by default: reads can't accidentally write, queries
can't hang forever, and output is dense and machine-readable.

## Why pgcrate over raw psql

- **Writes preview by default.** `sql` blocks writes; `--allow-write` *dry-runs*
  one (transaction → report → rollback), and only `--commit` actually applies it.
  Diagnostics never write. You can't `DROP` something by accident, and you can't
  forget to preview a destructive change.
- **Timeouts on everything.** Connect/statement/lock timeouts are enforced
  (defaults 5s / 30s / 500ms; override with `--connect-timeout`,
  `--statement-timeout`, `--lock-timeout`). A bad query fails fast instead of
  pinning a backend.
- **Structured output.** Add `--json` to query/diagnostic commands when the
  result feeds further work. Human tables otherwise — and those auto-densify
  when output is piped or captured (no box-drawing or padding, same info, fewer
  tokens); pass `--pretty` to force the decorated form.
- **Semantic exit codes** — branch on them, don't parse text:
  - `0` healthy / success
  - `1` warning (non-critical finding)
  - `2` critical finding
  - `10+` operational failure (`11` connection, `12` config, `13` permission,
    `130` interrupted) — i.e. "couldn't check", not "found a problem".

Connection comes from `-d <url>`, `DATABASE_URL`, a named `-C <name>` from
`pgcrate.toml`, or `--env <VAR>`. `--help-llm` on any command dumps the full
exhaustive reference; this skill is the *when/why*, that flag is the *what*.

## Session start — orient before you act

```bash
pgcrate brief            # whole DB in one screen: schemas → tables → est. rows, FKs, migrations, hazards
pgcrate context          # connection, server version, extensions, privileges, read/write mode
pgcrate capabilities     # what you're actually allowed to do here
pgcrate inspect table <schema.name>   # columns, indexes, constraints, stats
pgcrate inspect table <name> --dependents     # what breaks if you change it
pgcrate inspect roles            # roles/users; add --describe <name> for one
pgcrate inspect extensions       # installed extensions (--available for the rest)
```

**Run `brief` first on an unfamiliar database.** It's the one command that
orients you in a single shot: which instance you're on (db/host/port/user/mode —
check this before you touch anything; the worst mistake is the right query on the
wrong instance), every non-system schema with its tables and *estimated* row
counts (`reltuples`, never `count(*)` — so it's sub-second and never scans), the
foreign-key graph in compact `child → parents` form, migration state (applied N /
pending M) when a `pgcrate.toml` resolves, installed extensions + server version,
and any cheap at-a-glance hazards (sequence exhaustion, XID age). It's catalog-only
and read-only; health flags are *shown, not scored* (it always exits `0` — for a
scored health pass use `dba triage`). `--json` gives the full, uncapped structure
(`schema_id: pgcrate.brief`); human output caps long schemas to the largest tables.

`context` drills into the connection/server/privilege detail when you need it
(e.g. confirming you have permission for the diagnostics below).

## Querying

```bash
pgcrate sql -c "SELECT ..."             # read-only; blocks writes
echo "SELECT ..." | pgcrate sql         # reads from stdin if no -c
pgcrate sql -c "SELECT ..." --json      # structured rows for downstream use
pgcrate sql -c "SELECT ..." --limit 50  # cap rows (default 1000; --limit 0 uncaps)
```

Reads are read-only and capped. `SELECT` output is truncated at 1000 rows by
default with a `… +N more rows` trailer so a stray `SELECT *` on a huge table
can't flood your context; raise it with `--limit N`, or `--limit 0` to uncap.
`query` is an alias for `sql`. Statement/lock/connect timeouts apply here just
like the diagnostics (defaults 30s / 500ms / 5s; override with the global
`--statement-timeout` etc.).

### Writes preview themselves (dry-run by default)

A write — `UPDATE`/`DELETE`/`INSERT` or DDL — never just happens. By default
it's blocked; the two ways to run one are:

```bash
pgcrate sql -c "UPDATE ..." --allow-write   # PREVIEW: runs in a txn, reports, ROLLS BACK
pgcrate sql -c "UPDATE ..." --commit        # APPLY: actually commits (implies --allow-write)
```

`--allow-write` is a **dry run**: the statement runs inside a transaction,
reports how many rows it would affect (plus a sample of the affected rows for a
single DML statement), then **rolls back** — nothing changes. Preview first,
show the human the affected count/sample, then re-run with `--commit` to apply.
`--commit` is the only flag that writes; you can't forget to preview.

The statement executes **exactly once** under both flags. A dry run is a real
trial run, so a statement that *can't* succeed (unique/constraint violation,
type error) **fails the preview** — exit `10` with `ROLLED BACK — nothing
changed`, not a clean `DRY RUN` report. That's a feature: the preview tells you
the write would have failed before you reach `--commit`. Likewise a `--commit`
whose statement aborts is reported honestly — exit `10`, `ROLLED BACK`, never a
false `COMMITTED` — including deferred-constraint violations that only surface at
commit time.

Writes that hide inside a query are caught, not just bare `INSERT`/`UPDATE`/
`DELETE`: writable CTEs (`WITH d AS (DELETE … RETURNING *) SELECT * FROM d`),
leading-CTE DML, `SELECT … INTO new_table`, and `EXPLAIN ANALYZE <write>` (which
*executes* the statement) all classify as writes and go through the same
preview-or-`--commit` gate.

Before a write runs, pgcrate EXPLAINs it and warns (on stderr) if the estimated
cost is high — surface that warning to the human. With `--json` the cost
estimate is in the `write.cost` object (`estimated`, `threshold`,
`exceeds_threshold`) regardless of whether it crossed the threshold.
`--no-cost-check` skips the check.

Statements that can't run in a transaction (`CREATE INDEX CONCURRENTLY`,
`VACUUM`, `REINDEX`) can't be previewed; pgcrate refuses `--allow-write` for
them and tells you to use `--commit` directly.

**Known limitation:** side-effect functions called from a `SELECT`
(`SELECT setval(...)`, `SELECT nextval(...)`, `SELECT some_writing_func()`)
can't be detected statically — they read as queries. Without a write flag the
read-only connection still blocks them; but under `--allow-write` such a
`SELECT` runs in autocommit and its side effect is *not* rolled back. Don't rely
on dry-run to preview a `SELECT` that calls a writing function.

## Migration loop

Migrations are single `.sql` files with `-- up` / `-- down` sections.

```bash
pgcrate migrate new add_users      # scaffold timestamped file; then edit the SQL
pgcrate migrate up                 # apply pending migrations
pgcrate migrate status             # what's applied vs pending (supports --json)
pgcrate migrate up --dry-run       # preview without applying
```

Rolling back is **dev-only** and gated:

```bash
pgcrate migrate down --steps 1 --yes    # --steps is required; --yes confirms
```

Never run `migrate down` against production data. `pgcrate generate` (top-level)
can emit migration files from an existing schema (brownfield); `migrate baseline`
marks existing files as already-applied.

## Test data (seeds)

```bash
pgcrate seed list           # available seed files
pgcrate seed validate       # check files parse, without loading
pgcrate seed run            # load all (or name specific: pgcrate seed run public.users)
pgcrate seed diff           # compare seed files to current DB state
```

Use `seed diff` before `seed run` to see what would change.

## Production triage loop

Lead with triage, then drill into whatever it flags. All of these are read-only
and time-bounded, so they're safe to run against prod.

```bash
pgcrate dba triage --include-fixes   # one-shot health scan + suggested fixes
```

Then target the area triage (or the symptom) points at:

```bash
pgcrate dba locks                 # blocking chains, long/idle-in-tx (--blocking, --long-tx N)
pgcrate dba explain "SELECT ..."  # plan analysis (add --analyze to actually run it)
pgcrate dba queries               # top queries from pg_stat_statements (--by mean|calls)
pgcrate dba storage               # disk usage by table/index/TOAST
pgcrate dba indexes               # missing / unused / duplicate indexes
pgcrate dba bloat                 # table & index bloat estimates
pgcrate dba vacuum                # vacuum health and dead-tuple ratios
pgcrate dba sequences             # sequence exhaustion risk
pgcrate dba xid                   # transaction-ID wraparound risk
pgcrate dba connections           # connection usage vs max_connections
pgcrate dba cache                 # buffer cache hit ratios
pgcrate dba wal                   # WAL generation / archiving / disk
pgcrate dba replication           # streaming replication lag
pgcrate dba checkpoints           # checkpoint frequency/health
pgcrate dba config                # notable PostgreSQL settings
```

"Why is the DB slow?" → `dba triage`, then `dba locks` (blocking?),
`dba queries` (a hot statement?), `dba explain` on the suspect query,
`dba cache`/`dba bloat`/`dba indexes` (storage/IO).

Remediation is a separate, explicit, write step — each needs `--yes`:

```bash
pgcrate dba fix sequence <schema.seq> --upgrade-to bigint --yes
pgcrate dba fix index --drop <schema.index> --yes
pgcrate dba fix vacuum <schema.table> --yes
pgcrate dba fix bloat <schema.index> --yes      # REINDEX CONCURRENTLY by default
```

Every `fix` supports `--dry-run` (preview) and `--verify` (re-check after). Run
`--dry-run` first and surface the plan to the human before using `--yes`.

## Safety conventions

- Destructive operations require `--yes` — never pass it speculatively; show the
  plan/`--dry-run` first and let the human confirm intent.
- For `sql` writes: `--allow-write` is safe to reach for — it only previews
  (dry-run + rollback). Use it to show the human the affected count/sample, then
  only pass `--commit` once they've confirmed. Treat `--commit` like `--yes`:
  never speculative, always after a preview the human has seen.
- Never add `--read-write` (global) unless the task is explicitly a write.
  Default read-only is the guardrail; keep it on.
- Prefer `--json` whenever output is consumed by you for a next step; use human
  tables when reporting to the person.
- Treat exit `10+` as "I couldn't check" (fix connectivity/perms), distinct from
  `1`/`2` "the DB has a finding".

## Command reference

| Goal | Command |
|------|---------|
| Orient on an unfamiliar DB (run first) | `pgcrate brief` |
| Connection + server + privileges | `pgcrate context` |
| What I'm allowed to do | `pgcrate capabilities` |
| Describe a table | `pgcrate inspect table <name>` |
| Table dependents/dependencies | `pgcrate inspect table <name> --dependents` / `--dependencies` |
| Roles / grants / extensions | `pgcrate inspect roles` / `grants` / `extensions` |
| Compare two schemas | `pgcrate inspect diff --to <url>` |
| Run a read query | `pgcrate sql -c "SELECT ..."` (add `--json`, `--limit N`) |
| Preview a write (dry-run + rollback) | `pgcrate sql -c "..." --allow-write` |
| Apply a write (commit) | `pgcrate sql -c "..." --commit` |
| New migration | `pgcrate migrate new <name>` |
| Apply / status | `pgcrate migrate up` / `pgcrate migrate status` |
| Roll back (dev) | `pgcrate migrate down --steps N --yes` |
| Generate from DB / baseline | `pgcrate generate` / `pgcrate migrate baseline` |
| Load / diff seeds | `pgcrate seed run` / `pgcrate seed diff` |
| Health triage | `pgcrate dba triage --include-fixes` |
| Locks / slow queries / plans | `pgcrate dba locks` / `queries` / `explain "..."` |
| Storage / bloat / indexes / vacuum | `pgcrate dba storage` / `bloat` / `indexes` / `vacuum` |
| Sequences / XID / connections / WAL | `pgcrate dba sequences` / `xid` / `connections` / `wal` |
| Apply a fix | `pgcrate dba fix <sequence\|index\|vacuum\|bloat> ... --yes` |
| Full exhaustive reference | `pgcrate <cmd> --help-llm` |
