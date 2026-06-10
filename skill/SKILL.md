---
name: pgcrate
description: >-
  Use when working with a PostgreSQL database: running queries, inspecting
  schema, writing or applying migrations, loading seed/test data, or
  diagnosing health and performance ("why is the DB slow", locks, bloat,
  sequence exhaustion, slow queries). pgcrate is a single binary that wraps
  psql-class work with read-only-by-default guardrails, timeouts, structured
  JSON, and semantic exit codes so the work is safe to run unsupervised.
---

# pgcrate: the Postgres interface for agents

`pgcrate` is a CLI you reach for instead of raw `psql` when touching Postgres.
You are the user; the human supervising you is the customer. pgcrate exists so
that work is trustworthy by default: reads can't accidentally write, queries
can't hang forever, and output is dense and machine-readable.

## Why pgcrate over raw psql

- **Read-only by default.** `sql` refuses writes unless you pass `--allow-write`;
  diagnostics never write. You can't `DROP` something by accident.
- **Timeouts on everything.** Connect/statement/lock timeouts are enforced
  (defaults 5s / 30s / 500ms; override with `--connect-timeout`,
  `--statement-timeout`, `--lock-timeout`). A bad query fails fast instead of
  pinning a backend.
- **Structured output.** Add `--json` to query/diagnostic commands when the
  result feeds further work. Human tables otherwise.
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
pgcrate context          # connection, server version, extensions, privileges, read/write mode
pgcrate capabilities     # what you're actually allowed to do here
pgcrate inspect table <schema.name>   # columns, indexes, constraints, stats
pgcrate inspect table <name> --dependents     # what breaks if you change it
pgcrate inspect roles            # roles/users; add --describe <name> for one
pgcrate inspect extensions       # installed extensions (--available for the rest)
```

Run `context` first on an unfamiliar database — it tells you the PG version,
whether you're on a replica, and whether you even have permission for the
diagnostics below. (A one-shot `brief` summary command is coming; not available yet.)

## Querying

```bash
pgcrate sql -c "SELECT ..."             # read-only; blocks writes
echo "SELECT ..." | pgcrate sql         # reads from stdin if no -c
pgcrate sql -c "SELECT ..." --json      # structured rows for downstream use
pgcrate sql -c "UPDATE ..." --allow-write   # opt in to writes, explicitly
```

Default is read-only: a write statement errors with a prompt to re-run with
`--allow-write`. Only escalate when the human asked for a write. `query` is an
alias for `sql`.

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
- Never add `--read-write` (global) or `sql --allow-write` unless the task is
  explicitly a write. Default read-only is the guardrail; keep it on.
- Prefer `--json` whenever output is consumed by you for a next step; use human
  tables when reporting to the person.
- Treat exit `10+` as "I couldn't check" (fix connectivity/perms), distinct from
  `1`/`2` "the DB has a finding".

## Command reference

| Goal | Command |
|------|---------|
| Connection + server + privileges | `pgcrate context` |
| What I'm allowed to do | `pgcrate capabilities` |
| Describe a table | `pgcrate inspect table <name>` |
| Table dependents/dependencies | `pgcrate inspect table <name> --dependents` / `--dependencies` |
| Roles / grants / extensions | `pgcrate inspect roles` / `grants` / `extensions` |
| Compare two schemas | `pgcrate inspect diff --to <url>` |
| Run a read query | `pgcrate sql -c "SELECT ..."` (add `--json`) |
| Run a write | `pgcrate sql -c "..." --allow-write` |
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
