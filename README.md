# pgcrate

[![Crates.io](https://img.shields.io/crates/v/pgcrate)](https://crates.io/crates/pgcrate)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The PostgreSQL companion for teams not using Rails or Django.

Single binary. Pure SQL migrations. Plus seeds, models, introspection, diffing, health checks, anonymization, and snapshots.

**Perfect for:** Go, Rust, Node, Python backends without framework migrations. Microservices. Data teams.

## Installation

```bash
# From crates.io
cargo install pgcrate

# From source
cargo install --git https://github.com/jackschultz/pgcrate
```

## Quick Start

```bash
# Initialize project
pgcrate init

# Set database URL
export DATABASE_URL="postgres://localhost/myapp_dev"

# OPTION A: Create empty DB and run migrations
pgcrate db create
pgcrate migrate up

# OPTION B: Bootstrap from production (schema + anonymized data)
pgcrate bootstrap --from "postgres://prod-host/myapp_prod" --yes

# Check status
pgcrate migrate status
```

See [`examples/taskboard/`](examples/taskboard/) for a complete project example.

## Features

### Migrations

```bash
pgcrate migrate up                    # Run pending migrations
pgcrate migrate down --steps 1 --yes  # Roll back (dev/test only)
pgcrate migrate status                # Show migration status
pgcrate migrate new create_users      # Create new migration
pgcrate migrate baseline              # Mark existing migrations as applied (for adoption)
```

### Seeds

Load reference data from CSV or SQL files:

```bash
pgcrate seed list             # Show available seeds
pgcrate seed run              # Load all seeds
pgcrate seed run public.statuses  # Load specific seed
pgcrate seed validate         # Check seed files for errors
pgcrate seed diff             # Compare seeds to database
```

Seeds are CSV or SQL files under `seeds/<schema>/` (for example, `seeds/public/statuses.csv`). CSV files support type inference or explicit schemas via `.schema.toml` sidecar files (for example, `seeds/public/statuses.schema.toml`).

### Models

SQL transformations with dependency tracking:

```bash
pgcrate model run             # Run all models in DAG order
pgcrate model run -s tag:daily  # Run models with specific tag
pgcrate model run --init      # Create models/ if missing
pgcrate model compile         # Compile to target/compiled/
pgcrate model test            # Run data tests
pgcrate model docs            # Generate markdown documentation
pgcrate model graph           # Show dependency graph
pgcrate model lint deps       # Check dependency declarations
pgcrate model lint qualify    # Check for unqualified table references
pgcrate model check           # Run all lint checks
pgcrate model init            # Initialize models directory
pgcrate model new public.user_order_summary  # Scaffold a model file
```

Models support three materializations: `view`, `table`, and `incremental`. Define tests directly in SQL comments:

```sql
-- materialized: table
-- deps: staging.stg_users, staging.stg_orders
-- tests: not_null(user_id), unique(user_id)
SELECT user_id, COUNT(*) as order_count
FROM staging.stg_orders
GROUP BY user_id
```

Incremental models require a `unique_key` and use PostgreSQL 15+ MERGE for efficient upserts:

```sql
-- materialized: incremental
-- unique_key: user_id
-- deps: app.users, app.orders
SELECT user_id, COUNT(*) as order_count, MAX(created_at) as last_order
FROM app.orders
GROUP BY user_id
```

### Schema Inspection (`pgcrate inspect`)

```bash
pgcrate generate                      # Generate migration from existing DB
pgcrate inspect table users           # Deep table inspection (includes RLS policies)
pgcrate inspect diff --from db1 --to db2  # Compare two databases
pgcrate inspect roles                 # Show roles with attributes and memberships
pgcrate inspect roles --users         # Filter to login roles only
pgcrate inspect roles --describe myuser  # Detailed role info including owned objects
pgcrate inspect grants users          # Who can SELECT/INSERT/UPDATE/DELETE
pgcrate inspect grants --schema public  # All grants in a schema
pgcrate inspect grants --role myuser  # What can this role access?
pgcrate inspect extensions            # Installed extensions
pgcrate inspect extensions --available  # Extensions available to install
```

### DBA Diagnostics (`pgcrate dba`)

Agent-friendly health checks with JSON output for automation:

```bash
pgcrate dba                           # Quick health overview (alias for triage)
pgcrate dba triage                    # Quick health overview (locks, xid, sequences)
pgcrate dba triage --include-fixes --json # Include recommended fix actions
pgcrate context --json                # Connection context, server info, privileges
pgcrate capabilities --json           # What can this connection do?
pgcrate dba locks                     # Blocking locks and long transactions
pgcrate dba xid                       # Transaction ID wraparound analysis
pgcrate dba sequences                 # Sequence exhaustion check
pgcrate dba indexes                   # Missing, unused, duplicate indexes
pgcrate dba vacuum                    # Table bloat and vacuum health
pgcrate dba bloat                     # Estimate table and index bloat
pgcrate dba replication               # Streaming replication health
pgcrate dba queries                   # Top queries (requires pg_stat_statements)
pgcrate dba queries --by mean         # Sort by mean execution time
pgcrate dba connections               # Connection usage vs max_connections
pgcrate dba connections --by-user     # Group by user
pgcrate dba explain "SELECT ..."      # Query plan analysis with recommendations
pgcrate dba explain --include-actions # Include CREATE INDEX as fix actions
pgcrate dba storage                   # Disk usage (tables, indexes, TOAST)
pgcrate dba doctor                    # Health checks for CI
```

All diagnostic commands support timeout flags for production safety:
- `--connect-timeout <ms>` - Connection timeout (default: 5000ms)
- `--statement-timeout <ms>` - Query timeout (default: 30000ms)
- `--lock-timeout <ms>` - Lock wait timeout (default: 500ms)

### Fix Commands (`pgcrate dba fix`)

Safe remediation for issues found by diagnostics:

```bash
# Sequence fixes (prevent exhaustion)
pgcrate dba fix sequence public.order_seq --upgrade-to bigint --dry-run
pgcrate dba fix sequence public.order_seq --upgrade-to bigint --yes

# Index fixes (remove unused indexes)
pgcrate dba fix index --drop public.idx_unused --dry-run
pgcrate dba fix index --drop public.idx_unused --yes

# Vacuum fixes (reclaim table bloat)
pgcrate dba fix vacuum public.orders --dry-run
pgcrate dba fix vacuum public.orders --yes
pgcrate dba fix vacuum public.orders --full --yes    # ACCESS EXCLUSIVE lock
pgcrate dba fix vacuum public.orders --analyze --yes # Update statistics

# Bloat fixes (rebuild bloated indexes)
pgcrate dba fix bloat public.idx_orders_created --dry-run
pgcrate dba fix bloat public.idx_orders_created --yes  # REINDEX CONCURRENTLY (PG12+)
```

**Gate flags required for fix commands:**
- `--read-write` - Required for all fix operations
- `--primary` - Required for database-modifying operations
- `--yes` - Required for medium/high risk operations

**Risk levels:**
- **Low:** `ALTER SEQUENCE`, regular `VACUUM`
- **Medium:** `DROP INDEX CONCURRENTLY`, `REINDEX CONCURRENTLY` (requires `--yes`)
- **High:** `VACUUM FULL`, blocking `REINDEX` (requires `--yes`, takes exclusive lock)

Fix commands include evidence collection, safety checks, and optional verification:
```bash
pgcrate --read-write --primary dba fix sequence public.order_seq --upgrade-to bigint --yes --verify
```

### Data Operations

```bash
pgcrate bootstrap --from $URL         # Full environment setup (schema + anonymized data)
pgcrate anonymize setup               # Install anonymization helpers in DB
pgcrate anonymize dump -o safe.sql    # Export anonymized data based on TOML rules
pgcrate snapshot save <name> --profile <p>  # Selective snapshot via profile
pgcrate snapshot restore <name> --yes # Restore database state
pgcrate snapshot list                 # List all snapshots
pgcrate snapshot info <name>          # Show snapshot details
pgcrate snapshot delete <name> --yes  # Delete a snapshot
```

### CI/CD Integration

Commands support `--json` for machine-readable output with versioned schemas:

```bash
pgcrate migrate status --json         # JSON migration status
pgcrate dba triage --json             # JSON health check with severity
pgcrate context --json                # JSON connection/server info
pgcrate capabilities --json           # JSON capability discovery
pgcrate inspect diff --from $PROD --to $DEV --json  # JSON diff
pgcrate snapshot list --json          # JSON snapshot list
```

JSON output uses a consistent envelope:
```json
{
  "ok": true,
  "schema_id": "pgcrate.diagnostics.triage",
  "schema_version": "2.0.0",
  "tool_version": "0.4.0",
  "generated_at": "2026-01-19T12:00:00Z",
  "severity": "warning",
  "data": { ... }
}
```

Exit codes for diagnostics: `0` = healthy, `1` = warning, `2` = critical, `10+` = operational failure.

## Why pgcrate?

- **Single binary** - No runtime dependencies, works anywhere
- **PostgreSQL-native** - Uses `pg_get_*def()` for accurate SQL, not lowest-common-denominator
- **CI-ready** - JSON output, meaningful exit codes, health checks
- **Production safeguards** - `--yes` required, environment detection, dry-run support

## Configuration

### Database URL

```bash
# Environment variable (recommended)
DATABASE_URL="postgres://localhost/myapp_dev" pgcrate migrate up

# CLI argument
pgcrate migrate up -d "postgres://localhost/myapp_dev"

# Config file
pgcrate migrate up --config pgcrate.toml
```

### Config File (pgcrate.toml)

```toml
[paths]
migrations = "db/migrations"
models = "models"
seeds = "seeds"

[defaults]
with_down = true  # Include rollback stub in new migrations

[database]
url = "postgres://localhost/myapp_dev"  # Optional, env var preferred

[model]
sources = ["app.users", "app.orders"]  # Tables models can reference

[tools]
pg_dump = "/opt/homebrew/opt/postgresql@18/bin/pg_dump"  # Match Docker version
```

## Migration Format

Single files named `{timestamp}_{name}.sql`:

```sql
-- up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL
);

-- down
DROP TABLE users;
```

## Commands

| Command | Description |
|---------|-------------|
| `pgcrate init` | Initialize new project |
| `pgcrate migrate up` | Run pending migrations |
| `pgcrate migrate down` | Roll back migrations |
| `pgcrate migrate status` | Show migration status |
| `pgcrate migrate new <name>` | Create new migration |
| `pgcrate migrate baseline` | Mark migrations as applied without running |
| `pgcrate generate` | Generate migration from existing DB |
| `pgcrate describe <table>` | Show table details |
| `pgcrate diff` | Compare two databases |
| `pgcrate sql` | Run ad-hoc SQL (alias: `query`) |
| `pgcrate seed <cmd>` | List, run, validate, or diff seed data |
| `pgcrate model <cmd>` | Run, compile, test, lint, graph, new, or show models |
| `pgcrate triage` | Quick health check (locks, xid, sequences) |
| `pgcrate context` | Connection context, server info, privileges |
| `pgcrate capabilities` | What can this connection do? |
| `pgcrate locks` | Blocking locks and long transactions |
| `pgcrate xid` | Transaction ID wraparound analysis |
| `pgcrate sequences` | Sequence exhaustion check |
| `pgcrate indexes` | Missing, unused, duplicate indexes |
| `pgcrate vacuum` | Table bloat and vacuum health |
| `pgcrate bloat` | Estimate table and index bloat |
| `pgcrate replication` | Streaming replication health monitoring |
| `pgcrate queries` | Top queries from pg_stat_statements |
| `pgcrate connections` | Connection usage vs max_connections |
| `pgcrate fix sequence` | Upgrade sequence type to prevent exhaustion |
| `pgcrate fix index` | Drop unused/duplicate indexes |
| `pgcrate fix vacuum` | Run VACUUM on tables |
| `pgcrate doctor` | Run health checks |
| `pgcrate bootstrap` | Setup environment with anonymized data from source |
| `pgcrate snapshot <cmd>` | Save (with profiles), restore, list, or delete snapshots |
| `pgcrate anonymize <cmd>` | Setup helpers or dump anonymized data using TOML rules |
| `pgcrate db <cmd>` | Create or drop the database |
| `pgcrate reset` | Drop and recreate database |
| `pgcrate role <cmd>` | List roles or describe a specific role |
| `pgcrate grants` | Show grants on object, schema, or for a role |
| `pgcrate extension list` | Show installed or available extensions |
| `pgcrate help` | Show help (try `pgcrate --help-llm` for AI-friendly output) |

## Production Safety

- `--yes` required for destructive operations
- Production URL detection (RDS, Azure, Cloud SQL, etc.)
- `pgcrate.settings` table can hard-block operations
- `--dry-run` available on migrations, seeds, snapshots, and model run

## Alternatives

| Tool | Language | Notes |
|------|----------|-------|
| [dbmate](https://github.com/amacneil/dbmate) | Go | Similar philosophy, multi-DB support |
| [golang-migrate](https://github.com/golang-migrate/migrate) | Go | Library + CLI, many DB drivers |
| [Flyway](https://flywaydb.org/) | Java | Enterprise features, heavier |
| [sqitch](https://sqitch.org/) | Perl | Dependency-based, not timestamp-based |

pgcrate focuses on PostgreSQL-specific features (introspection, pg_dump snapshots, anonymization) that multi-database tools can't provide.

## License

MIT
