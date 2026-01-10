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

### Schema Introspection

```bash
pgcrate generate              # Generate migration from existing DB
pgcrate describe users        # Deep table inspection (includes RLS policies)
pgcrate diff --from db1 --to db2  # Compare two databases
```

### Roles & Permissions

```bash
pgcrate role list             # Show roles with attributes and memberships
pgcrate role list --users     # Filter to login roles only
pgcrate role describe <role>  # Detailed role info including owned objects
pgcrate grants <table>        # Who can SELECT/INSERT/UPDATE/DELETE
pgcrate grants --schema public  # All grants in a schema
pgcrate grants --role myuser  # What can this role access?
pgcrate extension list        # Installed extensions
pgcrate extension list --available  # Extensions available to install
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
pgcrate doctor                        # Health checks for CI
```

### CI/CD Integration

Query commands support `--json` for machine-readable output:

```bash
pgcrate migrate status --json     # JSON migration status
pgcrate doctor --json             # JSON health report
pgcrate diff --from $PROD --to $DEV --json  # JSON diff
pgcrate snapshot list --json      # JSON snapshot list
pgcrate snapshot info dev --json  # JSON snapshot details
```

Exit codes: `0` = success, `1` = action needed (e.g., pending migrations), `2` = error.

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
