# Taskboard Example

A comprehensive example demonstrating all `pgcrate` features with a realistic task management domain.

## Features Demonstrated

- **Migrations**: Up/down migrations, schema changes, data backfills
- **Models**: Views, tables, and incremental materializations with tests
- **Seeds**: CSV data, SQL seeds, and schema sidecar files
- **Snapshots**: Multiple profiles for different use cases
- **Anonymization**: PII protection with various strategies
- **Doctor**: Health checks and validation

## Quick Start

```bash
# Navigate to example
cd examples/taskboard

# Set up environment
cp .env.example .env

# Create database and apply migrations
pgcrate db create
pgcrate migrate up

# Load seed data
pgcrate seed run

# Run models
pgcrate model run

# Run tests
pgcrate model test

# Health check
pgcrate doctor
```

## Domain Model

**app schema:**
- `users` - User accounts
- `teams` - Team groupings
- `team_members` - User-team associations with roles
- `projects` - Projects owned by teams
- `tasks` - Tasks with status tracking
- `audit_events` - Activity log with JSONB payload

**analytics schema:**
- `tasks_by_status` - Task counts per status
- `team_productivity` - Team completion rates
- `daily_activity` - Daily task metrics

**seeds schema:**
- `task_statuses` - Reference data for valid statuses
- `priorities` - Priority level definitions

---

## Workflow Examples

### 1. Setup & Migrations

```bash
# Create database
pgcrate db create

# Apply all migrations
pgcrate migrate up

# Check status
pgcrate migrate status

# Preview what would run
pgcrate migrate up --dry-run
```

### 2. Seeds

```bash
# List available seeds
pgcrate seed list

# Load all seeds
pgcrate seed run

# Load specific seed
pgcrate seed run public.task_statuses

# Preview without loading
pgcrate seed run --dry-run

# Validate seed files
pgcrate seed validate

# Compare seeds to database
pgcrate seed diff
```

### 3. Models

```bash
# Run all models in DAG order
pgcrate model run

# Compile to target/compiled/
pgcrate model compile

# Run specific model
pgcrate model run -s marts.task_metrics

# Run with tag selector
pgcrate model run -s tag:daily

# Run with dependency selector
pgcrate model run -s deps:marts.user_activity

# Force full refresh (for incremental models)
pgcrate model run --full-refresh

# Preview execution plan
pgcrate model run --dry-run
```

### 4. Model Testing

```bash
# Run all tests
pgcrate model test

# Run tests for specific model
pgcrate model test -s marts.user_activity

# Run tests with tag
pgcrate model test -s tag:metrics
```

### 5. Model Linting

```bash
# Check dependency declarations
pgcrate model lint deps

# Auto-fix dependency issues
pgcrate model lint deps --fix

# Check for unqualified references
pgcrate model lint qualify

# Run all lints
pgcrate model check
```

### 6. Visualization & Documentation

```bash
# Show dependency graph (ASCII)
pgcrate model graph

# Export as Mermaid diagram
pgcrate model graph --format mermaid

# Export as DOT for Graphviz
pgcrate model graph --format dot

# Generate markdown docs
pgcrate model docs

# Describe a table
pgcrate describe app.tasks

# Show what depends on a table
pgcrate describe app.users --dependents

# Show what a table depends on
pgcrate describe app.tasks --dependencies
```

### 7. Snapshots

```bash
# Save snapshot with default profile
pgcrate snapshot save dev-baseline

# Save with specific profile
pgcrate snapshot save schema-only --profile schema_only

# List snapshots
pgcrate snapshot list

# Show snapshot details
pgcrate snapshot info dev-baseline

# Restore snapshot (destructive!)
pgcrate snapshot restore dev-baseline --yes

# Restore to different database
pgcrate snapshot restore dev-baseline --to postgres://localhost/other_db --yes

# Preview restore
pgcrate snapshot restore dev-baseline --dry-run

# Delete snapshot
pgcrate snapshot delete dev-baseline --yes
```

### 8. Anonymization

```bash
# Install helper functions
pgcrate anonymize setup

# Preview anonymized output
pgcrate anonymize dump --dry-run

# Export to file
pgcrate anonymize dump -o safe_data.sql

# Override seed for testing
pgcrate anonymize dump --seed test-seed-123 -o test_data.sql
```

### 9. Reset & Bootstrap

```bash
# Roll back all migrations
pgcrate reset --yes

# Full reset (drop and recreate database)
pgcrate reset --full --yes

# Bootstrap from production (anonymized)
pgcrate bootstrap --from postgres://prod-host/app --yes

# Preview bootstrap
pgcrate bootstrap --from postgres://prod-host/app --dry-run
```

### 10. Health Checks & CI/CD

```bash
# Run all health checks
pgcrate doctor

# Strict mode (warnings = errors)
pgcrate doctor --strict

# JSON output for CI
pgcrate doctor --json

# Compare schemas
pgcrate diff --from postgres://localhost/dev --to postgres://localhost/staging

# Generate migration from existing DB
pgcrate generate --dry-run
```

---

## Project Structure

```
taskboard/
├── pgcrate.toml              # Main configuration
├── pgcrate.anonymize.toml    # Anonymization rules
├── pgcrate.snapshot.toml     # Snapshot profiles
├── .env.example              # Environment template
├── README.md
├── db/
│   └── migrations/           # SQL migrations
│       ├── 20250101000000_create_schemas.sql
│       ├── 20250101000500_create_seed_tables.sql
│       ├── 20250101001000_create_users_teams.sql
│       ├── 20250101002000_create_projects_tasks.sql
│       ├── 20250101003000_create_audit_events.sql
│       ├── 20250101004000_add_task_status.sql
│       ├── 20250101005000_backfill_demo_data.sql
│       └── 20250101006000_create_analytics_views.sql
├── models/
│   ├── staging/
│   │   ├── stg_users.sql     # User staging view
│   │   └── stg_tasks.sql     # Task staging view with tests
│   └── marts/
│       ├── task_metrics.sql  # Task aggregates (table)
│       ├── user_activity.sql # User metrics (incremental)
│       └── team_summary.sql  # Team rollup (table)
└── seeds/
    └── public/
        ├── task_statuses.csv         # Status reference data
        ├── task_statuses.schema.toml # Explicit types
        ├── priorities.csv            # Priority levels
        └── demo_users.sql            # SQL seed with logic
```

---

## Configuration Files

### pgcrate.toml

Main configuration with paths, defaults, and feature settings.

### pgcrate.anonymize.toml

Defines anonymization rules:
- `fake_email` / `fake_name` - Generate realistic fake data
- `redact` - Replace with redacted text
- `skip` - Exclude table entirely

### pgcrate.snapshot.toml

Defines snapshot profiles:
- `schema_only` - Schema without data (CI/CD validation)
- `app_tables` - App tables excluding audit logs (dev snapshots)
- `full` - Everything (production backups)

---

## Model Features

### Materializations

| Type | Behavior | Use Case |
|------|----------|----------|
| `view` | CREATE OR REPLACE VIEW | Light transformations |
| `table` | DROP + CREATE TABLE | Heavy aggregations |
| `incremental` | MERGE/INSERT | Append-only data |

### Test Types

```sql
-- tests: not_null(column_name)
-- tests: unique(col1, col2)
-- tests: accepted_values(status, ['todo', 'in_progress', 'done'])
-- tests: relationships(assignee_id, staging.stg_users.id)
```

### Tags & Selectors

```bash
# By exact name
pgcrate model run -s marts.task_metrics

# By tag
pgcrate model run -s tag:daily

# Model + upstream dependencies
pgcrate model run -s deps:marts.user_activity

# Model + downstream dependents
pgcrate model run -s downstream:staging.stg_tasks

# Full lineage (up + down)
pgcrate model run -s tree:staging.stg_tasks
```

---

## Notes

- Migration `20250101005000_backfill_demo_data` has no down file (irreversible)
- The `demo_users.sql` seed creates `public.demo_users` if missing
- Incremental models use MERGE semantics with `unique_key`
- Snapshot profiles are defined in `pgcrate.snapshot.toml`
