# Simple App Example

A minimal example demonstrating pgcrate usage for database migrations and seeding.

## Structure

```
simple-app/
├── pgcrate.toml              # Configuration
├── db/
│   ├── migrations/           # SQL migrations
│   │   ├── 20240101000000_create_users.sql
│   │   └── 20240102000000_create_posts.sql
│   └── seeds/
│       └── public/           # Seed data (CSV files per table)
│           └── users.csv
└── README.md
```

## Setup

1. Create a PostgreSQL database:
   ```bash
   createdb simple_app_dev
   ```

2. Update `pgcrate.toml` with your database URL:
   ```toml
   [database]
   url = "postgres://localhost/simple_app_dev"
   ```

3. Run migrations:
   ```bash
   pgcrate migrate up
   ```

4. Load seed data:
   ```bash
   pgcrate seed run
   ```

## Commands

### Migrations

```bash
# Apply all pending migrations
pgcrate migrate up

# Check migration status
pgcrate migrate status

# Rollback last migration
pgcrate migrate down --steps 1 --yes

# Create a new migration
pgcrate migrate new add_comments
```

### Seeding

```bash
# Run all seeds
pgcrate seed run

# List available seeds
pgcrate seed list

# Validate seeds against schema
pgcrate seed validate
```

### Diagnostics

```bash
# Check database health
pgcrate doctor

# Run triage checks
pgcrate triage

# Check sequence exhaustion
pgcrate sequences

# Describe a table
pgcrate describe users
```

## Migration Format

Migrations use `-- up` and `-- down` markers:

```sql
-- Create users table

-- up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- down
DROP TABLE users;
```

## Seed Format

Seeds are CSV files in `db/seeds/<schema>/<table>.csv`:

```csv
email,name
alice@example.com,Alice
bob@example.com,Bob
```

The schema defaults to `public`. Column names in the CSV header must match table columns.
