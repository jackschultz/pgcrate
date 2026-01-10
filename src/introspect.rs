//! Database introspection module for pgcrate generate command.
//!
//! This module provides functionality to:
//! - Introspect a Postgres database schema
//! - Convert the schema model to SQL CREATE statements
//! - Support various output modes (single file, split by schema, split by table)

use crate::sql::quote_ident;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tokio_postgres::Client;

// =============================================================================
// Schema Model Types
// =============================================================================

/// Complete database schema representation
#[derive(Debug, Default)]
pub struct DatabaseSchema {
    pub extensions: Vec<Extension>,
    pub schemas: Vec<SchemaInfo>,
    pub enums: Vec<EnumType>,
    pub sequences: Vec<Sequence>,
    pub tables: Vec<Table>,
    pub views: Vec<View>,
    pub indexes: Vec<Index>,
    pub constraints: Vec<Constraint>,
    pub triggers: Vec<Trigger>,
    pub functions: Vec<Function>,
    pub materialized_views: Vec<MaterializedView>,
}

#[derive(Debug, Clone)]
pub struct Extension {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct EnumType {
    pub schema: String,
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Sequence {
    pub schema: String,
    pub name: String,
    pub data_type: String,
    pub start_value: i64,
    pub increment: i64,
    pub cycle: bool,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<PrimaryKey>,
    pub partition_info: Option<PartitionInfo>,
    pub is_partition: bool,
    pub parent_schema: Option<String>,
    pub parent_name: Option<String>,
    pub partition_bound: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub identity: Option<IdentityType>,
    pub is_serial: bool,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum IdentityType {
    Always,
    ByDefault,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub strategy: PartitionStrategy,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionStrategy {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone)]
pub struct View {
    pub schema: String,
    pub name: String,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    pub schema: String,
    pub table_name: String,
    pub name: String,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Constraint {
    pub schema: String,
    pub table_name: String,
    pub name: String,
    pub constraint_type: ConstraintType,
    pub definition: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum ConstraintType {
    /// Primary key constraint. Note: For generate/diff commands, PKs are typically
    /// handled via PrimaryKey struct on Table for inline column definitions.
    /// The describe command uses this variant to show PKs in the constraints list.
    PrimaryKey,
    Unique,
    ForeignKey,
    Check,
    Exclusion,
}

#[derive(Debug, Clone, Serialize)]
pub struct Trigger {
    pub schema: String,
    pub table_name: String,
    pub name: String,
    pub definition: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FunctionKind {
    Function,
    Procedure,
}

#[derive(Debug, Clone)]
pub struct Function {
    pub schema: String,
    pub identity: String, // name + arg types for uniqueness
    pub definition: String,
    pub kind: FunctionKind,
}

#[derive(Debug, Clone)]
pub struct MaterializedView {
    pub schema: String,
    pub name: String,
    pub definition: String,
    pub indexes: Vec<String>,
}

// =============================================================================
// Introspection Options
// =============================================================================

#[derive(Debug, Clone, Default)]
pub struct IntrospectOptions {
    pub include_schemas: Vec<String>,
    pub exclude_schemas: Vec<String>,
}

impl IntrospectOptions {
    /// Check if a schema should be included based on include/exclude lists
    pub fn should_include_schema(&self, schema: &str) -> bool {
        // Always exclude system schemas
        let system_schemas = [
            "pg_catalog",
            "information_schema",
            "pg_toast",
            "pg_temp_1",
            "pg_toast_temp_1",
        ];
        if system_schemas.contains(&schema) {
            return false;
        }

        // If include list is specified, schema must be in it
        if !self.include_schemas.is_empty() {
            return self.include_schemas.iter().any(|s| s == schema);
        }

        // Otherwise, check exclude list
        !self.exclude_schemas.iter().any(|s| s == schema)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum SplitMode {
    #[default]
    None,
    Schema,
    Table,
}

// =============================================================================
// Generated File Representation
// =============================================================================

#[derive(Debug, Clone)]
pub struct GeneratedFile {
    pub filename: String,
    pub content: String,
    pub stats: FileStats,
}

#[derive(Debug, Clone, Default)]
pub struct FileStats {
    pub schema_count: usize,
    pub enum_count: usize,
    pub table_count: usize,
    pub column_count: usize,
    pub index_count: usize,
    pub fk_count: usize,
    pub view_count: usize,
    pub function_count: usize,
    pub trigger_count: usize,
    pub matview_count: usize,
    pub extension_count: usize,
    pub sequence_count: usize,
}

// =============================================================================
// Introspection Implementation
// =============================================================================

/// Introspect database and return schema model
#[allow(clippy::field_reassign_with_default)] // Sequential async calls with schema_set dependency
pub async fn introspect(
    client: &Client,
    options: &IntrospectOptions,
) -> Result<DatabaseSchema, anyhow::Error> {
    let mut schema = DatabaseSchema::default();

    // Get extensions
    schema.extensions = get_extensions(client).await?;

    // Get schemas (filtered)
    schema.schemas = get_schemas(client, options).await?;
    let schema_set: HashSet<String> = schema.schemas.iter().map(|s| s.name.clone()).collect();

    // Get enums
    schema.enums = get_enums(client, &schema_set).await?;

    // Get standalone sequences (not identity/serial)
    schema.sequences = get_sequences(client, &schema_set).await?;

    // Get tables (including partition info)
    schema.tables = get_tables(client, &schema_set).await?;

    // Get views
    schema.views = get_views(client, &schema_set).await?;

    // Get indexes (excluding primary key indexes which are part of table definition)
    schema.indexes = get_indexes(client, &schema_set).await?;

    // Get constraints (foreign keys, check constraints, unique constraints)
    schema.constraints = get_constraints(client, &schema_set).await?;

    // Get triggers
    schema.triggers = get_triggers(client, &schema_set).await?;

    // Get functions
    schema.functions = get_functions(client, &schema_set).await?;

    // Get materialized views
    schema.materialized_views = get_materialized_views(client, &schema_set).await?;

    Ok(schema)
}

async fn get_extensions(client: &Client) -> Result<Vec<Extension>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| Extension {
            name: row.get("extname"),
        })
        .collect())
}

async fn get_schemas(
    client: &Client,
    options: &IntrospectOptions,
) -> Result<Vec<SchemaInfo>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT nspname FROM pg_namespace
             WHERE nspname NOT LIKE 'pg_%'
               AND nspname != 'information_schema'
               AND nspname != 'pgcrate'
             ORDER BY nspname",
            &[],
        )
        .await?;

    let all_schemas: Vec<String> = rows.iter().map(|row| row.get("nspname")).collect();

    let schemas: Vec<SchemaInfo> = rows
        .iter()
        .filter_map(|row| {
            let name: String = row.get("nspname");
            if options.should_include_schema(&name) {
                Some(SchemaInfo { name })
            } else {
                None
            }
        })
        .collect();

    // Validate that specified --schema filters matched something
    if !options.include_schemas.is_empty() {
        let found_schemas: HashSet<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        let missing: Vec<&String> = options
            .include_schemas
            .iter()
            .filter(|s| !found_schemas.contains(s.as_str()))
            .collect();

        if !missing.is_empty() {
            anyhow::bail!(
                "Schema(s) not found: {}. Available schemas: {}",
                missing
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                all_schemas.join(", ")
            );
        }
    }

    Ok(schemas)
}

async fn get_enums(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<EnumType>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT n.nspname AS schema, t.typname AS name,
                    array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
             FROM pg_type t
             JOIN pg_namespace n ON t.typnamespace = n.oid
             JOIN pg_enum e ON t.oid = e.enumtypid
             WHERE t.typtype = 'e'
             GROUP BY n.nspname, t.typname
             ORDER BY n.nspname, t.typname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let schema: String = row.get("schema");
            if schemas.contains(&schema) {
                Some(EnumType {
                    schema,
                    name: row.get("name"),
                    values: row.get("values"),
                })
            } else {
                None
            }
        })
        .collect())
}

async fn get_sequences(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<Sequence>, anyhow::Error> {
    // Only get standalone sequences (not owned by a column - those are identity/serial)
    let rows = client
        .query(
            "SELECT n.nspname AS schema,
                    c.relname AS name,
                    format_type(s.seqtypid, NULL) AS data_type,
                    s.seqstart AS start_value,
                    s.seqincrement AS increment,
                    s.seqcycle AS cycle
             FROM pg_sequence s
             JOIN pg_class c ON s.seqrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE NOT EXISTS (
                 SELECT 1 FROM pg_depend d
                 WHERE d.objid = c.oid
                   AND d.deptype = 'a'
                   AND d.classid = 'pg_class'::regclass
             )
             ORDER BY n.nspname, c.relname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let schema: String = row.get("schema");
            if schemas.contains(&schema) {
                Some(Sequence {
                    schema,
                    name: row.get("name"),
                    data_type: row.get("data_type"),
                    start_value: row.get("start_value"),
                    increment: row.get("increment"),
                    cycle: row.get("cycle"),
                })
            } else {
                None
            }
        })
        .collect())
}

async fn get_tables(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<Table>, anyhow::Error> {
    // Get tables with partition info
    let table_rows = client
        .query(
            "SELECT n.nspname AS schema,
                    c.relname AS name,
                    c.relkind AS kind,
                    CASE WHEN c.relispartition THEN
                        (SELECT pn.nspname
                         FROM pg_inherits i
                         JOIN pg_class pc ON i.inhparent = pc.oid
                         JOIN pg_namespace pn ON pc.relnamespace = pn.oid
                         WHERE i.inhrelid = c.oid)
                    END AS parent_schema,
                    CASE WHEN c.relispartition THEN
                        (SELECT pc.relname
                         FROM pg_inherits i
                         JOIN pg_class pc ON i.inhparent = pc.oid
                         WHERE i.inhrelid = c.oid)
                    END AS parent_name,
                    pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
                    CASE WHEN pt.partstrat IS NOT NULL THEN pt.partstrat END AS partition_strategy,
                    CASE WHEN pt.partattrs IS NOT NULL THEN
                        (SELECT array_agg(a.attname ORDER BY pos)
                         FROM unnest(pt.partattrs) WITH ORDINALITY AS cols(attnum, pos)
                         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = cols.attnum)
                    END AS partition_columns
             FROM pg_class c
             JOIN pg_namespace n ON c.relnamespace = n.oid
             LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
             WHERE c.relkind IN ('r', 'p')
               AND n.nspname NOT LIKE 'pg_%'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'pgcrate'
             ORDER BY n.nspname, c.relname",
            &[],
        )
        .await?;

    let mut tables = Vec::new();

    for row in table_rows {
        let schema: String = row.get("schema");
        if !schemas.contains(&schema) {
            continue;
        }

        let table_name: String = row.get("name");
        let kind: i8 = row.get("kind");
        let is_partitioned = kind == b'p' as i8;
        let parent_schema: Option<String> = row.get("parent_schema");
        let parent_name: Option<String> = row.get("parent_name");
        let partition_bound: Option<String> = row.get("partition_bound");
        let partition_strategy: Option<i8> = row.get("partition_strategy");
        let partition_columns: Option<Vec<String>> = row.get("partition_columns");

        // Get primary key for this table
        let primary_key = get_table_primary_key(client, &schema, &table_name).await?;
        let pk_columns: Vec<String> = primary_key
            .as_ref()
            .map(|pk| pk.columns.clone())
            .unwrap_or_default();

        // Get columns for this table (pass pk columns for single-column PK inline)
        let columns = get_table_columns(client, &schema, &table_name, &pk_columns).await?;

        let partition_info = if is_partitioned {
            Some(PartitionInfo {
                strategy: match partition_strategy {
                    Some(r) if r == b'r' as i8 => PartitionStrategy::Range,
                    Some(l) if l == b'l' as i8 => PartitionStrategy::List,
                    Some(h) if h == b'h' as i8 => PartitionStrategy::Hash,
                    _ => PartitionStrategy::Range,
                },
                columns: partition_columns.unwrap_or_default(),
            })
        } else {
            None
        };

        tables.push(Table {
            schema,
            name: table_name,
            columns,
            primary_key,
            partition_info,
            is_partition: parent_schema.is_some(),
            parent_schema,
            parent_name,
            partition_bound,
        });
    }

    Ok(tables)
}

async fn get_table_columns(
    client: &Client,
    schema: &str,
    table: &str,
    pk_columns: &[String],
) -> Result<Vec<Column>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT a.attname AS name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    NOT a.attnotnull AS nullable,
                    pg_get_expr(d.adbin, d.adrelid) AS default_expr,
                    a.attidentity AS identity,
                    CASE WHEN a.attidentity = '' AND d.adbin IS NOT NULL
                         AND pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%'
                         THEN true ELSE false END AS is_serial
             FROM pg_attribute a
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
             WHERE n.nspname = $1
               AND c.relname = $2
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get("name");
            let identity_char: i8 = row.get("identity");
            let identity = match identity_char as u8 as char {
                'a' => Some(IdentityType::Always),
                'd' => Some(IdentityType::ByDefault),
                _ => None,
            };
            let is_serial: bool = row.get("is_serial");
            let default_expr: Option<String> = row.get("default_expr");

            // For serial columns, don't include the default (it's implicit)
            let default = if is_serial || identity.is_some() {
                None
            } else {
                default_expr
            };

            // Check if this column is part of a single-column primary key
            let is_primary_key = pk_columns.len() == 1 && pk_columns.contains(&name);

            Column {
                name,
                data_type: row.get("data_type"),
                nullable: row.get("nullable"),
                default,
                identity,
                is_serial,
                is_primary_key,
            }
        })
        .collect())
}

async fn get_table_primary_key(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Option<PrimaryKey>, anyhow::Error> {
    let row = client
        .query_opt(
            "SELECT (SELECT array_agg(a.attname ORDER BY pos)
                     FROM unnest(con.conkey) WITH ORDINALITY AS cols(attnum, pos)
                     JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = cols.attnum
                    ) AS columns
             FROM pg_constraint con
             JOIN pg_class c ON con.conrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = $1
               AND c.relname = $2
               AND con.contype = 'p'",
            &[&schema, &table],
        )
        .await?;

    Ok(row.map(|r| PrimaryKey {
        columns: r.get("columns"),
    }))
}

async fn get_views(client: &Client, schemas: &HashSet<String>) -> Result<Vec<View>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT n.nspname AS schema,
                    c.relname AS name,
                    pg_get_viewdef(c.oid, true) AS definition
             FROM pg_class c
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE c.relkind = 'v'
               AND n.nspname NOT LIKE 'pg_%'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'pgcrate'
             ORDER BY n.nspname, c.relname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let schema: String = row.get("schema");
            if schemas.contains(&schema) {
                Some(View {
                    schema,
                    name: row.get("name"),
                    definition: row.get("definition"),
                })
            } else {
                None
            }
        })
        .collect())
}

async fn get_indexes(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<Index>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT n.nspname AS schema,
                    t.relname AS table_name,
                    i.relname AS index_name,
                    pg_get_indexdef(i.oid) AS definition
             FROM pg_index ix
             JOIN pg_class i ON ix.indexrelid = i.oid
             JOIN pg_class t ON ix.indrelid = t.oid
             JOIN pg_namespace n ON t.relnamespace = n.oid
             LEFT JOIN pg_constraint con ON con.conindid = i.oid
             WHERE n.nspname NOT LIKE 'pg_%'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'pgcrate'
               AND NOT ix.indisprimary
               AND (con.contype IS NULL OR con.contype != 'u')
             ORDER BY n.nspname, t.relname, i.relname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let schema: String = row.get("schema");
            if schemas.contains(&schema) {
                Some(Index {
                    schema,
                    table_name: row.get("table_name"),
                    name: row.get("index_name"),
                    definition: row.get("definition"),
                })
            } else {
                None
            }
        })
        .collect())
}

async fn get_constraints(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<Constraint>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT n.nspname AS schema,
                    c.relname AS table_name,
                    con.conname AS name,
                    con.contype AS constraint_type,
                    pg_get_constraintdef(con.oid, true) AS definition
             FROM pg_constraint con
             JOIN pg_class c ON con.conrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE con.contype IN ('f', 'c', 'u', 'x')
               AND n.nspname NOT LIKE 'pg_%'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'pgcrate'
             ORDER BY n.nspname, c.relname, con.conname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let schema: String = row.get("schema");
            if !schemas.contains(&schema) {
                return None;
            }

            let contype: i8 = row.get("constraint_type");
            // Note: 'p' (primary key) is excluded from query - PKs handled via get_table_primary_key
            let constraint_type = match contype as u8 as char {
                'u' => ConstraintType::Unique,
                'f' => ConstraintType::ForeignKey,
                'c' => ConstraintType::Check,
                'x' => ConstraintType::Exclusion,
                _ => return None,
            };

            Some(Constraint {
                schema,
                table_name: row.get("table_name"),
                name: row.get("name"),
                constraint_type,
                definition: row.get("definition"),
            })
        })
        .collect())
}

async fn get_triggers(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<Trigger>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT n.nspname AS schema,
                    c.relname AS table_name,
                    t.tgname AS name,
                    pg_get_triggerdef(t.oid, true) AS definition
             FROM pg_trigger t
             JOIN pg_class c ON t.tgrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE NOT t.tgisinternal
               AND n.nspname NOT LIKE 'pg_%'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'pgcrate'
             ORDER BY n.nspname, c.relname, t.tgname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let schema: String = row.get("schema");
            if schemas.contains(&schema) {
                Some(Trigger {
                    schema,
                    table_name: row.get("table_name"),
                    name: row.get("name"),
                    definition: row.get("definition"),
                })
            } else {
                None
            }
        })
        .collect())
}

async fn get_functions(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<Function>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT n.nspname AS schema,
                    p.oid::regprocedure::text AS identity,
                    pg_get_functiondef(p.oid) AS definition,
                    p.prokind
             FROM pg_proc p
             JOIN pg_namespace n ON p.pronamespace = n.oid
             WHERE n.nspname NOT LIKE 'pg_%'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'pgcrate'
               AND p.prokind IN ('f', 'p')
               AND NOT EXISTS (
                   SELECT 1 FROM pg_depend d
                   WHERE d.objid = p.oid
                     AND d.deptype = 'e'
               )
             ORDER BY n.nspname, p.proname",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let schema: String = row.get("schema");
            if schemas.contains(&schema) {
                let prokind: i8 = row.get("prokind");
                let kind = match prokind as u8 as char {
                    'p' => FunctionKind::Procedure,
                    _ => FunctionKind::Function, // 'f' and any other value default to function
                };
                Some(Function {
                    schema,
                    identity: row.get("identity"),
                    definition: row.get("definition"),
                    kind,
                })
            } else {
                None
            }
        })
        .collect())
}

async fn get_materialized_views(
    client: &Client,
    schemas: &HashSet<String>,
) -> Result<Vec<MaterializedView>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT n.nspname AS schema,
                    c.relname AS name,
                    pg_get_viewdef(c.oid, true) AS definition
             FROM pg_class c
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE c.relkind = 'm'
               AND n.nspname NOT LIKE 'pg_%'
               AND n.nspname != 'information_schema'
               AND n.nspname != 'pgcrate'
             ORDER BY n.nspname, c.relname",
            &[],
        )
        .await?;

    let mut matviews = Vec::new();

    for row in rows {
        let schema: String = row.get("schema");
        if !schemas.contains(&schema) {
            continue;
        }

        let name: String = row.get("name");

        // Get indexes for this materialized view
        let index_rows = client
            .query(
                "SELECT pg_get_indexdef(i.oid) AS definition
                 FROM pg_index ix
                 JOIN pg_class i ON ix.indexrelid = i.oid
                 JOIN pg_class t ON ix.indrelid = t.oid
                 JOIN pg_namespace n ON t.relnamespace = n.oid
                 WHERE n.nspname = $1 AND t.relname = $2
                 ORDER BY i.relname",
                &[&schema, &name],
            )
            .await?;

        let indexes: Vec<String> = index_rows.iter().map(|r| r.get("definition")).collect();

        matviews.push(MaterializedView {
            schema,
            name,
            definition: row.get("definition"),
            indexes,
        });
    }

    Ok(matviews)
}

// =============================================================================
// SQL Generation
// =============================================================================

/// Generate migration file(s) from schema
pub fn generate_files(
    schema: &DatabaseSchema,
    split_mode: SplitMode,
    base_time: DateTime<Utc>,
    database_url: &str,
) -> Vec<GeneratedFile> {
    match split_mode {
        SplitMode::None => vec![generate_single_file(schema, base_time, database_url)],
        SplitMode::Schema => generate_by_schema(schema, base_time, database_url),
        SplitMode::Table => generate_by_table(schema, base_time, database_url),
    }
}

fn generate_single_file(
    schema: &DatabaseSchema,
    base_time: DateTime<Utc>,
    database_url: &str,
) -> GeneratedFile {
    let timestamp = base_time.format("%Y%m%d%H%M%S");
    let filename = format!("{}_initial_schema.sql", timestamp);

    let (up_sql, stats) = schema_to_sql(schema);
    let down_sql = schema_to_drop_sql(schema);

    let content = format_migration_file(database_url, &base_time, &up_sql, &down_sql);

    GeneratedFile {
        filename,
        content,
        stats,
    }
}

fn generate_by_schema(
    schema: &DatabaseSchema,
    base_time: DateTime<Utc>,
    database_url: &str,
) -> Vec<GeneratedFile> {
    let mut files = Vec::new();

    // Collect all schemas represented
    let schema_names: Vec<String> = schema.schemas.iter().map(|s| s.name.clone()).collect();

    // First file: extensions (if any)
    if !schema.extensions.is_empty() {
        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_extensions.sql", timestamp);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();

        up_parts.push("-- Extensions".to_string());
        for ext in &schema.extensions {
            up_parts.push(format!("CREATE EXTENSION IF NOT EXISTS \"{}\";", ext.name));
        }

        down_parts.push("-- Extensions".to_string());
        for ext in schema.extensions.iter().rev() {
            down_parts.push(format!("DROP EXTENSION IF EXISTS \"{}\";", ext.name));
        }

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats: FileStats {
                extension_count: schema.extensions.len(),
                ..Default::default()
            },
        });
    }

    // Generate a file per schema
    for schema_name in &schema_names {
        let filtered = filter_schema_by_name(schema, schema_name);

        if is_schema_empty(&filtered) {
            continue;
        }

        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_schema_{}.sql", timestamp, schema_name);

        let (up_sql, stats) = schema_to_sql(&filtered);
        let down_sql = schema_to_drop_sql(&filtered);

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_sql,
            &down_sql,
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats,
        });
    }

    files
}

fn generate_by_table(
    schema: &DatabaseSchema,
    base_time: DateTime<Utc>,
    database_url: &str,
) -> Vec<GeneratedFile> {
    let mut files = Vec::new();

    // First file: extensions + schemas + enums + standalone sequences
    if !schema.extensions.is_empty()
        || !schema.schemas.is_empty()
        || !schema.enums.is_empty()
        || !schema.sequences.is_empty()
    {
        let timestamp = base_time.format("%Y%m%d%H%M%S");
        let filename = format!("{}_types_and_schemas.sql", timestamp);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();
        let mut stats = FileStats::default();

        // Extensions
        if !schema.extensions.is_empty() {
            up_parts.push("-- Extensions".to_string());
            for ext in &schema.extensions {
                up_parts.push(format!("CREATE EXTENSION IF NOT EXISTS \"{}\";", ext.name));
            }
            up_parts.push(String::new());
            stats.extension_count = schema.extensions.len();
        }

        // Schemas
        if !schema.schemas.is_empty() {
            up_parts.push("-- Schemas".to_string());
            for s in &schema.schemas {
                up_parts.push(format!(
                    "CREATE SCHEMA IF NOT EXISTS {};",
                    quote_ident(&s.name)
                ));
            }
            up_parts.push(String::new());
            stats.schema_count = schema.schemas.len();
        }

        // Enums
        if !schema.enums.is_empty() {
            up_parts.push("-- Types (enums)".to_string());
            for e in &schema.enums {
                let values: Vec<String> = e
                    .values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect();
                up_parts.push(format!(
                    "CREATE TYPE {}.{} AS ENUM ({});",
                    quote_ident(&e.schema),
                    quote_ident(&e.name),
                    values.join(", ")
                ));
            }
            up_parts.push(String::new());
            stats.enum_count = schema.enums.len();
        }

        // Sequences
        if !schema.sequences.is_empty() {
            up_parts.push("-- Sequences".to_string());
            for seq in &schema.sequences {
                up_parts.push(format_sequence_create(seq));
            }
            up_parts.push(String::new());
            stats.sequence_count = schema.sequences.len();
        }

        // Down in reverse
        for seq in schema.sequences.iter().rev() {
            down_parts.push(format!(
                "DROP SEQUENCE IF EXISTS {}.{};",
                quote_ident(&seq.schema),
                quote_ident(&seq.name)
            ));
        }
        for e in schema.enums.iter().rev() {
            down_parts.push(format!(
                "DROP TYPE IF EXISTS {}.{};",
                quote_ident(&e.schema),
                quote_ident(&e.name)
            ));
        }
        for s in schema.schemas.iter().rev() {
            down_parts.push(format!("DROP SCHEMA IF EXISTS {};", quote_ident(&s.name)));
        }
        for ext in schema.extensions.iter().rev() {
            down_parts.push(format!("DROP EXTENSION IF EXISTS \"{}\";", ext.name));
        }

        let content = format_migration_file(
            database_url,
            &base_time,
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats,
        });
    }

    // One file per table
    // Group partitions immediately after their parent table
    let mut regular_tables: Vec<&Table> =
        schema.tables.iter().filter(|t| !t.is_partition).collect();
    let partition_tables: Vec<&Table> = schema.tables.iter().filter(|t| t.is_partition).collect();

    // Sort regular tables: partitioned parents first, then alphabetically
    regular_tables.sort_by(|a, b| {
        let a_is_parent = a.partition_info.is_some();
        let b_is_parent = b.partition_info.is_some();
        b_is_parent
            .cmp(&a_is_parent)
            .then_with(|| a.schema.cmp(&b.schema).then_with(|| a.name.cmp(&b.name)))
    });

    // Group partitions by their parent table
    let mut partitions_by_parent: HashMap<String, Vec<&Table>> = HashMap::new();
    for partition in &partition_tables {
        if let (Some(ref ps), Some(ref pn)) = (&partition.parent_schema, &partition.parent_name) {
            let parent_key = format!("{}.{}", ps, pn);
            partitions_by_parent
                .entry(parent_key)
                .or_default()
                .push(partition);
        }
    }

    // Build ordered table list: each parent followed by its partitions
    let mut ordered_tables: Vec<&Table> = Vec::new();
    for table in &regular_tables {
        ordered_tables.push(table);
        // Add partitions immediately after their parent
        let parent_key = format!("{}.{}", table.schema, table.name);
        if let Some(partitions) = partitions_by_parent.get(&parent_key) {
            for partition in partitions {
                ordered_tables.push(partition);
            }
        }
    }

    for table in ordered_tables {
        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_{}_{}.sql", timestamp, table.schema, table.name);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();
        let mut stats = FileStats::default();

        // Table creation
        up_parts.push(format_table_create(table));
        stats.table_count = 1;
        stats.column_count = table.columns.len();

        // Indexes for this table
        let table_indexes: Vec<&Index> = schema
            .indexes
            .iter()
            .filter(|i| i.schema == table.schema && i.table_name == table.name)
            .collect();

        if !table_indexes.is_empty() {
            up_parts.push(String::new());
            up_parts.push("-- Indexes".to_string());
            for idx in &table_indexes {
                up_parts.push(format!("{};", idx.definition));
            }
            stats.index_count = table_indexes.len();
        }

        // Check, unique, and exclusion constraints for this table
        let table_constraints: Vec<&Constraint> = schema
            .constraints
            .iter()
            .filter(|c| {
                c.schema == table.schema
                    && c.table_name == table.name
                    && matches!(
                        c.constraint_type,
                        ConstraintType::Check | ConstraintType::Unique | ConstraintType::Exclusion
                    )
            })
            .collect();

        if !table_constraints.is_empty() {
            up_parts.push(String::new());
            up_parts.push("-- Constraints".to_string());
            for con in &table_constraints {
                up_parts.push(format!(
                    "ALTER TABLE {}.{} ADD CONSTRAINT {} {};",
                    quote_ident(&con.schema),
                    quote_ident(&con.table_name),
                    quote_ident(&con.name),
                    con.definition
                ));
            }
        }

        // Down
        for idx in table_indexes.iter().rev() {
            down_parts.push(format!(
                "DROP INDEX IF EXISTS {}.{};",
                quote_ident(&idx.schema),
                quote_ident(&idx.name)
            ));
        }
        down_parts.push(format!(
            "DROP TABLE IF EXISTS {}.{};",
            quote_ident(&table.schema),
            quote_ident(&table.name)
        ));

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats,
        });
    }

    // Views file
    if !schema.views.is_empty() {
        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_views.sql", timestamp);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();

        up_parts.push("-- Views".to_string());
        for view in &schema.views {
            up_parts.push(format!(
                "CREATE VIEW {}.{} AS\n{};",
                quote_ident(&view.schema),
                quote_ident(&view.name),
                view.definition.trim_end_matches(';').trim()
            ));
            up_parts.push(String::new());
        }

        down_parts.push("-- Views".to_string());
        for view in schema.views.iter().rev() {
            down_parts.push(format!(
                "DROP VIEW IF EXISTS {}.{};",
                quote_ident(&view.schema),
                quote_ident(&view.name)
            ));
        }

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats: FileStats {
                view_count: schema.views.len(),
                ..Default::default()
            },
        });
    }

    // Foreign keys file (all FKs in one file at the end)
    let fks: Vec<&Constraint> = schema
        .constraints
        .iter()
        .filter(|c| c.constraint_type == ConstraintType::ForeignKey)
        .collect();

    if !fks.is_empty() {
        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_foreign_keys.sql", timestamp);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();

        up_parts.push("-- Foreign Keys".to_string());
        for fk in &fks {
            up_parts.push(format!(
                "ALTER TABLE {}.{}\n    ADD CONSTRAINT {} {};",
                quote_ident(&fk.schema),
                quote_ident(&fk.table_name),
                quote_ident(&fk.name),
                fk.definition
            ));
        }

        down_parts.push("-- Foreign Keys".to_string());
        for fk in fks.iter().rev() {
            down_parts.push(format!(
                "ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {};",
                quote_ident(&fk.schema),
                quote_ident(&fk.table_name),
                quote_ident(&fk.name)
            ));
        }

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats: FileStats {
                fk_count: fks.len(),
                ..Default::default()
            },
        });
    }

    // Triggers file
    if !schema.triggers.is_empty() {
        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_triggers.sql", timestamp);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();

        up_parts.push("-- Triggers".to_string());
        for trigger in &schema.triggers {
            up_parts.push(format!("{};", trigger.definition));
        }

        down_parts.push("-- Triggers".to_string());
        for trigger in schema.triggers.iter().rev() {
            down_parts.push(format!(
                "DROP TRIGGER IF EXISTS {} ON {}.{};",
                quote_ident(&trigger.name),
                quote_ident(&trigger.schema),
                quote_ident(&trigger.table_name)
            ));
        }

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats: FileStats {
                trigger_count: schema.triggers.len(),
                ..Default::default()
            },
        });
    }

    // Functions file
    if !schema.functions.is_empty() {
        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_functions.sql", timestamp);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();

        up_parts.push("-- Functions".to_string());
        for func in &schema.functions {
            up_parts.push(func.definition.clone());
            up_parts.push(String::new());
        }

        down_parts.push("-- Functions".to_string());
        for func in schema.functions.iter().rev() {
            let keyword = match func.kind {
                FunctionKind::Function => "FUNCTION",
                FunctionKind::Procedure => "PROCEDURE",
            };
            down_parts.push(format!("DROP {} IF EXISTS {};", keyword, func.identity));
        }

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats: FileStats {
                function_count: schema.functions.len(),
                ..Default::default()
            },
        });
    }

    // Materialized views file
    if !schema.materialized_views.is_empty() {
        let timestamp = (base_time + Duration::seconds(files.len() as i64)).format("%Y%m%d%H%M%S");
        let filename = format!("{}_materialized_views.sql", timestamp);

        let mut up_parts = Vec::new();
        let mut down_parts = Vec::new();

        up_parts.push("-- Materialized Views".to_string());
        for mv in &schema.materialized_views {
            up_parts.push(format!(
                "CREATE MATERIALIZED VIEW {}.{} AS\n{};",
                quote_ident(&mv.schema),
                quote_ident(&mv.name),
                mv.definition.trim_end_matches(';').trim()
            ));
            for idx in &mv.indexes {
                up_parts.push(format!("{};", idx));
            }
            up_parts.push(String::new());
        }

        down_parts.push("-- Materialized Views".to_string());
        for mv in schema.materialized_views.iter().rev() {
            down_parts.push(format!(
                "DROP MATERIALIZED VIEW IF EXISTS {}.{};",
                quote_ident(&mv.schema),
                quote_ident(&mv.name)
            ));
        }

        let content = format_migration_file(
            database_url,
            &(base_time + Duration::seconds(files.len() as i64)),
            &up_parts.join("\n"),
            &down_parts.join("\n"),
        );

        files.push(GeneratedFile {
            filename,
            content,
            stats: FileStats {
                matview_count: schema.materialized_views.len(),
                ..Default::default()
            },
        });
    }

    files
}

/// Convert schema model to SQL CREATE statements
pub fn schema_to_sql(schema: &DatabaseSchema) -> (String, FileStats) {
    let mut parts = Vec::new();
    let mut stats = FileStats::default();

    // Extensions
    if !schema.extensions.is_empty() {
        parts.push("-- Extensions".to_string());
        for ext in &schema.extensions {
            parts.push(format!("CREATE EXTENSION IF NOT EXISTS \"{}\";", ext.name));
        }
        parts.push(String::new());
        stats.extension_count = schema.extensions.len();
    }

    // Schemas
    if !schema.schemas.is_empty() {
        parts.push("-- Schemas".to_string());
        for s in &schema.schemas {
            parts.push(format!(
                "CREATE SCHEMA IF NOT EXISTS {};",
                quote_ident(&s.name)
            ));
        }
        parts.push(String::new());
        stats.schema_count = schema.schemas.len();
    }

    // Enums
    if !schema.enums.is_empty() {
        parts.push("-- Types (enums)".to_string());
        for e in &schema.enums {
            let values: Vec<String> = e
                .values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect();
            parts.push(format!(
                "CREATE TYPE {}.{} AS ENUM ({});",
                quote_ident(&e.schema),
                quote_ident(&e.name),
                values.join(", ")
            ));
        }
        parts.push(String::new());
        stats.enum_count = schema.enums.len();
    }

    // Sequences
    if !schema.sequences.is_empty() {
        parts.push("-- Sequences".to_string());
        for seq in &schema.sequences {
            parts.push(format_sequence_create(seq));
        }
        parts.push(String::new());
        stats.sequence_count = schema.sequences.len();
    }

    // Tables (non-partitions first)
    let regular_tables: Vec<&Table> = schema.tables.iter().filter(|t| !t.is_partition).collect();
    let partition_tables: Vec<&Table> = schema.tables.iter().filter(|t| t.is_partition).collect();

    if !regular_tables.is_empty() {
        parts.push("-- Tables".to_string());
        for table in &regular_tables {
            parts.push(format_table_create(table));
            parts.push(String::new());
            stats.table_count += 1;
            stats.column_count += table.columns.len();
        }
    }

    // Partition tables
    if !partition_tables.is_empty() {
        parts.push("-- Partitions".to_string());
        for table in &partition_tables {
            parts.push(format_table_create(table));
            parts.push(String::new());
            stats.table_count += 1;
            stats.column_count += table.columns.len();
        }
    }

    // Views
    if !schema.views.is_empty() {
        parts.push("-- Views".to_string());
        for view in &schema.views {
            parts.push(format!(
                "CREATE VIEW {}.{} AS\n{};",
                quote_ident(&view.schema),
                quote_ident(&view.name),
                view.definition.trim_end_matches(';').trim()
            ));
            parts.push(String::new());
        }
        stats.view_count = schema.views.len();
    }

    // Indexes
    if !schema.indexes.is_empty() {
        parts.push("-- Indexes".to_string());
        for idx in &schema.indexes {
            parts.push(format!("{};", idx.definition));
        }
        parts.push(String::new());
        stats.index_count = schema.indexes.len();
    }

    // Check and Unique constraints
    let non_fk_constraints: Vec<&Constraint> = schema
        .constraints
        .iter()
        .filter(|c| c.constraint_type != ConstraintType::ForeignKey)
        .collect();

    if !non_fk_constraints.is_empty() {
        parts.push("-- Constraints".to_string());
        for con in &non_fk_constraints {
            parts.push(format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT {} {};",
                quote_ident(&con.schema),
                quote_ident(&con.table_name),
                quote_ident(&con.name),
                con.definition
            ));
        }
        parts.push(String::new());
    }

    // Foreign Keys
    let fks: Vec<&Constraint> = schema
        .constraints
        .iter()
        .filter(|c| c.constraint_type == ConstraintType::ForeignKey)
        .collect();

    if !fks.is_empty() {
        parts.push("-- Foreign Keys".to_string());
        for fk in &fks {
            parts.push(format!(
                "ALTER TABLE {}.{}\n    ADD CONSTRAINT {} {};",
                quote_ident(&fk.schema),
                quote_ident(&fk.table_name),
                quote_ident(&fk.name),
                fk.definition
            ));
        }
        parts.push(String::new());
        stats.fk_count = fks.len();
    }

    // Triggers
    if !schema.triggers.is_empty() {
        parts.push("-- Triggers".to_string());
        for trigger in &schema.triggers {
            parts.push(format!("{};", trigger.definition));
        }
        parts.push(String::new());
        stats.trigger_count = schema.triggers.len();
    }

    // Functions
    if !schema.functions.is_empty() {
        parts.push("-- Functions".to_string());
        for func in &schema.functions {
            parts.push(func.definition.clone());
            parts.push(String::new());
        }
        stats.function_count = schema.functions.len();
    }

    // Materialized Views
    if !schema.materialized_views.is_empty() {
        parts.push("-- Materialized Views".to_string());
        for mv in &schema.materialized_views {
            parts.push(format!(
                "CREATE MATERIALIZED VIEW {}.{} AS\n{};",
                quote_ident(&mv.schema),
                quote_ident(&mv.name),
                mv.definition.trim_end_matches(';').trim()
            ));
            for idx in &mv.indexes {
                parts.push(format!("{};", idx));
            }
            parts.push(String::new());
        }
        stats.matview_count = schema.materialized_views.len();
    }

    (parts.join("\n"), stats)
}

/// Convert schema model to SQL DROP statements (reverse order)
pub fn schema_to_drop_sql(schema: &DatabaseSchema) -> String {
    let mut parts = Vec::new();

    // Drop in reverse order of creation

    // Materialized Views
    if !schema.materialized_views.is_empty() {
        parts.push("-- Materialized Views".to_string());
        for mv in schema.materialized_views.iter().rev() {
            parts.push(format!(
                "DROP MATERIALIZED VIEW IF EXISTS {}.{};",
                quote_ident(&mv.schema),
                quote_ident(&mv.name)
            ));
        }
        parts.push(String::new());
    }

    // Functions
    if !schema.functions.is_empty() {
        parts.push("-- Functions".to_string());
        for func in schema.functions.iter().rev() {
            let keyword = match func.kind {
                FunctionKind::Function => "FUNCTION",
                FunctionKind::Procedure => "PROCEDURE",
            };
            parts.push(format!("DROP {} IF EXISTS {};", keyword, func.identity));
        }
        parts.push(String::new());
    }

    // Triggers
    if !schema.triggers.is_empty() {
        parts.push("-- Triggers".to_string());
        for trigger in schema.triggers.iter().rev() {
            parts.push(format!(
                "DROP TRIGGER IF EXISTS {} ON {}.{};",
                quote_ident(&trigger.name),
                quote_ident(&trigger.schema),
                quote_ident(&trigger.table_name)
            ));
        }
        parts.push(String::new());
    }

    // Foreign Keys
    let fks: Vec<&Constraint> = schema
        .constraints
        .iter()
        .filter(|c| c.constraint_type == ConstraintType::ForeignKey)
        .collect();

    if !fks.is_empty() {
        parts.push("-- Foreign Keys".to_string());
        for fk in fks.iter().rev() {
            parts.push(format!(
                "ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {};",
                quote_ident(&fk.schema),
                quote_ident(&fk.table_name),
                quote_ident(&fk.name)
            ));
        }
        parts.push(String::new());
    }

    // Indexes
    if !schema.indexes.is_empty() {
        parts.push("-- Indexes".to_string());
        for idx in schema.indexes.iter().rev() {
            parts.push(format!(
                "DROP INDEX IF EXISTS {}.{};",
                quote_ident(&idx.schema),
                quote_ident(&idx.name)
            ));
        }
        parts.push(String::new());
    }

    // Views
    if !schema.views.is_empty() {
        parts.push("-- Views".to_string());
        for view in schema.views.iter().rev() {
            parts.push(format!(
                "DROP VIEW IF EXISTS {}.{};",
                quote_ident(&view.schema),
                quote_ident(&view.name)
            ));
        }
        parts.push(String::new());
    }

    // Tables (partitions first, then regular tables)
    // Partitioned parent tables need CASCADE to drop their partitions
    let partition_tables: Vec<&Table> = schema.tables.iter().filter(|t| t.is_partition).collect();
    let regular_tables: Vec<&Table> = schema.tables.iter().filter(|t| !t.is_partition).collect();

    if !partition_tables.is_empty() || !regular_tables.is_empty() {
        parts.push("-- Tables".to_string());
        for table in partition_tables.iter().rev() {
            parts.push(format!(
                "DROP TABLE IF EXISTS {}.{};",
                quote_ident(&table.schema),
                quote_ident(&table.name)
            ));
        }
        for table in regular_tables.iter().rev() {
            // Partitioned parent tables need CASCADE to ensure partitions are dropped
            let cascade = if table.partition_info.is_some() {
                " CASCADE"
            } else {
                ""
            };
            parts.push(format!(
                "DROP TABLE IF EXISTS {}.{}{};",
                quote_ident(&table.schema),
                quote_ident(&table.name),
                cascade
            ));
        }
        parts.push(String::new());
    }

    // Sequences
    if !schema.sequences.is_empty() {
        parts.push("-- Sequences".to_string());
        for seq in schema.sequences.iter().rev() {
            parts.push(format!(
                "DROP SEQUENCE IF EXISTS {}.{};",
                quote_ident(&seq.schema),
                quote_ident(&seq.name)
            ));
        }
        parts.push(String::new());
    }

    // Enums
    if !schema.enums.is_empty() {
        parts.push("-- Types".to_string());
        for e in schema.enums.iter().rev() {
            parts.push(format!(
                "DROP TYPE IF EXISTS {}.{};",
                quote_ident(&e.schema),
                quote_ident(&e.name)
            ));
        }
        parts.push(String::new());
    }

    // Schemas
    if !schema.schemas.is_empty() {
        parts.push("-- Schemas".to_string());
        for s in schema.schemas.iter().rev() {
            parts.push(format!("DROP SCHEMA IF EXISTS {};", quote_ident(&s.name)));
        }
        parts.push(String::new());
    }

    // Extensions
    if !schema.extensions.is_empty() {
        parts.push("-- Extensions".to_string());
        for ext in schema.extensions.iter().rev() {
            parts.push(format!("DROP EXTENSION IF EXISTS \"{}\";", ext.name));
        }
    }

    parts.join("\n")
}

// =============================================================================
// Helper Functions
// =============================================================================

fn format_migration_file(
    database_url: &str,
    timestamp: &DateTime<Utc>,
    up_sql: &str,
    down_sql: &str,
) -> String {
    // Mask password in URL for display
    let masked_url = mask_database_url(database_url);

    format!(
        "-- Generated by pgcrate generate\n\
         -- Source: {}\n\
         -- Generated at: {}\n\
         \n\
         -- up\n\
         \n\
         {}\n\
         \n\
         -- down\n\
         \n\
         {}\n",
        masked_url,
        timestamp.format("%Y-%m-%dT%H:%M:%SZ"),
        up_sql.trim(),
        down_sql.trim()
    )
}

fn mask_database_url(url: &str) -> String {
    // Simple password masking: postgres://user:pass@host -> postgres://user:***@host
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            // Check if this colon is after "://" (i.e., it's the password separator)
            if let Some(scheme_end) = url.find("://") {
                if colon_pos > scheme_end + 3 {
                    return format!("{}***{}", &url[..colon_pos + 1], &url[at_pos..]);
                }
            }
        }
    }
    url.to_string()
}

fn format_table_create(table: &Table) -> String {
    let mut parts = Vec::new();

    if table.is_partition {
        // Partition table
        let parent_schema = table
            .parent_schema
            .as_deref()
            .expect("partition table without parent_schema");
        let parent_name = table
            .parent_name
            .as_deref()
            .expect("partition table without parent_name");
        parts.push(format!(
            "CREATE TABLE {}.{} PARTITION OF {}.{}",
            quote_ident(&table.schema),
            quote_ident(&table.name),
            quote_ident(parent_schema),
            quote_ident(parent_name)
        ));
        if let Some(ref bound) = table.partition_bound {
            parts.push(format!("    {};", bound));
        }
    } else {
        // Regular or partitioned parent table
        parts.push(format!(
            "CREATE TABLE {}.{} (",
            quote_ident(&table.schema),
            quote_ident(&table.name)
        ));

        let mut col_defs = Vec::new();
        for col in &table.columns {
            col_defs.push(format_column_def(col));
        }

        // Add composite primary key as table constraint (if more than 1 column)
        if let Some(ref pk) = table.primary_key {
            if pk.columns.len() > 1 {
                col_defs.push(format!(
                    "PRIMARY KEY ({})",
                    pk.columns
                        .iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }

        parts.push(format!("    {}", col_defs.join(",\n    ")));

        // Add partition clause if partitioned
        if let Some(ref part_info) = table.partition_info {
            let strategy = match part_info.strategy {
                PartitionStrategy::Range => "RANGE",
                PartitionStrategy::List => "LIST",
                PartitionStrategy::Hash => "HASH",
            };
            parts.push(format!(
                ") PARTITION BY {} ({});",
                strategy,
                part_info
                    .columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        } else {
            parts.push(");".to_string());
        }
    }

    parts.join("\n")
}

fn format_column_def(col: &Column) -> String {
    let mut parts = Vec::new();

    parts.push(quote_ident(&col.name));

    // Handle SERIAL/BIGSERIAL
    if col.is_serial {
        let serial_type = if col.data_type.to_lowercase().contains("big") {
            "BIGSERIAL"
        } else if col.data_type.to_lowercase().contains("small") {
            "SMALLSERIAL"
        } else {
            "SERIAL"
        };
        parts.push(serial_type.to_string());
    } else {
        parts.push(col.data_type.clone());

        // Identity column
        if let Some(ref identity) = col.identity {
            match identity {
                IdentityType::Always => parts.push("GENERATED ALWAYS AS IDENTITY".to_string()),
                IdentityType::ByDefault => {
                    parts.push("GENERATED BY DEFAULT AS IDENTITY".to_string())
                }
            }
        }
    }

    // PRIMARY KEY (for single-column primary keys only)
    if col.is_primary_key {
        parts.push("PRIMARY KEY".to_string());
    }

    // NOT NULL (skip if primary key or identity/serial - they're implicitly NOT NULL)
    if !col.nullable && col.identity.is_none() && !col.is_serial && !col.is_primary_key {
        parts.push("NOT NULL".to_string());
    }

    // Default
    if let Some(ref default) = col.default {
        parts.push(format!("DEFAULT {}", default));
    }

    parts.join(" ")
}

fn format_sequence_create(seq: &Sequence) -> String {
    let mut parts = vec![format!(
        "CREATE SEQUENCE {}.{}",
        quote_ident(&seq.schema),
        quote_ident(&seq.name)
    )];

    if seq.data_type != "bigint" {
        parts.push(format!("    AS {}", seq.data_type));
    }

    if seq.start_value != 1 {
        parts.push(format!("    START WITH {}", seq.start_value));
    }

    if seq.increment != 1 {
        parts.push(format!("    INCREMENT BY {}", seq.increment));
    }

    if seq.cycle {
        parts.push("    CYCLE".to_string());
    }

    format!("{};", parts.join("\n"))
}

fn filter_schema_by_name(schema: &DatabaseSchema, name: &str) -> DatabaseSchema {
    DatabaseSchema {
        extensions: Vec::new(), // Extensions are global, handled separately
        schemas: schema
            .schemas
            .iter()
            .filter(|s| s.name == name)
            .cloned()
            .collect(),
        enums: schema
            .enums
            .iter()
            .filter(|e| e.schema == name)
            .cloned()
            .collect(),
        sequences: schema
            .sequences
            .iter()
            .filter(|s| s.schema == name)
            .cloned()
            .collect(),
        tables: schema
            .tables
            .iter()
            .filter(|t| t.schema == name)
            .cloned()
            .collect(),
        views: schema
            .views
            .iter()
            .filter(|v| v.schema == name)
            .cloned()
            .collect(),
        indexes: schema
            .indexes
            .iter()
            .filter(|i| i.schema == name)
            .cloned()
            .collect(),
        constraints: schema
            .constraints
            .iter()
            .filter(|c| c.schema == name)
            .cloned()
            .collect(),
        triggers: schema
            .triggers
            .iter()
            .filter(|t| t.schema == name)
            .cloned()
            .collect(),
        functions: schema
            .functions
            .iter()
            .filter(|f| f.schema == name)
            .cloned()
            .collect(),
        materialized_views: schema
            .materialized_views
            .iter()
            .filter(|m| m.schema == name)
            .cloned()
            .collect(),
    }
}

fn is_schema_empty(schema: &DatabaseSchema) -> bool {
    schema.tables.is_empty()
        && schema.views.is_empty()
        && schema.enums.is_empty()
        && schema.functions.is_empty()
        && schema.triggers.is_empty()
        && schema.sequences.is_empty()
        && schema.materialized_views.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    // quote_ident tests are now in sql.rs module

    #[test]
    fn test_mask_database_url() {
        assert_eq!(
            mask_database_url("postgres://user:secret@localhost/db"),
            "postgres://user:***@localhost/db"
        );
        assert_eq!(
            mask_database_url("postgres://localhost/db"),
            "postgres://localhost/db"
        );
    }

    #[test]
    fn test_introspect_options_should_include_schema() {
        let opts = IntrospectOptions::default();
        assert!(opts.should_include_schema("app"));
        assert!(opts.should_include_schema("public"));
        assert!(!opts.should_include_schema("pg_catalog"));
        assert!(!opts.should_include_schema("information_schema"));

        let opts_with_include = IntrospectOptions {
            include_schemas: vec!["app".to_string()],
            exclude_schemas: vec![],
        };
        assert!(opts_with_include.should_include_schema("app"));
        assert!(!opts_with_include.should_include_schema("other"));

        let opts_with_exclude = IntrospectOptions {
            include_schemas: vec![],
            exclude_schemas: vec!["legacy".to_string()],
        };
        assert!(opts_with_exclude.should_include_schema("app"));
        assert!(!opts_with_exclude.should_include_schema("legacy"));
    }

    #[test]
    fn test_format_column_def_serial() {
        let col = Column {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            nullable: false,
            default: None,
            identity: None,
            is_serial: true,
            is_primary_key: false,
        };
        assert_eq!(format_column_def(&col), "\"id\" SERIAL");
    }

    #[test]
    fn test_format_column_def_serial_primary_key() {
        let col = Column {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            nullable: false,
            default: None,
            identity: None,
            is_serial: true,
            is_primary_key: true,
        };
        assert_eq!(format_column_def(&col), "\"id\" SERIAL PRIMARY KEY");
    }

    #[test]
    fn test_format_column_def_identity() {
        let col = Column {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            nullable: false,
            default: None,
            identity: Some(IdentityType::Always),
            is_serial: false,
            is_primary_key: false,
        };
        assert_eq!(
            format_column_def(&col),
            "\"id\" integer GENERATED ALWAYS AS IDENTITY"
        );
    }

    #[test]
    fn test_format_column_def_with_default() {
        let col = Column {
            name: "created_at".to_string(),
            data_type: "timestamp with time zone".to_string(),
            nullable: false,
            default: Some("now()".to_string()),
            identity: None,
            is_serial: false,
            is_primary_key: false,
        };
        assert_eq!(
            format_column_def(&col),
            "\"created_at\" timestamp with time zone NOT NULL DEFAULT now()"
        );
    }
}
