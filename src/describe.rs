use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::time::SystemTime;
use tokio_postgres::Client;

use crate::introspect::{Constraint, ConstraintType, IdentityType, Index, Trigger};
use crate::sql::quote_ident;

// ============================================================================
// Column Info for Describe Output
// ============================================================================

/// Extended column info for describe output (adds FK reference display)
#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub identity: Option<IdentityType>,
    pub is_serial: bool,
    pub default: Option<String>,
    pub fk_reference: Option<String>, // e.g., "app.teams(id)"
}

/// Result of resolving an object name to a schema-qualified table
#[derive(Debug, Serialize)]
pub struct ResolvedTable {
    pub schema: String,
    pub name: String,
    #[allow(dead_code)] // Retained for potential future use (e.g., oid-based queries)
    pub oid: i64,
}

/// Aggregated view of a single table for describe output
#[derive(Debug, Serialize)]
pub struct TableDescribe {
    #[allow(dead_code)] // Stored for completeness; callers use resolved.schema
    pub schema: String,
    #[allow(dead_code)] // Stored for completeness; callers use resolved.name
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<Index>,
    pub constraints: Vec<Constraint>,
    pub triggers: Vec<Trigger>,
    pub stats: Option<TableStats>,
    pub details: Option<TableDetails>, // Populated with --verbose
    pub rls: Option<RlsInfo>,          // Row-level security info
}

/// Table statistics from pg_stat_user_tables and size functions
///
/// Note: For partitioned tables, `pg_table_size`/`pg_indexes_size`/`pg_total_relation_size`
/// return the size of the parent table only (typically 0 since data lives in partitions).
/// The `is_partitioned` flag indicates when sizes should be interpreted as parent-only.
#[derive(Debug, Serialize)]
pub struct TableStats {
    pub row_estimate: i64,
    pub table_size: String,
    pub index_size: String,
    pub total_size: String,
    pub last_vacuum: Option<String>,
    pub last_analyze: Option<String>,
    // Verbose-only fields
    pub last_autovacuum: Option<String>,
    pub last_autoanalyze: Option<String>,
    // Set if stats were unavailable (e.g., permission denied)
    pub unavailable_reason: Option<String>,
    // True if this is a partitioned table (sizes are parent-only, not aggregate)
    pub is_partitioned: bool,
}

/// Additional table details shown with --verbose
#[derive(Debug, Serialize)]
pub struct TableDetails {
    pub owner: String,
    pub table_kind: String,  // "ordinary table" or "partitioned table"
    pub persistence: String, // "permanent", "temporary", "unlogged"
}

/// Foreign key reference (for dependents/dependencies)
#[derive(Debug, Serialize)]
pub struct ForeignKeyRef {
    #[allow(dead_code)] // Retained for potential verbose output or debugging
    pub constraint_name: String,
    pub from_schema: String,
    pub from_table: String,
    pub from_columns: Vec<String>,
    pub to_schema: String,
    pub to_table: String,
    pub to_columns: Vec<String>,
}

/// View reference
#[derive(Debug, Serialize)]
pub struct ViewRef {
    pub schema: String,
    pub name: String,
    pub is_materialized: bool,
}

/// Trigger reference (for dependents)
#[derive(Debug, Serialize)]
pub struct TriggerRef {
    pub schema: String,
    pub table_name: String,
    pub trigger_name: String,
}

/// Trigger function reference (for dependencies)
#[derive(Debug, Serialize)]
pub struct TriggerFunctionRef {
    pub function_schema: String,
    pub function_name: String,
    pub trigger_name: String,
}

/// Type reference (for dependencies)
#[derive(Debug, Serialize)]
pub struct TypeRef {
    pub schema: String,
    pub name: String,
    pub kind: String, // "enum", "domain", "composite"
}

/// Row-Level Security info for a table
#[derive(Debug, Serialize)]
pub struct RlsInfo {
    pub enabled: bool,
    pub forced: bool,
    pub policies: Vec<RlsPolicy>,
}

/// A single RLS policy
#[derive(Debug, Serialize)]
pub struct RlsPolicy {
    pub name: String,
    pub command: String,    // "ALL", "SELECT", "INSERT", "UPDATE", "DELETE"
    pub permissive: bool,   // true = PERMISSIVE, false = RESTRICTIVE
    pub roles: Vec<String>, // empty = applies to all roles
    pub using_expr: Option<String>,
    pub with_check_expr: Option<String>,
}

/// Objects that depend on a table
#[derive(Debug, Serialize)]
pub struct Dependents {
    pub foreign_keys: Vec<ForeignKeyRef>,
    pub views: Vec<ViewRef>,
    pub triggers: Vec<TriggerRef>,
}

/// Objects that a table depends on
#[derive(Debug, Serialize)]
pub struct Dependencies {
    pub foreign_keys: Vec<ForeignKeyRef>,
    pub trigger_functions: Vec<TriggerFunctionRef>,
    pub types: Vec<TypeRef>,
}

/// Parse object name into schema and table components
/// Returns (schema, table) where schema is None if unqualified
pub fn parse_object_name(object: &str) -> (Option<&str>, &str) {
    if let Some(dot_pos) = object.find('.') {
        let schema = &object[..dot_pos];
        let table = &object[dot_pos + 1..];
        (Some(schema), table)
    } else {
        (None, object)
    }
}

// ============================================================================
// Data Fetching Functions
// ============================================================================

/// Get detailed info about a table
pub async fn describe_table(
    client: &Client,
    schema: &str,
    name: &str,
    include_stats: bool,
    verbose: bool,
) -> Result<TableDescribe> {
    // Get primary key columns first (needed for column display)
    let pk_columns = get_primary_key_columns(client, schema, name).await?;

    // Get columns with FK info
    let columns = get_columns_with_fk(client, schema, name, &pk_columns).await?;

    // Get indexes
    let indexes = get_table_indexes(client, schema, name).await?;

    // Get constraints (including primary key for display)
    let constraints = get_table_constraints(client, schema, name).await?;

    // Get triggers
    let triggers = get_table_triggers(client, schema, name).await?;

    // Get stats if requested
    let stats = if include_stats {
        get_table_stats(client, schema, name).await?
    } else {
        None
    };

    // Get details if verbose
    let details = if verbose {
        get_table_details(client, schema, name).await?
    } else {
        None
    };

    // Get RLS info
    let rls = get_rls_info(client, schema, name).await?;

    Ok(TableDescribe {
        schema: schema.to_string(),
        name: name.to_string(),
        columns,
        indexes,
        constraints,
        triggers,
        stats,
        details,
        rls,
    })
}

/// Get primary key column names for a table
async fn get_primary_key_columns(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let row = client
        .query_opt(
            r#"
            SELECT (
                SELECT array_agg(a.attname ORDER BY pos)
                FROM unnest(con.conkey) WITH ORDINALITY AS cols(attnum, pos)
                JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = cols.attnum
            ) AS columns
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND con.contype = 'p'
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(row
        .and_then(|r| r.get::<_, Option<Vec<String>>>("columns"))
        .unwrap_or_default())
}

/// Get columns with FK reference info
async fn get_columns_with_fk(
    client: &Client,
    schema: &str,
    table: &str,
    pk_columns: &[String],
) -> Result<Vec<ColumnInfo>> {
    // First get basic column info
    let rows = client
        .query(
            r#"
            SELECT a.attname AS name,
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
            ORDER BY a.attnum
            "#,
            &[&schema, &table],
        )
        .await?;

    // Get FK references for this table's columns
    let fk_refs = get_column_fk_references(client, schema, table).await?;

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

            // Look up FK reference for this column
            let fk_reference = fk_refs.get(&name).cloned();

            ColumnInfo {
                name,
                data_type: row.get("data_type"),
                nullable: row.get("nullable"),
                is_primary_key,
                identity,
                is_serial,
                default,
                fk_reference,
            }
        })
        .collect())
}

/// Get FK references for columns (column_name -> "schema.table(column)")
///
/// Note: This query only returns single-column FKs because the JOIN on ANY(conkey/confkey)
/// would produce a cross-product for composite FKs. For column-level display, we only
/// show single-column FK references. Composite FKs are shown in the Constraints section.
async fn get_column_fk_references(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<HashMap<String, String>> {
    let rows = client
        .query(
            r#"
            SELECT
                a.attname AS fk_column,
                ref_ns.nspname AS ref_schema,
                ref_cl.relname AS ref_table,
                ref_a.attname AS ref_column
            FROM pg_constraint con
            JOIN pg_class fk_cl ON con.conrelid = fk_cl.oid
            JOIN pg_namespace fk_ns ON fk_cl.relnamespace = fk_ns.oid
            JOIN pg_class ref_cl ON con.confrelid = ref_cl.oid
            JOIN pg_namespace ref_ns ON ref_cl.relnamespace = ref_ns.oid
            JOIN pg_attribute a ON a.attrelid = fk_cl.oid AND a.attnum = ANY(con.conkey)
            JOIN pg_attribute ref_a ON ref_a.attrelid = ref_cl.oid AND ref_a.attnum = ANY(con.confkey)
            WHERE fk_ns.nspname = $1
              AND fk_cl.relname = $2
              AND con.contype = 'f'
              AND array_length(con.conkey, 1) = 1  -- Single-column FKs only (see function doc)
            "#,
            &[&schema, &table],
        )
        .await?;

    let mut refs = HashMap::new();
    for row in rows {
        let fk_column: String = row.get("fk_column");
        let ref_schema: String = row.get("ref_schema");
        let ref_table: String = row.get("ref_table");
        let ref_column: String = row.get("ref_column");
        refs.insert(
            fk_column,
            format!("{}.{}({})", ref_schema, ref_table, ref_column),
        );
    }
    Ok(refs)
}

/// Get indexes for a table
async fn get_table_indexes(client: &Client, schema: &str, table: &str) -> Result<Vec<Index>> {
    let rows = client
        .query(
            r#"
            SELECT
                i.relname AS index_name,
                pg_get_indexdef(i.oid) AS definition
            FROM pg_index ix
            JOIN pg_class i ON ix.indexrelid = i.oid
            JOIN pg_class t ON ix.indrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = $1
              AND t.relname = $2
            ORDER BY ix.indisprimary DESC, i.relname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| Index {
            schema: schema.to_string(),
            table_name: table.to_string(),
            name: row.get("index_name"),
            definition: row.get("definition"),
        })
        .collect())
}

/// Get constraints for a table (including primary key)
async fn get_table_constraints(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<Constraint>> {
    let rows = client
        .query(
            r#"
            SELECT
                con.conname AS name,
                con.contype AS constraint_type,
                pg_get_constraintdef(con.oid, true) AS definition
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND con.contype IN ('p', 'f', 'c', 'u', 'x')
            ORDER BY
                CASE con.contype
                    WHEN 'p' THEN 0  -- primary key first
                    WHEN 'u' THEN 1  -- unique
                    WHEN 'f' THEN 2  -- foreign key
                    WHEN 'c' THEN 3  -- check
                    WHEN 'x' THEN 4  -- exclusion
                END,
                con.conname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .filter_map(|row| {
            let contype: i8 = row.get("constraint_type");
            let constraint_type = match contype as u8 as char {
                'p' => Some(ConstraintType::PrimaryKey),
                'u' => Some(ConstraintType::Unique),
                'f' => Some(ConstraintType::ForeignKey),
                'c' => Some(ConstraintType::Check),
                'x' => Some(ConstraintType::Exclusion),
                _ => None,
            }?;

            Some(Constraint {
                schema: schema.to_string(),
                table_name: table.to_string(),
                name: row.get("name"),
                constraint_type,
                definition: row.get("definition"),
            })
        })
        .collect())
}

/// Get triggers for a table
async fn get_table_triggers(client: &Client, schema: &str, table: &str) -> Result<Vec<Trigger>> {
    let rows = client
        .query(
            r#"
            SELECT
                t.tgname AS name,
                pg_get_triggerdef(t.oid, true) AS definition
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND NOT t.tgisinternal
            ORDER BY t.tgname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| Trigger {
            schema: schema.to_string(),
            table_name: table.to_string(),
            name: row.get("name"),
            definition: row.get("definition"),
        })
        .collect())
}

/// Get table statistics
///
/// Returns stats if available, or a placeholder with `unavailable_reason` if
/// the query fails (e.g., due to insufficient privileges).
///
/// For partitioned tables, size functions return the parent table size only
/// (typically 0 since data lives in partitions). The `is_partitioned` flag
/// is set to indicate this.
async fn get_table_stats(client: &Client, schema: &str, table: &str) -> Result<Option<TableStats>> {
    let result = client
        .query_opt(
            r#"
            SELECT
                c.relkind,
                COALESCE(s.n_live_tup, 0)::bigint AS row_estimate,
                pg_size_pretty(pg_table_size(c.oid)) AS table_size,
                pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                s.last_vacuum,
                s.last_autovacuum,
                s.last_analyze,
                s.last_autoanalyze
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_stat_user_tables s
                ON s.schemaname = n.nspname AND s.relname = c.relname
            WHERE n.nspname = $1
              AND c.relname = $2
              AND c.relkind IN ('r', 'p')
            "#,
            &[&schema, &table],
        )
        .await;

    match result {
        Ok(Some(r)) => {
            let format_timestamp = |ts: Option<SystemTime>| -> Option<String> {
                ts.map(|t| {
                    let dt: DateTime<Utc> = t.into();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                })
            };

            let relkind: i8 = r.get("relkind");
            let is_partitioned = relkind == b'p' as i8;

            Ok(Some(TableStats {
                row_estimate: r.get("row_estimate"),
                table_size: r.get("table_size"),
                index_size: r.get("index_size"),
                total_size: r.get("total_size"),
                last_vacuum: format_timestamp(r.get("last_vacuum")),
                last_autovacuum: format_timestamp(r.get("last_autovacuum")),
                last_analyze: format_timestamp(r.get("last_analyze")),
                last_autoanalyze: format_timestamp(r.get("last_autoanalyze")),
                unavailable_reason: None,
                is_partitioned,
            }))
        }
        Ok(None) => Ok(None),
        Err(e) => {
            // Stats unavailable (likely permission denied) - return placeholder
            Ok(Some(TableStats {
                row_estimate: 0,
                table_size: "unavailable".to_string(),
                index_size: "unavailable".to_string(),
                total_size: "unavailable".to_string(),
                last_vacuum: None,
                last_autovacuum: None,
                last_analyze: None,
                last_autoanalyze: None,
                unavailable_reason: Some(format!(
                    "Could not retrieve stats: {} (use --no-stats to suppress)",
                    e
                )),
                is_partitioned: false,
            }))
        }
    }
}

/// Get table details (for --verbose)
async fn get_table_details(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Option<TableDetails>> {
    let row = client
        .query_opt(
            r#"
            SELECT
                pg_get_userbyid(c.relowner) AS owner,
                c.relkind,
                c.relpersistence
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1
              AND c.relname = $2
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(row.map(|r| {
        let relkind: i8 = r.get("relkind");
        let relpersistence: i8 = r.get("relpersistence");

        let table_kind = match relkind as u8 as char {
            'r' => "ordinary table",
            'p' => "partitioned table",
            _ => "table",
        }
        .to_string();

        let persistence = match relpersistence as u8 as char {
            'p' => "permanent",
            'u' => "unlogged",
            't' => "temporary",
            _ => "permanent",
        }
        .to_string();

        TableDetails {
            owner: r.get("owner"),
            table_kind,
            persistence,
        }
    }))
}

/// Get row-level security info for a table
async fn get_rls_info(client: &Client, schema: &str, table: &str) -> Result<Option<RlsInfo>> {
    // Check if RLS is enabled
    let rls_row = client
        .query_opt(
            r#"
            SELECT
                c.relrowsecurity AS enabled,
                c.relforcerowsecurity AS forced
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1
              AND c.relname = $2
            "#,
            &[&schema, &table],
        )
        .await?;

    let Some(rls_row) = rls_row else {
        return Ok(None);
    };

    let enabled: bool = rls_row.get("enabled");
    let forced: bool = rls_row.get("forced");

    // If RLS is not enabled, return early with empty policies
    if !enabled {
        return Ok(Some(RlsInfo {
            enabled: false,
            forced: false,
            policies: vec![],
        }));
    }

    // Get policies
    let policy_rows = client
        .query(
            r#"
            SELECT
                pol.polname AS name,
                pol.polcmd AS command,
                pol.polpermissive AS permissive,
                COALESCE(
                    ARRAY(
                        SELECT rolname
                        FROM pg_roles
                        WHERE oid = ANY(pol.polroles)
                    ),
                    ARRAY[]::text[]
                ) AS roles,
                pg_get_expr(pol.polqual, pol.polrelid) AS using_expr,
                pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check_expr
            FROM pg_policy pol
            JOIN pg_class c ON pol.polrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1
              AND c.relname = $2
            ORDER BY pol.polname
            "#,
            &[&schema, &table],
        )
        .await?;

    let policies: Vec<RlsPolicy> = policy_rows
        .iter()
        .map(|r| {
            let cmd: i8 = r.get("command");
            let command = match cmd as u8 as char {
                'r' => "SELECT",
                'a' => "INSERT",
                'w' => "UPDATE",
                'd' => "DELETE",
                '*' => "ALL",
                _ => "UNKNOWN",
            }
            .to_string();

            RlsPolicy {
                name: r.get("name"),
                command,
                permissive: r.get("permissive"),
                roles: r.get("roles"),
                using_expr: r.get("using_expr"),
                with_check_expr: r.get("with_check_expr"),
            }
        })
        .collect();

    Ok(Some(RlsInfo {
        enabled,
        forced,
        policies,
    }))
}

// ============================================================================
// Dependents / Dependencies
// ============================================================================

/// Get direct dependents of a table (objects that reference this table)
pub async fn get_dependents(client: &Client, schema: &str, table: &str) -> Result<Dependents> {
    // Get inbound foreign keys (tables referencing this table)
    let foreign_keys = get_inbound_foreign_keys(client, schema, table).await?;

    // Get views/materialized views depending on this table
    let views = get_dependent_views(client, schema, table).await?;

    // Get triggers defined on this table
    let triggers = get_table_trigger_refs(client, schema, table).await?;

    Ok(Dependents {
        foreign_keys,
        views,
        triggers,
    })
}

/// Get foreign keys that reference this table (inbound FKs)
async fn get_inbound_foreign_keys(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ForeignKeyRef>> {
    let rows = client
        .query(
            r#"
            SELECT
                con.conname AS constraint_name,
                fk_ns.nspname AS fk_schema,
                fk_cl.relname AS fk_table,
                ref_ns.nspname AS ref_schema,
                ref_cl.relname AS ref_table,
                (
                    SELECT array_agg(a.attname ORDER BY pos)
                    FROM unnest(con.conkey) WITH ORDINALITY AS cols(attnum, pos)
                    JOIN pg_attribute a ON a.attrelid = fk_cl.oid AND a.attnum = cols.attnum
                ) AS fk_columns,
                (
                    SELECT array_agg(a.attname ORDER BY pos)
                    FROM unnest(con.confkey) WITH ORDINALITY AS cols(attnum, pos)
                    JOIN pg_attribute a ON a.attrelid = ref_cl.oid AND a.attnum = cols.attnum
                ) AS ref_columns
            FROM pg_constraint con
            JOIN pg_class fk_cl ON con.conrelid = fk_cl.oid
            JOIN pg_namespace fk_ns ON fk_cl.relnamespace = fk_ns.oid
            JOIN pg_class ref_cl ON con.confrelid = ref_cl.oid
            JOIN pg_namespace ref_ns ON ref_cl.relnamespace = ref_ns.oid
            WHERE ref_ns.nspname = $1
              AND ref_cl.relname = $2
              AND con.contype = 'f'
            ORDER BY fk_ns.nspname, fk_cl.relname, con.conname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| ForeignKeyRef {
            constraint_name: r.get("constraint_name"),
            from_schema: r.get("fk_schema"),
            from_table: r.get("fk_table"),
            from_columns: r.get("fk_columns"),
            to_schema: r.get("ref_schema"),
            to_table: r.get("ref_table"),
            to_columns: r.get("ref_columns"),
        })
        .collect())
}

/// Get views and materialized views that depend on this table
/// Views depend on tables through pg_rewrite rules, so we join through pg_rewrite
async fn get_dependent_views(client: &Client, schema: &str, table: &str) -> Result<Vec<ViewRef>> {
    let rows = client
        .query(
            r#"
            SELECT DISTINCT
                v_ns.nspname AS view_schema,
                v_cl.relname AS view_name,
                v_cl.relkind AS view_kind
            FROM pg_depend d
            JOIN pg_rewrite r ON d.objid = r.oid
            JOIN pg_class v_cl ON r.ev_class = v_cl.oid
            JOIN pg_namespace v_ns ON v_cl.relnamespace = v_ns.oid
            JOIN pg_class ref_cl ON d.refobjid = ref_cl.oid
            JOIN pg_namespace ref_ns ON ref_cl.relnamespace = ref_ns.oid
            WHERE ref_ns.nspname = $1
              AND ref_cl.relname = $2
              AND v_cl.relkind IN ('v', 'm')
              AND d.classid = 'pg_rewrite'::regclass
            ORDER BY v_ns.nspname, v_cl.relname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let relkind: i8 = r.get("view_kind");
            ViewRef {
                schema: r.get("view_schema"),
                name: r.get("view_name"),
                is_materialized: relkind as u8 as char == 'm',
            }
        })
        .collect())
}

/// Get trigger references for a table (for dependents view)
async fn get_table_trigger_refs(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<TriggerRef>> {
    let rows = client
        .query(
            r#"
            SELECT
                n.nspname AS schema,
                c.relname AS table_name,
                t.tgname AS trigger_name
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND NOT t.tgisinternal
            ORDER BY t.tgname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| TriggerRef {
            schema: r.get("schema"),
            table_name: r.get("table_name"),
            trigger_name: r.get("trigger_name"),
        })
        .collect())
}

/// Get direct dependencies of a table (objects this table references)
pub async fn get_dependencies(client: &Client, schema: &str, table: &str) -> Result<Dependencies> {
    // Get outbound foreign keys (tables this table references)
    let foreign_keys = get_outbound_foreign_keys(client, schema, table).await?;

    // Get trigger functions used by triggers on this table
    let trigger_functions = get_trigger_function_refs(client, schema, table).await?;

    // Get user-defined types referenced by this table's columns
    let types = get_column_types(client, schema, table).await?;

    Ok(Dependencies {
        foreign_keys,
        trigger_functions,
        types,
    })
}

/// Get foreign keys this table has (outbound FKs)
async fn get_outbound_foreign_keys(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ForeignKeyRef>> {
    let rows = client
        .query(
            r#"
            SELECT
                con.conname AS constraint_name,
                fk_ns.nspname AS fk_schema,
                fk_cl.relname AS fk_table,
                ref_ns.nspname AS ref_schema,
                ref_cl.relname AS ref_table,
                (
                    SELECT array_agg(a.attname ORDER BY pos)
                    FROM unnest(con.conkey) WITH ORDINALITY AS cols(attnum, pos)
                    JOIN pg_attribute a ON a.attrelid = fk_cl.oid AND a.attnum = cols.attnum
                ) AS fk_columns,
                (
                    SELECT array_agg(a.attname ORDER BY pos)
                    FROM unnest(con.confkey) WITH ORDINALITY AS cols(attnum, pos)
                    JOIN pg_attribute a ON a.attrelid = ref_cl.oid AND a.attnum = cols.attnum
                ) AS ref_columns
            FROM pg_constraint con
            JOIN pg_class fk_cl ON con.conrelid = fk_cl.oid
            JOIN pg_namespace fk_ns ON fk_cl.relnamespace = fk_ns.oid
            JOIN pg_class ref_cl ON con.confrelid = ref_cl.oid
            JOIN pg_namespace ref_ns ON ref_cl.relnamespace = ref_ns.oid
            WHERE fk_ns.nspname = $1
              AND fk_cl.relname = $2
              AND con.contype = 'f'
            ORDER BY ref_ns.nspname, ref_cl.relname, con.conname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| ForeignKeyRef {
            constraint_name: r.get("constraint_name"),
            from_schema: r.get("fk_schema"),
            from_table: r.get("fk_table"),
            from_columns: r.get("fk_columns"),
            to_schema: r.get("ref_schema"),
            to_table: r.get("ref_table"),
            to_columns: r.get("ref_columns"),
        })
        .collect())
}

/// Get trigger functions used by triggers on this table
async fn get_trigger_function_refs(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<TriggerFunctionRef>> {
    let rows = client
        .query(
            r#"
            SELECT
                t.tgname AS trigger_name,
                pns.nspname AS fn_schema,
                p.proname AS fn_name
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            JOIN pg_namespace pns ON p.pronamespace = pns.oid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND NOT t.tgisinternal
            ORDER BY pns.nspname, p.proname, t.tgname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| TriggerFunctionRef {
            function_schema: r.get("fn_schema"),
            function_name: r.get("fn_name"),
            trigger_name: r.get("trigger_name"),
        })
        .collect())
}

/// Get user-defined types referenced by this table's columns
///
/// This includes both direct type references and array element types.
/// For example, a column `status[]` (array of enum) will report the `status` enum.
async fn get_column_types(client: &Client, schema: &str, table: &str) -> Result<Vec<TypeRef>> {
    let rows = client
        .query(
            r#"
            SELECT DISTINCT
                tns.nspname AS type_schema,
                t.typname AS type_name,
                t.typtype AS type_kind
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_type col_t ON a.atttypid = col_t.oid
            -- Resolve array element type if this is an array, otherwise use the column type directly
            JOIN pg_type t ON t.oid = CASE
                WHEN col_t.typelem != 0 THEN col_t.typelem  -- Array: use element type
                ELSE col_t.oid                              -- Non-array: use column type
            END
            JOIN pg_namespace tns ON t.typnamespace = tns.oid
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND tns.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND t.typtype IN ('e', 'd', 'c')  -- enum, domain, composite
            ORDER BY tns.nspname, t.typname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let typtype: i8 = r.get("type_kind");
            let kind = match typtype as u8 as char {
                'e' => "enum",
                'd' => "domain",
                'c' => "composite",
                _ => "type",
            }
            .to_string();

            TypeRef {
                schema: r.get("type_schema"),
                name: r.get("type_name"),
                kind,
            }
        })
        .collect())
}

impl Dependents {
    /// Format dependents output for display
    pub fn format(&self, schema: &str, table: &str) -> String {
        let mut output = Vec::new();
        let mut count = 0;

        // Foreign Keys section
        output.push("  Foreign Keys (tables referencing this table):".to_string());
        if self.foreign_keys.is_empty() {
            output.push("    (none)".to_string());
        } else {
            for fk in &self.foreign_keys {
                let from_cols = if fk.from_columns.len() == 1 {
                    fk.from_columns[0].clone()
                } else {
                    format!("({})", fk.from_columns.join(", "))
                };
                let to_cols = if fk.to_columns.len() == 1 {
                    fk.to_columns[0].clone()
                } else {
                    format!("({})", fk.to_columns.join(", "))
                };
                output.push(format!(
                    "    {}.{}.{} \u{2192} {}.{}.{}",
                    fk.from_schema, fk.from_table, from_cols, fk.to_schema, fk.to_table, to_cols
                ));
                count += 1;
            }
        }

        // Views section
        output.push(String::new());
        output.push("  Views:".to_string());
        if self.views.is_empty() {
            output.push("    (none)".to_string());
        } else {
            for view in &self.views {
                let suffix = if view.is_materialized {
                    " (materialized)"
                } else {
                    ""
                };
                output.push(format!("    {}.{}{}", view.schema, view.name, suffix));
                count += 1;
            }
        }

        // Triggers section
        output.push(String::new());
        output.push("  Triggers:".to_string());
        if self.triggers.is_empty() {
            output.push("    (none)".to_string());
        } else {
            for trg in &self.triggers {
                output.push(format!(
                    "    {} (on {}.{})",
                    trg.trigger_name, trg.schema, trg.table_name
                ));
                count += 1;
            }
        }

        // Summary
        output.push(String::new());
        output.push(format!("{} objects depend on {}.{}", count, schema, table));

        output.join("\n")
    }
}

impl Dependencies {
    /// Format dependencies output for display
    pub fn format(&self, schema: &str, table: &str) -> String {
        let mut output = Vec::new();
        let mut count = 0;

        // Foreign Keys section (this table references)
        output.push("  Foreign Keys (this table references):".to_string());
        if self.foreign_keys.is_empty() {
            output.push("    (none)".to_string());
        } else {
            for fk in &self.foreign_keys {
                let from_cols = if fk.from_columns.len() == 1 {
                    fk.from_columns[0].clone()
                } else {
                    format!("({})", fk.from_columns.join(", "))
                };
                let to_cols = if fk.to_columns.len() == 1 {
                    fk.to_columns[0].clone()
                } else {
                    format!("({})", fk.to_columns.join(", "))
                };
                output.push(format!(
                    "    {}.{}.{} \u{2192} {}.{}.{}",
                    fk.from_schema, fk.from_table, from_cols, fk.to_schema, fk.to_table, to_cols
                ));
                count += 1;
            }
        }

        // Trigger Functions section
        output.push(String::new());
        output.push("  Triggers (functions called):".to_string());
        if self.trigger_functions.is_empty() {
            output.push("    (none)".to_string());
        } else {
            for tf in &self.trigger_functions {
                output.push(format!(
                    "    {}.{}() via {} trigger",
                    tf.function_schema, tf.function_name, tf.trigger_name
                ));
                count += 1;
            }
        }

        // Types section
        output.push(String::new());
        output.push("  Types:".to_string());
        if self.types.is_empty() {
            output.push("    (none)".to_string());
        } else {
            for t in &self.types {
                output.push(format!("    {}.{} ({})", t.schema, t.name, t.kind));
                count += 1;
            }
        }

        // Summary
        output.push(String::new());
        output.push(format!(
            "{} objects that {}.{} depends on",
            count, schema, table
        ));

        output.join("\n")
    }
}

// ============================================================================
// Output Formatting
// ============================================================================

impl TableDescribe {
    /// Format the describe output for display
    /// If verbose is true, includes additional details and auto-vacuum/auto-analyze timestamps
    pub fn format(&self, verbose: bool) -> String {
        let mut output = Vec::new();

        // Details section (verbose only, at the top)
        if let Some(ref details) = self.details {
            output.push("Details:".to_string());
            output.push(format!("  Owner:        {}", details.owner));
            output.push(format!("  Type:         {}", details.table_kind));
            output.push(format!("  Persistence:  {}", details.persistence));
            output.push(String::new());
        }

        // Columns section
        output.push("Columns:".to_string());
        if self.columns.is_empty() {
            output.push("  (none)".to_string());
        } else {
            // Calculate column widths for alignment
            let max_name = self.columns.iter().map(|c| c.name.len()).max().unwrap_or(0);
            let max_type = self
                .columns
                .iter()
                .map(|c| c.data_type.len())
                .max()
                .unwrap_or(0);

            for col in &self.columns {
                let mut parts = Vec::new();

                // NOT NULL
                if !col.nullable {
                    parts.push("NOT NULL".to_string());
                }

                // PRIMARY KEY (single-column only)
                if col.is_primary_key {
                    parts.push("PRIMARY KEY".to_string());
                }

                // FK reference
                if let Some(ref fk) = col.fk_reference {
                    parts.push(format!("REFERENCES {}", fk));
                }

                // Identity
                if let Some(ref identity) = col.identity {
                    match identity {
                        IdentityType::Always => {
                            parts.push("GENERATED ALWAYS AS IDENTITY".to_string())
                        }
                        IdentityType::ByDefault => {
                            parts.push("GENERATED BY DEFAULT AS IDENTITY".to_string())
                        }
                    }
                } else if col.is_serial {
                    // Serial is shown as DEFAULT nextval(...) but we simplify it
                    parts.push("SERIAL".to_string());
                }

                // Default (if not identity/serial)
                if let Some(ref default) = col.default {
                    parts.push(format!("DEFAULT {}", default));
                }

                let suffix = if parts.is_empty() {
                    String::new()
                } else {
                    format!("  {}", parts.join("  "))
                };

                output.push(format!(
                    "  {:name_width$}  {:type_width$}{}",
                    col.name,
                    col.data_type,
                    suffix,
                    name_width = max_name,
                    type_width = max_type
                ));
            }
        }

        // Indexes section
        output.push(String::new());
        output.push("Indexes:".to_string());
        if self.indexes.is_empty() {
            output.push("  (none)".to_string());
        } else {
            // Display canonical definitions from pg_get_indexdef() directly.
            // This avoids misleading parsed summaries for complex indexes
            // (expression, partial, INCLUDE, etc.)
            for idx in &self.indexes {
                output.push(format!("  {}", idx.definition));
            }
        }

        // Constraints section
        output.push(String::new());
        output.push("Constraints:".to_string());
        if self.constraints.is_empty() {
            output.push("  (none)".to_string());
        } else {
            let max_name = self
                .constraints
                .iter()
                .map(|c| c.name.len())
                .max()
                .unwrap_or(0);
            for con in &self.constraints {
                output.push(format!(
                    "  {:width$}  {}",
                    con.name,
                    con.definition,
                    width = max_name
                ));
            }
        }

        // Triggers section
        output.push(String::new());
        output.push("Triggers:".to_string());
        if self.triggers.is_empty() {
            output.push("  (none)".to_string());
        } else {
            // Display canonical definitions from pg_get_triggerdef() directly.
            // This avoids misleading parsed summaries and ensures correctness
            // for all trigger patterns.
            for trg in &self.triggers {
                output.push(format!("  {}", trg.definition));
            }
        }

        // Stats section
        if let Some(ref stats) = self.stats {
            output.push(String::new());
            output.push("Stats:".to_string());

            // Check if stats retrieval failed (e.g., permission denied)
            if let Some(ref reason) = stats.unavailable_reason {
                output.push(format!("  {}", reason));
            } else {
                output.push(format!("  Rows (estimate):  ~{}", stats.row_estimate));
                output.push(format!("  Table size:       {}", stats.table_size));
                output.push(format!("  Index size:       {}", stats.index_size));
                output.push(format!("  Total size:       {}", stats.total_size));

                if let Some(ref ts) = stats.last_vacuum {
                    output.push(format!("  Last vacuum:      {}", ts));
                }
                if verbose {
                    if let Some(ref ts) = stats.last_autovacuum {
                        output.push(format!("  Last autovacuum:  {}", ts));
                    }
                }

                if let Some(ref ts) = stats.last_analyze {
                    output.push(format!("  Last analyze:     {}", ts));
                }
                if verbose {
                    if let Some(ref ts) = stats.last_autoanalyze {
                        output.push(format!("  Last autoanalyze: {}", ts));
                    }
                }

                // Add caveat for partitioned tables
                if stats.is_partitioned {
                    output.push(
                        "  (sizes are for parent table only; partitions not included)".to_string(),
                    );
                }
            }
        }

        // RLS section (only if enabled)
        if let Some(ref rls) = self.rls {
            if rls.enabled {
                output.push(String::new());
                let status = if rls.forced {
                    "Row-Level Security: ENABLED (forced)"
                } else {
                    "Row-Level Security: ENABLED"
                };
                output.push(status.to_string());

                if rls.policies.is_empty() {
                    output.push("  (no policies defined)".to_string());
                } else {
                    output.push("Policies:".to_string());
                    for policy in &rls.policies {
                        let permissive_label = if policy.permissive {
                            "PERMISSIVE"
                        } else {
                            "RESTRICTIVE"
                        };

                        // Format: policy_name (PERMISSIVE/RESTRICTIVE, command)
                        let roles_suffix = if policy.roles.is_empty() {
                            String::new()
                        } else {
                            format!(" [{}]", policy.roles.join(", "))
                        };

                        output.push(format!(
                            "  {} ({}, {}){}",
                            policy.name, permissive_label, policy.command, roles_suffix
                        ));

                        // Show USING expression
                        if let Some(ref expr) = policy.using_expr {
                            output.push(format!("    Using: {}", expr));
                        }

                        // Show WITH CHECK expression (only if different from USING)
                        if let Some(ref expr) = policy.with_check_expr {
                            if policy.using_expr.as_ref() != Some(expr) {
                                output.push(format!("    Check: {}", expr));
                            }
                        }
                    }
                }
            }
        }

        output.join("\n")
    }
}

// ============================================================================
// Name Resolution
// ============================================================================

/// Resolve an object name to a schema-qualified table
/// Supports:
/// - Fully qualified: schema.table
/// - Unqualified: table (defaults to public, errors if ambiguous)
///
/// Only resolves ordinary tables (relkind 'r') and partitioned tables (relkind 'p')
pub async fn resolve_table(client: &Client, object: &str) -> Result<ResolvedTable> {
    let (schema, table) = parse_object_name(object);

    if let Some(schema) = schema {
        // Fully qualified: check if exists
        let row = client
            .query_opt(
                r#"
                SELECT c.oid::bigint, n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = $1
                  AND c.relname = $2
                  AND c.relkind IN ('r', 'p')
                "#,
                &[&schema, &table],
            )
            .await?;

        match row {
            Some(row) => Ok(ResolvedTable {
                oid: row.get(0),
                schema: row.get(1),
                name: row.get(2),
            }),
            None => bail!(
                "Table {}.{} not found",
                quote_ident(schema),
                quote_ident(table)
            ),
        }
    } else {
        // Unqualified: find all matches, error if ambiguous
        let rows = client
            .query(
                r#"
                SELECT c.oid::bigint, n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relname = $1
                  AND c.relkind IN ('r', 'p')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY
                    CASE WHEN n.nspname = 'public' THEN 0 ELSE 1 END,
                    n.nspname
                "#,
                &[&table],
            )
            .await?;

        match rows.len() {
            0 => bail!("Table \"{}\" not found", table),
            1 => {
                let row = &rows[0];
                Ok(ResolvedTable {
                    oid: row.get(0),
                    schema: row.get(1),
                    name: row.get(2),
                })
            }
            _ => {
                let mut schemas: Vec<String> = rows.iter().map(|r| r.get::<_, String>(1)).collect();
                schemas.sort(); // Alphabetical order for stable error messages
                bail!(
                    "Table \"{}\" exists in multiple schemas: {}\nHint: Use fully qualified name (e.g., \"{}\".\"{}\")",
                    table,
                    schemas.join(", "),
                    schemas[0],
                    table
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Object Name Parsing Tests
    // =========================================================================

    #[test]
    fn test_parse_object_name_qualified() {
        let (schema, table) = parse_object_name("app.users");
        assert_eq!(schema, Some("app"));
        assert_eq!(table, "users");
    }

    #[test]
    fn test_parse_object_name_unqualified() {
        let (schema, table) = parse_object_name("users");
        assert_eq!(schema, None);
        assert_eq!(table, "users");
    }

    #[test]
    fn test_parse_object_name_multiple_dots() {
        // Only first dot is used for split
        let (schema, table) = parse_object_name("app.users.extra");
        assert_eq!(schema, Some("app"));
        assert_eq!(table, "users.extra");
    }

    #[test]
    fn test_parse_object_name_empty() {
        let (schema, table) = parse_object_name("");
        assert_eq!(schema, None);
        assert_eq!(table, "");
    }

    // =========================================================================
    // Formatter Tests
    // =========================================================================

    #[test]
    fn test_dependents_format_empty() {
        let deps = Dependents {
            foreign_keys: vec![],
            views: vec![],
            triggers: vec![],
        };
        let output = deps.format("public", "users");
        assert!(output.contains("(none)"));
        assert!(output.contains("0 objects depend on public.users"));
    }

    #[test]
    fn test_dependents_format_with_fk() {
        let deps = Dependents {
            foreign_keys: vec![ForeignKeyRef {
                constraint_name: "orders_user_id_fkey".to_string(),
                from_schema: "public".to_string(),
                from_table: "orders".to_string(),
                from_columns: vec!["user_id".to_string()],
                to_schema: "public".to_string(),
                to_table: "users".to_string(),
                to_columns: vec!["id".to_string()],
            }],
            views: vec![],
            triggers: vec![],
        };
        let output = deps.format("public", "users");
        assert!(output.contains("public.orders.user_id"));
        assert!(output.contains("→"));
        assert!(output.contains("1 objects depend on public.users"));
    }

    #[test]
    fn test_dependents_format_composite_fk() {
        let deps = Dependents {
            foreign_keys: vec![ForeignKeyRef {
                constraint_name: "child_parent_fkey".to_string(),
                from_schema: "app".to_string(),
                from_table: "child".to_string(),
                from_columns: vec!["a".to_string(), "b".to_string()],
                to_schema: "app".to_string(),
                to_table: "parent".to_string(),
                to_columns: vec!["x".to_string(), "y".to_string()],
            }],
            views: vec![],
            triggers: vec![],
        };
        let output = deps.format("app", "parent");
        // Composite FK should show grouped columns
        assert!(output.contains("(a, b)"));
        assert!(output.contains("(x, y)"));
    }

    #[test]
    fn test_dependencies_format_empty() {
        let deps = Dependencies {
            foreign_keys: vec![],
            trigger_functions: vec![],
            types: vec![],
        };
        let output = deps.format("public", "users");
        assert!(output.contains("(none)"));
        assert!(output.contains("0 objects that public.users depends on"));
    }

    #[test]
    fn test_dependencies_format_with_type() {
        let deps = Dependencies {
            foreign_keys: vec![],
            trigger_functions: vec![],
            types: vec![TypeRef {
                schema: "public".to_string(),
                name: "status".to_string(),
                kind: "enum".to_string(),
            }],
        };
        let output = deps.format("public", "tasks");
        assert!(output.contains("public.status (enum)"));
        assert!(output.contains("1 objects that public.tasks depends on"));
    }

    #[test]
    fn test_dependencies_format_with_trigger_function() {
        let deps = Dependencies {
            foreign_keys: vec![],
            trigger_functions: vec![TriggerFunctionRef {
                function_schema: "public".to_string(),
                function_name: "set_updated_at".to_string(),
                trigger_name: "update_timestamp".to_string(),
            }],
            types: vec![],
        };
        let output = deps.format("public", "users");
        assert!(output.contains("public.set_updated_at()"));
        assert!(output.contains("via update_timestamp trigger"));
    }

    // =========================================================================
    // Constraint Type Tests (PK vs UNIQUE)
    // =========================================================================

    #[test]
    fn test_constraint_type_primary_key_distinct_from_unique() {
        // Verify that PrimaryKey and Unique are distinct constraint types
        assert_ne!(ConstraintType::PrimaryKey, ConstraintType::Unique);
    }

    #[test]
    fn test_table_describe_format_shows_pk_constraint() {
        let table = TableDescribe {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                identity: None,
                is_serial: true,
                default: None,
                fk_reference: None,
            }],
            indexes: vec![],
            constraints: vec![Constraint {
                schema: "public".to_string(),
                table_name: "users".to_string(),
                name: "users_pkey".to_string(),
                constraint_type: ConstraintType::PrimaryKey,
                definition: "PRIMARY KEY (id)".to_string(),
            }],
            triggers: vec![],
            stats: None,
            details: None,
            rls: None,
        };

        let output = table.format(false);
        assert!(
            output.contains("Constraints:"),
            "Should have Constraints section"
        );
        assert!(output.contains("users_pkey"), "Should show constraint name");
        assert!(
            output.contains("PRIMARY KEY (id)"),
            "Should show PK definition"
        );
    }

    #[test]
    fn test_table_describe_format_shows_unique_constraint() {
        let table = TableDescribe {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![ColumnInfo {
                name: "email".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                is_primary_key: false,
                identity: None,
                is_serial: false,
                default: None,
                fk_reference: None,
            }],
            indexes: vec![],
            constraints: vec![Constraint {
                schema: "public".to_string(),
                table_name: "users".to_string(),
                name: "users_email_key".to_string(),
                constraint_type: ConstraintType::Unique,
                definition: "UNIQUE (email)".to_string(),
            }],
            triggers: vec![],
            stats: None,
            details: None,
            rls: None,
        };

        let output = table.format(false);
        assert!(
            output.contains("Constraints:"),
            "Should have Constraints section"
        );
        assert!(
            output.contains("users_email_key"),
            "Should show constraint name"
        );
        assert!(
            output.contains("UNIQUE (email)"),
            "Should show UNIQUE definition"
        );
    }

    #[test]
    fn test_table_describe_format_pk_and_unique_both_shown() {
        let table = TableDescribe {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    identity: None,
                    is_serial: true,
                    default: None,
                    fk_reference: None,
                },
                ColumnInfo {
                    name: "email".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    identity: None,
                    is_serial: false,
                    default: None,
                    fk_reference: None,
                },
            ],
            indexes: vec![],
            constraints: vec![
                Constraint {
                    schema: "public".to_string(),
                    table_name: "users".to_string(),
                    name: "users_pkey".to_string(),
                    constraint_type: ConstraintType::PrimaryKey,
                    definition: "PRIMARY KEY (id)".to_string(),
                },
                Constraint {
                    schema: "public".to_string(),
                    table_name: "users".to_string(),
                    name: "users_email_key".to_string(),
                    constraint_type: ConstraintType::Unique,
                    definition: "UNIQUE (email)".to_string(),
                },
            ],
            triggers: vec![],
            stats: None,
            details: None,
            rls: None,
        };

        let output = table.format(false);
        // Both constraints should be displayed distinctly
        assert!(
            output.contains("PRIMARY KEY (id)"),
            "Should show PK definition"
        );
        assert!(
            output.contains("UNIQUE (email)"),
            "Should show UNIQUE definition"
        );
        assert!(
            output.contains("users_pkey"),
            "Should show PK constraint name"
        );
        assert!(
            output.contains("users_email_key"),
            "Should show UNIQUE constraint name"
        );
    }
}
