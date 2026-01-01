//! Schema diffing module for pgcrate diff command.
//!
//! Compares two database schemas and reports differences.
//! Uses existing types from introspect.rs - no duplication.

use crate::introspect::{
    Column, Constraint, DatabaseSchema, EnumType, Extension, Function, IdentityType, Index,
    MaterializedView, SchemaInfo, Sequence, Table, Trigger, View,
};
use colored::Colorize;
use std::collections::{HashMap, HashSet};

// =============================================================================
// Diff Result Types
// =============================================================================

/// Complete diff between two database schemas
#[derive(Debug, Default)]
pub struct SchemaDiff {
    pub added_extensions: Vec<Extension>,
    pub removed_extensions: Vec<Extension>,

    pub added_schemas: Vec<SchemaInfo>,
    pub removed_schemas: Vec<SchemaInfo>,

    pub added_enums: Vec<EnumType>,
    pub removed_enums: Vec<EnumType>,
    pub modified_enums: Vec<EnumDiff>,

    pub added_sequences: Vec<Sequence>,
    pub removed_sequences: Vec<Sequence>,

    pub added_tables: Vec<Table>,
    pub removed_tables: Vec<Table>,
    pub modified_tables: Vec<TableDiff>,

    pub added_views: Vec<View>,
    pub removed_views: Vec<View>,
    pub modified_views: Vec<ViewDiff>,

    pub added_indexes: Vec<Index>,
    pub removed_indexes: Vec<Index>,

    pub added_constraints: Vec<Constraint>,
    pub removed_constraints: Vec<Constraint>,

    pub added_triggers: Vec<Trigger>,
    pub removed_triggers: Vec<Trigger>,

    pub added_functions: Vec<Function>,
    pub removed_functions: Vec<Function>,

    pub added_materialized_views: Vec<MaterializedView>,
    pub removed_materialized_views: Vec<MaterializedView>,
    pub modified_materialized_views: Vec<ViewDiff>,
}

/// Diff for a single table
#[derive(Debug)]
pub struct TableDiff {
    pub schema: String,
    pub name: String,
    pub added_columns: Vec<Column>,
    pub removed_columns: Vec<Column>,
    pub modified_columns: Vec<ColumnDiff>,
}

/// Diff for a single column
#[derive(Debug)]
pub struct ColumnDiff {
    pub name: String,
    pub from_type: String,
    pub to_type: String,
    pub from_nullable: bool,
    pub to_nullable: bool,
    pub from_default: Option<String>,
    pub to_default: Option<String>,
    pub from_identity: Option<IdentityType>,
    pub to_identity: Option<IdentityType>,
    pub from_is_serial: bool,
    pub to_is_serial: bool,
}

/// Diff for an enum type
#[derive(Debug)]
pub struct EnumDiff {
    pub schema: String,
    pub name: String,
    pub added_values: Vec<String>,
    pub removed_values: Vec<String>,
}

/// Diff for a view (name only; definition comparison deferred to verbose mode)
#[derive(Debug)]
pub struct ViewDiff {
    pub schema: String,
    pub name: String,
}

impl SchemaDiff {
    /// Check if schemas are identical (no differences)
    pub fn is_empty(&self) -> bool {
        self.added_extensions.is_empty()
            && self.removed_extensions.is_empty()
            && self.added_schemas.is_empty()
            && self.removed_schemas.is_empty()
            && self.added_enums.is_empty()
            && self.removed_enums.is_empty()
            && self.modified_enums.is_empty()
            && self.added_sequences.is_empty()
            && self.removed_sequences.is_empty()
            && self.added_tables.is_empty()
            && self.removed_tables.is_empty()
            && self.modified_tables.is_empty()
            && self.added_views.is_empty()
            && self.removed_views.is_empty()
            && self.modified_views.is_empty()
            && self.added_indexes.is_empty()
            && self.removed_indexes.is_empty()
            && self.added_constraints.is_empty()
            && self.removed_constraints.is_empty()
            && self.added_triggers.is_empty()
            && self.removed_triggers.is_empty()
            && self.added_functions.is_empty()
            && self.removed_functions.is_empty()
            && self.added_materialized_views.is_empty()
            && self.removed_materialized_views.is_empty()
            && self.modified_materialized_views.is_empty()
    }

    /// Get summary counts for display
    pub fn summary(&self) -> DiffSummary {
        DiffSummary {
            tables: self.added_tables.len()
                + self.removed_tables.len()
                + self.modified_tables.len(),
            columns: self
                .modified_tables
                .iter()
                .map(|t| t.added_columns.len() + t.removed_columns.len() + t.modified_columns.len())
                .sum(),
            indexes: self.added_indexes.len() + self.removed_indexes.len(),
            constraints: self.added_constraints.len() + self.removed_constraints.len(),
            enums: self.added_enums.len() + self.removed_enums.len() + self.modified_enums.len(),
            functions: self.added_functions.len() + self.removed_functions.len(),
            views: self.added_views.len() + self.removed_views.len() + self.modified_views.len(),
            triggers: self.added_triggers.len() + self.removed_triggers.len(),
            sequences: self.added_sequences.len() + self.removed_sequences.len(),
            extensions: self.added_extensions.len() + self.removed_extensions.len(),
            schemas: self.added_schemas.len() + self.removed_schemas.len(),
            materialized_views: self.added_materialized_views.len()
                + self.removed_materialized_views.len()
                + self.modified_materialized_views.len(),
        }
    }
}

#[derive(Debug, Default)]
pub struct DiffSummary {
    pub tables: usize,
    pub columns: usize,
    pub indexes: usize,
    pub constraints: usize,
    pub enums: usize,
    pub functions: usize,
    pub views: usize,
    pub triggers: usize,
    pub sequences: usize,
    pub extensions: usize,
    pub schemas: usize,
    pub materialized_views: usize,
}

// =============================================================================
// Diff Implementation
// =============================================================================

/// Compare two database schemas and return differences
pub fn diff_schemas(from: &DatabaseSchema, to: &DatabaseSchema) -> SchemaDiff {
    let mut diff = SchemaDiff::default();

    // Extensions (by name)
    diff_by_name(
        &from.extensions,
        &to.extensions,
        |e| e.name.clone(),
        &mut diff.added_extensions,
        &mut diff.removed_extensions,
    );

    // Schemas (by name)
    diff_by_name(
        &from.schemas,
        &to.schemas,
        |s| s.name.clone(),
        &mut diff.added_schemas,
        &mut diff.removed_schemas,
    );

    // Enums (by qualified name)
    let (added_enums, removed_enums, common_enums) = diff_by_key(&from.enums, &to.enums, |e| {
        format!("{}.{}", e.schema, e.name)
    });
    diff.added_enums = added_enums;
    diff.removed_enums = removed_enums;

    // Check modified enums (value changes)
    for (from_enum, to_enum) in common_enums {
        let enum_diff = diff_enum(from_enum, to_enum);
        if !enum_diff.added_values.is_empty() || !enum_diff.removed_values.is_empty() {
            diff.modified_enums.push(enum_diff);
        }
    }

    // Sequences (by qualified name)
    diff_by_name(
        &from.sequences,
        &to.sequences,
        |s| format!("{}.{}", s.schema, s.name),
        &mut diff.added_sequences,
        &mut diff.removed_sequences,
    );

    // Tables (by qualified name)
    let (added_tables, removed_tables, common_tables) =
        diff_by_key(&from.tables, &to.tables, |t| {
            format!("{}.{}", t.schema, t.name)
        });
    diff.added_tables = added_tables;
    diff.removed_tables = removed_tables;

    // Check modified tables (column changes)
    for (from_table, to_table) in common_tables {
        let table_diff = diff_table(from_table, to_table);
        if !table_diff.added_columns.is_empty()
            || !table_diff.removed_columns.is_empty()
            || !table_diff.modified_columns.is_empty()
        {
            diff.modified_tables.push(table_diff);
        }
    }

    // Views (by qualified name)
    let (added_views, removed_views, common_views) = diff_by_key(&from.views, &to.views, |v| {
        format!("{}.{}", v.schema, v.name)
    });
    diff.added_views = added_views;
    diff.removed_views = removed_views;

    // Check modified views (definition changes)
    for (from_view, to_view) in common_views {
        if from_view.definition.trim() != to_view.definition.trim() {
            diff.modified_views.push(ViewDiff {
                schema: to_view.schema.clone(),
                name: to_view.name.clone(),
            });
        }
    }

    // Indexes (by qualified name: schema.index_name)
    diff_by_name(
        &from.indexes,
        &to.indexes,
        |i| format!("{}.{}", i.schema, i.name),
        &mut diff.added_indexes,
        &mut diff.removed_indexes,
    );

    // Constraints (by qualified name: schema.table.constraint_name)
    diff_by_name(
        &from.constraints,
        &to.constraints,
        |c| format!("{}.{}.{}", c.schema, c.table_name, c.name),
        &mut diff.added_constraints,
        &mut diff.removed_constraints,
    );

    // Triggers (by qualified name: schema.table.trigger_name)
    diff_by_name(
        &from.triggers,
        &to.triggers,
        |t| format!("{}.{}.{}", t.schema, t.table_name, t.name),
        &mut diff.added_triggers,
        &mut diff.removed_triggers,
    );

    // Functions (by identity - includes arg types)
    diff_by_name(
        &from.functions,
        &to.functions,
        |f| f.identity.clone(),
        &mut diff.added_functions,
        &mut diff.removed_functions,
    );

    // Materialized Views (by qualified name)
    let (added_matviews, removed_matviews, common_matviews) =
        diff_by_key(&from.materialized_views, &to.materialized_views, |m| {
            format!("{}.{}", m.schema, m.name)
        });
    diff.added_materialized_views = added_matviews;
    diff.removed_materialized_views = removed_matviews;

    // Check modified materialized views (definition changes)
    for (from_mv, to_mv) in common_matviews {
        if from_mv.definition.trim() != to_mv.definition.trim() {
            diff.modified_materialized_views.push(ViewDiff {
                schema: to_mv.schema.clone(),
                name: to_mv.name.clone(),
            });
        }
    }

    // Sort all results for deterministic output
    sort_diff(&mut diff);

    diff
}

/// Sort all diff results for deterministic output ordering
fn sort_diff(diff: &mut SchemaDiff) {
    // Extensions by name
    diff.added_extensions.sort_by(|a, b| a.name.cmp(&b.name));
    diff.removed_extensions.sort_by(|a, b| a.name.cmp(&b.name));

    // Schemas by name
    diff.added_schemas.sort_by(|a, b| a.name.cmp(&b.name));
    diff.removed_schemas.sort_by(|a, b| a.name.cmp(&b.name));

    // Enums by qualified name
    diff.added_enums
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.removed_enums
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.modified_enums
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    // Sequences by qualified name
    diff.added_sequences
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.removed_sequences
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    // Tables by qualified name
    diff.added_tables
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.removed_tables
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.modified_tables
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    // Views by qualified name
    diff.added_views
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.removed_views
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.modified_views
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    // Indexes by qualified name
    diff.added_indexes
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.removed_indexes
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    // Constraints by schema.table.name
    diff.added_constraints.sort_by(|a, b| {
        (&a.schema, &a.table_name, &a.name).cmp(&(&b.schema, &b.table_name, &b.name))
    });
    diff.removed_constraints.sort_by(|a, b| {
        (&a.schema, &a.table_name, &a.name).cmp(&(&b.schema, &b.table_name, &b.name))
    });

    // Triggers by schema.table.name
    diff.added_triggers.sort_by(|a, b| {
        (&a.schema, &a.table_name, &a.name).cmp(&(&b.schema, &b.table_name, &b.name))
    });
    diff.removed_triggers.sort_by(|a, b| {
        (&a.schema, &a.table_name, &a.name).cmp(&(&b.schema, &b.table_name, &b.name))
    });

    // Functions by identity
    diff.added_functions
        .sort_by(|a, b| a.identity.cmp(&b.identity));
    diff.removed_functions
        .sort_by(|a, b| a.identity.cmp(&b.identity));

    // Materialized views by qualified name
    diff.added_materialized_views
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.removed_materialized_views
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    diff.modified_materialized_views
        .sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
}

/// Compare two tables and return column-level differences
fn diff_table(from: &Table, to: &Table) -> TableDiff {
    let mut diff = TableDiff {
        schema: to.schema.clone(),
        name: to.name.clone(),
        added_columns: Vec::new(),
        removed_columns: Vec::new(),
        modified_columns: Vec::new(),
    };

    // Build maps by column name
    let from_cols: HashMap<&str, &Column> =
        from.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let to_cols: HashMap<&str, &Column> = to.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    let from_names: HashSet<&str> = from_cols.keys().copied().collect();
    let to_names: HashSet<&str> = to_cols.keys().copied().collect();

    // Added columns (in to but not in from)
    for name in to_names.difference(&from_names) {
        diff.added_columns.push(to_cols[*name].clone());
    }

    // Removed columns (in from but not in to)
    for name in from_names.difference(&to_names) {
        diff.removed_columns.push(from_cols[*name].clone());
    }

    // Modified columns (in both but different)
    for name in from_names.intersection(&to_names) {
        let from_col = from_cols[*name];
        let to_col = to_cols[*name];

        if is_column_different(from_col, to_col) {
            diff.modified_columns.push(ColumnDiff {
                name: from_col.name.clone(),
                from_type: from_col.data_type.clone(),
                to_type: to_col.data_type.clone(),
                from_nullable: from_col.nullable,
                to_nullable: to_col.nullable,
                from_default: from_col.default.clone(),
                to_default: to_col.default.clone(),
                from_identity: from_col.identity.clone(),
                to_identity: to_col.identity.clone(),
                from_is_serial: from_col.is_serial,
                to_is_serial: to_col.is_serial,
            });
        }
    }

    diff
}

/// Check if two columns are different
fn is_column_different(from: &Column, to: &Column) -> bool {
    from.data_type != to.data_type
        || from.nullable != to.nullable
        || from.default != to.default
        || from.identity != to.identity
        || from.is_serial != to.is_serial
}

/// Compare two enums and return value-level differences
fn diff_enum(from: &EnumType, to: &EnumType) -> EnumDiff {
    let from_values: HashSet<&str> = from.values.iter().map(|s| s.as_str()).collect();
    let to_values: HashSet<&str> = to.values.iter().map(|s| s.as_str()).collect();

    EnumDiff {
        schema: to.schema.clone(),
        name: to.name.clone(),
        added_values: to_values
            .difference(&from_values)
            .map(|s| s.to_string())
            .collect(),
        removed_values: from_values
            .difference(&to_values)
            .map(|s| s.to_string())
            .collect(),
    }
}

/// Generic helper to diff two lists by a key function, returning only added/removed
fn diff_by_name<T: Clone, K: Eq + std::hash::Hash>(
    from: &[T],
    to: &[T],
    key_fn: impl Fn(&T) -> K,
    added: &mut Vec<T>,
    removed: &mut Vec<T>,
) {
    let from_map: HashMap<K, &T> = from.iter().map(|item| (key_fn(item), item)).collect();
    let to_map: HashMap<K, &T> = to.iter().map(|item| (key_fn(item), item)).collect();

    // Invariant: duplicate keys indicate an internal bug in key_fn choice
    assert_eq!(
        from_map.len(),
        from.len(),
        "Duplicate keys in source schema (internal bug in diff_by_name key_fn)"
    );
    assert_eq!(
        to_map.len(),
        to.len(),
        "Duplicate keys in target schema (internal bug in diff_by_name key_fn)"
    );

    // Added (in to but not in from)
    for (key, item) in &to_map {
        if !from_map.contains_key(key) {
            added.push((*item).clone());
        }
    }

    // Removed (in from but not in to)
    for (key, item) in &from_map {
        if !to_map.contains_key(key) {
            removed.push((*item).clone());
        }
    }
}

/// Generic helper that returns added, removed, and pairs of common items
fn diff_by_key<'a, T: Clone, K: Eq + std::hash::Hash>(
    from: &'a [T],
    to: &'a [T],
    key_fn: impl Fn(&T) -> K,
) -> (Vec<T>, Vec<T>, Vec<(&'a T, &'a T)>) {
    let from_map: HashMap<K, &T> = from.iter().map(|item| (key_fn(item), item)).collect();
    let to_map: HashMap<K, &T> = to.iter().map(|item| (key_fn(item), item)).collect();

    // Invariant: duplicate keys indicate an internal bug in key_fn choice
    assert_eq!(
        from_map.len(),
        from.len(),
        "Duplicate keys in source schema (internal bug in diff_by_key key_fn)"
    );
    assert_eq!(
        to_map.len(),
        to.len(),
        "Duplicate keys in target schema (internal bug in diff_by_key key_fn)"
    );

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut common = Vec::new();

    // Added (in to but not in from)
    for item in to {
        let key = key_fn(item);
        if !from_map.contains_key(&key) {
            added.push(item.clone());
        }
    }

    // Removed (in from but not in to) and common
    for item in from {
        let key = key_fn(item);
        if let Some(to_item) = to_map.get(&key) {
            common.push((item, *to_item));
        } else {
            removed.push(item.clone());
        }
    }

    (added, removed, common)
}

// =============================================================================
// Formatting
// =============================================================================

/// Format diff as human-readable string
pub fn format_diff(diff: &SchemaDiff, from_label: &str, to_label: &str) -> String {
    let mut output = Vec::new();

    output.push(format!("Comparing: {} → {}", from_label, to_label));
    output.push(String::new());
    output.push("Legend:".to_string());
    output.push(format!("  {} exists in TARGET (--to) only", "+".green()));
    output.push(format!("  {} exists in SOURCE (--from) only", "-".red()));
    output.push(format!("  {} exists in both but differs", "~".yellow()));

    // Extensions
    if !diff.added_extensions.is_empty() || !diff.removed_extensions.is_empty() {
        output.push(String::new());
        output.push("Extensions:".to_string());
        for ext in &diff.added_extensions {
            output.push(format!("  {} {}", "+".green(), ext.name));
        }
        for ext in &diff.removed_extensions {
            output.push(format!("  {} {}", "-".red(), ext.name));
        }
    }

    // Schemas
    if !diff.added_schemas.is_empty() || !diff.removed_schemas.is_empty() {
        output.push(String::new());
        output.push("Schemas:".to_string());
        for schema in &diff.added_schemas {
            output.push(format!("  {} {}", "+".green(), schema.name));
        }
        for schema in &diff.removed_schemas {
            output.push(format!("  {} {}", "-".red(), schema.name));
        }
    }

    // Enums
    if !diff.added_enums.is_empty()
        || !diff.removed_enums.is_empty()
        || !diff.modified_enums.is_empty()
    {
        output.push(String::new());
        output.push("Enums:".to_string());
        for e in &diff.added_enums {
            output.push(format!("  {} {}.{}", "+".green(), e.schema, e.name));
        }
        for e in &diff.removed_enums {
            output.push(format!("  {} {}.{}", "-".red(), e.schema, e.name));
        }
        for e in &diff.modified_enums {
            output.push(format!(
                "  {} {}.{} (differs)",
                "~".yellow(),
                e.schema,
                e.name
            ));
            for v in &e.added_values {
                output.push(format!("      {} value: {}", "+".green(), v));
            }
            for v in &e.removed_values {
                output.push(format!("      {} value: {}", "-".red(), v));
            }
        }
    }

    // Sequences
    if !diff.added_sequences.is_empty() || !diff.removed_sequences.is_empty() {
        output.push(String::new());
        output.push("Sequences:".to_string());
        for seq in &diff.added_sequences {
            output.push(format!("  {} {}.{}", "+".green(), seq.schema, seq.name));
        }
        for seq in &diff.removed_sequences {
            output.push(format!("  {} {}.{}", "-".red(), seq.schema, seq.name));
        }
    }

    // Tables
    if !diff.added_tables.is_empty()
        || !diff.removed_tables.is_empty()
        || !diff.modified_tables.is_empty()
    {
        output.push(String::new());
        output.push("Tables:".to_string());
        for table in &diff.added_tables {
            output.push(format!("  {} {}.{}", "+".green(), table.schema, table.name));
        }
        for table in &diff.removed_tables {
            output.push(format!("  {} {}.{}", "-".red(), table.schema, table.name));
        }
        for table in &diff.modified_tables {
            output.push(format!(
                "  {} {}.{} (differs)",
                "~".yellow(),
                table.schema,
                table.name
            ));
            for col in &table.added_columns {
                let nullable = if col.nullable { "nullable" } else { "NOT NULL" };
                output.push(format!(
                    "      {} column: {} ({}, {})",
                    "+".green(),
                    col.name,
                    col.data_type,
                    nullable
                ));
            }
            for col in &table.removed_columns {
                output.push(format!("      {} column: {}", "-".red(), col.name));
            }
            for col in &table.modified_columns {
                let changes = format_column_changes(col);
                output.push(format!(
                    "      {} column: {} ({})",
                    "~".yellow(),
                    col.name,
                    changes
                ));
            }
        }
    }

    // Indexes
    if !diff.added_indexes.is_empty() || !diff.removed_indexes.is_empty() {
        output.push(String::new());
        output.push("Indexes:".to_string());
        for idx in &diff.added_indexes {
            output.push(format!(
                "  {} {} ON {}.{}",
                "+".green(),
                idx.name,
                idx.schema,
                idx.table_name
            ));
        }
        for idx in &diff.removed_indexes {
            output.push(format!(
                "  {} {} ON {}.{}",
                "-".red(),
                idx.name,
                idx.schema,
                idx.table_name
            ));
        }
    }

    // Constraints
    if !diff.added_constraints.is_empty() || !diff.removed_constraints.is_empty() {
        output.push(String::new());
        output.push("Constraints:".to_string());
        for con in &diff.added_constraints {
            output.push(format!(
                "  {} {} ON {}.{}",
                "+".green(),
                con.name,
                con.schema,
                con.table_name
            ));
        }
        for con in &diff.removed_constraints {
            output.push(format!(
                "  {} {} ON {}.{}",
                "-".red(),
                con.name,
                con.schema,
                con.table_name
            ));
        }
    }

    // Functions
    if !diff.added_functions.is_empty() || !diff.removed_functions.is_empty() {
        output.push(String::new());
        output.push("Functions:".to_string());
        for func in &diff.added_functions {
            output.push(format!("  {} {}", "+".green(), func.identity));
        }
        for func in &diff.removed_functions {
            output.push(format!("  {} {}", "-".red(), func.identity));
        }
    }

    // Triggers
    if !diff.added_triggers.is_empty() || !diff.removed_triggers.is_empty() {
        output.push(String::new());
        output.push("Triggers:".to_string());
        for trig in &diff.added_triggers {
            output.push(format!(
                "  {} {} ON {}.{}",
                "+".green(),
                trig.name,
                trig.schema,
                trig.table_name
            ));
        }
        for trig in &diff.removed_triggers {
            output.push(format!(
                "  {} {} ON {}.{}",
                "-".red(),
                trig.name,
                trig.schema,
                trig.table_name
            ));
        }
    }

    // Views
    if !diff.added_views.is_empty()
        || !diff.removed_views.is_empty()
        || !diff.modified_views.is_empty()
    {
        output.push(String::new());
        output.push("Views:".to_string());
        for view in &diff.added_views {
            output.push(format!("  {} {}.{}", "+".green(), view.schema, view.name));
        }
        for view in &diff.removed_views {
            output.push(format!("  {} {}.{}", "-".red(), view.schema, view.name));
        }
        for view in &diff.modified_views {
            output.push(format!(
                "  {} {}.{} (definition differs)",
                "~".yellow(),
                view.schema,
                view.name
            ));
        }
    }

    // Materialized Views
    if !diff.added_materialized_views.is_empty()
        || !diff.removed_materialized_views.is_empty()
        || !diff.modified_materialized_views.is_empty()
    {
        output.push(String::new());
        output.push("Materialized Views:".to_string());
        for mv in &diff.added_materialized_views {
            output.push(format!("  {} {}.{}", "+".green(), mv.schema, mv.name));
        }
        for mv in &diff.removed_materialized_views {
            output.push(format!("  {} {}.{}", "-".red(), mv.schema, mv.name));
        }
        for mv in &diff.modified_materialized_views {
            output.push(format!(
                "  {} {}.{} (definition differs)",
                "~".yellow(),
                mv.schema,
                mv.name
            ));
        }
    }

    // Summary
    let summary = diff.summary();
    let mut summary_parts = Vec::new();

    if summary.tables > 0 {
        summary_parts.push(format!("{} tables", summary.tables));
    }
    if summary.columns > 0 {
        summary_parts.push(format!("{} columns", summary.columns));
    }
    if summary.indexes > 0 {
        summary_parts.push(format!("{} indexes", summary.indexes));
    }
    if summary.constraints > 0 {
        summary_parts.push(format!("{} constraints", summary.constraints));
    }
    if summary.enums > 0 {
        summary_parts.push(format!("{} enums", summary.enums));
    }
    if summary.functions > 0 {
        summary_parts.push(format!("{} functions", summary.functions));
    }
    if summary.views > 0 {
        summary_parts.push(format!("{} views", summary.views));
    }
    if summary.materialized_views > 0 {
        summary_parts.push(format!("{} materialized views", summary.materialized_views));
    }
    if summary.triggers > 0 {
        summary_parts.push(format!("{} triggers", summary.triggers));
    }
    if summary.sequences > 0 {
        summary_parts.push(format!("{} sequences", summary.sequences));
    }
    if summary.extensions > 0 {
        summary_parts.push(format!("{} extensions", summary.extensions));
    }
    if summary.schemas > 0 {
        summary_parts.push(format!("{} schemas", summary.schemas));
    }

    if !summary_parts.is_empty() {
        output.push(String::new());
        output.push(format!("Summary: {} differ", summary_parts.join(", ")));
    }

    output.join("\n")
}

/// Format column changes as a readable string
fn format_column_changes(col: &ColumnDiff) -> String {
    let mut changes = Vec::new();

    if col.from_type != col.to_type {
        changes.push(format!("{} → {}", col.from_type, col.to_type));
    }

    if col.from_nullable != col.to_nullable {
        let from = if col.from_nullable {
            "NULL"
        } else {
            "NOT NULL"
        };
        let to = if col.to_nullable { "NULL" } else { "NOT NULL" };
        changes.push(format!("{} → {}", from, to));
    }

    if col.from_default != col.to_default {
        let from = col.from_default.as_deref().unwrap_or("(none)");
        let to = col.to_default.as_deref().unwrap_or("(none)");
        changes.push(format!("default: {} → {}", from, to));
    }

    if col.from_identity != col.to_identity {
        let from = format_identity(&col.from_identity);
        let to = format_identity(&col.to_identity);
        changes.push(format!("identity: {} → {}", from, to));
    }

    if col.from_is_serial != col.to_is_serial {
        let from = if col.from_is_serial {
            "SERIAL"
        } else {
            "(none)"
        };
        let to = if col.to_is_serial { "SERIAL" } else { "(none)" };
        changes.push(format!("{} → {}", from, to));
    }

    changes.join(", ")
}

/// Format identity type for display
fn format_identity(identity: &Option<IdentityType>) -> &'static str {
    match identity {
        Some(IdentityType::Always) => "ALWAYS",
        Some(IdentityType::ByDefault) => "BY DEFAULT",
        None => "(none)",
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_column(name: &str, data_type: &str, nullable: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
            default: None,
            identity: None,
            is_serial: false,
            is_primary_key: false,
        }
    }

    fn make_table(schema: &str, name: &str, columns: Vec<Column>) -> Table {
        Table {
            schema: schema.to_string(),
            name: name.to_string(),
            columns,
            primary_key: None,
            partition_info: None,
            is_partition: false,
            parent_schema: None,
            parent_name: None,
            partition_bound: None,
        }
    }

    #[test]
    fn test_diff_empty_schemas() {
        let from = DatabaseSchema::default();
        let to = DatabaseSchema::default();
        let diff = diff_schemas(&from, &to);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_diff_added_table() {
        let from = DatabaseSchema::default();
        let to = DatabaseSchema {
            tables: vec![make_table(
                "public",
                "users",
                vec![make_column("id", "integer", false)],
            )],
            ..Default::default()
        };

        let diff = diff_schemas(&from, &to);
        assert_eq!(diff.added_tables.len(), 1);
        assert_eq!(diff.added_tables[0].name, "users");
        assert!(diff.removed_tables.is_empty());
    }

    #[test]
    fn test_diff_removed_table() {
        let from = DatabaseSchema {
            tables: vec![make_table(
                "public",
                "users",
                vec![make_column("id", "integer", false)],
            )],
            ..Default::default()
        };
        let to = DatabaseSchema::default();

        let diff = diff_schemas(&from, &to);
        assert!(diff.added_tables.is_empty());
        assert_eq!(diff.removed_tables.len(), 1);
        assert_eq!(diff.removed_tables[0].name, "users");
    }

    #[test]
    fn test_diff_added_column() {
        let from = DatabaseSchema {
            tables: vec![make_table(
                "public",
                "users",
                vec![make_column("id", "integer", false)],
            )],
            ..Default::default()
        };
        let to = DatabaseSchema {
            tables: vec![make_table(
                "public",
                "users",
                vec![
                    make_column("id", "integer", false),
                    make_column("name", "text", true),
                ],
            )],
            ..Default::default()
        };

        let diff = diff_schemas(&from, &to);
        assert!(diff.added_tables.is_empty());
        assert!(diff.removed_tables.is_empty());
        assert_eq!(diff.modified_tables.len(), 1);
        assert_eq!(diff.modified_tables[0].added_columns.len(), 1);
        assert_eq!(diff.modified_tables[0].added_columns[0].name, "name");
    }

    #[test]
    fn test_diff_modified_column_type() {
        let from = DatabaseSchema {
            tables: vec![make_table(
                "public",
                "users",
                vec![make_column("name", "varchar(100)", false)],
            )],
            ..Default::default()
        };
        let to = DatabaseSchema {
            tables: vec![make_table(
                "public",
                "users",
                vec![make_column("name", "text", false)],
            )],
            ..Default::default()
        };

        let diff = diff_schemas(&from, &to);
        assert_eq!(diff.modified_tables.len(), 1);
        assert_eq!(diff.modified_tables[0].modified_columns.len(), 1);
        assert_eq!(
            diff.modified_tables[0].modified_columns[0].from_type,
            "varchar(100)"
        );
        assert_eq!(diff.modified_tables[0].modified_columns[0].to_type, "text");
    }

    #[test]
    fn test_diff_enum_values() {
        let from = DatabaseSchema {
            enums: vec![EnumType {
                schema: "public".to_string(),
                name: "status".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            }],
            ..Default::default()
        };
        let to = DatabaseSchema {
            enums: vec![EnumType {
                schema: "public".to_string(),
                name: "status".to_string(),
                values: vec![
                    "active".to_string(),
                    "inactive".to_string(),
                    "pending".to_string(),
                ],
            }],
            ..Default::default()
        };

        let diff = diff_schemas(&from, &to);
        assert_eq!(diff.modified_enums.len(), 1);
        assert_eq!(diff.modified_enums[0].added_values, vec!["pending"]);
        assert!(diff.modified_enums[0].removed_values.is_empty());
    }

    #[test]
    fn test_is_empty_with_differences() {
        let diff = SchemaDiff {
            added_tables: vec![make_table("public", "users", vec![])],
            ..Default::default()
        };
        assert!(!diff.is_empty());
    }
}
