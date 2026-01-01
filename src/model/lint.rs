use anyhow::{bail, Context, Result};
use sqlparser::ast::{
    Expr, GroupByExpr, Ident, LimitClause, ObjectName, ObjectNamePart, OrderByKind, Query, Select,
    SetExpr, Statement, Table, TableFactor, TableWithJoins,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::Path;

use super::{Model, Project, Relation};

/// Result of linting a model's dependencies
#[derive(Debug)]
pub struct LintDepsResult {
    pub inferred_model_deps: Vec<Relation>,
    pub unknown_relations: Vec<String>,
    pub unqualified_relations: Vec<String>,
}

/// Result of qualifying unqualified references in SQL
#[derive(Debug)]
pub struct QualifyResult {
    pub changed: bool,
    pub unqualified: Vec<String>,
    pub ambiguous: Vec<String>,
    pub unknown: Vec<String>,
}

/// Parse SQL and extract all table references as name parts
pub fn infer_relations_from_sql(sql: &str) -> Result<BTreeSet<Vec<String>>> {
    let dialect = PostgreSqlDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).context("parse SQL (Postgres dialect)")?;
    if statements.len() != 1 {
        bail!(
            "expected exactly one SQL statement, found {}",
            statements.len()
        );
    }
    let stmt = statements.remove(0);
    let mut relations: BTreeSet<Vec<String>> = BTreeSet::new();
    let mut cte_stack = Vec::new();
    collect_relations_from_statement(&stmt, &mut relations, &mut cte_stack)?;
    Ok(relations)
}

fn collect_relations_from_statement(
    stmt: &Statement,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match stmt {
        Statement::Query(q) => collect_relations_from_query(q, out, cte_stack),
        other => bail!("unsupported statement kind in model (expected query): {other}"),
    }
}

fn collect_relations_from_query(
    query: &Query,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    if let Some(with) = &query.with {
        let mut ctes = HashSet::new();
        for cte in &with.cte_tables {
            ctes.insert(cte.alias.name.value.clone());
        }
        cte_stack.push(ctes);
    }

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_relations_from_query(&cte.query, out, cte_stack)?;
        }
    }

    collect_relations_from_setexpr(&query.body, out, cte_stack)?;

    if let Some(order_by) = &query.order_by {
        if let OrderByKind::Expressions(exprs) = &order_by.kind {
            for expr in exprs {
                collect_relations_from_expr(&expr.expr, out, cte_stack)?;
            }
        }
    }
    if let Some(limit_clause) = &query.limit_clause {
        match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if let Some(limit) = limit {
                    collect_relations_from_expr(limit, out, cte_stack)?;
                }
                if let Some(offset) = offset {
                    collect_relations_from_expr(&offset.value, out, cte_stack)?;
                }
                for expr in limit_by {
                    collect_relations_from_expr(expr, out, cte_stack)?;
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                collect_relations_from_expr(offset, out, cte_stack)?;
                collect_relations_from_expr(limit, out, cte_stack)?;
            }
        }
    }
    if let Some(fetch) = &query.fetch {
        if let Some(qty) = &fetch.quantity {
            collect_relations_from_expr(qty, out, cte_stack)?;
        }
    }

    if query.with.is_some() {
        cte_stack.pop();
    }

    Ok(())
}

fn collect_relations_from_setexpr(
    setexpr: &SetExpr,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match setexpr {
        SetExpr::Select(select) => collect_relations_from_select(select, out, cte_stack),
        SetExpr::Query(query) => collect_relations_from_query(query, out, cte_stack),
        SetExpr::SetOperation { left, right, .. } => {
            collect_relations_from_setexpr(left, out, cte_stack)?;
            collect_relations_from_setexpr(right, out, cte_stack)?;
            Ok(())
        }
        SetExpr::Values(_) => Ok(()),
        SetExpr::Insert(stmt) | SetExpr::Update(stmt) | SetExpr::Delete(stmt) => {
            collect_relations_from_statement(stmt, out, cte_stack)
        }
        SetExpr::Table(t) => collect_relations_from_table(t, out, cte_stack),
    }
}

fn collect_relations_from_table(
    table: &Table,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &[HashSet<String>],
) -> Result<()> {
    match (&table.schema_name, &table.table_name) {
        (Some(schema), Some(table)) => {
            out.insert(vec![schema.clone(), table.clone()]);
        }
        (None, Some(table)) => {
            if !is_cte_name(cte_stack, table) {
                out.insert(vec![table.clone()]);
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_relations_from_select(
    select: &Select,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    for table in &select.from {
        collect_relations_from_table_with_joins(table, out, cte_stack)?;
    }
    for expr in &select.projection {
        match expr {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => {
                collect_relations_from_expr(e, out, cte_stack)?
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                collect_relations_from_expr(expr, out, cte_stack)?
            }
            _ => {}
        }
    }
    if let Some(selection) = &select.selection {
        collect_relations_from_expr(selection, out, cte_stack)?;
    }
    match &select.group_by {
        GroupByExpr::All(_) => {}
        GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                collect_relations_from_expr(expr, out, cte_stack)?;
            }
        }
    }
    if let Some(having) = &select.having {
        collect_relations_from_expr(having, out, cte_stack)?;
    }
    Ok(())
}

fn collect_relations_from_table_with_joins(
    table: &TableWithJoins,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    collect_relations_from_table_factor(&table.relation, out, cte_stack)?;
    for join in &table.joins {
        collect_relations_from_table_factor(&join.relation, out, cte_stack)?;
        let constraint = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(c)
            | sqlparser::ast::JoinOperator::LeftOuter(c)
            | sqlparser::ast::JoinOperator::RightOuter(c)
            | sqlparser::ast::JoinOperator::FullOuter(c)
            | sqlparser::ast::JoinOperator::LeftSemi(c)
            | sqlparser::ast::JoinOperator::RightSemi(c)
            | sqlparser::ast::JoinOperator::LeftAnti(c)
            | sqlparser::ast::JoinOperator::RightAnti(c) => Some(c),
            _ => None,
        };
        if let Some(sqlparser::ast::JoinConstraint::On(expr)) = constraint {
            collect_relations_from_expr(expr, out, cte_stack)?;
        }
    }
    Ok(())
}

fn collect_relations_from_table_factor(
    tf: &TableFactor,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match tf {
        TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name.0.iter().map(|p| p.to_string()).collect();
            if parts.len() == 1 && is_cte_name(cte_stack, &parts[0]) {
                return Ok(());
            }
            out.insert(parts);
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => {
            collect_relations_from_query(subquery, out, cte_stack)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_relations_from_table_with_joins(table_with_joins, out, cte_stack),
        _ => Ok(()),
    }
}

fn collect_relations_from_expr(
    expr: &Expr,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match expr {
        Expr::Subquery(q)
        | Expr::Exists { subquery: q, .. }
        | Expr::InSubquery { subquery: q, .. } => collect_relations_from_query(q, out, cte_stack),
        Expr::BinaryOp { left, right, .. } => {
            collect_relations_from_expr(left, out, cte_stack)?;
            collect_relations_from_expr(right, out, cte_stack)?;
            Ok(())
        }
        Expr::UnaryOp { expr, .. } => collect_relations_from_expr(expr, out, cte_stack),
        Expr::Nested(e) => collect_relations_from_expr(e, out, cte_stack),
        Expr::Cast { expr, .. } => collect_relations_from_expr(expr, out, cte_stack),
        Expr::Extract { expr, .. } => collect_relations_from_expr(expr, out, cte_stack),
        Expr::Collate { expr, .. } => collect_relations_from_expr(expr, out, cte_stack),
        Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsNotTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => collect_relations_from_expr(e, out, cte_stack),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_relations_from_expr(expr, out, cte_stack)?;
            collect_relations_from_expr(low, out, cte_stack)?;
            collect_relations_from_expr(high, out, cte_stack)?;
            Ok(())
        }
        Expr::InList { expr, list, .. } => {
            collect_relations_from_expr(expr, out, cte_stack)?;
            for item in list {
                collect_relations_from_expr(item, out, cte_stack)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_relations_from_expr(op, out, cte_stack)?;
            }
            for when in conditions {
                collect_relations_from_expr(&when.condition, out, cte_stack)?;
                collect_relations_from_expr(&when.result, out, cte_stack)?;
            }
            if let Some(e) = else_result {
                collect_relations_from_expr(e, out, cte_stack)?;
            }
            Ok(())
        }
        Expr::Function(f) => {
            collect_relations_from_function_args(&f.parameters, out, cte_stack)?;
            collect_relations_from_function_args(&f.args, out, cte_stack)?;
            if let Some(filter) = &f.filter {
                collect_relations_from_expr(filter, out, cte_stack)?;
            }
            for ob in &f.within_group {
                collect_relations_from_expr(&ob.expr, out, cte_stack)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_relations_from_function_args(
    args: &sqlparser::ast::FunctionArguments,
    out: &mut BTreeSet<Vec<String>>,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match args {
        sqlparser::ast::FunctionArguments::None => Ok(()),
        sqlparser::ast::FunctionArguments::Subquery(q) => {
            collect_relations_from_query(q, out, cte_stack)
        }
        sqlparser::ast::FunctionArguments::List(list) => {
            for arg in &list.args {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(a) => {
                        if let sqlparser::ast::FunctionArgExpr::Expr(e) = a {
                            collect_relations_from_expr(e, out, cte_stack)?
                        }
                    }
                    sqlparser::ast::FunctionArg::Named { arg, .. } => {
                        if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                            collect_relations_from_expr(e, out, cte_stack)?
                        }
                    }
                    sqlparser::ast::FunctionArg::ExprNamed { name, arg, .. } => {
                        collect_relations_from_expr(name, out, cte_stack)?;
                        if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                            collect_relations_from_expr(e, out, cte_stack)?
                        }
                    }
                }
            }
            for clause in &list.clauses {
                if let sqlparser::ast::FunctionArgumentClause::OrderBy(order_by_exprs) = clause {
                    for ob in order_by_exprs {
                        collect_relations_from_expr(&ob.expr, out, cte_stack)?;
                    }
                }
            }
            Ok(())
        }
    }
}

fn is_cte_name(cte_stack: &[HashSet<String>], name: &str) -> bool {
    for scope in cte_stack.iter().rev() {
        if scope.contains(name) {
            return true;
        }
    }
    false
}

/// Lint a model's declared deps against what's inferred from SQL
pub fn lint_deps(project: &Project, model: &Model) -> Result<LintDepsResult> {
    let rels = infer_relations_from_sql(&model.body_sql)?;

    let mut inferred_model_deps: BTreeSet<Relation> = BTreeSet::new();
    let mut unknown_relations: BTreeSet<String> = BTreeSet::new();
    let mut unqualified_relations: BTreeSet<String> = BTreeSet::new();

    for parts in rels {
        match parts.len() {
            1 => {
                unqualified_relations.insert(parts[0].clone());
            }
            2 => {
                let rel = Relation {
                    schema: parts[0].clone(),
                    name: parts[1].clone(),
                };
                if project.models.contains_key(&rel) {
                    if rel != model.id {
                        inferred_model_deps.insert(rel);
                    }
                } else if !project.sources.contains(&rel) {
                    // Not a model and not a declared source = unknown
                    unknown_relations.insert(rel.to_string());
                }
                // If it's a source, we don't need to track it
            }
            _ => {
                // Over-qualified (e.g. db.schema.table) - treat as unknown
                unknown_relations.insert(parts.join("."));
            }
        }
    }

    Ok(LintDepsResult {
        inferred_model_deps: inferred_model_deps.into_iter().collect(),
        unknown_relations: unknown_relations.into_iter().collect(),
        unqualified_relations: unqualified_relations.into_iter().collect(),
    })
}

/// Rewrite the deps line in a model file
pub fn rewrite_deps_line(model_path: &Path, new_deps: &[Relation]) -> Result<()> {
    let text = fs::read_to_string(model_path)
        .with_context(|| format!("read model for rewrite: {}", model_path.display()))?;
    let mut out: Vec<String> = Vec::new();

    let new_line = if new_deps.is_empty() {
        "-- deps:".to_string()
    } else {
        let mut parts: Vec<String> = new_deps.iter().map(|r| r.to_string()).collect();
        parts.sort();
        format!("-- deps: {}", parts.join(", "))
    };

    let mut in_header = true;
    let mut replaced = false;

    for line in text.lines() {
        if in_header {
            if line.trim_start().starts_with("--") || line.trim().is_empty() {
                if line.trim_start().starts_with("-- deps:") {
                    out.push(new_line.clone());
                    replaced = true;
                } else {
                    out.push(line.to_string());
                }
                continue;
            }
            in_header = false;
        }
        out.push(line.to_string());
    }

    if !replaced {
        bail!(
            "missing required '-- deps:' line in header: {}",
            model_path.display()
        );
    }

    fs::write(model_path, out.join("\n") + "\n")
        .with_context(|| format!("write model: {}", model_path.display()))?;
    Ok(())
}

/// Qualify unqualified table references in model SQL
pub fn qualify_model_sql(
    project: &Project,
    model: &Model,
) -> Result<(QualifyResult, Option<String>)> {
    let dialect = PostgreSqlDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, &model.body_sql).context("parse SQL (Postgres dialect)")?;
    if statements.len() != 1 {
        bail!(
            "expected exactly one SQL statement, found {}",
            statements.len()
        );
    }

    let mut stmt = statements.remove(0);
    let mut result = QualifyResult {
        changed: false,
        unqualified: Vec::new(),
        ambiguous: Vec::new(),
        unknown: Vec::new(),
    };

    let mut cte_stack = Vec::new();
    qualify_statement_in_place(project, model, &mut stmt, &mut result, &mut cte_stack)?;

    let new_sql = if result.changed {
        Some(stmt.to_string())
    } else {
        None
    };

    Ok((result, new_sql))
}

fn qualify_statement_in_place(
    project: &Project,
    model: &Model,
    stmt: &mut Statement,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match stmt {
        Statement::Query(q) => qualify_query_in_place(project, model, q, result, cte_stack),
        other => bail!("unsupported statement kind in model (expected query): {other}"),
    }
}

fn qualify_query_in_place(
    project: &Project,
    model: &Model,
    query: &mut Query,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    if let Some(with) = &mut query.with {
        let mut ctes = HashSet::new();
        for cte in &with.cte_tables {
            ctes.insert(cte.alias.name.value.clone());
        }
        cte_stack.push(ctes);
        for cte in &mut with.cte_tables {
            qualify_query_in_place(project, model, &mut cte.query, result, cte_stack)?;
        }
    }

    qualify_setexpr_in_place(project, model, &mut query.body, result, cte_stack)?;

    if let Some(order_by) = &mut query.order_by {
        if let OrderByKind::Expressions(exprs) = &mut order_by.kind {
            for expr in exprs {
                qualify_expr_in_place(project, model, &mut expr.expr, result, cte_stack)?;
            }
        }
    }

    if let Some(limit_clause) = &mut query.limit_clause {
        match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if let Some(limit) = limit {
                    qualify_expr_in_place(project, model, limit, result, cte_stack)?;
                }
                if let Some(offset) = offset {
                    qualify_expr_in_place(project, model, &mut offset.value, result, cte_stack)?;
                }
                for expr in limit_by {
                    qualify_expr_in_place(project, model, expr, result, cte_stack)?;
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                qualify_expr_in_place(project, model, offset, result, cte_stack)?;
                qualify_expr_in_place(project, model, limit, result, cte_stack)?;
            }
        }
    }

    if let Some(fetch) = &mut query.fetch {
        if let Some(qty) = &mut fetch.quantity {
            qualify_expr_in_place(project, model, qty, result, cte_stack)?;
        }
    }

    if query.with.is_some() {
        cte_stack.pop();
    }

    Ok(())
}

fn qualify_setexpr_in_place(
    project: &Project,
    model: &Model,
    setexpr: &mut SetExpr,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match setexpr {
        SetExpr::Select(select) => {
            qualify_select_in_place(project, model, select, result, cte_stack)
        }
        SetExpr::Query(query) => qualify_query_in_place(project, model, query, result, cte_stack),
        SetExpr::SetOperation { left, right, .. } => {
            qualify_setexpr_in_place(project, model, left, result, cte_stack)?;
            qualify_setexpr_in_place(project, model, right, result, cte_stack)?;
            Ok(())
        }
        SetExpr::Values(_) => Ok(()),
        SetExpr::Insert(stmt) | SetExpr::Update(stmt) | SetExpr::Delete(stmt) => {
            qualify_statement_in_place(project, model, stmt, result, cte_stack)
        }
        SetExpr::Table(_) => Ok(()),
    }
}

fn qualify_select_in_place(
    project: &Project,
    model: &Model,
    select: &mut Select,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    for table in &mut select.from {
        qualify_table_with_joins_in_place(project, model, table, result, cte_stack)?;
    }

    for item in &mut select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => {
                qualify_expr_in_place(project, model, e, result, cte_stack)?;
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                qualify_expr_in_place(project, model, expr, result, cte_stack)?;
            }
            _ => {}
        }
    }

    if let Some(selection) = &mut select.selection {
        qualify_expr_in_place(project, model, selection, result, cte_stack)?;
    }

    match &mut select.group_by {
        GroupByExpr::All(_) => {}
        GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                qualify_expr_in_place(project, model, expr, result, cte_stack)?;
            }
        }
    }

    if let Some(having) = &mut select.having {
        qualify_expr_in_place(project, model, having, result, cte_stack)?;
    }

    Ok(())
}

fn qualify_table_with_joins_in_place(
    project: &Project,
    model: &Model,
    table: &mut TableWithJoins,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    qualify_table_factor_in_place(project, model, &mut table.relation, result, cte_stack)?;

    for join in &mut table.joins {
        qualify_table_factor_in_place(project, model, &mut join.relation, result, cte_stack)?;
        let constraint = match &mut join.join_operator {
            sqlparser::ast::JoinOperator::Inner(c)
            | sqlparser::ast::JoinOperator::LeftOuter(c)
            | sqlparser::ast::JoinOperator::RightOuter(c)
            | sqlparser::ast::JoinOperator::FullOuter(c)
            | sqlparser::ast::JoinOperator::LeftSemi(c)
            | sqlparser::ast::JoinOperator::RightSemi(c)
            | sqlparser::ast::JoinOperator::LeftAnti(c)
            | sqlparser::ast::JoinOperator::RightAnti(c) => Some(c),
            _ => None,
        };
        if let Some(sqlparser::ast::JoinConstraint::On(expr)) = constraint {
            qualify_expr_in_place(project, model, expr, result, cte_stack)?;
        }
    }
    Ok(())
}

#[derive(Debug)]
enum QualifyError {
    Unknown,
    Ambiguous(Vec<Relation>),
}

fn qualify_table_factor_in_place(
    project: &Project,
    model: &Model,
    tf: &mut TableFactor,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match tf {
        TableFactor::Table { name, .. } => {
            if name.0.len() == 1 {
                let raw = name.0[0].to_string();
                if is_cte_name(cte_stack, &raw) {
                    return Ok(());
                }
                match unique_qualification(project, model, &raw) {
                    Ok(Some(qualified)) => {
                        *name = relation_to_object_name(&qualified);
                        result.changed = true;
                    }
                    Ok(None) => {
                        result.unqualified.push(raw);
                    }
                    Err(QualifyError::Ambiguous(candidates)) => {
                        result.ambiguous.push(format!(
                            "{} (candidates: {})",
                            raw,
                            candidates
                                .into_iter()
                                .map(|r| r.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ));
                    }
                    Err(QualifyError::Unknown) => {
                        result.unknown.push(raw);
                    }
                }
            }
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => {
            qualify_query_in_place(project, model, subquery, result, cte_stack)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => qualify_table_with_joins_in_place(project, model, table_with_joins, result, cte_stack),
        _ => Ok(()),
    }
}

fn qualify_expr_in_place(
    project: &Project,
    model: &Model,
    expr: &mut Expr,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match expr {
        Expr::Subquery(q)
        | Expr::Exists { subquery: q, .. }
        | Expr::InSubquery { subquery: q, .. } => {
            qualify_query_in_place(project, model, q, result, cte_stack)
        }
        Expr::BinaryOp { left, right, .. } => {
            qualify_expr_in_place(project, model, left, result, cte_stack)?;
            qualify_expr_in_place(project, model, right, result, cte_stack)?;
            Ok(())
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Collate { expr, .. } => {
            qualify_expr_in_place(project, model, expr, result, cte_stack)
        }
        Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsNotTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => qualify_expr_in_place(project, model, e, result, cte_stack),
        Expr::Between {
            expr, low, high, ..
        } => {
            qualify_expr_in_place(project, model, expr, result, cte_stack)?;
            qualify_expr_in_place(project, model, low, result, cte_stack)?;
            qualify_expr_in_place(project, model, high, result, cte_stack)?;
            Ok(())
        }
        Expr::InList { expr, list, .. } => {
            qualify_expr_in_place(project, model, expr, result, cte_stack)?;
            for item in list {
                qualify_expr_in_place(project, model, item, result, cte_stack)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                qualify_expr_in_place(project, model, op, result, cte_stack)?;
            }
            for when in conditions {
                qualify_expr_in_place(project, model, &mut when.condition, result, cte_stack)?;
                qualify_expr_in_place(project, model, &mut when.result, result, cte_stack)?;
            }
            if let Some(e) = else_result {
                qualify_expr_in_place(project, model, e, result, cte_stack)?;
            }
            Ok(())
        }
        Expr::Function(f) => {
            qualify_function_args_in_place(project, model, &mut f.parameters, result, cte_stack)?;
            qualify_function_args_in_place(project, model, &mut f.args, result, cte_stack)?;
            if let Some(filter) = &mut f.filter {
                qualify_expr_in_place(project, model, filter, result, cte_stack)?;
            }
            for ob in &mut f.within_group {
                qualify_expr_in_place(project, model, &mut ob.expr, result, cte_stack)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn qualify_function_args_in_place(
    project: &Project,
    model: &Model,
    args: &mut sqlparser::ast::FunctionArguments,
    result: &mut QualifyResult,
    cte_stack: &mut Vec<HashSet<String>>,
) -> Result<()> {
    match args {
        sqlparser::ast::FunctionArguments::None => Ok(()),
        sqlparser::ast::FunctionArguments::Subquery(q) => {
            qualify_query_in_place(project, model, q, result, cte_stack)
        }
        sqlparser::ast::FunctionArguments::List(list) => {
            for arg in &mut list.args {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(a) => {
                        if let sqlparser::ast::FunctionArgExpr::Expr(e) = a {
                            qualify_expr_in_place(project, model, e, result, cte_stack)?
                        }
                    }
                    sqlparser::ast::FunctionArg::Named { arg, .. } => {
                        if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                            qualify_expr_in_place(project, model, e, result, cte_stack)?
                        }
                    }
                    sqlparser::ast::FunctionArg::ExprNamed { name, arg, .. } => {
                        qualify_expr_in_place(project, model, name, result, cte_stack)?;
                        if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                            qualify_expr_in_place(project, model, e, result, cte_stack)?
                        }
                    }
                }
            }
            for clause in &mut list.clauses {
                if let sqlparser::ast::FunctionArgumentClause::OrderBy(order_by_exprs) = clause {
                    for ob in order_by_exprs {
                        qualify_expr_in_place(project, model, &mut ob.expr, result, cte_stack)?;
                    }
                }
            }
            Ok(())
        }
    }
}

fn unique_qualification(
    project: &Project,
    model: &Model,
    unqualified: &str,
) -> std::result::Result<Option<Relation>, QualifyError> {
    let candidates = candidates_by_name(project, unqualified);
    if candidates.is_empty() {
        return Err(QualifyError::Unknown);
    }

    let candidates: Vec<Relation> = candidates.into_iter().filter(|r| r != &model.id).collect();
    if candidates.is_empty() {
        return Ok(None);
    }
    if candidates.len() == 1 {
        return Ok(Some(candidates[0].clone()));
    }
    Err(QualifyError::Ambiguous(candidates))
}

fn candidates_by_name(project: &Project, name: &str) -> Vec<Relation> {
    let mut out = Vec::new();
    for rel in project.models.keys() {
        if rel.name == name {
            out.push(rel.clone());
        }
    }
    for rel in project.sources.iter() {
        if rel.name == name {
            out.push(rel.clone());
        }
    }
    out.sort();
    out
}

fn relation_to_object_name(rel: &Relation) -> ObjectName {
    ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new(rel.schema.clone())),
        ObjectNamePart::Identifier(Ident::new(rel.name.clone())),
    ])
}

/// Rewrite the SQL body of a model file
pub fn rewrite_model_body_sql(model_path: &Path, new_body_sql: &str) -> Result<()> {
    let text = fs::read_to_string(model_path)
        .with_context(|| format!("read model for rewrite: {}", model_path.display()))?;
    let mut out: Vec<String> = Vec::new();

    let lines = text.lines();
    for line in lines {
        if line.trim_start().starts_with("--") || line.trim().is_empty() {
            out.push(line.to_string());
            continue;
        }
        break;
    }

    // Ensure exactly one blank line between header and body
    while out.last().is_some_and(|l| l.trim().is_empty()) {
        out.pop();
    }
    out.push(String::new());

    let mut body = new_body_sql.trim().to_string();
    if !body.ends_with(';') {
        body.push(';');
    }
    out.push(body);

    fs::write(model_path, out.join("\n") + "\n")
        .with_context(|| format!("write model: {}", model_path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_simple_select() {
        let sql = "SELECT * FROM analytics.users";
        let rels = infer_relations_from_sql(sql).unwrap();
        assert_eq!(rels.len(), 1);
        assert!(rels
            .iter()
            .any(|r| r == &vec!["analytics".to_string(), "users".to_string()]));
    }

    #[test]
    fn test_infer_join() {
        let sql = "SELECT * FROM a.x JOIN b.y ON a.x.id = b.y.id";
        let rels = infer_relations_from_sql(sql).unwrap();
        assert_eq!(rels.len(), 2);
    }

    #[test]
    fn test_infer_cte() {
        let sql = "WITH temp AS (SELECT * FROM a.source) SELECT * FROM temp";
        let rels = infer_relations_from_sql(sql).unwrap();
        assert_eq!(rels.len(), 1);
        assert!(rels
            .iter()
            .any(|r| r == &vec!["a".to_string(), "source".to_string()]));
    }

    #[test]
    fn test_infer_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM a.inner) AS sub";
        let rels = infer_relations_from_sql(sql).unwrap();
        assert_eq!(rels.len(), 1);
    }

    #[test]
    fn test_infer_union() {
        let sql = "SELECT * FROM a.x UNION ALL SELECT * FROM b.y";
        let rels = infer_relations_from_sql(sql).unwrap();
        assert_eq!(rels.len(), 2);
    }

    #[test]
    fn test_infer_exists() {
        let sql = "SELECT * FROM a.x WHERE EXISTS (SELECT 1 FROM b.y WHERE b.y.id = a.x.id)";
        let rels = infer_relations_from_sql(sql).unwrap();
        assert_eq!(rels.len(), 2);
    }

    #[test]
    fn test_infer_unqualified() {
        let sql = "SELECT * FROM users";
        let rels = infer_relations_from_sql(sql).unwrap();
        assert_eq!(rels.len(), 1);
        assert!(rels.iter().any(|r| r == &vec!["users".to_string()]));
    }
}
