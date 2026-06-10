//! `pgcrate sql` (alias `query`) — the guarded SQL gateway.
//!
//! Read-only by default. Writes preview themselves: with `--allow-write` a
//! statement runs inside a transaction, reports what it *would* do (affected
//! count + sample rows), then **rolls back**. `--commit` (which implies
//! `--allow-write`) is the only way to actually apply a change. Expensive
//! statements are flagged via EXPLAIN before they run, and runaway result sets
//! are capped.

use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::io::Read;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::diagnostic::{DiagnosticSession, TimeoutConfig};

/// Default cap on rows printed/returned for a SELECT. `--limit 0` uncaps.
pub const DEFAULT_ROW_CAP: usize = 1000;

/// How many sample rows a dry-run write previews.
const DRY_RUN_SAMPLE_LIMIT: usize = 5;

/// Default EXPLAIN total-cost threshold above which a warning prints.
/// Postgres "cost" is in arbitrary planner units; ~50k corresponds roughly to
/// a sizable sequential scan. Tune via `[sql] cost_warn_threshold`.
pub const DEFAULT_COST_WARN_THRESHOLD: f64 = 50_000.0;

/// Options resolved from CLI flags + config for a single `sql` invocation.
pub struct SqlOptions {
    pub allow_write: bool,
    pub commit: bool,
    pub limit: Option<usize>,
    pub no_cost_check: bool,
    pub cost_warn_threshold: f64,
    /// Connect/statement/lock timeouts (from global CLI flags, same as DBA).
    pub timeouts: TimeoutConfig,
    pub quiet: bool,
    pub json: bool,
}

#[derive(Serialize)]
struct SqlResponse {
    ok: bool,
    /// Present only on a write path; describes whether changes were committed.
    #[serde(skip_serializing_if = "Option::is_none")]
    write: Option<WriteOutcome>,
    results: Vec<SqlResult>,
}

#[derive(Serialize)]
struct WriteOutcome {
    /// `true` when changes were committed, `false` when rolled back (dry run).
    committed: bool,
    /// Total rows affected across all write statements.
    rows_affected: u64,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum SqlResult {
    #[serde(rename = "query")]
    Query {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
        /// Rows withheld by the row cap (0 when nothing was truncated).
        #[serde(skip_serializing_if = "is_zero")]
        truncated: usize,
    },
    #[serde(rename = "command")]
    CommandComplete { rows: u64 },
    /// Sample rows previewed for a dry-run write (the wrapped RETURNING result).
    #[serde(rename = "sample")]
    Sample {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// Classification of a single parsed statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StmtKind {
    /// SELECT, SET, EXPLAIN, transaction control — never mutates.
    Read,
    /// INSERT/UPDATE/DELETE that the planner can EXPLAIN and that can be
    /// previewed with a RETURNING wrapper.
    Dml,
    /// Transactional DDL (CREATE TABLE/VIEW, ALTER, DROP, TRUNCATE, …). Runs
    /// inside a transaction so it can be dry-run, but EXPLAIN doesn't apply.
    Ddl,
    /// Statement that cannot run inside a transaction block (CREATE INDEX
    /// CONCURRENTLY, VACUUM). Dry-run is impossible; requires `--commit`.
    NonTransactional,
}

impl StmtKind {
    fn is_write(self) -> bool {
        !matches!(self, StmtKind::Read)
    }
}

/// Statement-level facts gathered from a single parse of the input.
struct ParsedSql {
    kinds: Vec<StmtKind>,
    /// `true` when the input is exactly one DML statement with no RETURNING
    /// clause — the only case where we wrap with RETURNING to sample rows.
    single_dml_no_returning: bool,
}

impl ParsedSql {
    fn has_write(&self) -> bool {
        self.kinds.iter().any(|k| k.is_write())
    }

    fn non_transactional(&self) -> bool {
        self.kinds
            .iter()
            .any(|k| matches!(k, StmtKind::NonTransactional))
    }

    /// Statements eligible for EXPLAIN cost gating (only DML; DDL/utility
    /// statements aren't plannable).
    fn explainable(&self) -> bool {
        self.kinds.iter().any(|k| matches!(k, StmtKind::Dml))
    }
}

pub async fn sql(database_url: &str, command: Option<&str>, opts: SqlOptions) -> Result<()> {
    let raw = match command {
        Some(c) => c.to_string(),
        None => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("read SQL from stdin")?;
            buf
        }
    };

    let sql = raw.trim();
    if sql.is_empty() {
        bail!(
            "No SQL provided. Use: pgcrate sql -c \"SELECT 1\" or echo \"SELECT 1\" | pgcrate sql"
        );
    }

    let parsed = classify(sql)?;
    let is_write = parsed.has_write();

    // Gate: a write with neither --allow-write nor --commit is refused, and the
    // message teaches the new preview-by-default model.
    if is_write && !opts.allow_write {
        bail!(
            "This SQL writes. By default writes are blocked.\n  \
             Preview it (runs in a transaction, then rolls back):\n    \
             pgcrate sql -c \"…\" --allow-write\n  \
             Apply it for real:\n    \
             pgcrate sql -c \"…\" --commit"
        );
    }

    // Non-transactional writes can't be dry-run. Under --allow-write (preview)
    // we refuse and point at --commit; under --commit we run them directly.
    if parsed.non_transactional() && !opts.commit {
        bail!(
            "This SQL contains a statement that cannot run inside a transaction \
             (e.g. CREATE INDEX CONCURRENTLY, VACUUM), so it cannot be previewed.\n  \
             Re-run with --commit to execute it directly:\n    \
             pgcrate sql -c \"…\" --commit"
        );
    }

    let session = DiagnosticSession::connect(database_url, opts.timeouts.clone()).await?;
    crate::diagnostic::setup_ctrlc_handler(session.cancel_token());
    let client = session.client();

    // Cost gating runs before any execution. EXPLAIN only applies to DML.
    let cost_warning = if is_write && !opts.no_cost_check && parsed.explainable() {
        cost_gate(client, sql, opts.cost_warn_threshold).await?
    } else {
        None
    };

    if is_write {
        run_write(client, sql, &parsed, &opts, cost_warning).await
    } else {
        run_read(client, sql, &opts).await
    }
}

/// Read-only path: execute and print/return rows, capping the result set.
async fn run_read(client: &Client, sql: &str, opts: &SqlOptions) -> Result<()> {
    let messages = client.simple_query(sql).await.context("execute SQL")?;
    let cap = row_cap(opts.limit);
    let results = collect_results(messages, cap);

    if opts.json {
        emit_json(opts, None, results);
        return Ok(());
    }
    if opts.quiet {
        return Ok(());
    }
    print_results(&results);
    Ok(())
}

/// Write path: dry-run (transaction → report → ROLLBACK) unless `--commit`.
async fn run_write(
    client: &Client,
    sql: &str,
    parsed: &ParsedSql,
    opts: &SqlOptions,
    cost_warning: Option<String>,
) -> Result<()> {
    // Non-transactional + --commit: execute directly, no transaction wrapper.
    if parsed.non_transactional() {
        let messages = client.simple_query(sql).await.context("execute SQL")?;
        let results = collect_results(messages, usize::MAX);
        let rows_affected = total_affected(&results);
        finish_write(opts, true, rows_affected, results, cost_warning);
        return Ok(());
    }

    client
        .batch_execute("BEGIN")
        .await
        .context("begin transaction")?;

    // From here on, ensure we always close the transaction even on error.
    let outcome = execute_in_tx(client, sql, parsed, opts).await;

    let (results, rows_affected) = match outcome {
        Ok(v) => v,
        Err(e) => {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(e);
        }
    };

    if opts.commit {
        client
            .batch_execute("COMMIT")
            .await
            .context("commit transaction")?;
        finish_write(opts, true, rows_affected, results, cost_warning);
    } else {
        client
            .batch_execute("ROLLBACK")
            .await
            .context("rollback transaction")?;
        finish_write(opts, false, rows_affected, results, cost_warning);
    }

    Ok(())
}

/// Run the write inside the open transaction, gathering command tags and (when
/// safe) a sample of affected rows via a RETURNING wrapper.
async fn execute_in_tx(
    client: &Client,
    sql: &str,
    parsed: &ParsedSql,
    opts: &SqlOptions,
) -> Result<(Vec<SqlResult>, u64)> {
    let messages = client.simple_query(sql).await.context("execute SQL")?;
    let mut results = collect_results(messages, usize::MAX);
    let rows_affected = total_affected(&results);

    // Sample rows only when it's safe and worthwhile: a single DML statement
    // without its own RETURNING. We wrap it so the rows would-be-affected can
    // be shown. The wrapper still rolls back with the outer transaction.
    if parsed.single_dml_no_returning && rows_affected > 0 {
        if let Some(sample) = sample_affected(client, sql, opts).await? {
            results.push(sample);
        }
    }

    Ok((results, rows_affected))
}

/// Wrap a single DML statement to capture a few affected rows for preview.
/// Returns `None` (silently) if wrapping fails — sampling is best-effort.
async fn sample_affected(
    client: &Client,
    sql: &str,
    opts: &SqlOptions,
) -> Result<Option<SqlResult>> {
    let stmt = sql.trim().trim_end_matches(';');
    let limit = match opts.limit {
        Some(0) => DRY_RUN_SAMPLE_LIMIT, // uncapped reads still bound the preview
        Some(n) => n.min(DRY_RUN_SAMPLE_LIMIT),
        None => DRY_RUN_SAMPLE_LIMIT,
    };
    let wrapped = format!(
        "WITH __pgcrate_preview AS ({stmt} RETURNING *) \
         SELECT * FROM __pgcrate_preview LIMIT {limit}"
    );

    match client.simple_query(&wrapped).await {
        Ok(messages) => {
            let collected = collect_results(messages, limit);
            // The wrapped statement produces exactly one query result.
            let found = collected.into_iter().find_map(|r| match r {
                SqlResult::Query { columns, rows, .. } => Some((columns, rows)),
                _ => None,
            });
            Ok(found.map(|(columns, rows)| SqlResult::Sample { columns, rows }))
        }
        // Wrapping isn't always valid (e.g. statement already errors, or a
        // construct RETURNING can't express). Fall back to count-only.
        Err(_) => Ok(None),
    }
}

/// EXPLAIN the input and return a warning string if estimated total cost
/// exceeds the threshold. Returns `None` when under threshold or not plannable.
async fn cost_gate(client: &Client, sql: &str, threshold: f64) -> Result<Option<String>> {
    let stmt = sql.trim().trim_end_matches(';');
    // Only a single statement is reliably EXPLAIN-able here; multi-statement
    // writes skip the gate (the parser already flagged them as a write).
    if stmt.contains(';') {
        return Ok(None);
    }

    let explain_sql = format!("EXPLAIN (FORMAT JSON) {stmt}");
    let row = match client.query_one(&explain_sql, &[]).await {
        Ok(r) => r,
        // EXPLAIN can fail (non-plannable statement, permissions). Don't block.
        Err(_) => return Ok(None),
    };

    let plan_json: serde_json::Value = row.get(0);
    let total_cost = plan_json
        .get(0)
        .and_then(|p| p.get("Plan"))
        .and_then(|p| p.get("Total Cost"))
        .and_then(|c| c.as_f64());

    match total_cost {
        Some(cost) if cost > threshold => Ok(Some(format!(
            "estimated query cost {cost:.0} exceeds threshold {threshold:.0} \
             — this may be expensive. Pass --no-cost-check to skip this check."
        ))),
        _ => Ok(None),
    }
}

/// Resolve the effective row cap. `Some(0)` means uncapped.
fn row_cap(limit: Option<usize>) -> usize {
    match limit {
        Some(0) => usize::MAX,
        Some(n) => n,
        None => DEFAULT_ROW_CAP,
    }
}

/// Sum affected-row counts across command-complete results.
fn total_affected(results: &[SqlResult]) -> u64 {
    results
        .iter()
        .filter_map(|r| match r {
            SqlResult::CommandComplete { rows } => Some(*rows),
            _ => None,
        })
        .sum()
}

/// Drive output for a completed write (human or JSON).
fn finish_write(
    opts: &SqlOptions,
    committed: bool,
    rows_affected: u64,
    results: Vec<SqlResult>,
    cost_warning: Option<String>,
) {
    if let Some(warning) = &cost_warning {
        if !opts.json {
            eprintln!("pgcrate: warning: {warning}");
        }
    }

    if opts.json {
        let write = Some(WriteOutcome {
            committed,
            rows_affected,
        });
        emit_json(opts, write, results);
        return;
    }

    if opts.quiet {
        return;
    }

    // Print any sample rows first, then the summary banner.
    print_results(&results);

    if committed {
        println!("\nCOMMITTED — {rows_affected} row(s) affected.");
    } else {
        println!(
            "\nDRY RUN — {rows_affected} row(s) would be affected. \
             Nothing was changed (rolled back)."
        );
        println!("Re-run with --commit to apply.");
    }
}

/// Convert simple-query messages into structured results, capping query rows.
fn collect_results(messages: Vec<SimpleQueryMessage>, cap: usize) -> Vec<SqlResult> {
    let mut results: Vec<SqlResult> = Vec::new();
    let mut current_columns: Option<Vec<String>> = None;
    let mut current_rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut current_total: usize = 0;

    for msg in messages {
        match msg {
            SimpleQueryMessage::RowDescription(cols) => {
                current_columns = Some(cols.iter().map(|c| c.name().to_string()).collect());
            }
            SimpleQueryMessage::Row(row) => {
                if current_columns.is_none() {
                    current_columns =
                        Some(row.columns().iter().map(|c| c.name().to_string()).collect());
                }
                current_total += 1;
                if current_rows.len() < cap {
                    let values: Vec<Option<String>> = (0..row.len())
                        .map(|i| row.get(i).map(|s| s.to_string()))
                        .collect();
                    current_rows.push(values);
                }
            }
            SimpleQueryMessage::CommandComplete(rows) => {
                if let Some(cols) = current_columns.take() {
                    let truncated = current_total.saturating_sub(current_rows.len());
                    results.push(SqlResult::Query {
                        columns: cols,
                        rows: std::mem::take(&mut current_rows),
                        truncated,
                    });
                    current_total = 0;
                }
                results.push(SqlResult::CommandComplete { rows });
            }
            _ => {}
        }
    }

    if let Some(cols) = current_columns.take() {
        let truncated = current_total.saturating_sub(current_rows.len());
        results.push(SqlResult::Query {
            columns: cols,
            rows: std::mem::take(&mut current_rows),
            truncated,
        });
    }

    results
}

fn emit_json(opts: &SqlOptions, write: Option<WriteOutcome>, results: Vec<SqlResult>) {
    let payload = SqlResponse {
        ok: true,
        write,
        results,
    };
    match serde_json::to_string_pretty(&payload) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("pgcrate: failed to serialize JSON: {e}"),
    }
    let _ = opts;
}

fn print_results(results: &[SqlResult]) {
    for result in results {
        match result {
            SqlResult::Query {
                columns,
                rows,
                truncated,
            } => {
                print_table(columns, rows);
                if *truncated > 0 {
                    println!("… +{truncated} more row(s) — use --limit N (or --limit 0 to uncap)");
                }
            }
            SqlResult::Sample { columns, rows } => {
                println!("Sample of affected rows:");
                print_table(columns, rows);
            }
            SqlResult::CommandComplete { rows } => {
                println!("OK ({rows} rows)");
            }
        }
    }
}

/// Statements that cannot run inside a transaction *and* that sqlparser 0.58
/// does not parse into a dedicated `Statement` variant. We detect these by
/// leading keyword so the user gets the "needs --commit" guidance rather than
/// a raw parse error.
fn leading_non_transactional(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    head.starts_with("VACUUM") || head.starts_with("REINDEX")
}

/// Parse the input once and classify each statement.
fn classify(sql: &str) -> Result<ParsedSql> {
    use sqlparser::ast::Statement;

    // VACUUM / REINDEX aren't parseable by sqlparser; short-circuit so the
    // caller can route them to the "requires --commit" path.
    if leading_non_transactional(sql) {
        return Ok(ParsedSql {
            kinds: vec![StmtKind::NonTransactional],
            single_dml_no_returning: false,
        });
    }

    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql).context("parse SQL")?;

    let mut kinds = Vec::with_capacity(statements.len());
    let mut dml_returnings: Vec<bool> = Vec::new();

    for stmt in &statements {
        let (kind, has_returning) = match stmt {
            Statement::Query(_)
            | Statement::Set(_)
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Explain { .. }
            | Statement::ExplainTable { .. } => (StmtKind::Read, false),

            Statement::Insert(insert) => (StmtKind::Dml, insert.returning.is_some()),
            Statement::Update { returning, .. } => (StmtKind::Dml, returning.is_some()),
            Statement::Delete(delete) => (StmtKind::Dml, delete.returning.is_some()),

            // CREATE INDEX CONCURRENTLY cannot run in a transaction.
            Statement::CreateIndex(idx) if idx.concurrently => (StmtKind::NonTransactional, false),

            // Everything else that mutates: transactional DDL/utility.
            _ => (StmtKind::Ddl, false),
        };

        if kind == StmtKind::Dml {
            dml_returnings.push(has_returning);
        }
        kinds.push(kind);
    }

    let single_dml_no_returning = statements.len() == 1
        && kinds.first() == Some(&StmtKind::Dml)
        && dml_returnings.first() == Some(&false);

    Ok(ParsedSql {
        kinds,
        single_dml_no_returning,
    })
}

fn print_table(columns: &[String], rows: &[Vec<Option<String>>]) {
    if columns.is_empty() {
        return;
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i >= widths.len() {
                continue;
            }
            let s = cell.as_deref().unwrap_or("NULL");
            widths[i] = widths[i].max(s.len());
        }
    }

    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    for row in rows {
        let line: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let s = row.get(i).and_then(|v| v.as_deref()).unwrap_or("NULL");
                format!("{:width$}", s, width = widths[i])
            })
            .collect();
        println!("{}", line.join(" | "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_is_read() {
        let p = classify("SELECT 1").unwrap();
        assert!(!p.has_write());
        assert!(!p.non_transactional());
    }

    #[test]
    fn update_is_write_and_samplable() {
        let p = classify("UPDATE users SET name = 'x' WHERE id = 1").unwrap();
        assert!(p.has_write());
        assert!(p.explainable());
        assert!(p.single_dml_no_returning);
    }

    #[test]
    fn update_with_returning_not_wrapped() {
        let p = classify("UPDATE users SET name = 'x' WHERE id = 1 RETURNING id").unwrap();
        assert!(p.has_write());
        assert!(!p.single_dml_no_returning);
    }

    #[test]
    fn insert_delete_are_dml() {
        assert!(
            classify("INSERT INTO t (a) VALUES (1)")
                .unwrap()
                .single_dml_no_returning
        );
        assert!(
            classify("DELETE FROM t WHERE a = 1")
                .unwrap()
                .single_dml_no_returning
        );
    }

    #[test]
    fn multi_statement_write_not_single() {
        let p = classify("UPDATE t SET a = 1; UPDATE t SET a = 2").unwrap();
        assert!(p.has_write());
        assert!(!p.single_dml_no_returning);
    }

    #[test]
    fn ddl_is_write_not_explainable() {
        let p = classify("CREATE TABLE t (id int)").unwrap();
        assert!(p.has_write());
        assert!(!p.explainable());
        assert!(!p.non_transactional());
    }

    #[test]
    fn drop_and_truncate_are_writes() {
        assert!(classify("DROP TABLE t").unwrap().has_write());
        assert!(classify("TRUNCATE t").unwrap().has_write());
        assert!(classify("ALTER TABLE t ADD COLUMN c int")
            .unwrap()
            .has_write());
    }

    #[test]
    fn create_index_concurrently_is_non_transactional() {
        let p = classify("CREATE INDEX CONCURRENTLY idx ON t (a)").unwrap();
        assert!(p.has_write());
        assert!(p.non_transactional());
    }

    #[test]
    fn plain_create_index_is_transactional() {
        let p = classify("CREATE INDEX idx ON t (a)").unwrap();
        assert!(p.has_write());
        assert!(!p.non_transactional());
    }

    #[test]
    fn vacuum_is_non_transactional() {
        assert!(classify("VACUUM ANALYZE t").unwrap().non_transactional());
    }

    #[test]
    fn row_cap_resolution() {
        assert_eq!(row_cap(None), DEFAULT_ROW_CAP);
        assert_eq!(row_cap(Some(0)), usize::MAX);
        assert_eq!(row_cap(Some(10)), 10);
    }
}
