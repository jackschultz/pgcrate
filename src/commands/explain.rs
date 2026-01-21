//! Explain command: Query plan analysis with recommendations.
//!
//! Analyzes query execution plans to identify performance issues and
//! suggest optimizations. Safe by default (EXPLAIN only), with optional
//! EXPLAIN ANALYZE for actual execution statistics.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use super::fix::common::{ActionGates, ActionType, Risk, StructuredAction};

/// Issue severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IssueSeverity {
    Info,
    Warning,
    Critical,
}

/// Types of plan issues we detect
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueType {
    SeqScanLargeTable,
    MissingIndexOnFilter,
    NestedLoopLargeSet,
    SortWithoutIndex,
    HighCostEstimate,
}

/// A detected issue in the query plan
#[derive(Debug, Clone, Serialize)]
pub struct PlanIssue {
    pub issue_type: IssueType,
    pub severity: IssueSeverity,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

/// Types of recommendations
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum RecommendationType {
    CreateIndex,
    ConsiderIndex,
    ReviewQuery,
}

/// A recommendation for improving the query
#[derive(Debug, Clone, Serialize)]
pub struct Recommendation {
    pub recommendation_type: RecommendationType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    pub rationale: String,
}

/// Statistics from the explain output
#[derive(Debug, Clone, Serialize)]
pub struct PlanStats {
    pub estimated_startup_cost: f64,
    pub estimated_total_cost: f64,
    pub estimated_rows: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_time_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_rows: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_loops: Option<i64>,
}

/// Full explain results
#[derive(Debug, Serialize)]
pub struct ExplainResult {
    pub query: String,
    pub plan_text: String,
    pub plan_json: serde_json::Value,
    pub analyzed: bool,
    pub issues: Vec<PlanIssue>,
    pub recommendations: Vec<Recommendation>,
    pub stats: PlanStats,
    /// Structured fix actions (when --include-actions is used)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<Vec<StructuredAction>>,
}

/// PostgreSQL EXPLAIN JSON format (simplified)
#[derive(Debug, Deserialize)]
struct ExplainOutput(Vec<ExplainPlan>);

#[derive(Debug, Deserialize)]
struct ExplainPlan {
    #[serde(rename = "Plan")]
    plan: PlanNode,
    #[serde(rename = "Planning Time")]
    #[allow(dead_code)]
    planning_time: Option<f64>,
    #[serde(rename = "Execution Time")]
    execution_time: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)] // Many fields from PostgreSQL EXPLAIN JSON we don't use yet
struct PlanNode {
    #[serde(rename = "Node Type")]
    node_type: String,
    #[serde(rename = "Relation Name")]
    relation_name: Option<String>,
    #[serde(rename = "Schema")]
    schema: Option<String>,
    #[serde(rename = "Alias")]
    #[allow(dead_code)]
    alias: Option<String>,
    #[serde(rename = "Startup Cost")]
    startup_cost: Option<f64>,
    #[serde(rename = "Total Cost")]
    total_cost: Option<f64>,
    #[serde(rename = "Plan Rows")]
    plan_rows: Option<i64>,
    #[serde(rename = "Plan Width")]
    #[allow(dead_code)]
    plan_width: Option<i32>,
    #[serde(rename = "Actual Startup Time")]
    actual_startup_time: Option<f64>,
    #[serde(rename = "Actual Total Time")]
    actual_total_time: Option<f64>,
    #[serde(rename = "Actual Rows")]
    actual_rows: Option<i64>,
    #[serde(rename = "Actual Loops")]
    actual_loops: Option<i64>,
    #[serde(rename = "Filter")]
    filter: Option<String>,
    #[serde(rename = "Index Cond")]
    #[allow(dead_code)]
    index_cond: Option<String>,
    #[serde(rename = "Index Name")]
    #[allow(dead_code)]
    index_name: Option<String>,
    #[serde(rename = "Sort Key")]
    sort_key: Option<Vec<String>>,
    #[serde(rename = "Plans")]
    plans: Option<Vec<PlanNode>>,
}

/// Threshold for "large table" in sequential scan detection
const LARGE_TABLE_ROWS: i64 = 10_000;
/// Threshold for "high cost" warning
const HIGH_COST_THRESHOLD: f64 = 10_000.0;
/// Threshold for nested loop warning
const NESTED_LOOP_ROWS: i64 = 1_000;

/// Run EXPLAIN on a query and analyze the plan
/// Get set of existing indexes as "schema.table.column" keys
async fn get_existing_indexes(client: &Client) -> Result<std::collections::HashSet<String>> {
    let query = r#"
        SELECT
            n.nspname AS schema,
            t.relname AS table,
            a.attname AS column
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    "#;

    let rows = client.query(query, &[]).await?;
    let mut indexes = std::collections::HashSet::new();

    for row in rows {
        let schema: String = row.get("schema");
        let table: String = row.get("table");
        let column: String = row.get("column");
        indexes.insert(format!("{}.{}.{}", schema, table, column));
    }

    Ok(indexes)
}

/// Parse index target from CREATE INDEX SQL
/// Returns (schema, table, column) if parseable
fn parse_index_target(sql: &str) -> Option<(String, String, String)> {
    // Pattern: CREATE INDEX ... ON schema.table(column);
    let sql_upper = sql.to_uppercase();
    let on_pos = sql_upper.find(" ON ")?;
    let after_on = &sql[on_pos + 4..];

    // Find the opening paren
    let paren_pos = after_on.find('(')?;
    let table_part = after_on[..paren_pos].trim();

    // Parse schema.table
    let (schema, table) = if let Some(dot_pos) = table_part.find('.') {
        (
            table_part[..dot_pos].to_string(),
            table_part[dot_pos + 1..].to_string(),
        )
    } else {
        ("public".to_string(), table_part.to_string())
    };

    // Parse column (between parens)
    let close_paren = after_on.find(')')?;
    let column = after_on[paren_pos + 1..close_paren].trim().to_string();

    Some((schema, table, column))
}

pub async fn run_explain(client: &Client, query: &str, analyze: bool) -> Result<ExplainResult> {
    // Build EXPLAIN command
    let explain_opts = if analyze {
        "ANALYZE, FORMAT JSON, VERBOSE, BUFFERS"
    } else {
        "FORMAT JSON, VERBOSE"
    };

    let explain_query = format!("EXPLAIN ({}) {}", explain_opts, query);

    // Execute EXPLAIN - returns JSON directly with tokio-postgres serde_json feature
    let row = client
        .query_one(&explain_query, &[])
        .await
        .context("Failed to execute EXPLAIN")?;

    // Get JSON directly (requires tokio-postgres with-serde_json-1 feature)
    let plan_json: serde_json::Value = row.get(0);

    // Also get text format for human output
    let text_query = format!("EXPLAIN {}", query);
    let text_rows = client.query(&text_query, &[]).await?;
    let plan_text: String = text_rows
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");

    // Parse the JSON plan
    let explain_output: ExplainOutput =
        serde_json::from_value(plan_json.clone()).context("Failed to parse EXPLAIN JSON output")?;

    let plan = &explain_output.0[0];
    let root = &plan.plan;

    // Analyze the plan for issues
    let mut issues = Vec::new();
    let mut recommendations = Vec::new();

    analyze_plan_node(root, &mut issues, &mut recommendations);

    // Filter out index recommendations for columns that already have indexes
    let existing_indexes = get_existing_indexes(client).await?;
    recommendations.retain(|rec| {
        if !matches!(rec.recommendation_type, RecommendationType::CreateIndex) {
            return true; // Keep non-index recommendations
        }
        // Check if this recommendation is for a column that already has an index
        if let Some(ref sql) = rec.sql {
            // Parse table and column from SQL like "CREATE INDEX idx_t_c ON schema.table(column);"
            if let Some((schema, table, column)) = parse_index_target(sql) {
                let key = format!("{}.{}.{}", schema, table, column);
                if existing_indexes.contains(&key) {
                    return false; // Filter out - index already exists
                }
            }
        }
        true
    });

    // Extract stats
    let stats = PlanStats {
        estimated_startup_cost: root.startup_cost.unwrap_or(0.0),
        estimated_total_cost: root.total_cost.unwrap_or(0.0),
        estimated_rows: root.plan_rows.unwrap_or(0),
        actual_time_ms: if analyze {
            plan.execution_time.or(root.actual_total_time)
        } else {
            None
        },
        actual_rows: if analyze { root.actual_rows } else { None },
        actual_loops: if analyze { root.actual_loops } else { None },
    };

    // Check for high overall cost
    if stats.estimated_total_cost > HIGH_COST_THRESHOLD {
        issues.push(PlanIssue {
            issue_type: IssueType::HighCostEstimate,
            severity: IssueSeverity::Warning,
            message: format!(
                "High estimated cost: {:.0} (consider reviewing query)",
                stats.estimated_total_cost
            ),
            table: None,
            column: None,
        });
    }

    Ok(ExplainResult {
        query: query.to_string(),
        plan_text,
        plan_json,
        analyzed: analyze,
        issues,
        recommendations,
        stats,
        actions: None,
    })
}

/// Recursively analyze plan nodes for issues
fn analyze_plan_node(
    node: &PlanNode,
    issues: &mut Vec<PlanIssue>,
    recommendations: &mut Vec<Recommendation>,
) {
    let table_name = node.relation_name.clone();
    let schema_name = node.schema.clone().unwrap_or_else(|| "public".to_string());
    let estimated_rows = node.plan_rows.unwrap_or(0);

    match node.node_type.as_str() {
        "Seq Scan" => {
            // Sequential scan on potentially large table
            if estimated_rows > LARGE_TABLE_ROWS {
                let qualified_table = table_name
                    .as_ref()
                    .map(|t| format!("{}.{}", schema_name, t))
                    .unwrap_or_else(|| "<unknown>".to_string());

                issues.push(PlanIssue {
                    issue_type: IssueType::SeqScanLargeTable,
                    severity: if estimated_rows > LARGE_TABLE_ROWS * 10 {
                        IssueSeverity::Critical
                    } else {
                        IssueSeverity::Warning
                    },
                    message: format!(
                        "Sequential scan on table with ~{} estimated rows",
                        estimated_rows
                    ),
                    table: table_name.clone(),
                    column: None,
                });

                // If there's a filter, suggest an index
                if let Some(ref filter) = node.filter {
                    // Extract column name from filter (simplified)
                    let column = extract_column_from_filter(filter);

                    if let Some(ref col) = column {
                        issues.push(PlanIssue {
                            issue_type: IssueType::MissingIndexOnFilter,
                            severity: IssueSeverity::Warning,
                            message: format!("Filter on potentially non-indexed column: {}", col),
                            table: table_name.clone(),
                            column: Some(col.clone()),
                        });

                        if let Some(ref tbl) = table_name {
                            recommendations.push(Recommendation {
                                recommendation_type: RecommendationType::CreateIndex,
                                sql: Some(format!(
                                    "CREATE INDEX idx_{}_{} ON {}.{}({});",
                                    tbl, col, schema_name, tbl, col
                                )),
                                rationale: format!(
                                    "Index on {} could convert Seq Scan to Index Scan",
                                    col
                                ),
                            });
                        }
                    }
                } else {
                    recommendations.push(Recommendation {
                        recommendation_type: RecommendationType::ConsiderIndex,
                        sql: None,
                        rationale: format!(
                            "Consider adding an index on {} for frequently accessed columns",
                            qualified_table
                        ),
                    });
                }
            }
        }
        "Nested Loop" => {
            // Nested loop on large sets
            if estimated_rows > NESTED_LOOP_ROWS {
                issues.push(PlanIssue {
                    issue_type: IssueType::NestedLoopLargeSet,
                    severity: IssueSeverity::Info,
                    message: format!(
                        "Nested loop with ~{} estimated rows (may be slow for large datasets)",
                        estimated_rows
                    ),
                    table: None,
                    column: None,
                });
            }
        }
        "Sort" => {
            // Sort without index
            if estimated_rows > LARGE_TABLE_ROWS {
                if let Some(ref sort_keys) = node.sort_key {
                    issues.push(PlanIssue {
                        issue_type: IssueType::SortWithoutIndex,
                        severity: IssueSeverity::Info,
                        message: format!(
                            "Sort on {} rows (sort keys: {})",
                            estimated_rows,
                            sort_keys.join(", ")
                        ),
                        table: None,
                        column: None,
                    });

                    recommendations.push(Recommendation {
                        recommendation_type: RecommendationType::ConsiderIndex,
                        sql: None,
                        rationale: format!(
                            "Consider index on sort columns ({}) if this query runs frequently",
                            sort_keys.join(", ")
                        ),
                    });
                }
            }
        }
        _ => {}
    }

    // Recurse into child plans
    if let Some(ref children) = node.plans {
        for child in children {
            analyze_plan_node(child, issues, recommendations);
        }
    }
}

/// Extract column name from a filter expression (simplified heuristic)
fn extract_column_from_filter(filter: &str) -> Option<String> {
    // PostgreSQL EXPLAIN shows filters in various formats:
    // - Simple: (status = 'active'::text)
    // - With cast: ((product_name)::text = 'Product 42'::text)
    // - Function: (upper(name) = 'FOO')
    //
    // We want to extract the column name, handling common patterns.

    let filter = filter.trim();

    // Strip outer parens repeatedly (handles ((col)::text = ...))
    let mut cleaned = filter;
    while cleaned.starts_with('(') && cleaned.ends_with(')') {
        cleaned = &cleaned[1..cleaned.len() - 1];
    }

    // Split on common operators
    for op in &[
        " = ",
        " IS ",
        " > ",
        " < ",
        " >= ",
        " <= ",
        " <> ",
        " != ",
        " LIKE ",
        " ~~",
        " IN ",
        " ANY ",
        " BETWEEN ",
    ] {
        if let Some(pos) = cleaned.find(op) {
            let left = cleaned[..pos].trim();

            // Handle PostgreSQL's ((col)::type) pattern
            // Strip parens from left side too
            let mut col_part = left;
            while col_part.starts_with('(') && col_part.ends_with(')') {
                col_part = &col_part[1..col_part.len() - 1];
            }
            // Also handle case where it's just (col)::type (no closing paren before ::)
            if col_part.starts_with('(') {
                col_part = &col_part[1..];
                // Find the closing paren or end
                if let Some(end) = col_part.find(')') {
                    col_part = &col_part[..end];
                }
            }

            // Remove type casts
            let col = col_part.split("::").next().unwrap_or(col_part).trim();

            // Skip if it looks like a function call (contains open paren after stripping)
            // or is empty, or contains spaces (complex expression)
            if col.is_empty() || col.contains('(') || col.contains(' ') {
                continue;
            }

            // Strip table qualifier if present (e.g., "orders.status" -> "status")
            let col = if let Some(dot_pos) = col.rfind('.') {
                &col[dot_pos + 1..]
            } else {
                col
            };

            return Some(col.to_string());
        }
    }

    None
}

/// Print explain in human-readable format
pub fn print_human(result: &ExplainResult, verbose: bool) {
    println!("QUERY PLAN ANALYSIS");
    println!("{}", "=".repeat(60));
    println!();

    // Show query (truncated)
    let query_display = if result.query.len() > 100 {
        format!("{}...", &result.query[..97])
    } else {
        result.query.clone()
    };
    println!("Query: {}", query_display);
    println!();

    if result.analyzed {
        println!("Mode: EXPLAIN ANALYZE (query was executed)");
    } else {
        println!("Mode: EXPLAIN (estimates only)");
    }
    println!();

    // Stats
    println!("STATISTICS:");
    println!(
        "  Estimated cost: {:.2}..{:.2}",
        result.stats.estimated_startup_cost, result.stats.estimated_total_cost
    );
    println!("  Estimated rows: {}", result.stats.estimated_rows);

    if let Some(actual_time) = result.stats.actual_time_ms {
        println!("  Actual time: {:.3} ms", actual_time);
    }
    if let Some(actual_rows) = result.stats.actual_rows {
        println!("  Actual rows: {}", actual_rows);
    }
    println!();

    // Plan text
    if verbose {
        println!("PLAN:");
        for line in result.plan_text.lines() {
            println!("  {}", line);
        }
        println!();
    }

    // Issues
    if !result.issues.is_empty() {
        println!("ISSUES:");
        for issue in &result.issues {
            let icon = match issue.severity {
                IssueSeverity::Info => "ℹ",
                IssueSeverity::Warning => "⚠",
                IssueSeverity::Critical => "✗",
            };
            println!("  {} {}", icon, issue.message);
            if let Some(ref table) = issue.table {
                println!("    Table: {}", table);
            }
        }
        println!();
    } else {
        println!("No significant issues detected.");
        println!();
    }

    // Recommendations
    if !result.recommendations.is_empty() {
        println!("RECOMMENDATIONS:");
        for (i, rec) in result.recommendations.iter().enumerate() {
            println!("  {}. {}", i + 1, rec.rationale);
            if let Some(ref sql) = rec.sql {
                println!("     SQL: {}", sql);
            }
        }
    }
}

/// Print explain as JSON with schema versioning
pub fn print_json(
    result: &ExplainResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    // Determine severity from issues
    let severity = result
        .issues
        .iter()
        .map(|i| match i.severity {
            IssueSeverity::Critical => 3,
            IssueSeverity::Warning => 2,
            IssueSeverity::Info => 1,
        })
        .max()
        .map(|s| match s {
            3 => Severity::Critical,
            2 => Severity::Warning,
            _ => Severity::Healthy,
        })
        .unwrap_or(Severity::Healthy);

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::EXPLAIN, result, severity, t),
        None => DiagnosticOutput::new(schema::EXPLAIN, result, severity),
    };
    output.print()?;
    Ok(())
}

/// Extract index name from CREATE INDEX SQL statement.
///
/// Handles variations: CREATE INDEX, CREATE UNIQUE INDEX, CREATE INDEX CONCURRENTLY,
/// CREATE INDEX IF NOT EXISTS, etc.
fn extract_index_name_from_sql(sql: &str, fallback_idx: usize) -> String {
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // Find position after INDEX keyword, skipping UNIQUE/CONCURRENTLY/IF NOT EXISTS
    let mut i = 0;
    while i < tokens.len() {
        let upper_token = tokens[i].to_uppercase();
        if upper_token == "INDEX" {
            i += 1;
            // Skip optional keywords after INDEX
            while i < tokens.len() {
                let next_upper = tokens[i].to_uppercase();
                if next_upper == "CONCURRENTLY"
                    || next_upper == "IF"
                    || next_upper == "NOT"
                    || next_upper == "EXISTS"
                {
                    i += 1;
                } else {
                    break;
                }
            }
            // tokens[i] should now be the index name
            if i < tokens.len() && !tokens[i].to_uppercase().starts_with("ON") {
                return tokens[i].to_string();
            }
            break;
        }
        i += 1;
    }

    // Fallback if parsing fails
    format!("idx_{}", fallback_idx)
}

/// Generate structured actions from explain recommendations.
///
/// Converts CreateIndex recommendations with SQL into StructuredAction objects
/// that can be consumed by automation tools.
pub fn generate_actions(
    result: &ExplainResult,
    read_write: bool,
    is_primary: bool,
) -> Vec<StructuredAction> {
    let mut actions = Vec::new();

    for (idx, rec) in result.recommendations.iter().enumerate() {
        // Only CreateIndex recommendations with concrete SQL become actions.
        // ConsiderIndex requires human analysis, ReviewQuery is informational.
        if !matches!(rec.recommendation_type, RecommendationType::CreateIndex) {
            continue;
        }

        let Some(ref sql) = rec.sql else {
            continue;
        };

        let index_name = extract_index_name_from_sql(sql, idx);
        let action_id = format!("explain.index.create.{}", index_name);

        let action = StructuredAction::builder(action_id, ActionType::Fix)
            .command("pgcrate")
            .args(vec!["sql".to_string(), sql.clone()])
            .description(rec.rationale.clone())
            .mutates(true)
            .risk(Risk::Low)
            .gates(ActionGates::write_primary())
            .sql_preview(vec![sql.clone()])
            .build(read_write, is_primary, false); // Require confirmation for DDL

        actions.push(action);
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_column_simple() {
        assert_eq!(
            extract_column_from_filter("(email = 'test@example.com'::text)"),
            Some("email".to_string())
        );
    }

    #[test]
    fn test_extract_column_is_null() {
        assert_eq!(
            extract_column_from_filter("(status IS NOT NULL)"),
            Some("status".to_string())
        );
    }

    #[test]
    fn test_extract_column_comparison() {
        assert_eq!(
            extract_column_from_filter("(age > 18)"),
            Some("age".to_string())
        );
    }

    #[test]
    fn test_extract_column_postgres_cast_format() {
        // PostgreSQL's common EXPLAIN output format: ((col)::type = value)
        assert_eq!(
            extract_column_from_filter("((product_name)::text = 'Product 42'::text)"),
            Some("product_name".to_string())
        );
    }

    #[test]
    fn test_extract_column_like_operator() {
        assert_eq!(
            extract_column_from_filter("((name)::text ~~ 'foo%'::text)"),
            Some("name".to_string())
        );
    }

    #[test]
    fn test_extract_column_nested_parens() {
        assert_eq!(
            extract_column_from_filter("(((status)::text = 'active'::text))"),
            Some("status".to_string())
        );
    }

    #[test]
    fn test_extract_column_function_call_skipped() {
        // Function calls should return None (we can't index on functions easily)
        assert_eq!(extract_column_from_filter("(upper(name) = 'FOO')"), None);
    }

    #[test]
    fn test_extract_column_strips_table_qualifier() {
        // PostgreSQL EXPLAIN sometimes includes table alias in filter
        assert_eq!(
            extract_column_from_filter("(orders.status = 'pending'::text)"),
            Some("status".to_string())
        );
        assert_eq!(
            extract_column_from_filter("((o.amount)::numeric > 100)"),
            Some("amount".to_string())
        );
    }

    #[test]
    fn test_parse_index_target() {
        assert_eq!(
            parse_index_target("CREATE INDEX idx_orders_status ON public.orders(status);"),
            Some((
                "public".to_string(),
                "orders".to_string(),
                "status".to_string()
            ))
        );
        assert_eq!(
            parse_index_target("CREATE INDEX idx_users_email ON users(email);"),
            Some((
                "public".to_string(),
                "users".to_string(),
                "email".to_string()
            ))
        );
        assert_eq!(
            parse_index_target("CREATE INDEX CONCURRENTLY idx_foo ON app.bar(baz);"),
            Some(("app".to_string(), "bar".to_string(), "baz".to_string()))
        );
    }

    #[test]
    fn test_issue_severity_critical() {
        let issue = PlanIssue {
            issue_type: IssueType::SeqScanLargeTable,
            severity: IssueSeverity::Critical,
            message: "test".to_string(),
            table: None,
            column: None,
        };
        assert_eq!(issue.severity, IssueSeverity::Critical);
    }

    fn test_result_with_recommendations(recommendations: Vec<Recommendation>) -> ExplainResult {
        ExplainResult {
            query: "SELECT * FROM users".to_string(),
            plan_text: "Seq Scan on users".to_string(),
            plan_json: serde_json::json!([]),
            analyzed: false,
            issues: vec![],
            recommendations,
            stats: PlanStats {
                estimated_startup_cost: 0.0,
                estimated_total_cost: 100.0,
                estimated_rows: 1000,
                actual_time_ms: None,
                actual_rows: None,
                actual_loops: None,
            },
            actions: None,
        }
    }

    #[test]
    fn test_generate_actions_create_index() {
        let rec = Recommendation {
            recommendation_type: RecommendationType::CreateIndex,
            sql: Some("CREATE INDEX idx_users_email ON public.users(email);".to_string()),
            rationale: "Index on email could convert Seq Scan to Index Scan".to_string(),
        };
        let result = test_result_with_recommendations(vec![rec]);

        let actions = generate_actions(&result, true, true);

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action_id, "explain.index.create.idx_users_email");
        assert!(actions[0].available);
        assert!(actions[0].mutates);
        assert!(actions[0].sql_preview.is_some());
    }

    #[test]
    fn test_generate_actions_consider_index_skipped() {
        let rec = Recommendation {
            recommendation_type: RecommendationType::ConsiderIndex,
            sql: None,
            rationale: "Consider adding an index".to_string(),
        };
        let result = test_result_with_recommendations(vec![rec]);

        let actions = generate_actions(&result, true, true);

        assert!(actions.is_empty());
    }

    #[test]
    fn test_generate_actions_blocked_without_write() {
        let rec = Recommendation {
            recommendation_type: RecommendationType::CreateIndex,
            sql: Some("CREATE INDEX idx_users_email ON public.users(email);".to_string()),
            rationale: "Index on email".to_string(),
        };
        let result = test_result_with_recommendations(vec![rec]);

        let actions = generate_actions(&result, false, true);

        assert_eq!(actions.len(), 1);
        assert!(!actions[0].available);
        assert!(actions[0].blocked_reason.is_some());
    }

    #[test]
    fn test_generate_actions_multiple_indexes() {
        let recs = vec![
            Recommendation {
                recommendation_type: RecommendationType::CreateIndex,
                sql: Some("CREATE INDEX idx_users_email ON users(email);".to_string()),
                rationale: "Index on email".to_string(),
            },
            Recommendation {
                recommendation_type: RecommendationType::CreateIndex,
                sql: Some("CREATE INDEX idx_users_status ON users(status);".to_string()),
                rationale: "Index on status".to_string(),
            },
        ];
        let result = test_result_with_recommendations(recs);
        let actions = generate_actions(&result, true, true);

        assert_eq!(actions.len(), 2);
        // Verify unique action IDs
        assert_ne!(actions[0].action_id, actions[1].action_id);
        assert_eq!(actions[0].action_id, "explain.index.create.idx_users_email");
        assert_eq!(
            actions[1].action_id,
            "explain.index.create.idx_users_status"
        );
    }

    #[test]
    fn test_extract_index_name_concurrently() {
        assert_eq!(
            extract_index_name_from_sql("CREATE INDEX CONCURRENTLY idx_foo ON bar(col);", 0),
            "idx_foo"
        );
    }

    #[test]
    fn test_extract_index_name_unique() {
        assert_eq!(
            extract_index_name_from_sql("CREATE UNIQUE INDEX idx_foo ON bar(col);", 0),
            "idx_foo"
        );
    }

    #[test]
    fn test_extract_index_name_if_not_exists() {
        assert_eq!(
            extract_index_name_from_sql("CREATE INDEX IF NOT EXISTS idx_foo ON bar(col);", 0),
            "idx_foo"
        );
    }

    #[test]
    fn test_extract_index_name_malformed() {
        // Incomplete SQL falls back to idx_{n}
        assert_eq!(extract_index_name_from_sql("CREATE INDEX", 5), "idx_5");
    }
}
