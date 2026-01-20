//! Explain command: Query plan analysis with recommendations.
//!
//! Analyzes query execution plans to identify performance issues and
//! suggest optimizations. Safe by default (EXPLAIN only), with optional
//! EXPLAIN ANALYZE for actual execution statistics.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

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
    // Common patterns: (column = value), (column IS NOT NULL), column::type
    // This is a simplified extraction - real implementation would need proper parsing

    // Look for pattern like (column_name = or (column_name IS
    let filter = filter.trim_start_matches('(').trim_end_matches(')');

    // Split on common operators
    for op in &[
        " = ", " IS ", " > ", " < ", " >= ", " <= ", " <> ", " != ", " LIKE ", " IN ",
    ] {
        if let Some(pos) = filter.find(op) {
            let left = filter[..pos].trim();
            // Skip if it looks like a function call or complex expression
            if !left.contains('(') && !left.contains(' ') {
                // Remove any type casts
                let col = left.split("::").next().unwrap_or(left);
                return Some(col.to_string());
            }
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
}
