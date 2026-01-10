use anyhow::{bail, Context, Result};
use colored::Colorize;
use serde::Serialize;
use std::fs;
use std::io::IsTerminal;
use std::path::Path;

use crate::config::Config;
use crate::model::{
    apply_selectors, compile_model, ensure_schema, execute_model, generate_first_run_sql,
    generate_merge_sql, generate_upsert_sql, lint_deps as model_lint_deps, load_project,
    qualify_model_sql, rewrite_deps_line, rewrite_model_body_sql, topo_sort, topo_sort_layers,
    Model, Project, Relation, Test,
};
use crate::tips::{show_tip, TipContext};

use super::connect;

fn maybe_init_models(
    root: &Path,
    config: &Config,
    init_models_dir: bool,
    quiet: bool,
) -> Result<()> {
    if !init_models_dir {
        return Ok(());
    }
    let models_dir = root.join(config.models_dir());
    if models_dir.is_dir() {
        return Ok(());
    }
    init(root, config, false, quiet)
}

/// Get declared model dependencies, excluding sources.
/// Sources are valid dependencies but aren't tracked in inferred_model_deps,
/// so we exclude them when comparing declared vs inferred to avoid false positives.
fn declared_model_deps(model: &Model, project: &Project) -> std::collections::BTreeSet<Relation> {
    model
        .header
        .deps
        .iter()
        .filter(|dep| !project.sources.contains(dep))
        .cloned()
        .collect()
}

/// Compile models to target/compiled/
pub fn compile(
    root: &Path,
    config: &Config,
    selectors: &[String],
    excludes: &[String],
    init_models_dir: bool,
    quiet: bool,
) -> Result<()> {
    maybe_init_models(root, config, init_models_dir, quiet)?;
    let project = load_project(root, config).context("load project")?;

    let models_to_compile = apply_selectors(&project, selectors, excludes)?;

    if models_to_compile.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(());
    }

    for rel in &models_to_compile {
        let model = project.models.get(rel).unwrap();
        let output = compile_model(&project, model)?;
        if !quiet {
            println!(
                "{} {} -> {}",
                "Compiled".green(),
                rel,
                output.output_path.display()
            );
        }
    }

    if !quiet {
        println!(
            "\n{} {} model(s) compiled",
            "Done.".green().bold(),
            models_to_compile.len()
        );
    }

    Ok(())
}

/// Run models against the database
#[allow(clippy::too_many_arguments)]
pub async fn run(
    root: &Path,
    config: &Config,
    database_url: &str,
    selectors: &[String],
    excludes: &[String],
    dry_run: bool,
    full_refresh: bool,
    init_models_dir: bool,
    quiet: bool,
    verbose: bool,
) -> Result<()> {
    maybe_init_models(root, config, init_models_dir, quiet)?;
    let project = load_project(root, config).context("load project")?;

    let models_to_run = apply_selectors(&project, selectors, excludes)?;

    if models_to_run.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(());
    }

    if dry_run {
        println!("{}", "Compiled SQL (dry-run):".bold());
        for rel in &models_to_run {
            let model = project.models.get(rel).unwrap();
            println!("\n-- {}", rel);
            println!("{}", dry_run_sql(model, full_refresh));
        }
        println!("\n{} models would be executed", models_to_run.len());
        return Ok(());
    }

    if full_refresh && !quiet {
        println!("{}", "Running with --full-refresh".yellow());
    }

    let client = connect(database_url).await?;

    for rel in &models_to_run {
        let model = project.models.get(rel).unwrap();

        if ensure_schema(&client, &model.id.schema).await? && !quiet {
            println!("{} schema '{}'", "Created".green(), model.id.schema);
        }

        if !quiet {
            print!("{} {}... ", "Running".cyan(), rel);
        }
        if verbose {
            eprintln!("\n{}", dry_run_sql(model, full_refresh));
        }
        let exec = execute_model(&client, model, full_refresh).await?;
        if !quiet {
            let mut extra: Vec<String> = Vec::new();
            if !model.header.tests.is_empty() {
                extra.push(format!(
                    "{} {}",
                    model.header.tests.len(),
                    pluralize(model.header.tests.len(), "test", "tests")
                ));
            }

            let status = if let Some(inc) = exec.incremental {
                let (action, verb) = match inc.action {
                    crate::model::IncrementalAction::CreatedTable => ("created table", "inserted"),
                    crate::model::IncrementalAction::Merged => ("merged", "affected"),
                    crate::model::IncrementalAction::Upserted => ("upserted", "affected"),
                };
                let mut s = format!(
                    "{} {}; {} {} {}",
                    action,
                    model.id,
                    verb,
                    inc.inserted,
                    pluralize_u64(inc.inserted, "row", "rows")
                );
                if matches!(
                    inc.action,
                    crate::model::IncrementalAction::Merged
                        | crate::model::IncrementalAction::Upserted
                ) && inc.inserted == 0
                {
                    s.push_str(" (no new keys; existing keys may have been updated)");
                }
                s
            } else {
                let mut s = format!("ok ({})", model.header.materialized.as_str());
                if let Some(rows) = exec.rows_affected {
                    s.push_str(&format!(
                        " ({} {})",
                        rows,
                        pluralize_u64(rows, "row", "rows")
                    ));
                }
                s
            };

            if !extra.is_empty() {
                println!("{} {} ({})", "ok".green(), status, extra.join(", "));
            } else {
                println!("{} {}", "ok".green(), status);
            }
        }
    }

    if !quiet {
        println!(
            "\n{} {} model(s) executed",
            "Done.".green().bold(),
            models_to_run.len()
        );
    }

    // Show contextual tip
    let had_incremental = models_to_run.iter().any(|rel| {
        project
            .models
            .get(rel)
            .map(|m| {
                matches!(
                    m.header.materialized,
                    crate::model::Materialized::Incremental
                )
            })
            .unwrap_or(false)
    });
    show_tip(TipContext::RunSuccess { had_incremental }, quiet);

    Ok(())
}

/// Create a new model file at models/<schema>/<name>.sql
pub fn new_model(
    root: &Path,
    config: &Config,
    id: &str,
    materialized: &str,
    yes: bool,
    force: bool,
    quiet: bool,
) -> Result<()> {
    let rel = Relation::parse(id)?;
    let mat = crate::model::Materialized::parse(materialized)
        .with_context(|| format!("invalid materialized value: {}", materialized))?;

    let models_dir = root.join(config.models_dir());
    let schema_dir = models_dir.join(&rel.schema);
    fs::create_dir_all(&schema_dir).with_context(|| format!("create {}", schema_dir.display()))?;

    let path = schema_dir.join(format!("{}.sql", rel.name));
    if path.exists() && !force {
        let overwrite = if yes {
            true
        } else if std::io::stdin().is_terminal() {
            dialoguer::Confirm::new()
                .with_prompt(format!(
                    "{} exists. Overwrite?",
                    path.strip_prefix(root).unwrap_or(&path).display()
                ))
                .default(false)
                .interact()?
        } else {
            bail!(
                "Model file already exists: {} (use -y or --force to overwrite)",
                path.display()
            );
        };

        if !overwrite {
            if !quiet {
                println!("Aborted.");
            }
            return Ok(());
        }
    }

    let header = match mat {
        crate::model::Materialized::View => "-- materialized: view\n-- deps: staging.source_table\n-- tests: not_null(id), unique(id)\n-- Tip: Run `pgcrate model status` before `pgcrate model run`\n-- Tip: Common layout is models/staging, models/intermediate, models/marts\n\n".to_string(),
        crate::model::Materialized::Table => "-- materialized: table\n-- deps: staging.source_table\n-- tests: not_null(id), unique(id)\n-- Tip: Run `pgcrate model status` before `pgcrate model run`\n-- Tip: Common layout is models/staging, models/intermediate, models/marts\n\n".to_string(),
        crate::model::Materialized::Incremental => "-- materialized: incremental\n-- unique_key: id\n-- watermark: updated_at\n-- deps: staging.source_table\n-- tests: not_null(id), unique(id)\n-- Tip: watermark filters to only new rows; remove it for full scan each run\n-- Tip: Add '-- lookback: 2 days' to reprocess recent data (late arrivals)\n-- Tip: For custom logic, use @base/@incremental sections instead\n\n".to_string(),
    };

    let body = "SELECT 1 AS id;\n";
    fs::write(&path, format!("{}{}", header, body))
        .with_context(|| format!("write {}", path.display()))?;

    if !quiet {
        println!(
            "{} {}",
            "Created".green(),
            path.strip_prefix(root).unwrap_or(&path).display()
        );
    }

    show_tip(TipContext::New, quiet);

    Ok(())
}

#[derive(Serialize)]
struct ModelShowJson {
    ok: bool,
    model: ModelShowModel,
    materialized: String,
    sql: ModelShowSql,
}

#[derive(Serialize)]
struct ModelShowModel {
    id: String,
    path: String,
}

#[derive(Serialize)]
struct ModelShowSql {
    #[serde(skip_serializing_if = "Option::is_none")]
    create: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    merge: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upsert: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    run: Option<String>,
}

pub async fn show(
    root: &Path,
    config: &Config,
    database_url: &str,
    id: &str,
    quiet: bool,
    json: bool,
) -> Result<()> {
    let project = load_project(root, config).context("load project")?;
    let rel = Relation::parse(id)?;
    let model = project
        .models
        .get(&rel)
        .ok_or_else(|| anyhow::anyhow!("model not found: {}", rel))?;

    let (create_sql, merge_sql, upsert_sql, run_sql) = match model.header.materialized {
        crate::model::Materialized::Incremental => {
            if database_url.trim().is_empty() {
                bail!("DATABASE_URL not set (required for incremental model show)");
            }
            let client = connect(database_url).await?;

            // Use @base section for first run
            let base_body = model.first_run_sql().trim().trim_end_matches(';').trim();
            let create_sql = generate_first_run_sql(model, base_body, &model.header.unique_key);

            // Use @incremental section (with ${this} substituted) for subsequent runs
            let incr_body = model.incremental_run_sql();
            let incr_body = incr_body.trim().trim_end_matches(';').trim();

            // Get columns by describing the base query
            let describe_sql = format!("SELECT * FROM (\n{}\n) AS s LIMIT 0", base_body);
            let stmt = client
                .prepare(&describe_sql)
                .await
                .with_context(|| format!("describe model query for {}", rel))?;
            let cols: Vec<String> = stmt
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect();

            let merge_sql = generate_merge_sql(model, &cols, incr_body, &model.header.unique_key);
            let upsert_sql = generate_upsert_sql(model, &cols, incr_body, &model.header.unique_key);
            (Some(create_sql), Some(merge_sql), Some(upsert_sql), None)
        }
        _ => {
            let mut sql = crate::model::generate_run_sql(model);
            if !sql.trim_end().ends_with(';') {
                sql.push(';');
            }
            (None, None, None, Some(sql))
        }
    };

    if json {
        let payload = ModelShowJson {
            ok: true,
            model: ModelShowModel {
                id: rel.to_string(),
                path: model.path.display().to_string(),
            },
            materialized: model.header.materialized.as_str().to_string(),
            sql: ModelShowSql {
                create: create_sql,
                merge: merge_sql,
                upsert: upsert_sql,
                run: run_sql,
            },
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    if quiet {
        return Ok(());
    }

    println!("Model: {}", rel);
    println!("Path:  {}", model.path.display());
    println!("Materialized: {}", model.header.materialized.as_str());
    if !model.header.unique_key.is_empty() {
        println!("Unique Key: {}", model.header.unique_key.join(", "));
    }

    if let Some(sql) = &create_sql {
        println!("\n=== First Run / Full Refresh (@base) ===\n{}\n", sql);
    }
    if let Some(sql) = &merge_sql {
        println!(
            "\n=== Incremental Run (@incremental) - PostgreSQL 17+ ===\n{}\n",
            sql
        );
    }
    if let Some(sql) = &upsert_sql {
        println!(
            "\n=== Incremental Run (@incremental) - PostgreSQL 9.5-16 ===\n{}\n",
            sql
        );
    }
    if let Some(sql) = &run_sql {
        println!("\n{}\n", sql);
    }

    Ok(())
}

#[derive(Debug, Clone)]
enum ModelSyncStatus {
    Synced,
    Missing,
    TypeMismatch { expected: String, actual: String },
}

#[derive(Serialize)]
struct ModelStatusRow {
    relation: String,
    materialized: String,
    status: String,
    exists: bool,
    expected_type: String,
    actual_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_count: Option<i64>,
}

/// Show model sync status vs database
/// Returns exit code: 0=all synced, 1=needs run
pub async fn status(
    root: &Path,
    config: &Config,
    database_url: &str,
    selectors: &[String],
    excludes: &[String],
    quiet: bool,
    json: bool,
) -> Result<i32> {
    let project = load_project(root, config).context("load project")?;
    let models = apply_selectors(&project, selectors, excludes)?;

    if models.is_empty() {
        if !quiet && !json {
            println!("No models found");
        }
        return Ok(0);
    }

    let client = connect(database_url).await?;

    // Tuple: (relation, status, actual_type, row_count)
    let mut rows_out: Vec<(Relation, ModelSyncStatus, Option<String>, Option<i64>)> = Vec::new();
    for rel in &models {
        let model = project.models.get(rel).unwrap();
        let expected_type = match model.header.materialized {
            crate::model::Materialized::View => "VIEW",
            crate::model::Materialized::Table | crate::model::Materialized::Incremental => {
                "BASE TABLE"
            }
        }
        .to_string();

        let db_rows = client
            .query(
                "SELECT table_type
                 FROM information_schema.tables
                 WHERE table_schema = $1 AND table_name = $2",
                &[&rel.schema, &rel.name],
            )
            .await
            .with_context(|| format!("check model exists: {}", rel))?;

        if db_rows.is_empty() {
            rows_out.push((
                rel.clone(),
                ModelSyncStatus::Missing,
                Some(expected_type),
                None,
            ));
            continue;
        }

        let actual_type: String = db_rows[0].get(0);

        // Query row count for existing models
        let row_count: Option<i64> = client
            .query_one(
                &format!(
                    "SELECT COUNT(*)::bigint FROM {}.{}",
                    crate::sql::quote_ident(&rel.schema),
                    crate::sql::quote_ident(&rel.name)
                ),
                &[],
            )
            .await
            .ok()
            .map(|row| row.get(0));

        if actual_type == expected_type {
            rows_out.push((
                rel.clone(),
                ModelSyncStatus::Synced,
                Some(actual_type),
                row_count,
            ));
        } else {
            rows_out.push((
                rel.clone(),
                ModelSyncStatus::TypeMismatch {
                    expected: expected_type,
                    actual: actual_type.clone(),
                },
                Some(actual_type),
                row_count,
            ));
        }
    }

    let needs_sync = rows_out
        .iter()
        .any(|(_, s, _, _)| !matches!(s, ModelSyncStatus::Synced));
    let exit_code = if needs_sync { 1 } else { 0 };

    if json {
        let json_rows: Vec<ModelStatusRow> = rows_out
            .iter()
            .map(|(rel, status, actual_type, row_count)| {
                let model = project.models.get(rel).unwrap();
                let expected_type = match model.header.materialized {
                    crate::model::Materialized::View => "VIEW",
                    crate::model::Materialized::Table | crate::model::Materialized::Incremental => {
                        "BASE TABLE"
                    }
                }
                .to_string();

                let (status_str, exists, actual) = match status {
                    ModelSyncStatus::Synced => ("synced".to_string(), true, actual_type.clone()),
                    ModelSyncStatus::Missing => ("missing".to_string(), false, None),
                    ModelSyncStatus::TypeMismatch { .. } => {
                        ("type_mismatch".to_string(), true, actual_type.clone())
                    }
                };

                ModelStatusRow {
                    relation: rel.to_string(),
                    materialized: model.header.materialized.as_str().to_string(),
                    status: status_str,
                    exists,
                    expected_type,
                    actual_type: actual,
                    row_count: *row_count,
                }
            })
            .collect();

        println!("{}", serde_json::to_string_pretty(&json_rows)?);
        return Ok(exit_code);
    }

    if quiet {
        return Ok(exit_code);
    }

    let total = rows_out.len();
    let max_name = rows_out
        .iter()
        .map(|(r, _, _, _)| r.to_string().len())
        .max()
        .unwrap_or(0);

    println!("\nModels ({} total):", total);
    for (rel, status, _actual_type, row_count) in &rows_out {
        let model = project.models.get(rel).unwrap();
        let mat = model.header.materialized.as_str();
        let name = format!("{:width$}", rel, width = max_name);

        // Format row count with thousands separator
        let count_str = match row_count {
            Some(n) => format!("({} rows)", format_number(*n)),
            None => String::new(),
        };

        match status {
            ModelSyncStatus::Synced => {
                println!(
                    "  {}  {:<12}  {}  {}",
                    name,
                    mat,
                    "✓ exists".green(),
                    count_str.dimmed()
                );
            }
            ModelSyncStatus::Missing => {
                println!(
                    "  {}  {:<12}  {}   (run: pgcrate model run -s {})",
                    name,
                    mat,
                    "✗ missing".red(),
                    rel
                );
            }
            ModelSyncStatus::TypeMismatch { expected, actual } => {
                println!(
                    "  {}  {:<12}  {} (expected {}, found {})  {}",
                    name,
                    mat,
                    "! type mismatch".yellow(),
                    expected,
                    actual,
                    count_str.dimmed()
                );
            }
        }
    }

    let synced = rows_out
        .iter()
        .filter(|(_, s, _, _)| matches!(s, ModelSyncStatus::Synced))
        .count();
    let missing = rows_out
        .iter()
        .filter(|(_, s, _, _)| matches!(s, ModelSyncStatus::Missing))
        .count();
    let mismatched = rows_out
        .iter()
        .filter(|(_, s, _, _)| matches!(s, ModelSyncStatus::TypeMismatch { .. }))
        .count();

    println!(
        "\nSummary: {} synced, {} missing, {} type mismatched",
        synced, missing, mismatched
    );

    show_tip(TipContext::Status { missing }, quiet);

    Ok(exit_code)
}

/// Run data tests defined in model headers
/// Returns exit code: 0=all pass, 1=failures, 2=error
pub async fn test(
    root: &Path,
    config: &Config,
    database_url: &str,
    selectors: &[String],
    excludes: &[String],
    init_models_dir: bool,
    quiet: bool,
) -> Result<i32> {
    maybe_init_models(root, config, init_models_dir, quiet)?;
    let project = load_project(root, config).context("load project")?;

    let selected = apply_selectors(&project, selectors, excludes)?;
    let models_to_test: Vec<&Model> = selected
        .iter()
        .filter_map(|rel| project.models.get(rel))
        .collect();

    // Filter to models that have tests
    let models_with_tests: Vec<_> = models_to_test
        .into_iter()
        .filter(|m| !m.header.tests.is_empty())
        .collect();

    if models_with_tests.is_empty() {
        if !quiet {
            println!("No tests found");
        }
        return Ok(0);
    }

    let client = connect(database_url).await?;

    let mut passed = 0;
    let mut failed = 0;

    for model in &models_with_tests {
        if !quiet {
            println!("Testing {}...", model.id);
        }

        for test in &model.header.tests {
            let result = run_single_test(&client, model, test).await;

            match result {
                Ok(true) => {
                    passed += 1;
                    if !quiet {
                        println!("  {}     {}", test.description(), "PASS".green());
                    }
                }
                Ok(false) => {
                    failed += 1;
                    if !quiet {
                        println!("  {}     {}", test.description(), "FAIL".red());
                    }
                }
                Err(e) => {
                    failed += 1;
                    if !quiet {
                        println!("  {}     {} ({})", test.description(), "ERROR".red(), e);
                    }
                }
            }
        }
    }

    if !quiet {
        println!(
            "\nResults: {} passed, {} failed",
            passed.to_string().green(),
            if failed > 0 {
                failed.to_string().red()
            } else {
                failed.to_string().normal()
            }
        );
    }

    Ok(if failed > 0 { 1 } else { 0 })
}

async fn run_single_test(
    client: &tokio_postgres::Client,
    model: &Model,
    test: &Test,
) -> Result<bool> {
    let sql = test.to_sql(&model.id);
    let rows = client
        .query(&sql, &[])
        .await
        .with_context(|| format!("execute test {} on {}", test.description(), model.id))?;

    // For not_null/accepted_values/relationships: check if violations count > 0
    // For unique: check if any rows returned (duplicates exist)
    match test {
        Test::NotNull { .. } | Test::AcceptedValues { .. } | Test::Relationships { .. } => {
            let count: i64 = rows[0].get("violations");
            Ok(count == 0)
        }
        Test::Unique { .. } => Ok(rows.is_empty()),
    }
}

/// Generate markdown documentation for models
pub fn docs(
    root: &Path,
    config: &Config,
    selectors: &[String],
    excludes: &[String],
    quiet: bool,
) -> Result<()> {
    let project = load_project(root, config).context("load project")?;

    let selected = apply_selectors(&project, selectors, excludes)?;
    let models_to_doc: Vec<&Model> = selected
        .iter()
        .filter_map(|rel| project.models.get(rel))
        .collect();

    if models_to_doc.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(());
    }

    let documenting_all = selectors.is_empty() && excludes.is_empty();

    let docs_dir = root.join("target/docs");
    fs::create_dir_all(&docs_dir).context("create target/docs")?;

    // Generate index.md with mermaid DAG (only when documenting all models)
    if documenting_all {
        let sorted = topo_sort(&project)?;
        let mut index = String::from("# Models\n\n");
        index.push_str("## Dependency Graph\n\n");
        index.push_str("```mermaid\ngraph LR\n");
        for rel in &sorted {
            let model = project.models.get(rel).unwrap();
            for dep in &model.header.deps {
                index.push_str(&format!("    {} --> {}\n", dep.name, rel.name));
            }
            if model.header.deps.is_empty() {
                index.push_str(&format!("    {}\n", rel.name));
            }
        }
        index.push_str("```\n\n");
        index.push_str("## Models\n\n");
        for rel in &sorted {
            index.push_str(&format!("- [{rel}]({}/{}.md)\n", rel.schema, rel.name));
        }
        fs::write(docs_dir.join("index.md"), &index).context("write index.md")?;
    }

    // Generate per-model docs
    for model in &models_to_doc {
        let schema_dir = docs_dir.join(&model.id.schema);
        fs::create_dir_all(&schema_dir).context("create schema dir")?;

        let mut doc = format!("# {}\n\n", model.id);
        doc.push_str(&format!(
            "**Materialized as:** {}\n\n",
            model.header.materialized.as_str()
        ));

        if !model.header.deps.is_empty() {
            doc.push_str("## Dependencies\n\n");
            for dep in &model.header.deps {
                doc.push_str(&format!("- {dep}\n"));
            }
            doc.push('\n');
        }

        if !model.header.tests.is_empty() {
            doc.push_str("## Tests\n\n");
            for test in &model.header.tests {
                doc.push_str(&format!("- {}\n", test.description()));
            }
            doc.push('\n');
        }

        doc.push_str("## SQL\n\n```sql\n");
        doc.push_str(&model.body_sql);
        doc.push_str("\n```\n");

        let doc_path = schema_dir.join(format!("{}.md", model.id.name));
        fs::write(&doc_path, &doc).context("write model doc")?;

        if !quiet {
            println!("{} {}", "Generated".green(), doc_path.display());
        }
    }

    if !quiet {
        println!(
            "\n{} Documentation written to {}",
            "Done.".green().bold(),
            docs_dir.display()
        );
    }

    Ok(())
}

/// Show model dependency graph
pub fn graph(
    root: &Path,
    config: &Config,
    selectors: &[String],
    excludes: &[String],
    format: &str,
    quiet: bool,
) -> Result<()> {
    let project = load_project(root, config).context("load project")?;

    let models = apply_selectors(&project, selectors, excludes)?;

    if models.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(());
    }

    match format {
        "ascii" => print_ascii_graph(&project, &models),
        "dot" => print_dot_graph(&project, &models),
        "json" => print_json_graph(&project, &models),
        "mermaid" => print_mermaid_graph(&project, &models),
        _ => bail!("Unknown format: {}. Use: ascii, dot, json, mermaid", format),
    }

    Ok(())
}

fn print_ascii_graph(project: &crate::model::Project, _models: &[Relation]) {
    // Use layer-aware output for better execution order visibility
    let layers = match topo_sort_layers(project) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
        }
    };

    for (i, layer) in layers.iter().enumerate() {
        println!("Layer {}:", i);
        for rel in layer {
            let model = project.models.get(rel).unwrap();
            let deps: Vec<_> = model.header.deps.iter().map(|d| d.to_string()).collect();
            if deps.is_empty() {
                println!("  {}", rel);
            } else {
                println!("  {} <- [{}]", rel, deps.join(", "));
            }
        }
    }
}

fn print_dot_graph(project: &crate::model::Project, models: &[Relation]) {
    println!("digraph models {{");
    println!("    rankdir=LR;");
    for rel in models {
        let model = project.models.get(rel).unwrap();
        for dep in &model.header.deps {
            println!("    \"{}\" -> \"{}\";", dep, rel);
        }
    }
    println!("}}");
}

fn print_json_graph(project: &crate::model::Project, _models: &[Relation]) {
    let layers = match topo_sort_layers(project) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("{{\"error\": \"{}\"}}", e);
            return;
        }
    };

    println!("{{");
    println!("  \"layers\": [");
    for (i, layer) in layers.iter().enumerate() {
        let comma = if i < layers.len() - 1 { "," } else { "" };
        let models_str: Vec<String> = layer.iter().map(|r| format!("\"{}\"", r)).collect();
        println!("    [{}]{}", models_str.join(", "), comma);
    }
    println!("  ],");
    println!("  \"edges\": [");
    let mut edges: Vec<(String, String)> = Vec::new();
    for layer in &layers {
        for rel in layer {
            let model = project.models.get(rel).unwrap();
            for dep in &model.header.deps {
                edges.push((dep.to_string(), rel.to_string()));
            }
        }
    }
    for (i, (from, to)) in edges.iter().enumerate() {
        let comma = if i < edges.len() - 1 { "," } else { "" };
        println!(
            "    {{\"from\": \"{}\", \"to\": \"{}\"}}{}",
            from, to, comma
        );
    }
    println!("  ]");
    println!("}}");
}

fn print_mermaid_graph(project: &crate::model::Project, models: &[Relation]) {
    println!("graph LR");
    for rel in models {
        let model = project.models.get(rel).unwrap();
        if model.header.deps.is_empty() {
            println!("    {}", rel.name);
        } else {
            for dep in &model.header.deps {
                println!("    {} --> {}", dep.name, rel.name);
            }
        }
    }
}

/// Lint model dependencies - check declared deps match inferred deps
/// Returns exit code: 0=ok, 1=issues found
pub fn lint_deps(
    root: &Path,
    config: &Config,
    selectors: &[String],
    excludes: &[String],
    fix: bool,
    quiet: bool,
) -> Result<i32> {
    let project = load_project(root, config).context("load project")?;

    let selected = apply_selectors(&project, selectors, excludes)?;
    let models_to_lint: Vec<&Model> = selected
        .iter()
        .filter_map(|rel| project.models.get(rel))
        .collect();

    if models_to_lint.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(0);
    }

    let mut issues = 0;

    for model in &models_to_lint {
        let result = model_lint_deps(&project, model)?;

        let declared = declared_model_deps(model, &project);
        let inferred: std::collections::BTreeSet<_> =
            result.inferred_model_deps.iter().cloned().collect();

        let mut model_issues = Vec::new();

        // Check for unqualified references
        if !result.unqualified_relations.is_empty() {
            model_issues.push(format!(
                "unqualified: {}",
                result.unqualified_relations.join(", ")
            ));
        }

        // Check for unknown references
        if !result.unknown_relations.is_empty() {
            model_issues.push(format!("unknown: {}", result.unknown_relations.join(", ")));
        }

        // Check for mismatched deps
        if declared != inferred {
            let missing: Vec<_> = inferred.difference(&declared).collect();
            let extra: Vec<_> = declared.difference(&inferred).collect();

            if !missing.is_empty() {
                model_issues.push(format!(
                    "missing deps: {}",
                    missing
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !extra.is_empty() {
                model_issues.push(format!(
                    "extra deps: {}",
                    extra
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }

            if fix && result.unqualified_relations.is_empty() {
                rewrite_deps_line(&model.path, &result.inferred_model_deps)?;
                if !quiet {
                    println!("{} {} (fixed deps)", "Fixed".green(), model.id);
                }
                continue;
            }
        }

        if !model_issues.is_empty() {
            issues += 1;
            if !quiet {
                println!("{} {}", "Issue".red(), model.id);
                for issue in model_issues {
                    println!("  {}", issue);
                }
            }
        }
    }

    if !quiet {
        if issues == 0 {
            println!("\n{} All deps match", "OK".green().bold());
        } else {
            println!(
                "\n{} {} model(s) with issues",
                "Issues".red().bold(),
                issues
            );
        }
    }

    Ok(if issues > 0 { 1 } else { 0 })
}

/// Lint model SQL for unqualified table references
/// Returns exit code: 0=ok, 1=issues found
pub fn lint_qualify(
    root: &Path,
    config: &Config,
    selectors: &[String],
    excludes: &[String],
    fix: bool,
    quiet: bool,
) -> Result<i32> {
    let project = load_project(root, config).context("load project")?;

    let selected = apply_selectors(&project, selectors, excludes)?;
    let models_to_lint: Vec<&Model> = selected
        .iter()
        .filter_map(|rel| project.models.get(rel))
        .collect();

    if models_to_lint.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(0);
    }

    let mut issues = 0;

    for model in &models_to_lint {
        let (result, new_sql) = qualify_model_sql(&project, model)?;

        let mut model_issues = Vec::new();

        if !result.unqualified.is_empty() {
            model_issues.push(format!("unqualified: {}", result.unqualified.join(", ")));
        }
        if !result.ambiguous.is_empty() {
            model_issues.push(format!("ambiguous: {}", result.ambiguous.join(", ")));
        }
        if !result.unknown.is_empty() {
            model_issues.push(format!("unknown: {}", result.unknown.join(", ")));
        }

        if fix && result.changed {
            if let Some(sql) = new_sql {
                rewrite_model_body_sql(&model.path, &sql)?;
                if !quiet {
                    println!("{} {} (qualified references)", "Fixed".green(), model.id);
                }
                continue;
            }
        }

        if !model_issues.is_empty() {
            issues += 1;
            if !quiet {
                println!("{} {}", "Issue".red(), model.id);
                for issue in model_issues {
                    println!("  {}", issue);
                }
            }
        }
    }

    if !quiet {
        if issues == 0 {
            println!("\n{} All references qualified", "OK".green().bold());
        } else {
            println!(
                "\n{} {} model(s) with issues",
                "Issues".red().bold(),
                issues
            );
        }
    }

    Ok(if issues > 0 { 1 } else { 0 })
}

/// Run all lints (deps + qualify)
/// Returns exit code: 0=ok, 1=issues found
pub fn check(
    root: &Path,
    config: &Config,
    selectors: &[String],
    excludes: &[String],
    quiet: bool,
) -> Result<i32> {
    let project = load_project(root, config).context("load project")?;

    let selected = apply_selectors(&project, selectors, excludes)?;
    let models_to_check: Vec<&Model> = selected
        .iter()
        .filter_map(|rel| project.models.get(rel))
        .collect();

    if models_to_check.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(0);
    }

    let mut total_issues = 0;

    for model in &models_to_check {
        let mut model_issues = Vec::new();

        // Check deps
        let deps_result = model_lint_deps(&project, model)?;
        let declared = declared_model_deps(model, &project);
        let inferred: std::collections::BTreeSet<_> =
            deps_result.inferred_model_deps.iter().cloned().collect();

        if !deps_result.unqualified_relations.is_empty() {
            model_issues.push(format!(
                "unqualified refs: {}",
                deps_result.unqualified_relations.join(", ")
            ));
        }
        if !deps_result.unknown_relations.is_empty() {
            model_issues.push(format!(
                "unknown refs: {}",
                deps_result.unknown_relations.join(", ")
            ));
        }
        if declared != inferred {
            let missing: Vec<_> = inferred.difference(&declared).collect();
            let extra: Vec<_> = declared.difference(&inferred).collect();
            if !missing.is_empty() {
                model_issues.push(format!(
                    "missing deps: {}",
                    missing
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !extra.is_empty() {
                model_issues.push(format!(
                    "extra deps: {}",
                    extra
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }

        // Check qualification
        let (qualify_result, _) = qualify_model_sql(&project, model)?;
        if !qualify_result.unqualified.is_empty() {
            model_issues.push(format!(
                "unqualified tables: {}",
                qualify_result.unqualified.join(", ")
            ));
        }
        if !qualify_result.ambiguous.is_empty() {
            model_issues.push(format!(
                "ambiguous tables: {}",
                qualify_result.ambiguous.join(", ")
            ));
        }

        if !model_issues.is_empty() {
            total_issues += 1;
            if !quiet {
                println!("{} {}", "Issue".red(), model.id);
                for issue in model_issues {
                    println!("  {}", issue);
                }
            }
        }
    }

    if !quiet {
        if total_issues == 0 {
            println!(
                "\n{} {} model(s) checked, no issues",
                "OK".green().bold(),
                models_to_check.len()
            );
        } else {
            println!(
                "\n{} {} model(s) with issues",
                "Issues".red().bold(),
                total_issues
            );
        }
    }

    Ok(if total_issues > 0 { 1 } else { 0 })
}

/// Initialize models directory structure
pub fn init(root: &Path, config: &Config, example: bool, quiet: bool) -> Result<()> {
    let models_dir = root.join(config.models_dir());

    if models_dir.exists() {
        bail!("models directory already exists: {}", models_dir.display());
    }

    // Create models directory
    fs::create_dir_all(&models_dir).context("create models/")?;
    fs::write(models_dir.join(".gitkeep"), "").context("create .gitkeep")?;
    if !quiet {
        println!(
            "{} {}/",
            "Created".green(),
            models_dir
                .strip_prefix(root)
                .unwrap_or(&models_dir)
                .display()
        );
    }

    // Create default schema directory for convenience (matches common Postgres usage).
    let public_dir = models_dir.join("public");
    fs::create_dir_all(&public_dir).context("create models/public/")?;
    fs::write(public_dir.join(".gitkeep"), "").context("create models/public/.gitkeep")?;
    if !quiet {
        println!(
            "{} {}/",
            "Created".green(),
            public_dir
                .strip_prefix(root)
                .unwrap_or(&public_dir)
                .display()
        );
    }

    // Add target/ to .gitignore if not already there
    let gitignore_path = root.join(".gitignore");
    let needs_target = if gitignore_path.exists() {
        let content = fs::read_to_string(&gitignore_path).unwrap_or_default();
        !content
            .lines()
            .any(|l| l.trim() == "target/" || l.trim() == "target")
    } else {
        true
    };

    if needs_target {
        let mut content = if gitignore_path.exists() {
            fs::read_to_string(&gitignore_path).unwrap_or_default()
        } else {
            String::new()
        };
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str("target/\n");
        fs::write(&gitignore_path, content).context("update .gitignore")?;
        if !quiet {
            println!("{} .gitignore (added target/)", "Updated".green());
        }
    }

    // Create example model if requested
    if example {
        let example_dir = models_dir.join("example");
        fs::create_dir_all(&example_dir).context("create models/example/")?;

        let example_sql = r#"-- materialized: view
-- deps: staging.source_table
-- tests: not_null(id), unique(id)
-- Tip: Run `pgcrate model status` before `pgcrate model run`

SELECT 1 AS id, 'Hello from pgcrate models!' AS message;
"#;
        fs::write(example_dir.join("hello.sql"), example_sql).context("create hello.sql")?;
        if !quiet {
            println!("{} models/example/hello.sql", "Created".green());
        }
    }

    if !quiet {
        println!("\n{} Models initialized", "Done.".green().bold());
        if !example {
            println!("Create your first model in models/<schema>/<name>.sql");
            println!("Tip: `pgcrate model new public.my_model` scaffolds a template.");
        }
    }

    Ok(())
}

fn dry_run_sql(model: &Model, full_refresh: bool) -> String {
    match model.header.materialized {
        crate::model::Materialized::Incremental if !full_refresh => {
            let body = model.body_sql.trim().trim_end_matches(';').trim();
            let uk = model.header.unique_key.join(", ");
            format!(
                "-- incremental model: {} (unique_key: {})\n-- Note: MERGE is generated at runtime based on existing table state\n{};\n",
                model.id, uk, body
            )
        }
        _ => {
            let mut sql = crate::model::generate_run_sql(model);
            if !sql.trim_end().ends_with(';') {
                sql.push(';');
            }
            sql.push('\n');
            sql
        }
    }
}

/// Move/rename a model from one id to another
#[allow(clippy::too_many_arguments)]
pub async fn move_model(
    root: &Path,
    config: &Config,
    database_url: Option<&str>,
    source_id: &str,
    dest_id: &str,
    drop_old: bool,
    yes: bool,
    quiet: bool,
) -> Result<()> {
    let source_rel = Relation::parse(source_id)?;
    let dest_rel = Relation::parse(dest_id)?;

    if source_rel == dest_rel {
        bail!("Source and destination are the same: {}", source_rel);
    }

    let models_dir = root.join(config.models_dir());

    // Check source exists
    let source_path = models_dir
        .join(&source_rel.schema)
        .join(format!("{}.sql", source_rel.name));
    if !source_path.exists() {
        bail!(
            "Source model not found: {} ({})",
            source_rel,
            source_path.display()
        );
    }

    // Check destination doesn't exist (unless --force/--yes)
    let dest_schema_dir = models_dir.join(&dest_rel.schema);
    let dest_path = dest_schema_dir.join(format!("{}.sql", dest_rel.name));
    if dest_path.exists() && !yes {
        if std::io::stdin().is_terminal() {
            let overwrite = dialoguer::Confirm::new()
                .with_prompt(format!(
                    "Destination {} already exists. Overwrite?",
                    dest_path.strip_prefix(root).unwrap_or(&dest_path).display()
                ))
                .default(false)
                .interact()?;
            if !overwrite {
                if !quiet {
                    println!("Aborted.");
                }
                return Ok(());
            }
        } else {
            bail!(
                "Destination model already exists: {} (use -y to overwrite)",
                dest_path.display()
            );
        }
    }

    // Drop old database object if requested
    if drop_old {
        if let Some(url) = database_url {
            let client = connect(url).await?;

            // Check what type of object exists
            let view_row = client
                .query_opt(
                    "SELECT 1 FROM information_schema.views WHERE table_schema = $1 AND table_name = $2",
                    &[&source_rel.schema, &source_rel.name],
                )
                .await
                .context("check if view exists")?;
            let table_row = client
                .query_opt(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'",
                    &[&source_rel.schema, &source_rel.name],
                )
                .await
                .context("check if table exists")?;

            if view_row.is_some() {
                let drop_sql = format!(
                    "DROP VIEW {}.{} CASCADE",
                    crate::sql::quote_ident(&source_rel.schema),
                    crate::sql::quote_ident(&source_rel.name)
                );
                client
                    .batch_execute(&drop_sql)
                    .await
                    .with_context(|| format!("drop view {}", source_rel))?;
                if !quiet {
                    println!("{} view {}", "Dropped".yellow(), source_rel);
                }
            } else if table_row.is_some() {
                let drop_sql = format!(
                    "DROP TABLE {}.{} CASCADE",
                    crate::sql::quote_ident(&source_rel.schema),
                    crate::sql::quote_ident(&source_rel.name)
                );
                client
                    .batch_execute(&drop_sql)
                    .await
                    .with_context(|| format!("drop table {}", source_rel))?;
                if !quiet {
                    println!("{} table {}", "Dropped".yellow(), source_rel);
                }
            } else if !quiet {
                println!(
                    "{} {} (no database object found)",
                    "Skipped drop".dimmed(),
                    source_rel
                );
            }
        }
    }

    // Create destination schema directory if needed
    fs::create_dir_all(&dest_schema_dir)
        .with_context(|| format!("create directory {}", dest_schema_dir.display()))?;

    // Move the file
    fs::rename(&source_path, &dest_path)
        .with_context(|| format!("move {} to {}", source_path.display(), dest_path.display()))?;

    if !quiet {
        println!(
            "{} {} -> {}",
            "Moved".green(),
            source_path
                .strip_prefix(root)
                .unwrap_or(&source_path)
                .display(),
            dest_path.strip_prefix(root).unwrap_or(&dest_path).display()
        );
    }

    // Clean up empty source schema directory
    let source_schema_dir = models_dir.join(&source_rel.schema);
    if source_schema_dir.is_dir() {
        let is_empty = source_schema_dir
            .read_dir()
            .map(|mut d| d.next().is_none())
            .unwrap_or(false);
        if is_empty {
            fs::remove_dir(&source_schema_dir).ok(); // Ignore errors
        }
    }

    if !quiet {
        println!(
            "\n{} Run `pgcrate model run -s {}` to create the new database object",
            "Next:".cyan(),
            dest_rel
        );
    }

    show_tip(TipContext::Move, quiet);

    Ok(())
}

fn pluralize<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 {
        singular
    } else {
        plural
    }
}

fn pluralize_u64<'a>(count: u64, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 {
        singular
    } else {
        plural
    }
}

/// Format a number with thousands separators (e.g., 1234567 -> "1,234,567")
fn format_number(n: i64) -> String {
    let s = n.to_string();
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::new();
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(*c);
    }
    result
}
