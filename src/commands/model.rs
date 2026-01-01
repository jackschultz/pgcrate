use anyhow::{bail, Context, Result};
use colored::Colorize;
use std::fs;
use std::path::Path;

use crate::config::Config;
use crate::model::{
    apply_selectors, compile_model, execute_model, lint_deps as model_lint_deps, load_project,
    qualify_model_sql, rewrite_deps_line, rewrite_model_body_sql, topo_sort, Model, Project,
    Relation, Test,
};

use super::connect;

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
    quiet: bool,
) -> Result<()> {
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
    quiet: bool,
) -> Result<()> {
    let project = load_project(root, config).context("load project")?;

    let models_to_run = apply_selectors(&project, selectors, excludes)?;

    if models_to_run.is_empty() {
        if !quiet {
            println!("No models found");
        }
        return Ok(());
    }

    if dry_run {
        if full_refresh {
            println!("{}", "Execution plan (full refresh):".bold());
        } else {
            println!("{}", "Execution plan:".bold());
        }
        for (i, rel) in models_to_run.iter().enumerate() {
            let model = project.models.get(rel).unwrap();
            println!(
                "  {}. {} ({})",
                i + 1,
                rel,
                model.header.materialized.as_str()
            );
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
        if !quiet {
            print!("{} {}... ", "Running".cyan(), rel);
        }
        execute_model(&client, model, full_refresh).await?;
        if !quiet {
            println!("{}", "ok".green());
        }
    }

    if !quiet {
        println!(
            "\n{} {} model(s) executed",
            "Done.".green().bold(),
            models_to_run.len()
        );
    }

    Ok(())
}

/// Run data tests defined in model headers
/// Returns exit code: 0=all pass, 1=failures, 2=error
pub async fn test(
    root: &Path,
    config: &Config,
    database_url: &str,
    selectors: &[String],
    excludes: &[String],
    quiet: bool,
) -> Result<i32> {
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

fn print_ascii_graph(project: &crate::model::Project, models: &[Relation]) {
    for rel in models {
        let model = project.models.get(rel).unwrap();
        let deps: Vec<_> = model.header.deps.iter().map(|d| d.to_string()).collect();
        if deps.is_empty() {
            println!("{}", rel);
        } else {
            println!("{} <- [{}]", rel, deps.join(", "));
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

fn print_json_graph(project: &crate::model::Project, models: &[Relation]) {
    println!("{{");
    println!("  \"nodes\": [");
    for (i, rel) in models.iter().enumerate() {
        let comma = if i < models.len() - 1 { "," } else { "" };
        println!("    \"{}\"{}", rel, comma);
    }
    println!("  ],");
    println!("  \"edges\": [");
    let mut edges: Vec<(String, String)> = Vec::new();
    for rel in models {
        let model = project.models.get(rel).unwrap();
        for dep in &model.header.deps {
            edges.push((dep.to_string(), rel.to_string()));
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
pub fn init(root: &Path, example: bool, quiet: bool) -> Result<()> {
    let models_dir = root.join("models");

    if models_dir.exists() {
        bail!("models/ directory already exists");
    }

    // Create models directory
    fs::create_dir_all(&models_dir).context("create models/")?;
    fs::write(models_dir.join(".gitkeep"), "").context("create .gitkeep")?;
    if !quiet {
        println!("{} models/", "Created".green());
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
-- deps:

SELECT 'Hello from pgcrate models!' AS message;
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
        }
    }

    Ok(())
}
