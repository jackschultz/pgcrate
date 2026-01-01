use anyhow::{anyhow, bail, Context, Result};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use walkdir::WalkDir;

use super::{parse_model_file, Model, Project, Relation};
use crate::config::Config;

/// Load a project from filesystem using config for paths and sources.
pub fn load_project(root: &Path, config: &Config) -> Result<Project> {
    let models_dir = root.join(config.models_dir());
    if !models_dir.is_dir() {
        bail!("models directory not found: {}", models_dir.display());
    }

    let sources: HashSet<Relation> = config
        .model_sources()
        .iter()
        .map(|s| Relation::parse(s))
        .collect::<Result<_>>()
        .context("parse sources from config")?;

    let mut models = HashMap::<Relation, Model>::new();
    for entry in WalkDir::new(&models_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(|s| s.to_str()) != Some("sql") {
            continue;
        }

        let rel = model_id_from_path(&models_dir, entry.path())?;
        let (header, body_sql) = parse_model_file(entry.path())?;
        let model = Model {
            id: rel.clone(),
            path: entry.path().to_path_buf(),
            header,
            body_sql,
        };
        if models.insert(rel.clone(), model).is_some() {
            bail!("duplicate model: {}", rel);
        }
    }

    Ok(Project {
        root: root.to_path_buf(),
        models,
        sources,
    })
}

/// Derive model id from file path: models/<schema>/<name>.sql -> schema.name
fn model_id_from_path(models_dir: &Path, path: &Path) -> Result<Relation> {
    let rel = path.strip_prefix(models_dir).with_context(|| {
        format!(
            "path {} not under models dir {}",
            path.display(),
            models_dir.display()
        )
    })?;

    let mut comps = rel.components();
    let schema = comps
        .next()
        .ok_or_else(|| anyhow!("model path missing schema: {}", path.display()))?
        .as_os_str()
        .to_string_lossy()
        .to_string();
    let filename = comps
        .next()
        .ok_or_else(|| anyhow!("model path missing filename: {}", path.display()))?
        .as_os_str()
        .to_string_lossy()
        .to_string();

    if comps.next().is_some() {
        bail!(
            "nested paths not supported (expected models/<schema>/<name>.sql): {}",
            path.display()
        );
    }

    let name = filename
        .strip_suffix(".sql")
        .ok_or_else(|| anyhow!("model file must end in .sql: {}", path.display()))?
        .to_string();

    Ok(Relation { schema, name })
}

/// Topological sort of all models. Returns execution order (deps before dependents).
pub fn topo_sort(project: &Project) -> Result<Vec<Relation>> {
    let mut in_degree: HashMap<Relation, usize> = HashMap::new();
    let mut dependents: HashMap<Relation, Vec<Relation>> = HashMap::new();

    for rel in project.models.keys() {
        in_degree.insert(rel.clone(), 0);
        dependents.insert(rel.clone(), Vec::new());
    }

    for (rel, model) in &project.models {
        for dep in &model.header.deps {
            if project.models.contains_key(dep) {
                *in_degree.get_mut(rel).unwrap() += 1;
                dependents.get_mut(dep).unwrap().push(rel.clone());
            }
        }
    }

    // Kahn's algorithm
    let mut queue: VecDeque<Relation> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(rel, _)| rel.clone())
        .collect();

    let mut result: Vec<Relation> = Vec::new();
    while let Some(rel) = queue.pop_front() {
        result.push(rel.clone());
        for dependent in dependents.get(&rel).unwrap_or(&Vec::new()) {
            let deg = in_degree.get_mut(dependent).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(dependent.clone());
            }
        }
    }

    if result.len() != project.models.len() {
        let in_cycle: Vec<String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg > 0)
            .map(|(rel, _)| rel.to_string())
            .collect();
        bail!("circular dependency: {}", in_cycle.join(", "));
    }

    Ok(result)
}

/// Get execution order for a model and all its upstream dependencies.
pub fn get_upstream_order(project: &Project, target: &Relation) -> Result<Vec<Relation>> {
    if !project.models.contains_key(target) {
        bail!("unknown model: {}", target);
    }

    let mut visited: HashSet<Relation> = HashSet::new();
    let mut in_stack: HashSet<Relation> = HashSet::new();
    let mut order: Vec<Relation> = Vec::new();

    fn visit(
        project: &Project,
        rel: &Relation,
        visited: &mut HashSet<Relation>,
        in_stack: &mut HashSet<Relation>,
        order: &mut Vec<Relation>,
    ) -> Result<()> {
        if visited.contains(rel) {
            return Ok(());
        }
        if in_stack.contains(rel) {
            bail!("circular dependency involving: {}", rel);
        }

        in_stack.insert(rel.clone());

        if let Some(model) = project.models.get(rel) {
            for dep in &model.header.deps {
                if project.models.contains_key(dep) {
                    visit(project, dep, visited, in_stack, order)?;
                }
            }
        }

        in_stack.remove(rel);
        visited.insert(rel.clone());
        order.push(rel.clone());
        Ok(())
    }

    visit(project, target, &mut visited, &mut in_stack, &mut order)?;
    Ok(order)
}

/// Get execution order for a model and all its downstream dependents.
/// Returns models in DAG order (the target model first, then dependents).
pub fn get_downstream_order(project: &Project, target: &Relation) -> Result<Vec<Relation>> {
    if !project.models.contains_key(target) {
        bail!("unknown model: {}", target);
    }

    // Build reverse dependency map: model -> models that depend on it
    let mut dependents: HashMap<Relation, Vec<Relation>> = HashMap::new();
    for rel in project.models.keys() {
        dependents.insert(rel.clone(), Vec::new());
    }
    for (rel, model) in &project.models {
        for dep in &model.header.deps {
            if let Some(list) = dependents.get_mut(dep) {
                list.push(rel.clone());
            }
        }
    }

    // Collect all downstream models using BFS
    let mut visited: HashSet<Relation> = HashSet::new();
    let mut queue: VecDeque<Relation> = VecDeque::new();
    queue.push_back(target.clone());
    visited.insert(target.clone());

    while let Some(rel) = queue.pop_front() {
        if let Some(deps) = dependents.get(&rel) {
            for dependent in deps {
                if !visited.contains(dependent) {
                    visited.insert(dependent.clone());
                    queue.push_back(dependent.clone());
                }
            }
        }
    }

    // Return in DAG order (filter topo_sort to only include downstream models)
    let all_sorted = topo_sort(project)?;
    let result: Vec<Relation> = all_sorted
        .into_iter()
        .filter(|rel| visited.contains(rel))
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Materialized, ModelHeader};
    use std::path::PathBuf;

    fn make_project(models: Vec<(&str, Vec<&str>)>) -> Project {
        let mut project = Project {
            root: PathBuf::new(),
            sources: HashSet::new(),
            models: HashMap::new(),
        };
        for (name, deps) in models {
            let rel = Relation::parse(name).unwrap();
            let header = ModelHeader {
                materialized: Materialized::View,
                deps: deps.iter().map(|d| Relation::parse(d).unwrap()).collect(),
                unique_key: Vec::new(),
                tests: Vec::new(),
                tags: Vec::new(),
            };
            project.models.insert(
                rel.clone(),
                Model {
                    id: rel,
                    path: PathBuf::new(),
                    header,
                    body_sql: String::new(),
                },
            );
        }
        project
    }

    #[test]
    fn test_topo_sort_empty() {
        let project = make_project(vec![]);
        let order = topo_sort(&project).unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_topo_sort_single() {
        let project = make_project(vec![("a.x", vec![])]);
        let order = topo_sort(&project).unwrap();
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn test_topo_sort_linear() {
        let project = make_project(vec![
            ("a.a", vec![]),
            ("a.b", vec!["a.a"]),
            ("a.c", vec!["a.b"]),
        ]);
        let order = topo_sort(&project).unwrap();
        assert_eq!(order.len(), 3);
        let pos_a = order.iter().position(|r| r.name == "a").unwrap();
        let pos_b = order.iter().position(|r| r.name == "b").unwrap();
        let pos_c = order.iter().position(|r| r.name == "c").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn test_topo_sort_diamond() {
        let project = make_project(vec![
            ("s.a", vec![]),
            ("s.b", vec!["s.a"]),
            ("s.c", vec!["s.a"]),
            ("s.d", vec!["s.b", "s.c"]),
        ]);
        let order = topo_sort(&project).unwrap();
        assert_eq!(order.len(), 4);
        let pos_a = order.iter().position(|r| r.name == "a").unwrap();
        let pos_b = order.iter().position(|r| r.name == "b").unwrap();
        let pos_c = order.iter().position(|r| r.name == "c").unwrap();
        let pos_d = order.iter().position(|r| r.name == "d").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_a < pos_c);
        assert!(pos_b < pos_d);
        assert!(pos_c < pos_d);
    }

    #[test]
    fn test_topo_sort_cycle() {
        let project = make_project(vec![("a.x", vec!["a.y"]), ("a.y", vec!["a.x"])]);
        let err = topo_sort(&project).unwrap_err();
        assert!(err.to_string().contains("circular"));
    }

    #[test]
    fn test_upstream_order_single() {
        let project = make_project(vec![("a.x", vec![])]);
        let target = Relation::parse("a.x").unwrap();
        let order = get_upstream_order(&project, &target).unwrap();
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], target);
    }

    #[test]
    fn test_upstream_order_with_deps() {
        let project = make_project(vec![
            ("a.a", vec![]),
            ("a.b", vec!["a.a"]),
            ("a.c", vec!["a.b"]),
        ]);
        let target = Relation::parse("a.c").unwrap();
        let order = get_upstream_order(&project, &target).unwrap();
        assert_eq!(order.len(), 3);
        assert_eq!(order[0].name, "a");
        assert_eq!(order[1].name, "b");
        assert_eq!(order[2].name, "c");
    }

    #[test]
    fn test_upstream_order_unknown_model() {
        let project = make_project(vec![]);
        let target = Relation::parse("a.x").unwrap();
        let err = get_upstream_order(&project, &target).unwrap_err();
        assert!(err.to_string().contains("unknown model"));
    }

    #[test]
    fn test_downstream_order_single() {
        let project = make_project(vec![("a.x", vec![])]);
        let target = Relation::parse("a.x").unwrap();
        let order = get_downstream_order(&project, &target).unwrap();
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], target);
    }

    #[test]
    fn test_downstream_order_with_dependents() {
        let project = make_project(vec![
            ("a.a", vec![]),
            ("a.b", vec!["a.a"]),
            ("a.c", vec!["a.a"]),
            ("a.d", vec!["a.b", "a.c"]),
        ]);
        // Get downstream of a.a - should include a.b, a.c, a.d
        let target = Relation::parse("a.a").unwrap();
        let order = get_downstream_order(&project, &target).unwrap();
        assert_eq!(order.len(), 4);
        // Should be in DAG order: a first, then b/c, then d
        assert_eq!(order[0].name, "a");
        assert_eq!(order[3].name, "d");
    }

    #[test]
    fn test_downstream_order_leaf() {
        let project = make_project(vec![
            ("a.a", vec![]),
            ("a.b", vec!["a.a"]),
            ("a.c", vec!["a.b"]),
        ]);
        // Get downstream of a.c - only a.c (it's a leaf)
        let target = Relation::parse("a.c").unwrap();
        let order = get_downstream_order(&project, &target).unwrap();
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].name, "c");
    }

    #[test]
    fn test_downstream_order_unknown_model() {
        let project = make_project(vec![]);
        let target = Relation::parse("a.x").unwrap();
        let err = get_downstream_order(&project, &target).unwrap_err();
        assert!(err.to_string().contains("unknown model"));
    }
}
