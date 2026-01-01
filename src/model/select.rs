use anyhow::{bail, Result};
use std::collections::HashSet;
use std::str::FromStr;

use super::{Project, Relation};

/// A selector for filtering models
#[derive(Clone, Debug, PartialEq)]
pub enum Selector {
    /// Exact model match: schema.name
    Exact(Relation),
    /// Models with tag: tag:name
    Tag(String),
    /// Model + upstream dependencies: deps:schema.name
    Deps(Relation),
    /// Model + downstream dependents: downstream:schema.name
    Downstream(Relation),
    /// Full lineage (up + down): tree:schema.name
    Tree(Relation),
}

impl FromStr for Selector {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();

        if let Some(tag) = s.strip_prefix("tag:") {
            let tag = tag.trim();
            if tag.is_empty() {
                bail!("empty tag in selector: {}", s);
            }
            return Ok(Selector::Tag(tag.to_lowercase()));
        }

        if let Some(model) = s.strip_prefix("deps:") {
            let model = model.trim();
            if model.is_empty() {
                bail!("empty model in selector: {}", s);
            }
            return Ok(Selector::Deps(Relation::parse(model)?));
        }

        if let Some(model) = s.strip_prefix("downstream:") {
            let model = model.trim();
            if model.is_empty() {
                bail!("empty model in selector: {}", s);
            }
            return Ok(Selector::Downstream(Relation::parse(model)?));
        }

        if let Some(model) = s.strip_prefix("tree:") {
            let model = model.trim();
            if model.is_empty() {
                bail!("empty model in selector: {}", s);
            }
            return Ok(Selector::Tree(Relation::parse(model)?));
        }

        // No prefix - must be exact model match
        if !s.contains('.') {
            bail!(
                "invalid selector '{}': expected 'schema.name' or prefix like 'tag:', 'deps:', 'downstream:', 'tree:'",
                s
            );
        }

        Ok(Selector::Exact(Relation::parse(s)?))
    }
}

/// Resolve a selector to a set of model relations
pub fn resolve_selector(project: &Project, selector: &Selector) -> Result<HashSet<Relation>> {
    let mut result = HashSet::new();

    match selector {
        Selector::Exact(rel) => {
            if !project.models.contains_key(rel) {
                bail!("model not found: {}", rel);
            }
            result.insert(rel.clone());
        }
        Selector::Tag(tag) => {
            for (rel, model) in &project.models {
                if model.header.tags.contains(tag) {
                    result.insert(rel.clone());
                }
            }
            // Note: empty result for non-existent tag is OK (no error)
        }
        Selector::Deps(rel) => {
            if !project.models.contains_key(rel) {
                bail!("model not found: {}", rel);
            }
            let upstream = super::get_upstream_order(project, rel)?;
            result.extend(upstream);
        }
        Selector::Downstream(rel) => {
            if !project.models.contains_key(rel) {
                bail!("model not found: {}", rel);
            }
            let downstream = super::get_downstream_order(project, rel)?;
            result.extend(downstream);
        }
        Selector::Tree(rel) => {
            if !project.models.contains_key(rel) {
                bail!("model not found: {}", rel);
            }
            let upstream = super::get_upstream_order(project, rel)?;
            let downstream = super::get_downstream_order(project, rel)?;
            result.extend(upstream);
            result.extend(downstream);
        }
    }

    Ok(result)
}

/// Apply selectors and excludes to get final model set
/// Returns models in DAG order
pub fn apply_selectors(
    project: &Project,
    selectors: &[String],
    excludes: &[String],
) -> Result<Vec<Relation>> {
    // Parse selectors
    let parsed_selectors: Vec<Selector> = selectors
        .iter()
        .map(|s| s.parse())
        .collect::<Result<Vec<_>>>()?;

    let parsed_excludes: Vec<Selector> = excludes
        .iter()
        .map(|s| s.parse())
        .collect::<Result<Vec<_>>>()?;

    // Build selected set
    let mut selected: HashSet<Relation> = if parsed_selectors.is_empty() {
        // No selectors = all models
        project.models.keys().cloned().collect()
    } else {
        // Union of all selector matches
        let mut set = HashSet::new();
        for selector in &parsed_selectors {
            let matches = resolve_selector(project, selector)?;
            set.extend(matches);
        }
        set
    };

    // Apply excludes
    for exclude in &parsed_excludes {
        let to_remove = resolve_selector(project, exclude)?;
        for rel in to_remove {
            selected.remove(&rel);
        }
    }

    // Return in DAG order
    let all_sorted = super::topo_sort(project)?;
    let result: Vec<Relation> = all_sorted
        .into_iter()
        .filter(|rel| selected.contains(rel))
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exact() {
        let sel: Selector = "analytics.users".parse().unwrap();
        assert!(matches!(sel, Selector::Exact(r) if r.schema == "analytics" && r.name == "users"));
    }

    #[test]
    fn test_parse_tag() {
        let sel: Selector = "tag:daily".parse().unwrap();
        assert!(matches!(sel, Selector::Tag(t) if t == "daily"));
    }

    #[test]
    fn test_parse_tag_uppercase_normalized() {
        let sel: Selector = "tag:DAILY".parse().unwrap();
        assert!(matches!(sel, Selector::Tag(t) if t == "daily"));
    }

    #[test]
    fn test_parse_deps() {
        let sel: Selector = "deps:analytics.users".parse().unwrap();
        assert!(matches!(sel, Selector::Deps(r) if r.schema == "analytics" && r.name == "users"));
    }

    #[test]
    fn test_parse_downstream() {
        let sel: Selector = "downstream:staging.raw".parse().unwrap();
        assert!(matches!(sel, Selector::Downstream(r) if r.schema == "staging" && r.name == "raw"));
    }

    #[test]
    fn test_parse_tree() {
        let sel: Selector = "tree:analytics.orders".parse().unwrap();
        assert!(matches!(sel, Selector::Tree(r) if r.schema == "analytics" && r.name == "orders"));
    }

    #[test]
    fn test_parse_empty_tag_error() {
        let err = "tag:".parse::<Selector>().unwrap_err();
        assert!(err.to_string().contains("empty tag"));
    }

    #[test]
    fn test_parse_empty_deps_error() {
        let err = "deps:".parse::<Selector>().unwrap_err();
        assert!(err.to_string().contains("empty model"));
    }

    #[test]
    fn test_parse_unqualified_error() {
        let err = "users".parse::<Selector>().unwrap_err();
        assert!(err.to_string().contains("schema.name"));
    }

    #[test]
    fn test_parse_invalid_relation_error() {
        let err = "a.b.c".parse::<Selector>().unwrap_err();
        assert!(err.to_string().contains("expected schema"));
    }
}
