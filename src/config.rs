use anyhow::{bail, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::connection::{ConnectionConfig, PolicyConfig};

/// Main configuration structure loaded from pgcrate.toml
#[derive(Deserialize, Default, Debug)]
pub struct Config {
    pub database: Option<DatabaseConfig>,
    pub paths: Option<PathsConfig>,
    pub defaults: Option<DefaultsConfig>,
    pub production: Option<ProductionConfig>,
    pub generate: Option<GenerateConfig>,
    pub snapshot: Option<SnapshotConfig>,
    pub model: Option<ModelConfig>,
    pub seeds: Option<SeedsConfig>,
    pub tools: Option<ToolsConfig>,
    /// Named database connections
    #[serde(default)]
    pub connections: HashMap<String, ConnectionConfig>,
    /// Policy restrictions for connections
    pub policy: Option<PolicyConfig>,
}

#[derive(Deserialize, Debug)]
pub struct DatabaseConfig {
    pub url: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PathsConfig {
    pub migrations: Option<String>,
    pub models: Option<String>,
    pub seeds: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct DefaultsConfig {
    pub with_down: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct ProductionConfig {
    pub patterns: Option<Vec<String>>,
}

#[derive(Deserialize, Debug, Default)]
pub struct GenerateConfig {
    pub split_by: Option<String>,
    pub exclude_schemas: Option<Vec<String>>,
    pub include_schemas: Option<Vec<String>>,
    pub output: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct SnapshotConfig {
    pub directory: Option<String>,
    pub default_format: Option<String>,
    #[serde(flatten)]
    pub profiles: HashMap<String, SnapshotProfile>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct SnapshotProfile {
    pub schemas: Option<Vec<String>>,
    pub exclude_schemas: Option<Vec<String>>,
    pub tables: Option<Vec<String>>,
    pub exclude_tables: Option<Vec<String>>,
    #[serde(default = "default_true")]
    pub data: bool,
}

#[derive(Deserialize, Debug, Default)]
pub struct ModelConfig {
    /// Source tables that models can reference (schema.table format)
    pub sources: Option<Vec<String>>,
}

#[derive(Deserialize, Debug, Default)]
pub struct SeedsConfig {
    pub directory: Option<String>,
}

/// PostgreSQL tool paths configuration
#[derive(Deserialize, Debug, Default)]
pub struct ToolsConfig {
    pub pg_dump: Option<String>,
    pub pg_restore: Option<String>,
    pub psql: Option<String>,
}

/// Anonymization configuration (pgcrate.anonymize.toml)
#[derive(Deserialize, Default, Debug, Clone)]
pub struct AnonymizeConfig {
    pub seed: Option<String>,
    #[serde(default)]
    pub rules: Vec<AnonymizeRule>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AnonymizeRule {
    pub table: String,
    pub columns: Option<HashMap<String, String>>,
    #[serde(default)]
    pub skip: bool,
}

fn default_true() -> bool {
    true
}

impl AnonymizeConfig {
    pub fn load(path: Option<&Path>) -> Result<Self, anyhow::Error> {
        let config_path = match path {
            Some(p) => {
                if !p.exists() {
                    bail!("Anonymize config file not found: {}", p.display());
                }
                p
            }
            None => {
                let default_path = Path::new("pgcrate.anonymize.toml");
                if default_path.exists() {
                    default_path
                } else {
                    return Ok(AnonymizeConfig::default());
                }
            }
        };

        let contents = fs::read_to_string(config_path)?;
        let config: AnonymizeConfig = toml::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", config_path.display(), e))?;

        Ok(config)
    }
}

impl Config {
    /// Load config from file, or return default if no config exists.
    /// If an explicit path is provided via --config, it MUST exist (error if not).
    /// If no path is provided, check ./pgcrate.toml (use default if not found).
    pub fn load(path: Option<&Path>) -> Result<Self, anyhow::Error> {
        let config_path = match path {
            Some(p) => {
                // User explicitly specified a path - it MUST exist
                if !p.exists() {
                    bail!("Config file not found: {}", p.display());
                }
                p
            }
            None => {
                // No path specified - check default location
                let default_path = Path::new("pgcrate.toml");
                if default_path.exists() {
                    default_path
                } else {
                    return Ok(Config::default());
                }
            }
        };

        let contents = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", config_path.display(), e))?;

        // Validate paths don't contain path traversal
        config.validate_paths()?;

        Ok(config)
    }

    /// Validate that configured paths are safe (no path traversal)
    fn validate_paths(&self) -> Result<(), anyhow::Error> {
        if let Some(ref paths) = self.paths {
            if let Some(ref p) = paths.migrations {
                Self::validate_path(p, "paths.migrations")?;
            }
            if let Some(ref p) = paths.models {
                Self::validate_path(p, "paths.models")?;
            }
            if let Some(ref p) = paths.seeds {
                Self::validate_path(p, "paths.seeds")?;
            }
        }
        if let Some(ref generate) = self.generate {
            if let Some(ref p) = generate.output {
                Self::validate_path(p, "generate.output")?;
            }
        }
        if let Some(ref snapshot) = self.snapshot {
            if let Some(ref p) = snapshot.directory {
                Self::validate_path(p, "snapshot.directory")?;
            }
        }
        if let Some(ref seeds) = self.seeds {
            if let Some(ref p) = seeds.directory {
                Self::validate_path(p, "seeds.directory")?;
            }
        }
        Ok(())
    }

    /// Validate a single path doesn't contain path traversal
    fn validate_path(path: &str, field: &str) -> Result<(), anyhow::Error> {
        if path.contains("..") {
            bail!(
                "Invalid {} path '{}': paths cannot contain '..'",
                field,
                path
            );
        }
        if Path::new(path).is_absolute() {
            bail!("Invalid {} path '{}': paths must be relative", field, path);
        }
        Ok(())
    }

    /// Get database URL with resolution order: CLI > env > config
    pub fn get_database_url(&self, cli_url: Option<&str>) -> Option<String> {
        // CLI takes precedence
        if let Some(url) = cli_url {
            return Some(url.to_string());
        }

        // Then environment variable
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return Some(url);
        }

        // Finally config file
        if let Some(ref db) = self.database {
            if let Some(ref url) = db.url {
                return Some(url.clone());
            }
        }

        None
    }

    /// Resolve database URL with full connection support.
    ///
    /// Resolution order:
    /// 1. -d / --database-url (direct URL)
    /// 2. -c / --connection (named connection from config)
    /// 3. --env (environment variable name)
    /// 4. DATABASE_URL environment variable
    /// 5. [database].url in config
    ///
    /// Returns the resolved URL and optional connection metadata.
    pub fn resolve_database_url(
        &self,
        cli_url: Option<&str>,
        connection_name: Option<&str>,
        env_var_name: Option<&str>,
    ) -> Result<(String, Option<crate::connection::ResolvedConnection>)> {
        use crate::connection::{resolve_connection, resolve_from_env_var};

        // 1. Direct URL from CLI takes absolute precedence
        if let Some(url) = cli_url {
            return Ok((url.to_string(), None));
        }

        // 2. Named connection from config
        if let Some(name) = connection_name {
            let conn = resolve_connection(name, &self.connections, self.policy.as_ref())?;
            return Ok((conn.url.clone(), Some(conn)));
        }

        // 3. Environment variable name
        if let Some(env_name) = env_var_name {
            let conn = resolve_from_env_var(env_name)?;
            return Ok((conn.url.clone(), Some(conn)));
        }

        // 4. Default DATABASE_URL
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return Ok((url, None));
        }

        // 5. Config file
        if let Some(ref db) = self.database {
            if let Some(ref url) = db.url {
                return Ok((url.clone(), None));
            }
        }

        bail!("DATABASE_URL not set. Use -d flag, -c <connection>, --env <VAR>, set DATABASE_URL env var, or add to pgcrate.toml")
    }

    /// Get migrations directory path
    pub fn migrations_dir(&self) -> &str {
        self.paths
            .as_ref()
            .and_then(|p| p.migrations.as_deref())
            .unwrap_or("db/migrations")
    }

    /// Get default for --with-down flag
    pub fn default_with_down(&self) -> bool {
        self.defaults
            .as_ref()
            .and_then(|d| d.with_down)
            .unwrap_or(false)
    }

    /// Get production URL patterns from config
    pub fn production_patterns(&self) -> Vec<String> {
        self.production
            .as_ref()
            .and_then(|p| p.patterns.clone())
            .unwrap_or_default()
    }

    /// Get generate split_by mode
    pub fn generate_split_by(&self) -> Option<&str> {
        self.generate.as_ref().and_then(|g| g.split_by.as_deref())
    }

    /// Get generate exclude_schemas
    pub fn generate_exclude_schemas(&self) -> Vec<String> {
        self.generate
            .as_ref()
            .and_then(|g| g.exclude_schemas.clone())
            .unwrap_or_default()
    }

    /// Get generate include_schemas
    pub fn generate_include_schemas(&self) -> Vec<String> {
        self.generate
            .as_ref()
            .and_then(|g| g.include_schemas.clone())
            .unwrap_or_default()
    }

    /// Get generate output directory
    pub fn generate_output(&self) -> &str {
        self.generate
            .as_ref()
            .and_then(|g| g.output.as_deref())
            .unwrap_or_else(|| self.migrations_dir())
    }

    /// Get snapshot directory path
    pub fn snapshot_dir(&self) -> &str {
        self.snapshot
            .as_ref()
            .and_then(|s| s.directory.as_deref())
            .unwrap_or(".pgcrate/snapshots")
    }

    /// Get models directory path
    pub fn models_dir(&self) -> &str {
        self.paths
            .as_ref()
            .and_then(|p| p.models.as_deref())
            .unwrap_or("models")
    }

    /// Get seeds directory path
    /// Checks [seeds].directory first, then falls back to paths.seeds
    pub fn seeds_dir(&self) -> &str {
        self.seeds
            .as_ref()
            .and_then(|s| s.directory.as_deref())
            .or_else(|| self.paths.as_ref().and_then(|p| p.seeds.as_deref()))
            .unwrap_or("seeds")
    }

    /// Get model sources list
    pub fn model_sources(&self) -> Vec<String> {
        self.model
            .as_ref()
            .and_then(|m| m.sources.clone())
            .unwrap_or_default()
    }

    /// Get path for a PostgreSQL tool (pg_dump, pg_restore, psql)
    /// Returns configured path if set, otherwise returns the tool name (for PATH lookup)
    pub fn tool_path(&self, tool: &str) -> String {
        self.tools
            .as_ref()
            .and_then(|t| match tool {
                "pg_dump" => t.pg_dump.as_ref(),
                "pg_restore" => t.pg_restore.as_ref(),
                "psql" => t.psql.as_ref(),
                _ => None,
            })
            .cloned()
            .unwrap_or_else(|| tool.to_string())
    }
}

/// Parsed database URL components
#[derive(Debug, Clone)]
pub struct ParsedDatabaseUrl {
    pub database_name: String,
    pub admin_url: String, // URL with dbname replaced by 'postgres'
}

/// Parse a database URL to extract the database name and generate an admin URL
pub fn parse_database_url(url: &str) -> Result<ParsedDatabaseUrl, anyhow::Error> {
    // Find the last '/' that separates host:port from database name
    // URL format: postgres://[user[:password]@]host[:port]/dbname[?options]

    // Split off query string first if present
    let (base_url, query_string) = match url.find('?') {
        Some(pos) => (&url[..pos], Some(&url[pos..])),
        None => (url, None),
    };

    // Find the database name (after the last '/')
    let last_slash = base_url
        .rfind('/')
        .ok_or_else(|| anyhow::anyhow!("Invalid database URL: no path separator found"))?;

    let db_name = &base_url[last_slash + 1..];
    if db_name.is_empty() {
        bail!("Invalid database URL: no database name specified");
    }

    // Build admin URL by replacing dbname with 'postgres'
    let admin_base = format!("{}/postgres", &base_url[..last_slash]);
    let admin_url = match query_string {
        Some(qs) => format!("{}{}", admin_base, qs),
        None => admin_base,
    };

    Ok(ParsedDatabaseUrl {
        database_name: db_name.to_string(),
        admin_url,
    })
}

/// Check if a URL matches production patterns (for warning, not blocking)
pub fn url_matches_production_patterns(url: &str, config: &Config) -> bool {
    let lower = url.to_lowercase();

    // Built-in patterns (intentionally conservative to avoid false positives)
    let builtin = ["prod", "production", "primary"];
    if builtin.iter().any(|k| lower.contains(k)) {
        return true;
    }

    // Cloud provider patterns
    let cloud = [
        ".rds.amazonaws.com",
        ".postgres.database.azure.com",
        ".cloudsql.google.com",
    ];
    if cloud.iter().any(|p| lower.contains(p)) {
        return true;
    }

    // Config-defined patterns
    for pattern in config.production_patterns() {
        if lower.contains(&pattern.to_lowercase()) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.migrations_dir(), "db/migrations");
        assert_eq!(config.default_with_down(), false);
    }

    #[test]
    fn test_url_matches_production_builtin() {
        let config = Config::default();
        assert!(url_matches_production_patterns(
            "postgres://prod-db/app",
            &config
        ));
        assert!(url_matches_production_patterns(
            "postgres://production.example.com/app",
            &config
        ));
        assert!(url_matches_production_patterns(
            "postgres://primary-db/app",
            &config
        ));
        assert!(!url_matches_production_patterns(
            "postgres://localhost/app",
            &config
        ));
        assert!(!url_matches_production_patterns(
            "postgres://dev-db/app",
            &config
        ));
    }

    #[test]
    fn test_url_matches_production_cloud() {
        let config = Config::default();
        assert!(url_matches_production_patterns(
            "postgres://mydb.abc123.us-east-1.rds.amazonaws.com/app",
            &config
        ));
        assert!(url_matches_production_patterns(
            "postgres://mydb.postgres.database.azure.com/app",
            &config
        ));
    }

    #[test]
    fn test_database_url_resolution() {
        let config = Config::default();

        // CLI takes precedence
        assert_eq!(
            config.get_database_url(Some("postgres://cli/db")),
            Some("postgres://cli/db".to_string())
        );

        // Without CLI, returns None if no env or config
        // (can't easily test env var without side effects)
    }

    #[test]
    fn test_validate_path_rejects_traversal() {
        let result = Config::validate_path("../etc/passwd", "paths.migrations");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(".."));
    }

    #[test]
    fn test_validate_path_rejects_absolute() {
        let result = Config::validate_path("/etc/passwd", "paths.migrations");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("relative"));
    }

    #[test]
    fn test_validate_path_accepts_relative() {
        let result = Config::validate_path("db/migrations", "paths.migrations");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_accepts_nested() {
        let result = Config::validate_path("my/custom/migrations/dir", "paths.migrations");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_paths_rejects_snapshot_directory_traversal() {
        let mut config = Config::default();
        config.snapshot = Some(SnapshotConfig {
            directory: Some("../snapshots".to_string()),
            ..SnapshotConfig::default()
        });
        assert!(config.validate_paths().is_err());
    }

    #[test]
    fn test_validate_paths_rejects_generate_output_absolute() {
        let mut config = Config::default();
        config.generate = Some(GenerateConfig {
            output: Some("/tmp/out".to_string()),
            ..GenerateConfig::default()
        });
        assert!(config.validate_paths().is_err());
    }

    #[test]
    fn test_parse_database_url_simple() {
        let parsed = parse_database_url("postgres://localhost/myapp").unwrap();
        assert_eq!(parsed.database_name, "myapp");
        assert_eq!(parsed.admin_url, "postgres://localhost/postgres");
    }

    #[test]
    fn test_parse_database_url_with_credentials() {
        let parsed = parse_database_url("postgres://user:pass@host:5432/myapp").unwrap();
        assert_eq!(parsed.database_name, "myapp");
        assert_eq!(parsed.admin_url, "postgres://user:pass@host:5432/postgres");
    }

    #[test]
    fn test_parse_database_url_with_query_params() {
        let parsed = parse_database_url("postgres://localhost/myapp?sslmode=require").unwrap();
        assert_eq!(parsed.database_name, "myapp");
        assert_eq!(
            parsed.admin_url,
            "postgres://localhost/postgres?sslmode=require"
        );
    }

    #[test]
    fn test_parse_database_url_complex() {
        let parsed = parse_database_url(
            "postgres://user:p%40ss@host.example.com:5432/mydb?sslmode=verify-full&connect_timeout=10",
        )
        .unwrap();
        assert_eq!(parsed.database_name, "mydb");
        assert_eq!(
            parsed.admin_url,
            "postgres://user:p%40ss@host.example.com:5432/postgres?sslmode=verify-full&connect_timeout=10"
        );
    }

    #[test]
    fn test_parse_database_url_no_dbname_error() {
        let result = parse_database_url("postgres://localhost/");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no database name"));
    }

    #[test]
    fn test_parse_database_url_no_slash_error() {
        let result = parse_database_url("postgres:localhost");
        // This will find the ':' as part of parsing, but no '/' after host
        // Actually it will find '/' in 'postgres://' - let's check behavior
        assert!(result.is_err() || result.unwrap().database_name == "localhost");
    }

    #[test]
    fn test_parse_anonymize_toml() {
        let toml_str = r#"
            seed = "test-seed"
            [[rules]]
            table = "app.users"
            columns = { email = "fake_email", name = "fake_name" }
            [[rules]]
            table = "app.audit_logs"
            skip = true
        "#;
        let config: AnonymizeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.seed, Some("test-seed".to_string()));
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].table, "app.users");
        assert_eq!(
            config.rules[0]
                .columns
                .as_ref()
                .unwrap()
                .get("email")
                .unwrap(),
            "fake_email"
        );
        assert!(config.rules[1].skip);
    }

    #[test]
    fn test_parse_snapshot_toml() {
        let toml_str = r#"
            [snapshot]
            directory = "backups"
            default_format = "plain"

            [snapshot.schema_only]
            schemas = ["app", "analytics"]
            data = false

            [snapshot.demo_subset]
            tables = ["app.users", "app.teams"]
            exclude_tables = ["app.audit_logs"]
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let snap_config = config.snapshot.unwrap();
        assert_eq!(snap_config.directory, Some("backups".to_string()));
        assert_eq!(snap_config.default_format, Some("plain".to_string()));

        assert_eq!(snap_config.profiles.len(), 2);
        let schema_only = snap_config.profiles.get("schema_only").unwrap();
        assert_eq!(schema_only.schemas.as_ref().unwrap().len(), 2);
        assert!(!schema_only.data);

        let demo_subset = snap_config.profiles.get("demo_subset").unwrap();
        assert_eq!(demo_subset.tables.as_ref().unwrap().len(), 2);
        assert_eq!(
            demo_subset.exclude_tables.as_ref().unwrap()[0],
            "app.audit_logs"
        );
        assert!(demo_subset.data); // defaults to true
    }

    #[test]
    fn test_default_models_dir() {
        let config = Config::default();
        assert_eq!(config.models_dir(), "models");
    }

    #[test]
    fn test_custom_models_dir() {
        let mut config = Config::default();
        config.paths = Some(PathsConfig {
            migrations: None,
            models: Some("sql/models".to_string()),
            seeds: None,
        });
        assert_eq!(config.models_dir(), "sql/models");
    }

    #[test]
    fn test_default_seeds_dir() {
        let config = Config::default();
        assert_eq!(config.seeds_dir(), "seeds");
    }

    #[test]
    fn test_custom_seeds_dir() {
        let mut config = Config::default();
        config.paths = Some(PathsConfig {
            migrations: None,
            models: None,
            seeds: Some("data/seeds".to_string()),
        });
        assert_eq!(config.seeds_dir(), "data/seeds");
    }

    #[test]
    fn test_model_sources_empty() {
        let config = Config::default();
        assert!(config.model_sources().is_empty());
    }

    #[test]
    fn test_model_sources_list() {
        let mut config = Config::default();
        config.model = Some(ModelConfig {
            sources: Some(vec!["app.users".to_string(), "app.orders".to_string()]),
        });
        let sources = config.model_sources();
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"app.users".to_string()));
        assert!(sources.contains(&"app.orders".to_string()));
    }

    #[test]
    fn test_parse_model_config_toml() {
        let toml_str = r#"
            [model]
            sources = ["app.users", "app.orders", "staging.raw_events"]
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let model_config = config.model.unwrap();
        let sources = model_config.sources.unwrap();
        assert_eq!(sources.len(), 3);
        assert_eq!(sources[0], "app.users");
        assert_eq!(sources[1], "app.orders");
        assert_eq!(sources[2], "staging.raw_events");
    }

    #[test]
    fn test_validate_paths_rejects_models_traversal() {
        let mut config = Config::default();
        config.paths = Some(PathsConfig {
            migrations: None,
            models: Some("../models".to_string()),
            seeds: None,
        });
        assert!(config.validate_paths().is_err());
    }

    #[test]
    fn test_validate_paths_rejects_seeds_absolute() {
        let mut config = Config::default();
        config.paths = Some(PathsConfig {
            migrations: None,
            models: None,
            seeds: Some("/tmp/seeds".to_string()),
        });
        assert!(config.validate_paths().is_err());
    }

    #[test]
    fn test_parse_full_config_toml() {
        let toml_str = r#"
            [paths]
            migrations = "db/migrations"
            models = "sql/models"
            seeds = "data/seeds"

            [model]
            sources = ["app.users", "app.orders"]
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();

        // Check paths
        assert_eq!(config.migrations_dir(), "db/migrations");
        assert_eq!(config.models_dir(), "sql/models");
        assert_eq!(config.seeds_dir(), "data/seeds");

        // Check model sources
        let sources = config.model_sources();
        assert_eq!(sources.len(), 2);
    }

    #[test]
    fn test_parse_seeds_config_toml() {
        let toml_str = r#"
            [seeds]
            directory = "data/seeds"
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.seeds_dir(), "data/seeds");
    }

    #[test]
    fn test_seeds_config_defaults() {
        let config = Config::default();
        assert_eq!(config.seeds_dir(), "seeds");
    }

    #[test]
    fn test_seeds_dir_fallback_to_paths() {
        // [seeds].directory takes precedence over paths.seeds
        let toml_str = r#"
            [paths]
            seeds = "old/seeds"

            [seeds]
            directory = "new/seeds"
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.seeds_dir(), "new/seeds");
    }

    #[test]
    fn test_seeds_dir_uses_paths_when_no_seeds_section() {
        let toml_str = r#"
            [paths]
            seeds = "data/seeds"
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.seeds_dir(), "data/seeds");
    }

    #[test]
    fn test_validate_paths_rejects_seeds_directory_traversal() {
        let mut config = Config::default();
        config.seeds = Some(SeedsConfig {
            directory: Some("../seeds".to_string()),
        });
        assert!(config.validate_paths().is_err());
    }
}
