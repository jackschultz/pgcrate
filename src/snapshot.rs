use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Snapshot dump format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotFormat {
    #[default]
    Custom,
    Plain,
}

impl std::fmt::Display for SnapshotFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Custom => write!(f, "custom"),
            Self::Plain => write!(f, "plain"),
        }
    }
}

impl std::str::FromStr for SnapshotFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "custom" => Ok(Self::Custom),
            "plain" => Ok(Self::Plain),
            _ => bail!("Invalid format '{}'. Must be 'custom' or 'plain'.", s),
        }
    }
}

impl SnapshotFormat {
    pub fn dump_filename(&self) -> &'static str {
        match self {
            Self::Custom => "dump.pgdump",
            Self::Plain => "dump.sql",
        }
    }
}

/// Snapshot metadata stored in metadata.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub database: String,
    #[serde(default)]
    pub source_host: Option<String>,
    #[serde(default)]
    pub pg_version: Option<String>,
    #[serde(default)]
    pub pg_dump_version: Option<String>,
    pub migration_version: Option<String>,
    pub applied_migrations: usize,
    #[serde(default)]
    pub owner_roles: Vec<String>,
    pub size_bytes: u64,
    pub message: Option<String>,
    #[serde(default)]
    pub format: SnapshotFormat,
    #[serde(default = "default_true")]
    pub include_owner: bool,
    #[serde(default = "default_true")]
    pub include_privileges: bool,
    pub profile_name: Option<String>,
    pub included_schemas: Option<Vec<String>>,
    pub excluded_schemas: Option<Vec<String>>,
    pub included_tables: Option<Vec<String>>,
    pub excluded_tables: Option<Vec<String>>,
    #[serde(default = "default_true")]
    pub include_data: bool,
    pub pgcrate_version: String,
}

fn default_true() -> bool {
    true
}

impl SnapshotMetadata {
    /// Create new snapshot metadata
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &str,
        database: &str,
        migration_version: Option<String>,
        applied_migrations: usize,
        size_bytes: u64,
        message: Option<String>,
        source_host: Option<String>,
        pg_version: Option<String>,
        pg_dump_version: Option<String>,
        owner_roles: Vec<String>,
        format: SnapshotFormat,
        include_owner: bool,
        include_privileges: bool,
        profile_name: Option<String>,
        included_schemas: Option<Vec<String>>,
        excluded_schemas: Option<Vec<String>>,
        included_tables: Option<Vec<String>>,
        excluded_tables: Option<Vec<String>>,
        include_data: bool,
    ) -> Self {
        Self {
            name: name.to_string(),
            created_at: Utc::now(),
            database: database.to_string(),
            source_host,
            pg_version,
            pg_dump_version,
            migration_version,
            applied_migrations,
            owner_roles,
            size_bytes,
            message,
            format,
            include_owner,
            include_privileges,
            profile_name,
            included_schemas,
            excluded_schemas,
            included_tables,
            excluded_tables,
            include_data,
            pgcrate_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    pub fn load(snapshot_dir: &Path) -> Result<Self> {
        let path = snapshot_dir.join("metadata.json");
        let content = fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("Failed to read metadata: {}", e))?;
        serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse metadata: {}", e))
    }

    pub fn save(&self, snapshot_dir: &Path) -> Result<()> {
        let path = snapshot_dir.join("metadata.json");
        fs::write(path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn format_size(&self) -> String {
        format_bytes(self.size_bytes)
    }
}

/// Validate snapshot name (a-z, A-Z, 0-9, -, _, max 64 chars, can't start with -)
pub fn validate_snapshot_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("Snapshot name cannot be empty.");
    }
    if name.len() > 64 {
        bail!(
            "Invalid snapshot name \"{}\"\nNames must be at most 64 characters.",
            name
        );
    }
    if name.starts_with('-') {
        bail!(
            "Invalid snapshot name \"{}\"\nNames cannot start with a hyphen.",
            name
        );
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!("Invalid snapshot name \"{}\"\nNames must contain only letters, numbers, hyphens, and underscores.", name);
    }
    Ok(())
}

pub fn snapshots_dir(override_path: Option<&str>) -> PathBuf {
    if let Some(p) = override_path {
        PathBuf::from(p)
    } else {
        PathBuf::from(".pgcrate").join("snapshots")
    }
}

pub fn snapshot_dir(name: &str, override_path: Option<&str>) -> PathBuf {
    snapshots_dir(override_path).join(name)
}

pub fn snapshot_exists(name: &str, override_path: Option<&str>) -> bool {
    let dir = snapshot_dir(name, override_path);
    dir.exists()
        && dir.join("metadata.json").exists()
        && (dir.join("dump.pgdump").exists() || dir.join("dump.sql").exists())
}

pub fn list_snapshots(override_path: Option<&str>) -> Result<Vec<SnapshotMetadata>> {
    let dir = snapshots_dir(override_path);
    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut snapshots = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.is_dir() {
            let has_dump = path.join("dump.pgdump").exists() || path.join("dump.sql").exists();
            if path.join("metadata.json").exists() && has_dump {
                match SnapshotMetadata::load(&path) {
                    Ok(m) => snapshots.push(m),
                    Err(e) => eprintln!(
                        "Warning: Failed to load snapshot at {}: {}",
                        path.display(),
                        e
                    ),
                }
            }
        }
    }
    snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(snapshots)
}

pub fn available_snapshots(override_path: Option<&str>) -> Vec<String> {
    list_snapshots(override_path)
        .map(|s| s.into_iter().map(|m| m.name).collect())
        .unwrap_or_default()
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Check if a binary exists (at path or in PATH)
fn check_binary(path: &str) -> Result<()> {
    match Command::new(path).arg("--version").output() {
        Ok(output) if output.status.success() => Ok(()),
        _ => {
            // Extract just the binary name for the error message
            let name = std::path::Path::new(path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(path);
            bail!(
                "{} not found.\nHint: Install PostgreSQL client tools, add to PATH, or configure [tools] in pgcrate.toml.",
                name
            )
        }
    }
}

pub fn check_pg_dump(path: &str) -> Result<()> {
    check_binary(path)
}

pub fn check_pg_restore(path: &str) -> Result<()> {
    check_binary(path)
}

pub fn check_psql(path: &str) -> Result<()> {
    check_binary(path)
}

pub fn get_pg_dump_version(path: &str) -> Result<String> {
    let output = Command::new(path).arg("--version").output()?;
    if !output.status.success() {
        bail!("Failed to get pg_dump version");
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .last()
        .unwrap_or("unknown")
        .to_string())
}

pub fn extract_host(database_url: &str) -> Option<String> {
    url::Url::parse(database_url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

pub fn parse_major_version(version: &str) -> Option<u32> {
    version.split('.').next()?.parse().ok()
}

pub fn should_warn_version_downgrade(snapshot_version: &str, target_version: &str) -> bool {
    match (
        parse_major_version(snapshot_version),
        parse_major_version(target_version),
    ) {
        (Some(snap), Some(target)) => target < snap,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_snapshot_name("my-snapshot").is_ok());
        assert!(validate_snapshot_name("snapshot_1").is_ok());
        assert!(validate_snapshot_name("Test123").is_ok());
        assert!(validate_snapshot_name("a").is_ok());
    }

    #[test]
    fn test_validate_name_invalid() {
        assert!(validate_snapshot_name("").is_err());
        assert!(validate_snapshot_name(&"a".repeat(65)).is_err());
        assert!(validate_snapshot_name("-snapshot").is_err());
        assert!(validate_snapshot_name("my snapshot").is_err());
        assert!(validate_snapshot_name("snapshot!").is_err());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = SnapshotMetadata::new(
            "test-snap",
            "myapp_dev",
            Some("20250610180000".to_string()),
            12,
            15234567,
            Some("Test snapshot".to_string()),
            None,
            None,
            None,
            vec![],
            SnapshotFormat::Custom,
            true,
            true,
            Some("demo".to_string()),
            Some(vec!["app".to_string()]),
            None,
            None,
            None,
            true,
        );

        let json = serde_json::to_string(&metadata).unwrap();
        let parsed: SnapshotMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name, "test-snap");
        assert_eq!(parsed.database, "myapp_dev");
        assert_eq!(parsed.applied_migrations, 12);
    }

    #[test]
    fn test_metadata_backwards_compat() {
        // v0.9.0 format (missing new fields)
        let json = r#"{
            "name": "old-snap",
            "created_at": "2025-01-01T00:00:00Z",
            "database": "test",
            "migration_version": null,
            "applied_migrations": 0,
            "size_bytes": 1000,
            "message": null,
            "pgcrate_version": "0.9.0"
        }"#;

        let metadata: SnapshotMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.format, SnapshotFormat::Custom);
        assert!(metadata.include_owner);
        assert!(metadata.include_privileges);
        assert!(metadata.owner_roles.is_empty());
    }

    #[test]
    fn test_snapshot_format() {
        assert_eq!(SnapshotFormat::Custom.to_string(), "custom");
        assert_eq!(SnapshotFormat::Plain.to_string(), "plain");
        assert_eq!(
            "custom".parse::<SnapshotFormat>().unwrap(),
            SnapshotFormat::Custom
        );
        assert_eq!(
            "PLAIN".parse::<SnapshotFormat>().unwrap(),
            SnapshotFormat::Plain
        );
        assert!("invalid".parse::<SnapshotFormat>().is_err());
        assert_eq!(SnapshotFormat::Custom.dump_filename(), "dump.pgdump");
        assert_eq!(SnapshotFormat::Plain.dump_filename(), "dump.sql");
    }

    #[test]
    fn test_version_helpers() {
        assert_eq!(parse_major_version("16.2"), Some(16));
        assert_eq!(parse_major_version("invalid"), None);
        assert!(should_warn_version_downgrade("16.2", "14.1"));
        assert!(!should_warn_version_downgrade("14.1", "16.2"));
    }

    #[test]
    fn test_extract_host() {
        assert_eq!(
            extract_host("postgres://user:pass@localhost:5432/db"),
            Some("localhost".to_string())
        );
        assert_eq!(extract_host("invalid-url"), None);
    }
}
