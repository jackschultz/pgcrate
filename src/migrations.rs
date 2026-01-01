use anyhow::{bail, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Represents a single migration file with embedded up/down sections.
#[derive(Debug, Clone)]
pub struct Migration {
    pub version: String,
    pub name: String,
    pub up_sql: String,
    pub down_sql: Option<String>,
}

/// Discover and parse all migration files in the directory.
/// Uses the single-file format: `{version}_{name}.sql` with `-- up` / `-- down` markers.
pub fn discover_migrations(dir: &Path) -> Result<Vec<Migration>, anyhow::Error> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut migrations: HashMap<String, Migration> = HashMap::new();

    for entry in fs::read_dir(dir)?.filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let filename = entry.file_name().to_string_lossy().to_string();

        // Explicitly fail fast on legacy paired files.
        if filename.ends_with(".up.sql") || filename.ends_with(".down.sql") {
            bail!(
                "Found legacy migration file '{}'. pgcrate now expects single files like \
                 YYYYMMDDHHMMSS_name.sql containing `-- up` and `-- down` markers. \
                 Please merge the old pair into one file.",
                filename
            );
        }

        if path.extension().map(|ext| ext != "sql").unwrap_or(true) {
            continue;
        }

        let (version, name) = parse_migration_filename(&filename)?;
        if migrations.contains_key(&version) {
            bail!(
                "Multiple migrations found for version {} (e.g., {}). Use unique versions.",
                version,
                filename
            );
        }

        let (up_sql, down_sql) = parse_migration_file(&path)?;
        migrations.insert(
            version.clone(),
            Migration {
                version,
                name,
                up_sql,
                down_sql,
            },
        );
    }

    let mut result: Vec<Migration> = migrations.into_values().collect();
    result.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(result)
}

/// Parse migration filename to extract version and name.
/// Expected format: 14-digit timestamp followed by `_name.sql`
fn parse_migration_filename(filename: &str) -> Result<(String, String), anyhow::Error> {
    if filename.ends_with(".up.sql") || filename.ends_with(".down.sql") {
        bail!("Invalid migration filename: {}. Single-file migrations must end with .sql and contain both sections.",
            filename
        );
    }

    if !filename.ends_with(".sql") {
        bail!(
            "Invalid migration filename: {}. Expected .sql extension.",
            filename
        );
    }

    let base = filename.trim_end_matches(".sql");
    let parts: Vec<&str> = base.splitn(2, '_').collect();
    if parts.len() != 2 {
        bail!(
            "Invalid migration filename: {}. Expected format: YYYYMMDDHHMMSS_name.sql",
            filename
        );
    }

    let version = parts[0].to_string();
    let name = parts[1].to_string();

    if version.len() != 14 || !version.chars().all(|c| c.is_ascii_digit()) {
        bail!("Invalid migration version in filename: {}. Expected 14-digit timestamp (YYYYMMDDHHMMSS).",
            filename
        );
    }

    if name.is_empty() {
        bail!(
            "Invalid migration filename: {}. Name cannot be empty.",
            filename
        );
    }

    Ok((version, name))
}

/// Parse a migration file into up/down SQL sections.
fn parse_migration_file(path: &Path) -> Result<(String, Option<String>), anyhow::Error> {
    let content = fs::read_to_string(path)?;
    let lines: Vec<&str> = content.lines().collect();

    let mut up_idx: Option<usize> = None;
    let mut down_idx: Option<usize> = None;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("-- up") {
            if up_idx.is_some() {
                bail!(
                    "{}: multiple `-- up` markers found. Each migration must have exactly one.",
                    path.display()
                );
            }
            up_idx = Some(i);
        } else if trimmed.eq_ignore_ascii_case("-- down") {
            if down_idx.is_some() {
                bail!(
                    "{}: multiple `-- down` markers found. Use one down section per file.",
                    path.display()
                );
            }
            down_idx = Some(i);
        }
    }

    let up_idx = up_idx.ok_or_else(|| {
        anyhow::anyhow!(
            "{}: missing `-- up` marker. Add `-- up` and `-- down` markers to the file.",
            path.display()
        )
    })?;

    if let Some(down_marker) = down_idx {
        if down_marker < up_idx {
            bail!(
                "{}: `-- down` appears before `-- up`. Place `-- up` first.",
                path.display()
            );
        }
    }

    // Only allow comments/blank lines before the up marker.
    for line in &lines[..up_idx] {
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") {
            bail!(
                "{}: only comments or blank lines are allowed before the `-- up` marker.",
                path.display()
            );
        }
    }

    let up_end = down_idx.unwrap_or(lines.len());
    let up_section = lines[up_idx + 1..up_end].join("\n");
    let down_section = down_idx.map(|idx| lines[idx + 1..].join("\n"));

    let down_sql = match down_section {
        Some(section) if !section_is_effectively_empty(&section) => Some(section),
        _ => None,
    };

    Ok((up_section, down_sql))
}

fn section_is_effectively_empty(section: &str) -> bool {
    section.lines().all(|line| {
        let trimmed = line.trim();
        trimmed.is_empty() || trimmed.starts_with("--")
    })
}

/// Load migrations (alias for discover_migrations for callers that expect the older name).
pub fn load_migrations(dir: &Path) -> Result<Vec<Migration>, anyhow::Error> {
    discover_migrations(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filename() {
        let (version, name) = parse_migration_filename("20250101120000_create_users.sql").unwrap();
        assert_eq!(version, "20250101120000");
        assert_eq!(name, "create_users");
    }

    #[test]
    fn test_parse_filename_rejects_legacy_suffix() {
        let result = parse_migration_filename("20250101120000_create_users.up.sql");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_filename_invalid_format() {
        let result = parse_migration_filename("20250101120000.sql");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_migration_file_with_down_section() {
        use std::fs;
        let dir = std::env::temp_dir().join("pgcrate_parse_with_down");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let path = dir.join("20250101120000_create_users.sql");
        fs::write(
            &path,
            "\
-- Create users table

-- up
CREATE TABLE users (id serial primary key);

-- down
DROP TABLE users;",
        )
        .unwrap();

        let migrations = discover_migrations(&dir).unwrap();
        assert_eq!(migrations.len(), 1);
        let migration = &migrations[0];
        assert!(migration.up_sql.contains("CREATE TABLE users"));
        assert_eq!(
            migration.down_sql.as_ref().unwrap().trim(),
            "DROP TABLE users;"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_parse_allows_no_down_section() {
        use std::fs;
        let dir = std::env::temp_dir().join("pgcrate_parse_no_down");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let path = dir.join("20250101120000_add_data.sql");
        fs::write(
            &path,
            "\
-- Seed data

-- up
INSERT INTO foo VALUES (1);
",
        )
        .unwrap();

        let migrations = discover_migrations(&dir).unwrap();
        assert!(migrations[0].down_sql.is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_parse_treats_empty_down_as_none() {
        use std::fs;
        let dir = std::env::temp_dir().join("pgcrate_parse_empty_down");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let path = dir.join("20250101120000_add_logs.sql");
        fs::write(
            &path,
            "\
-- up
SELECT 1;

-- down
-- intentionally irreversible
",
        )
        .unwrap();

        let migrations = discover_migrations(&dir).unwrap();
        assert!(migrations[0].down_sql.is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_error_on_content_before_up() {
        use std::fs;
        let dir = std::env::temp_dir().join("pgcrate_parse_bad_prefix");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let path = dir.join("20250101120000_bad.sql");
        fs::write(
            &path,
            "\
SELECT 1;
-- up
SELECT 2;",
        )
        .unwrap();

        let err = discover_migrations(&dir).unwrap_err().to_string();
        assert!(err.contains("comments or blank lines"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_error_on_duplicate_version() {
        use std::fs;
        let dir = std::env::temp_dir().join("pgcrate_parse_dup_version");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        fs::write(dir.join("20250101120000_first.sql"), "-- up\nSELECT 1;").unwrap();
        fs::write(dir.join("20250101120000_second.sql"), "-- up\nSELECT 2;").unwrap();

        let err = discover_migrations(&dir).unwrap_err().to_string();
        assert!(err.contains("Multiple migrations found for version 20250101120000"));

        let _ = fs::remove_dir_all(&dir);
    }
}
