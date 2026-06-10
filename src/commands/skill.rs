//! Skill command: print or install the embedded Claude Code skill.
//!
//! The skill teaches an agent *when and how* to reach for pgcrate. It ships
//! compiled into the binary (`include_str!`) so `pgcrate skill install` always
//! writes the version that matches the running binary.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

/// The skill document, compiled into the binary.
pub const SKILL_MD: &str = include_str!("../../skill/SKILL.md");

/// Print the embedded SKILL.md to stdout.
pub fn show() {
    print!("{SKILL_MD}");
}

/// Resolve the default install directory: `~/.claude/skills/pgcrate`.
fn default_install_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .context("HOME is not set; pass --path to choose an install destination")?;
    Ok(PathBuf::from(home)
        .join(".claude")
        .join("skills")
        .join("pgcrate"))
}

/// Install the embedded skill to a Claude Code skills directory.
///
/// Writes `<dir>/SKILL.md`, creating parent directories as needed. If a file
/// already exists with different content, refuses unless `force` is set so a
/// human's local edits are never clobbered silently.
pub fn install(path: Option<&Path>, force: bool, quiet: bool) -> Result<()> {
    let dir = match path {
        Some(p) => p.to_path_buf(),
        None => default_install_dir()?,
    };
    let dest = dir.join("SKILL.md");

    if dest.exists() && !force {
        let existing = std::fs::read_to_string(&dest)
            .with_context(|| format!("failed to read existing skill at {}", dest.display()))?;
        if existing == SKILL_MD {
            if !quiet {
                println!("Already up to date: {}", dest.display());
            }
            return Ok(());
        }
        bail!(
            "{} already exists and differs from the bundled skill. \
             Re-run with --force to overwrite, or use --path to choose another destination.",
            dest.display()
        );
    }

    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create skill directory {}", dir.display()))?;
    std::fs::write(&dest, SKILL_MD)
        .with_context(|| format!("failed to write skill to {}", dest.display()))?;

    if !quiet {
        println!("Installed pgcrate skill: {}", dest.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_md_has_frontmatter() {
        assert!(SKILL_MD.starts_with("---\n"));
        assert!(SKILL_MD.contains("name: pgcrate"));
        assert!(SKILL_MD.contains("description:"));
    }

    #[test]
    fn install_writes_skill_to_path() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("skills").join("pgcrate");
        install(Some(&dir), false, true).unwrap();
        let written = std::fs::read_to_string(dir.join("SKILL.md")).unwrap();
        assert_eq!(written, SKILL_MD);
    }

    #[test]
    fn install_is_idempotent_without_force() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("pgcrate");
        install(Some(&dir), false, true).unwrap();
        // Second install with identical content should succeed (no-op), not error.
        install(Some(&dir), false, true).unwrap();
    }

    #[test]
    fn install_refuses_to_overwrite_modified_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("pgcrate");
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("SKILL.md");
        std::fs::write(&dest, "locally edited skill").unwrap();

        let err = install(Some(&dir), false, true).unwrap_err();
        assert!(err.to_string().contains("--force"));
        // Original content is preserved.
        assert_eq!(
            std::fs::read_to_string(&dest).unwrap(),
            "locally edited skill"
        );
    }

    #[test]
    fn install_force_overwrites_modified_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("pgcrate");
        std::fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("SKILL.md");
        std::fs::write(&dest, "locally edited skill").unwrap();

        install(Some(&dir), true, true).unwrap();
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), SKILL_MD);
    }
}
