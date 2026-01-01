//! v0.8.0: `pgcrate doctor` command model + formatting.

use serde::Serialize;

pub const DOCTOR_SCHEMA_VERSION: &str = "0.8.0";

pub fn mask_database_url(url: &str) -> String {
    let masked = mask_password_kv(url);

    // Best-effort masking for URLs shaped like:
    // - postgres://user:password@host:5432/db
    // - postgresql://user:password@host/db
    //
    // If there is no password portion, returns the original string.
    let Some((scheme, rest)) = masked.split_once("://") else {
        return masked;
    };
    let Some(at_pos) = rest.find('@') else {
        return masked;
    };

    let (creds, after_at) = rest.split_at(at_pos);
    let after_at = &after_at[1..]; // strip '@'

    let Some((user, _password)) = creds.split_once(':') else {
        return masked;
    };

    format!("{}://{}:****@{}", scheme, user, after_at)
}

fn mask_password_kv(input: &str) -> String {
    // Also handle:
    // - postgres://host/db?user=me&password=secret
    // - host=... user=... password=secret
    //
    // This is a best-effort string transform; it doesn't parse quoting/escaping.
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    let mut last = 0;

    while i < bytes.len() {
        if !is_password_key_start(bytes, i) {
            i += 1;
            continue;
        }

        // Require a reasonable boundary before the key.
        if i > 0 {
            let prev = bytes[i - 1];
            if !(prev == b'?' || prev == b'&' || prev.is_ascii_whitespace()) {
                i += 1;
                continue;
            }
        }

        let Some(key_end) = consume_password_key(bytes, i) else {
            i += 1;
            continue;
        };

        out.push_str(&input[last..i]);
        out.push_str(&input[i..key_end]); // preserve original casing/spaces

        let mut j = key_end;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }

        // Best-effort quoted value handling.
        if j < bytes.len() && (bytes[j] == b'\'' || bytes[j] == b'"') {
            let quote = bytes[j];
            j += 1;
            while j < bytes.len() && bytes[j] != quote {
                j += 1;
            }
            if j < bytes.len() {
                j += 1; // include closing quote
            }
        } else {
            while j < bytes.len() {
                let b = bytes[j];
                if b == b'&' || b.is_ascii_whitespace() {
                    break;
                }
                j += 1;
            }
        }

        out.push_str("****");
        last = j;
        i = j;
    }

    out.push_str(&input[last..]);
    out
}

fn is_password_key_start(bytes: &[u8], i: usize) -> bool {
    bytes.get(i).is_some_and(|b| b.eq_ignore_ascii_case(&b'p'))
}

fn consume_password_key(bytes: &[u8], i: usize) -> Option<usize> {
    // Match case-insensitive "password", then optional spaces, then '='.
    const KEY: &[u8] = b"password";
    if i + KEY.len() > bytes.len() {
        return None;
    }
    for (offset, expected) in KEY.iter().enumerate() {
        if bytes[i + offset].to_ascii_lowercase() != *expected {
            return None;
        }
    }

    let mut j = i + KEY.len();
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b'=' {
        return None;
    }
    Some(j + 1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorFatal {
    Connection,
    Config,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorStatus {
    Pass,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DoctorItem {
    pub status: DoctorStatus,
    pub message: String,
}

impl DoctorItem {
    pub fn pass(message: impl Into<String>) -> Self {
        Self {
            status: DoctorStatus::Pass,
            message: message.into(),
        }
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            status: DoctorStatus::Warning,
            message: message.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: DoctorStatus::Error,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DoctorSummary {
    pub pass: usize,
    pub warning: usize,
    pub error: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorJsonReport {
    pub schema_version: String,
    pub generated_at: String,
    pub exit_code: i32,
    pub connection: Vec<DoctorItem>,
    pub schema: Vec<DoctorItem>,
    pub migrations: Vec<DoctorItem>,
    pub config: Vec<DoctorItem>,
    pub summary: DoctorSummary,
}

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub generated_at: String,
    pub fatal: Option<DoctorFatal>,
    pub connection: Vec<DoctorItem>,
    pub schema: Vec<DoctorItem>,
    pub migrations: Vec<DoctorItem>,
    pub config: Vec<DoctorItem>,
}

impl DoctorReport {
    pub fn new(generated_at: impl Into<String>) -> Self {
        Self {
            generated_at: generated_at.into(),
            fatal: None,
            connection: Vec::new(),
            schema: Vec::new(),
            migrations: Vec::new(),
            config: Vec::new(),
        }
    }

    pub fn fatal_connection(generated_at: impl Into<String>, message: impl Into<String>) -> Self {
        let mut report = Self::new(generated_at);
        report.fatal = Some(DoctorFatal::Connection);
        report.connection.push(DoctorItem::error(message));
        report
    }

    pub fn fatal_config(generated_at: impl Into<String>, message: impl Into<String>) -> Self {
        let mut report = Self::new(generated_at);
        report.fatal = Some(DoctorFatal::Config);
        report.config.push(DoctorItem::error(message));
        report
    }

    pub fn summary(&self) -> DoctorSummary {
        let mut pass = 0;
        let mut warning = 0;
        let mut error = 0;

        for item in self.all_items() {
            match item.status {
                DoctorStatus::Pass => pass += 1,
                DoctorStatus::Warning => warning += 1,
                DoctorStatus::Error => error += 1,
            }
        }

        DoctorSummary {
            pass,
            warning,
            error,
        }
    }

    pub fn exit_code(&self, strict: bool) -> i32 {
        if self.fatal.is_some() {
            return 2;
        }

        let summary = self.summary();
        if summary.error > 0 {
            return 1;
        }

        if strict && summary.warning > 0 {
            return 1;
        }

        0
    }

    pub fn to_json(&self, strict: bool) -> DoctorJsonReport {
        let exit_code = self.exit_code(strict);
        let summary = self.summary();

        DoctorJsonReport {
            schema_version: DOCTOR_SCHEMA_VERSION.to_string(),
            generated_at: self.generated_at.clone(),
            exit_code,
            connection: self.connection.clone(),
            schema: self.schema.clone(),
            migrations: self.migrations.clone(),
            config: self.config.clone(),
            summary,
        }
    }

    pub fn format_human(&self, verbose: bool) -> String {
        let mut out = String::new();
        out.push_str("pgcrate doctor\n\n");

        out.push_str(&format_section(
            "Connection",
            &self.connection,
            verbose,
            self.fatal,
        ));
        out.push('\n');
        out.push_str(&format_section("Schema", &self.schema, verbose, self.fatal));
        out.push('\n');
        out.push_str(&format_section(
            "Migrations",
            &self.migrations,
            verbose,
            self.fatal,
        ));
        out.push('\n');
        out.push_str(&format_section("Config", &self.config, verbose, self.fatal));
        out.push('\n');

        let summary = self.summary();
        let summary_line = if summary.error == 0 && summary.warning == 0 {
            "Summary: OK".to_string()
        } else if summary.error > 0 && summary.warning > 0 {
            format!(
                "Summary: {} error(s), {} warning(s)",
                summary.error, summary.warning
            )
        } else if summary.error > 0 {
            format!("Summary: {} error(s)", summary.error)
        } else {
            format!("Summary: {} warning(s)", summary.warning)
        };

        out.push_str(&summary_line);
        out
    }

    fn all_items(&self) -> impl Iterator<Item = &DoctorItem> {
        self.connection
            .iter()
            .chain(self.schema.iter())
            .chain(self.migrations.iter())
            .chain(self.config.iter())
    }
}

fn format_section(
    title: &str,
    items: &[DoctorItem],
    verbose: bool,
    fatal: Option<DoctorFatal>,
) -> String {
    let mut out = String::new();
    out.push_str(title);
    out.push('\n');

    if items.is_empty() {
        if fatal.is_some() {
            out.push_str("  (skipped)\n");
        } else {
            out.push_str("  ✓ OK\n");
        }
        return out;
    }

    if verbose {
        for item in items {
            out.push_str("  ");
            out.push_str(icon(item.status));
            out.push(' ');
            out.push_str(&item.message);
            out.push('\n');
        }
        return out;
    }

    let mut printed_any = false;
    for item in items {
        if item.status == DoctorStatus::Pass {
            continue;
        }
        printed_any = true;
        out.push_str("  ");
        out.push_str(icon(item.status));
        out.push(' ');
        out.push_str(&item.message);
        out.push('\n');
    }

    if !printed_any {
        out.push_str("  ✓ OK\n");
    }

    out
}

fn icon(status: DoctorStatus) -> &'static str {
    match status {
        DoctorStatus::Pass => "✓",
        DoctorStatus::Warning => "⚠",
        DoctorStatus::Error => "✗",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exit_code_warnings_non_strict() {
        let mut report = DoctorReport::new("2025-12-13T00:00:00Z");
        report
            .migrations
            .push(DoctorItem::warning("2 pending migrations"));

        assert_eq!(report.exit_code(false), 0);
    }

    #[test]
    fn test_exit_code_warnings_strict() {
        let mut report = DoctorReport::new("2025-12-13T00:00:00Z");
        report
            .config
            .push(DoctorItem::warning("pgcrate.toml missing"));

        assert_eq!(report.exit_code(true), 1);
    }

    #[test]
    fn test_exit_code_errors() {
        let mut report = DoctorReport::new("2025-12-13T00:00:00Z");
        report
            .schema
            .push(DoctorItem::error("schema_migrations missing"));

        assert_eq!(report.exit_code(false), 1);
        assert_eq!(report.exit_code(true), 1);
    }

    #[test]
    fn test_exit_code_fatal() {
        let report = DoctorReport::fatal_connection("2025-12-13T00:00:00Z", "cannot connect");
        assert_eq!(report.exit_code(false), 2);
        assert_eq!(report.exit_code(true), 2);
    }

    #[test]
    fn test_json_schema_version() {
        let report = DoctorReport::new("2025-12-13T00:00:00Z");
        let json = report.to_json(false);
        assert_eq!(json.schema_version, DOCTOR_SCHEMA_VERSION);
    }

    #[test]
    fn test_mask_database_url_masks_password_in_authority() {
        let masked = mask_database_url("postgres://user:secret@localhost/db");
        assert_eq!(masked, "postgres://user:****@localhost/db");
    }

    #[test]
    fn test_mask_database_url_masks_password_query_param() {
        let masked =
            mask_database_url("postgres://localhost/db?user=u&password=secret&sslmode=require");
        assert!(masked.contains("password=****"));
        assert!(!masked.contains("password=secret"));
    }

    #[test]
    fn test_mask_database_url_masks_password_kv() {
        let masked = mask_database_url("host=localhost user=u password=secret dbname=db");
        assert!(masked.contains("password=****"));
        assert!(!masked.contains("password=secret"));
    }

    #[test]
    fn test_mask_database_url_masks_password_kv_case_insensitive() {
        let masked = mask_database_url("host=localhost user=u Password=secret dbname=db");
        assert!(masked.contains("Password=****"));
        assert!(!masked.contains("Password=secret"));
    }

    #[test]
    fn test_mask_database_url_masks_password_kv_with_spaces() {
        let masked = mask_database_url("host=localhost user=u password = secret dbname=db");
        assert!(masked.to_lowercase().contains("password =****"));
        assert!(!masked.contains("secret"));
    }
}
