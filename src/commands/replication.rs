//! Replication command: Monitor streaming replication health.
//!
//! Shows replica lag, slot status, and WAL receiver info.
//! Works on both primary and standby servers.

use anyhow::Result;
use serde::Serialize;
use tokio_postgres::Client;

const LAG_WARNING_SECS: f64 = 30.0;
const LAG_CRITICAL_SECS: f64 = 300.0; // 5 minutes
const SLOT_RETAINED_WARNING_BYTES: i64 = 1_073_741_824; // 1GB
const SLOT_RETAINED_CRITICAL_BYTES: i64 = 10_737_418_240; // 10GB

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationStatus {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerRole {
    Primary,
    Standby,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaInfo {
    pub application_name: String,
    pub client_addr: Option<String>,
    pub state: String,
    pub sync_state: String,
    pub sent_lsn: Option<String>,
    pub write_lsn: Option<String>,
    pub flush_lsn: Option<String>,
    pub replay_lsn: Option<String>,
    pub write_lag_secs: Option<f64>,
    pub flush_lag_secs: Option<f64>,
    pub replay_lag_secs: Option<f64>,
    pub lag_bytes: Option<i64>,
    pub status: ReplicationStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotInfo {
    pub slot_name: String,
    pub slot_type: String,
    pub database: Option<String>,
    pub active: bool,
    pub wal_status: Option<String>,
    pub retained_bytes: Option<i64>,
    pub status: ReplicationStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalReceiverInfo {
    pub status: String,
    pub sender_host: Option<String>,
    pub sender_port: Option<i32>,
    pub slot_name: Option<String>,
    pub received_lsn: Option<String>,
    pub latest_end_lsn: Option<String>,
    pub lag_bytes: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ReplicationResult {
    pub server_role: ServerRole,
    pub replicas: Vec<ReplicaInfo>,
    pub slots: Vec<SlotInfo>,
    pub wal_receiver: Option<WalReceiverInfo>,
    pub overall_status: ReplicationStatus,
}

async fn get_server_role(client: &Client) -> Result<ServerRole> {
    let row = client.query_one("SELECT pg_is_in_recovery()", &[]).await?;
    let in_recovery: bool = row.get(0);
    Ok(if in_recovery {
        ServerRole::Standby
    } else {
        ServerRole::Primary
    })
}

async fn get_replicas(client: &Client) -> Result<Vec<ReplicaInfo>> {
    let query = r#"
SELECT
    application_name,
    client_addr::text,
    state,
    sync_state,
    sent_lsn::text,
    write_lsn::text,
    flush_lsn::text,
    replay_lsn::text,
    EXTRACT(EPOCH FROM write_lag)::float8 AS write_lag_secs,
    EXTRACT(EPOCH FROM flush_lag)::float8 AS flush_lag_secs,
    EXTRACT(EPOCH FROM replay_lag)::float8 AS replay_lag_secs,
    pg_wal_lsn_diff(sent_lsn, replay_lsn)::bigint AS lag_bytes
FROM pg_stat_replication
ORDER BY application_name
"#;

    let rows = client.query(query, &[]).await?;
    let mut results = Vec::with_capacity(rows.len());

    for row in rows {
        let replay_lag_secs: Option<f64> = row.get("replay_lag_secs");
        let status = match replay_lag_secs {
            Some(lag) if lag >= LAG_CRITICAL_SECS => ReplicationStatus::Critical,
            Some(lag) if lag >= LAG_WARNING_SECS => ReplicationStatus::Warning,
            _ => ReplicationStatus::Healthy,
        };

        results.push(ReplicaInfo {
            application_name: row.get("application_name"),
            client_addr: row.get("client_addr"),
            state: row.get("state"),
            sync_state: row.get("sync_state"),
            sent_lsn: row.get("sent_lsn"),
            write_lsn: row.get("write_lsn"),
            flush_lsn: row.get("flush_lsn"),
            replay_lsn: row.get("replay_lsn"),
            write_lag_secs: row.get("write_lag_secs"),
            flush_lag_secs: row.get("flush_lag_secs"),
            replay_lag_secs,
            lag_bytes: row.get("lag_bytes"),
            status,
        });
    }

    Ok(results)
}

async fn get_slots(client: &Client) -> Result<Vec<SlotInfo>> {
    // Check if pg_current_wal_lsn exists (PG10+) or use pg_current_xlog_location (PG9)
    let query = r#"
SELECT
    slot_name,
    slot_type,
    database,
    active,
    wal_status,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_bytes
FROM pg_replication_slots
ORDER BY slot_name
"#;

    let rows = client.query(query, &[]).await?;
    let mut results = Vec::with_capacity(rows.len());

    for row in rows {
        let active: bool = row.get("active");
        let wal_status: Option<String> = row.get("wal_status");
        let retained_bytes: Option<i64> = row.get("retained_bytes");

        let status = if wal_status.as_deref() == Some("lost") {
            ReplicationStatus::Critical
        } else if !active {
            match retained_bytes {
                Some(bytes) if bytes >= SLOT_RETAINED_CRITICAL_BYTES => ReplicationStatus::Critical,
                Some(bytes) if bytes >= SLOT_RETAINED_WARNING_BYTES => ReplicationStatus::Warning,
                _ => ReplicationStatus::Healthy,
            }
        } else {
            ReplicationStatus::Healthy
        };

        results.push(SlotInfo {
            slot_name: row.get("slot_name"),
            slot_type: row.get("slot_type"),
            database: row.get("database"),
            active,
            wal_status,
            retained_bytes,
            status,
        });
    }

    Ok(results)
}

async fn get_wal_receiver(client: &Client) -> Result<Option<WalReceiverInfo>> {
    let query = r#"
SELECT
    status,
    sender_host,
    sender_port,
    slot_name,
    received_lsn::text,
    latest_end_lsn::text,
    CASE
        WHEN latest_end_lsn IS NOT NULL AND received_lsn IS NOT NULL
        THEN pg_wal_lsn_diff(latest_end_lsn, received_lsn)::bigint
        ELSE NULL
    END AS lag_bytes
FROM pg_stat_wal_receiver
LIMIT 1
"#;

    let rows = client.query(query, &[]).await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    Ok(Some(WalReceiverInfo {
        status: row.get("status"),
        sender_host: row.get("sender_host"),
        sender_port: row.get("sender_port"),
        slot_name: row.get("slot_name"),
        received_lsn: row.get("received_lsn"),
        latest_end_lsn: row.get("latest_end_lsn"),
        lag_bytes: row.get("lag_bytes"),
    }))
}

pub async fn get_replication(client: &Client) -> Result<ReplicationResult> {
    let server_role = get_server_role(client).await?;

    let replicas = if server_role == ServerRole::Primary {
        get_replicas(client).await?
    } else {
        vec![]
    };

    let slots = get_slots(client).await?;

    let wal_receiver = if server_role == ServerRole::Standby {
        get_wal_receiver(client).await?
    } else {
        None
    };

    // Calculate overall status
    let worst_replica = replicas.iter().map(|r| &r.status).max_by_key(|s| match s {
        ReplicationStatus::Healthy => 0,
        ReplicationStatus::Warning => 1,
        ReplicationStatus::Critical => 2,
    });

    let worst_slot = slots.iter().map(|s| &s.status).max_by_key(|s| match s {
        ReplicationStatus::Healthy => 0,
        ReplicationStatus::Warning => 1,
        ReplicationStatus::Critical => 2,
    });

    let overall_status = match (worst_replica, worst_slot) {
        (Some(r), Some(s)) => {
            if matches!(r, ReplicationStatus::Critical) || matches!(s, ReplicationStatus::Critical)
            {
                ReplicationStatus::Critical
            } else if matches!(r, ReplicationStatus::Warning)
                || matches!(s, ReplicationStatus::Warning)
            {
                ReplicationStatus::Warning
            } else {
                ReplicationStatus::Healthy
            }
        }
        (Some(s), None) | (None, Some(s)) => *s,
        (None, None) => ReplicationStatus::Healthy,
    };

    Ok(ReplicationResult {
        server_role,
        replicas,
        slots,
        wal_receiver,
        overall_status,
    })
}

fn format_bytes(bytes: i64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn format_lag(secs: Option<f64>) -> String {
    match secs {
        Some(s) if s >= 60.0 => format!("{:.1}m", s / 60.0),
        Some(s) => format!("{:.1}s", s),
        None => "-".to_string(),
    }
}

fn status_emoji(status: &ReplicationStatus) -> &'static str {
    match status {
        ReplicationStatus::Healthy => "✓",
        ReplicationStatus::Warning => "⚠",
        ReplicationStatus::Critical => "✗",
    }
}

pub fn print_human(result: &ReplicationResult, quiet: bool) {
    let role_str = match result.server_role {
        ServerRole::Primary => "PRIMARY",
        ServerRole::Standby => "STANDBY",
    };

    if !quiet {
        println!(
            "REPLICATION STATUS: {} ({})",
            role_str,
            status_emoji(&result.overall_status)
        );
        println!();
    }

    // Replicas (primary only)
    if result.server_role == ServerRole::Primary {
        if result.replicas.is_empty() {
            if !quiet {
                println!("REPLICAS: none");
            }
        } else {
            println!("REPLICAS:");
            println!(
                "  {:3} {:20} {:15} {:10} {:>10} {:>10}",
                "", "APPLICATION", "CLIENT", "STATE", "LAG", "BYTES"
            );
            println!("  {}", "-".repeat(72));

            for r in &result.replicas {
                let client = r.client_addr.as_deref().unwrap_or("-");
                let client_display = if client.len() > 15 {
                    format!("{}...", &client[..12])
                } else {
                    client.to_string()
                };
                let app_display = if r.application_name.len() > 20 {
                    format!("{}...", &r.application_name[..17])
                } else {
                    r.application_name.clone()
                };

                println!(
                    "  {} {:20} {:15} {:10} {:>10} {:>10}",
                    status_emoji(&r.status),
                    app_display,
                    client_display,
                    r.state,
                    format_lag(r.replay_lag_secs),
                    r.lag_bytes
                        .map(format_bytes)
                        .unwrap_or_else(|| "-".to_string())
                );
            }
            println!();
        }
    }

    // WAL Receiver (standby only)
    if let Some(ref wr) = result.wal_receiver {
        println!("WAL RECEIVER:");
        println!("  Status: {}", wr.status);
        if let Some(ref host) = wr.sender_host {
            let port = wr
                .sender_port
                .map(|p| format!(":{}", p))
                .unwrap_or_default();
            println!("  Sender: {}{}", host, port);
        }
        if let Some(ref slot) = wr.slot_name {
            println!("  Slot: {}", slot);
        }
        if let Some(bytes) = wr.lag_bytes {
            println!("  Lag: {}", format_bytes(bytes));
        }
        println!();
    }

    // Slots
    if result.slots.is_empty() {
        if !quiet {
            println!("REPLICATION SLOTS: none");
        }
    } else {
        println!("REPLICATION SLOTS:");
        println!(
            "  {:3} {:30} {:10} {:8} {:>12} {:>10}",
            "", "SLOT", "TYPE", "ACTIVE", "WAL STATUS", "RETAINED"
        );
        println!("  {}", "-".repeat(78));

        for s in &result.slots {
            let name_display = if s.slot_name.len() > 30 {
                format!("{}...", &s.slot_name[..27])
            } else {
                s.slot_name.clone()
            };
            let active_str = if s.active { "yes" } else { "no" };
            let wal_status = s.wal_status.as_deref().unwrap_or("-");
            let retained = s
                .retained_bytes
                .map(format_bytes)
                .unwrap_or_else(|| "-".to_string());

            println!(
                "  {} {:30} {:10} {:8} {:>12} {:>10}",
                status_emoji(&s.status),
                name_display,
                s.slot_type,
                active_str,
                wal_status,
                retained
            );
        }
    }
}

pub fn print_json(
    result: &ReplicationResult,
    timeouts: Option<crate::diagnostic::EffectiveTimeouts>,
) -> Result<()> {
    use crate::output::{schema, DiagnosticOutput, Severity};

    let severity = match result.overall_status {
        ReplicationStatus::Healthy => Severity::Healthy,
        ReplicationStatus::Warning => Severity::Warning,
        ReplicationStatus::Critical => Severity::Critical,
    };

    let output = match timeouts {
        Some(t) => DiagnosticOutput::with_timeouts(schema::REPLICATION, result, severity, t),
        None => DiagnosticOutput::new(schema::REPLICATION, result, severity),
    };
    output.print()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_lag() {
        assert_eq!(format_lag(Some(5.0)), "5.0s");
        assert_eq!(format_lag(Some(90.0)), "1.5m");
        assert_eq!(format_lag(None), "-");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1_500_000), "1.4 MB");
        assert_eq!(format_bytes(2_000_000_000), "1.9 GB");
    }
}
