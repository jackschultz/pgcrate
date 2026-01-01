//! Role and grants commands for pgcrate CLI.

use anyhow::{bail, Result};
use colored::Colorize;

use super::connect;

/// List database roles
pub async fn role_list(
    database_url: &str,
    users_only: bool,
    groups_only: bool,
    quiet: bool,
) -> Result<()> {
    if users_only && groups_only {
        bail!("Cannot specify both --users and --groups");
    }

    let client = connect(database_url).await?;

    let rows = client
        .query(
            r#"
            SELECT
                r.rolname AS name,
                r.rolsuper AS superuser,
                r.rolcreaterole AS createrole,
                r.rolcreatedb AS createdb,
                r.rolcanlogin AS login,
                r.rolreplication AS replication,
                r.rolbypassrls AS bypassrls,
                r.rolconnlimit AS connlimit,
                COALESCE(
                    ARRAY(
                        SELECT m.rolname
                        FROM pg_auth_members am
                        JOIN pg_roles m ON am.roleid = m.oid
                        WHERE am.member = r.oid
                    ),
                    ARRAY[]::text[]
                ) AS member_of
            FROM pg_roles r
            WHERE r.rolname NOT LIKE 'pg_%'
            ORDER BY r.rolname
            "#,
            &[],
        )
        .await?;

    if rows.is_empty() {
        if !quiet {
            println!("{}", "No roles found.".yellow());
        }
        return Ok(());
    }

    // Filter based on flags
    let filtered: Vec<_> = rows
        .iter()
        .filter(|r| {
            let login: bool = r.get("login");
            if users_only {
                login
            } else if groups_only {
                !login
            } else {
                true
            }
        })
        .collect();

    if filtered.is_empty() {
        if !quiet {
            let msg = if users_only {
                "No login roles (users) found."
            } else {
                "No group roles found."
            };
            println!("{}", msg.yellow());
        }
        return Ok(());
    }

    if !quiet {
        let header = if users_only {
            "Login roles (users):"
        } else if groups_only {
            "Group roles:"
        } else {
            "Roles:"
        };
        println!("{}\n", header);

        for row in &filtered {
            let name: String = row.get("name");
            let superuser: bool = row.get("superuser");
            let createrole: bool = row.get("createrole");
            let createdb: bool = row.get("createdb");
            let login: bool = row.get("login");
            let replication: bool = row.get("replication");
            let bypassrls: bool = row.get("bypassrls");
            let member_of: Vec<String> = row.get("member_of");

            // Build attributes string
            let mut attrs = Vec::new();
            if superuser {
                attrs.push("superuser".red().to_string());
            }
            if createrole {
                attrs.push("create role".to_string());
            }
            if createdb {
                attrs.push("create db".to_string());
            }
            if replication {
                attrs.push("replication".to_string());
            }
            if bypassrls {
                attrs.push("bypass rls".yellow().to_string());
            }
            if login {
                attrs.push("login".green().to_string());
            }

            let attrs_str = if attrs.is_empty() {
                "(no special attributes)".dimmed().to_string()
            } else {
                attrs.join(", ")
            };

            // Show role name and attributes
            println!("  {:<20} {}", name.bold(), attrs_str);

            // Show memberships if any
            if !member_of.is_empty() {
                println!("  {:<20} member of: {}", "", member_of.join(", ").dimmed());
            }
        }

        // Summary
        let login_count = filtered
            .iter()
            .filter(|r| r.get::<_, bool>("login"))
            .count();
        let group_count = filtered.len() - login_count;
        println!(
            "\n{} role(s): {} login, {} group",
            filtered.len(),
            login_count,
            group_count
        );
    }

    Ok(())
}

/// Describe a specific role in detail
pub async fn role_describe(database_url: &str, name: &str, quiet: bool) -> Result<()> {
    let client = connect(database_url).await?;

    // Get role info
    let role_row = client
        .query_opt(
            r#"
            SELECT
                r.rolname AS name,
                r.rolsuper AS superuser,
                r.rolcreaterole AS createrole,
                r.rolcreatedb AS createdb,
                r.rolcanlogin AS login,
                r.rolreplication AS replication,
                r.rolbypassrls AS bypassrls,
                r.rolconnlimit AS connlimit,
                r.rolvaliduntil AS valid_until
            FROM pg_roles r
            WHERE r.rolname = $1
            "#,
            &[&name],
        )
        .await?;

    let Some(role) = role_row else {
        bail!("Role '{}' not found", name);
    };

    // Get memberships (roles this role is a member of)
    let member_of_rows = client
        .query(
            r#"
            SELECT m.rolname AS name, am.admin_option
            FROM pg_auth_members am
            JOIN pg_roles m ON am.roleid = m.oid
            JOIN pg_roles r ON am.member = r.oid
            WHERE r.rolname = $1
            ORDER BY m.rolname
            "#,
            &[&name],
        )
        .await?;

    // Get members (roles that are members of this role)
    let members_rows = client
        .query(
            r#"
            SELECT m.rolname AS name, am.admin_option
            FROM pg_auth_members am
            JOIN pg_roles m ON am.member = m.oid
            JOIN pg_roles r ON am.roleid = r.oid
            WHERE r.rolname = $1
            ORDER BY m.rolname
            "#,
            &[&name],
        )
        .await?;

    // Get owned object counts
    let owned_rows = client
        .query(
            r#"
            SELECT
                CASE c.relkind
                    WHEN 'r' THEN 'tables'
                    WHEN 'v' THEN 'views'
                    WHEN 'm' THEN 'materialized views'
                    WHEN 'S' THEN 'sequences'
                    WHEN 'i' THEN 'indexes'
                    ELSE 'other'
                END AS kind,
                COUNT(*) AS count
            FROM pg_class c
            JOIN pg_roles r ON c.relowner = r.oid
            WHERE r.rolname = $1
              AND c.relkind IN ('r', 'v', 'm', 'S')
            GROUP BY c.relkind
            ORDER BY c.relkind
            "#,
            &[&name],
        )
        .await?;

    // Get function count
    let func_count: i64 = client
        .query_one(
            r#"
            SELECT COUNT(*) AS count
            FROM pg_proc p
            JOIN pg_roles r ON p.proowner = r.oid
            WHERE r.rolname = $1
            "#,
            &[&name],
        )
        .await?
        .get("count");

    if quiet {
        return Ok(());
    }

    // Display
    let role_name: String = role.get("name");
    println!("\nRole: {}\n", role_name.bold());

    // Attributes
    println!("Attributes:");
    let login: bool = role.get("login");
    let superuser: bool = role.get("superuser");
    let createrole: bool = role.get("createrole");
    let createdb: bool = role.get("createdb");
    let replication: bool = role.get("replication");
    let bypassrls: bool = role.get("bypassrls");
    let connlimit: i32 = role.get("connlimit");

    println!(
        "  Login:            {}",
        if login { "yes".green() } else { "no".normal() }
    );
    println!(
        "  Superuser:        {}",
        if superuser {
            "yes".red()
        } else {
            "no".normal()
        }
    );
    println!(
        "  Create role:      {}",
        if createrole { "yes" } else { "no" }
    );
    println!(
        "  Create DB:        {}",
        if createdb { "yes" } else { "no" }
    );
    println!(
        "  Replication:      {}",
        if replication { "yes" } else { "no" }
    );
    println!(
        "  Bypass RLS:       {}",
        if bypassrls {
            "yes".yellow()
        } else {
            "no".normal()
        }
    );
    println!(
        "  Connection limit: {}",
        if connlimit < 0 {
            "unlimited".to_string()
        } else {
            connlimit.to_string()
        }
    );

    // Member of
    if !member_of_rows.is_empty() {
        println!("\nMember of:");
        for row in &member_of_rows {
            let member_name: String = row.get("name");
            let admin: bool = row.get("admin_option");
            let suffix = if admin { " (admin)" } else { "" };
            println!("  {}{}", member_name, suffix.dimmed());
        }
    }

    // Members
    if !members_rows.is_empty() {
        println!("\nMembers (roles that inherit from this):");
        for row in &members_rows {
            let member_name: String = row.get("name");
            let admin: bool = row.get("admin_option");
            let suffix = if admin { " (admin)" } else { "" };
            println!("  {}{}", member_name, suffix.dimmed());
        }
    }

    // Owned objects
    if !owned_rows.is_empty() || func_count > 0 {
        println!("\nOwned objects:");
        for row in &owned_rows {
            let kind: String = row.get("kind");
            let count: i64 = row.get("count");
            println!("  {:<20} {}", kind.capitalize(), count);
        }
        if func_count > 0 {
            println!("  {:<20} {}", "Functions", func_count);
        }
    }

    println!();
    Ok(())
}

/// Show grants on database objects
pub async fn grants(
    database_url: &str,
    object: Option<&str>,
    schema: Option<&str>,
    role: Option<&str>,
    quiet: bool,
) -> Result<()> {
    // Validate: need exactly one of object, schema, or role
    let specified = [object.is_some(), schema.is_some(), role.is_some()]
        .iter()
        .filter(|&&x| x)
        .count();

    if specified == 0 {
        bail!("Specify one of: <object>, --schema <schema>, or --role <role>");
    }
    if specified > 1 {
        bail!("Specify only one of: <object>, --schema <schema>, or --role <role>");
    }

    let client = connect(database_url).await?;

    if let Some(obj) = object {
        show_object_grants(&client, obj, quiet).await?;
    } else if let Some(schema_name) = schema {
        show_schema_grants(&client, schema_name, quiet).await?;
    } else if let Some(role_name) = role {
        show_role_grants(&client, role_name, quiet).await?;
    }

    Ok(())
}

/// Show grants on a specific table
async fn show_object_grants(
    client: &tokio_postgres::Client,
    object: &str,
    quiet: bool,
) -> Result<()> {
    // Parse schema.table
    let (schema, table) = if object.contains('.') {
        let parts: Vec<&str> = object.splitn(2, '.').collect();
        (parts[0], parts[1])
    } else {
        ("public", object)
    };

    // Check table exists
    let exists = client
        .query_opt(
            r#"
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1 AND c.relname = $2
            "#,
            &[&schema, &table],
        )
        .await?;

    if exists.is_none() {
        bail!("Table '{}.{}' not found", schema, table);
    }

    // Get grants
    let rows = client
        .query(
            r#"
            SELECT
                grantee,
                privilege_type,
                is_grantable
            FROM information_schema.table_privileges
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY grantee, privilege_type
            "#,
            &[&schema, &table],
        )
        .await?;

    if quiet {
        return Ok(());
    }

    println!("\nGrants on {}.{}:\n", schema, table);

    if rows.is_empty() {
        println!("{}", "No explicit grants found.".dimmed());
        return Ok(());
    }

    // Group by grantee
    let mut grants_by_role: std::collections::BTreeMap<String, Vec<(String, bool)>> =
        std::collections::BTreeMap::new();

    for row in &rows {
        let grantee: String = row.get("grantee");
        let privilege: String = row.get("privilege_type");
        let with_grant_str: String = row.get("is_grantable");
        let with_grant = with_grant_str == "YES";
        grants_by_role
            .entry(grantee)
            .or_default()
            .push((privilege, with_grant));
    }

    // Print header
    println!(
        "{:<20} {:<50} {}",
        "Role".bold(),
        "Privileges".bold(),
        "Grant Option".bold()
    );
    println!("{}", "─".repeat(80));

    for (role, privs) in &grants_by_role {
        let priv_list: Vec<&str> = privs.iter().map(|(p, _)| p.as_str()).collect();
        let has_grant: Vec<&str> = privs
            .iter()
            .filter(|(_, g)| *g)
            .map(|(p, _)| p.as_str())
            .collect();

        let grant_str = if has_grant.is_empty() {
            "-".to_string()
        } else if has_grant.len() == privs.len() {
            "all".to_string()
        } else {
            has_grant.join(", ")
        };

        println!("{:<20} {:<50} {}", role, priv_list.join(", "), grant_str);
    }

    // Check for column-level grants
    let col_grants = client
        .query(
            r#"
            SELECT
                grantee,
                column_name,
                privilege_type
            FROM information_schema.column_privileges
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY grantee, column_name
            "#,
            &[&schema, &table],
        )
        .await?;

    if !col_grants.is_empty() {
        println!("\nColumn-level grants:");
        let mut col_by_role: std::collections::BTreeMap<String, Vec<(String, String)>> =
            std::collections::BTreeMap::new();

        for row in &col_grants {
            let grantee: String = row.get("grantee");
            let column: String = row.get("column_name");
            let privilege: String = row.get("privilege_type");
            col_by_role
                .entry(grantee)
                .or_default()
                .push((column, privilege));
        }

        for (role, cols) in &col_by_role {
            let col_str: Vec<String> = cols.iter().map(|(c, p)| format!("{}({})", p, c)).collect();
            println!("  {}: {}", role, col_str.join(", "));
        }
    }

    println!();
    Ok(())
}

/// Show grants summary for all tables in a schema
async fn show_schema_grants(
    client: &tokio_postgres::Client,
    schema: &str,
    quiet: bool,
) -> Result<()> {
    // Check schema exists
    let exists = client
        .query_opt("SELECT 1 FROM pg_namespace WHERE nspname = $1", &[&schema])
        .await?;

    if exists.is_none() {
        bail!("Schema '{}' not found", schema);
    }

    let rows = client
        .query(
            r#"
            SELECT
                table_name,
                grantee,
                privilege_type
            FROM information_schema.table_privileges
            WHERE table_schema = $1
              AND grantee NOT IN ('postgres', 'PUBLIC')
            ORDER BY table_name, grantee, privilege_type
            "#,
            &[&schema],
        )
        .await?;

    if quiet {
        return Ok(());
    }

    println!("\nGrants in schema: {}\n", schema);

    if rows.is_empty() {
        println!(
            "{}",
            "No explicit grants found (besides owner/PUBLIC).".dimmed()
        );
        return Ok(());
    }

    // Group by table, then by grantee
    let mut by_table: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, Vec<String>>,
    > = std::collections::BTreeMap::new();

    for row in &rows {
        let table: String = row.get("table_name");
        let grantee: String = row.get("grantee");
        let privilege: String = row.get("privilege_type");

        by_table
            .entry(table)
            .or_default()
            .entry(grantee)
            .or_default()
            .push(privilege);
    }

    println!(
        "{:<30} {:<20} {}",
        "Table".bold(),
        "Role".bold(),
        "Privileges".bold()
    );
    println!("{}", "─".repeat(80));

    for (table, grantees) in &by_table {
        let mut first = true;
        for (grantee, privs) in grantees {
            let table_col = if first {
                first = false;
                table.as_str()
            } else {
                ""
            };
            println!("{:<30} {:<20} {}", table_col, grantee, privs.join(", "));
        }
    }

    println!("\n{} table(s) with explicit grants", by_table.len());
    Ok(())
}

/// Show what a specific role can access
async fn show_role_grants(client: &tokio_postgres::Client, role: &str, quiet: bool) -> Result<()> {
    // Check role exists
    let exists = client
        .query_opt("SELECT 1 FROM pg_roles WHERE rolname = $1", &[&role])
        .await?;

    if exists.is_none() {
        bail!("Role '{}' not found", role);
    }

    let rows = client
        .query(
            r#"
            SELECT
                table_schema,
                table_name,
                privilege_type
            FROM information_schema.table_privileges
            WHERE grantee = $1
            ORDER BY table_schema, table_name, privilege_type
            "#,
            &[&role],
        )
        .await?;

    if quiet {
        return Ok(());
    }

    println!("\nGrants for role: {}\n", role);

    if rows.is_empty() {
        println!("{}", "No direct table grants found.".dimmed());
        println!(
            "{}",
            "(Role may have access through group membership)".dimmed()
        );
        return Ok(());
    }

    // Group by schema.table
    let mut by_table: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();

    for row in &rows {
        let schema: String = row.get("table_schema");
        let table: String = row.get("table_name");
        let privilege: String = row.get("privilege_type");

        let full_name = format!("{}.{}", schema, table);
        by_table.entry(full_name).or_default().push(privilege);
    }

    println!("{:<40} {}", "Table".bold(), "Privileges".bold());
    println!("{}", "─".repeat(70));

    for (table, privs) in &by_table {
        println!("{:<40} {}", table, privs.join(", "));
    }

    println!("\n{} table(s) accessible", by_table.len());

    // Also show schema usage
    let schema_rows = client
        .query(
            r#"
            SELECT
                nspname AS schema_name,
                has_schema_privilege($1, oid, 'USAGE') AS has_usage,
                has_schema_privilege($1, oid, 'CREATE') AS has_create
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
              AND nspname != 'information_schema'
              AND (has_schema_privilege($1, oid, 'USAGE')
                   OR has_schema_privilege($1, oid, 'CREATE'))
            ORDER BY nspname
            "#,
            &[&role],
        )
        .await?;

    if !schema_rows.is_empty() {
        println!("\nSchema access:");
        for row in &schema_rows {
            let schema_name: String = row.get("schema_name");
            let has_usage: bool = row.get("has_usage");
            let has_create: bool = row.get("has_create");

            let mut perms = Vec::new();
            if has_usage {
                perms.push("USAGE");
            }
            if has_create {
                perms.push("CREATE");
            }
            println!("  {:<20} {}", schema_name, perms.join(", "));
        }
    }

    println!();
    Ok(())
}

trait Capitalize {
    fn capitalize(&self) -> String;
}

impl Capitalize for str {
    fn capitalize(&self) -> String {
        let mut chars = self.chars();
        match chars.next() {
            None => String::new(),
            Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        }
    }
}
