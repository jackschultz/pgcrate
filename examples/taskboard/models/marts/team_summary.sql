-- materialized: table
-- deps: app.teams, app.projects, staging.stg_tasks
-- tags: summary
-- tests: not_null(team_id), unique(team_id)
-- description: Team-level summary with project and task counts

SELECT
    t.id AS team_id,
    t.name AS team_name,
    t.created_at AS team_created_at,
    COUNT(DISTINCT p.id) AS project_count,
    COUNT(DISTINCT tk.id) AS total_tasks,
    COUNT(DISTINCT tk.id) FILTER (WHERE tk.status = 'done') AS completed_tasks,
    COUNT(DISTINCT tk.id) FILTER (WHERE tk.status = 'in_progress') AS active_tasks,
    ROUND(
        100.0 * COUNT(DISTINCT tk.id) FILTER (WHERE tk.status = 'done') /
        NULLIF(COUNT(DISTINCT tk.id), 0),
        2
    ) AS completion_rate
FROM app.teams t
LEFT JOIN app.projects p ON p.team_id = t.id
LEFT JOIN staging.stg_tasks tk ON tk.project_id = p.id
GROUP BY t.id, t.name, t.created_at
