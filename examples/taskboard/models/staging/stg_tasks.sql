-- materialized: view
-- deps: app.tasks, app.projects
-- tests: not_null(id), accepted_values(status, ['todo', 'in_progress', 'done']), relationships(assignee_id, staging.stg_users.id)
-- description: Staging view for tasks with project context

SELECT
    t.id,
    t.project_id,
    p.name AS project_name,
    p.team_id,
    t.title,
    t.description,
    t.assignee_id,
    t.status,
    t.created_at,
    t.completed_at,
    CASE WHEN t.completed_at IS NOT NULL
         THEN EXTRACT(HOURS FROM t.completed_at - t.created_at)::integer
         ELSE NULL
    END AS hours_to_complete
FROM app.tasks t
JOIN app.projects p ON p.id = t.project_id
