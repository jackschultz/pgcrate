-- up
-- Create analytics views for reporting
-- These provide pre-aggregated data for dashboards

CREATE OR REPLACE VIEW analytics.tasks_by_status AS
SELECT
    status,
    COUNT(*) AS task_count,
    COUNT(DISTINCT project_id) AS project_count
FROM app.tasks
GROUP BY status;

CREATE OR REPLACE VIEW analytics.team_productivity AS
SELECT
    t.id AS team_id,
    t.name AS team_name,
    COUNT(DISTINCT p.id) AS project_count,
    COUNT(DISTINCT tk.id) AS task_count,
    COUNT(DISTINCT tk.id) FILTER (WHERE tk.status = 'done') AS completed_count,
    ROUND(
        100.0 * COUNT(DISTINCT tk.id) FILTER (WHERE tk.status = 'done') /
        NULLIF(COUNT(DISTINCT tk.id), 0),
        2
    ) AS completion_rate
FROM app.teams t
LEFT JOIN app.projects p ON p.team_id = t.id
LEFT JOIN app.tasks tk ON tk.project_id = p.id
GROUP BY t.id, t.name;

CREATE OR REPLACE VIEW analytics.daily_activity AS
SELECT
    DATE(t.created_at) AS activity_date,
    COUNT(*) AS tasks_created,
    COUNT(*) FILTER (WHERE t.status = 'done') AS tasks_completed
FROM app.tasks t
GROUP BY DATE(t.created_at);

-- down
DROP VIEW IF EXISTS analytics.daily_activity;
DROP VIEW IF EXISTS analytics.team_productivity;
DROP VIEW IF EXISTS analytics.tasks_by_status;
