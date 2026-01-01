-- materialized: table
-- deps: staging.stg_tasks
-- tags: metrics, daily
-- tests: not_null(status), unique(status)
-- description: Aggregate task metrics by status

SELECT
    status,
    COUNT(*) AS task_count,
    COUNT(DISTINCT project_id) AS project_count,
    COUNT(DISTINCT assignee_id) AS assignee_count,
    AVG(hours_to_complete)::numeric(10,2) AS avg_hours_to_complete,
    MIN(created_at) AS oldest_task,
    MAX(created_at) AS newest_task
FROM staging.stg_tasks
GROUP BY status
