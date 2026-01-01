-- materialized: incremental
-- unique_key: user_id, activity_date
-- deps: staging.stg_tasks, staging.stg_users
-- tags: daily, metrics
-- tests: not_null(user_id), unique(user_id, activity_date)
-- description: Daily user activity metrics for incremental refresh

SELECT
    u.id AS user_id,
    u.name AS user_name,
    DATE(t.created_at) AS activity_date,
    COUNT(*) AS tasks_created,
    COUNT(*) FILTER (WHERE t.status = 'done') AS tasks_completed,
    COUNT(*) FILTER (WHERE t.status = 'in_progress') AS tasks_in_progress
FROM staging.stg_users u
LEFT JOIN staging.stg_tasks t ON t.assignee_id = u.id
WHERE t.created_at IS NOT NULL
GROUP BY u.id, u.name, DATE(t.created_at)
