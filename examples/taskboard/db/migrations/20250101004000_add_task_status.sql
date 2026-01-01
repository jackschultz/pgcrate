-- Add status column to tasks

-- up
-- Add column as nullable first
ALTER TABLE app.tasks ADD COLUMN status TEXT;

-- Backfill: completed tasks get 'done', others get 'todo'
UPDATE app.tasks SET status = CASE
    WHEN completed_at IS NOT NULL THEN 'done'
    ELSE 'todo'
END;

-- Now make it NOT NULL with a constraint
ALTER TABLE app.tasks ALTER COLUMN status SET NOT NULL;
ALTER TABLE app.tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('todo', 'in_progress', 'done'));

CREATE INDEX idx_tasks_status ON app.tasks(status);

-- down
-- Remove status column from tasks (irreversible for status history)
DROP INDEX IF EXISTS app.idx_tasks_status;
ALTER TABLE app.tasks DROP CONSTRAINT IF EXISTS tasks_status_check;
ALTER TABLE app.tasks DROP COLUMN IF EXISTS status;
