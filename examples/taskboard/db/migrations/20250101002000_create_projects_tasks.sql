-- Projects and tasks tables

-- up
CREATE TABLE app.projects (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES app.teams(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (team_id, name)
);

CREATE INDEX idx_projects_team ON app.projects(team_id);

CREATE TABLE app.tasks (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES app.projects(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    description TEXT,
    assignee_id INTEGER REFERENCES app.users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (project_id, title)
);

CREATE INDEX idx_tasks_project ON app.tasks(project_id);
CREATE INDEX idx_tasks_assignee ON app.tasks(assignee_id);

-- down
DROP TABLE IF EXISTS app.tasks;
DROP TABLE IF EXISTS app.projects;
