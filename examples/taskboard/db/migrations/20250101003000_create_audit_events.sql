-- Audit events: activity log recording user actions
-- Used for: activity feeds, analytics, debugging, compliance
-- Example actions: 'task.created', 'task.completed', 'user.login'

-- up
CREATE TABLE app.audit_events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES app.users(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_events_user ON app.audit_events(user_id);
CREATE INDEX idx_audit_events_entity ON app.audit_events(entity_type, entity_id);
CREATE INDEX idx_audit_events_created ON app.audit_events(created_at);

-- down
DROP TABLE IF EXISTS app.audit_events;
