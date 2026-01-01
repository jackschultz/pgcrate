-- Create application schemas

-- up
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS analytics;

-- down
DROP SCHEMA IF EXISTS analytics CASCADE;
DROP SCHEMA IF EXISTS app CASCADE;
