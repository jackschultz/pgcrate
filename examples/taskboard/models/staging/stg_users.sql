-- materialized: view
-- deps: app.users
-- description: Staging view for users with computed fields

SELECT
    id,
    email,
    name,
    created_at,
    EXTRACT(DAYS FROM now() - created_at)::integer AS days_since_signup
FROM app.users
