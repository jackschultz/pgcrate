-- SQL seed for demo users with complex insert logic
-- Demonstrates SQL seeds for scenarios requiring custom logic.

CREATE TABLE IF NOT EXISTS public.demo_users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

TRUNCATE public.demo_users RESTART IDENTITY;

-- Insert with generate_series for dynamic data
INSERT INTO public.demo_users (email, name, role, created_at)
SELECT
    'demo' || n || '@example.com',
    'Demo User ' || n,
    CASE
        WHEN n = 1 THEN 'admin'
        WHEN n <= 3 THEN 'manager'
        ELSE 'member'
    END,
    now() - (n || ' days')::interval
FROM generate_series(1, 10) AS n;
