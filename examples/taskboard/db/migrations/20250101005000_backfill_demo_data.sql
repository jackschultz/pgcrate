-- Backfill computed fields for demo purposes
-- NO DOWN FILE: This migration cannot be rolled back

-- up
-- Example: Update any tasks without proper timestamps
UPDATE app.tasks
SET created_at = now() - interval '7 days'
WHERE created_at > now();

-- Example: Ensure all users have lowercase emails
UPDATE app.users
SET email = lower(email)
WHERE email != lower(email);

-- down
-- Irreversible migration: history will only be rewound
