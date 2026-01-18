-- Create users table with basic fields

-- up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255),
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index on email for faster lookups
CREATE INDEX users_email_idx ON users(email);

-- down
DROP INDEX IF EXISTS users_email_idx;
DROP TABLE users;
