-- Create posts table

-- up
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title VARCHAR(255) NOT NULL,
    body TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX posts_user_id_idx ON posts(user_id);

-- down
DROP INDEX IF EXISTS posts_user_id_idx;
DROP TABLE posts;
