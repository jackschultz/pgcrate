-- Create posts table with foreign key to users

-- up
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    body TEXT,
    published BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for finding posts by user
CREATE INDEX posts_user_id_idx ON posts(user_id);

-- Index for finding published posts
CREATE INDEX posts_published_idx ON posts(published) WHERE published = TRUE;

-- down
DROP INDEX IF EXISTS posts_published_idx;
DROP INDEX IF EXISTS posts_user_id_idx;
DROP TABLE posts;
