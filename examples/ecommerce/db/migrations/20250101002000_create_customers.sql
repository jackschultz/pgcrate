-- Customer table with sequences and foreign keys

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    country_code CHAR(2) REFERENCES countries(code),
    phone VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index on country for joining
CREATE INDEX idx_customers_country ON customers(country_code);

-- Index on created_at for time-based queries
CREATE INDEX idx_customers_created_at ON customers(created_at);

---- down
DROP INDEX IF EXISTS idx_customers_created_at;
DROP INDEX IF EXISTS idx_customers_country;
DROP TABLE IF EXISTS customers;
