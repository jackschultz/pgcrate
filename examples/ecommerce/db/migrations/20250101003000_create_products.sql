-- Products and categories

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    parent_id INTEGER REFERENCES categories(id),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Sequence for generating SKUs
CREATE SEQUENCE product_sku_seq START 1000;

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE DEFAULT 'SKU-' || nextval('product_sku_seq')::TEXT,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category_id INTEGER REFERENCES categories(id),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    reorder_level INTEGER NOT NULL DEFAULT 10,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index on category for filtering
CREATE INDEX idx_products_category ON products(category_id);

-- Index for active products lookup
CREATE INDEX idx_products_active ON products(is_active) WHERE is_active = TRUE;

---- down
DROP INDEX IF EXISTS idx_products_active;
DROP INDEX IF EXISTS idx_products_category;
DROP TABLE IF EXISTS products;
DROP SEQUENCE IF EXISTS product_sku_seq;
DROP TABLE IF EXISTS categories;
