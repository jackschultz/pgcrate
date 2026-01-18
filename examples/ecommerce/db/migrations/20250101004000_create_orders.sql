-- Orders and order items

-- Sequence for order numbers (human-readable IDs)
CREATE SEQUENCE order_number_seq START 10000;

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(20) NOT NULL UNIQUE DEFAULT 'ORD-' || nextval('order_number_seq')::TEXT,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    shipping_method_id INTEGER NOT NULL REFERENCES shipping_methods(id),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    shipping_address TEXT,
    shipping_country CHAR(2) REFERENCES countries(code),
    subtotal DECIMAL(10,2) NOT NULL DEFAULT 0,
    shipping_cost DECIMAL(10,2) NOT NULL DEFAULT 0,
    tax DECIMAL(10,2) NOT NULL DEFAULT 0,
    total DECIMAL(10,2) NOT NULL DEFAULT 0,
    notes TEXT,
    ordered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    shipped_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index on customer for lookup
CREATE INDEX idx_orders_customer ON orders(customer_id);

-- Index on status for filtering
CREATE INDEX idx_orders_status ON orders(status);

-- Index on ordered_at for time-based queries
CREATE INDEX idx_orders_ordered_at ON orders(ordered_at);

-- Note: shipping_method_id FK has no index - good for pgcrate indexes to detect

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index on order for lookup
CREATE INDEX idx_order_items_order ON order_items(order_id);

-- Note: product_id FK has no index - good for pgcrate indexes to detect

---- down
DROP INDEX IF EXISTS idx_order_items_order;
DROP TABLE IF EXISTS order_items;
DROP INDEX IF EXISTS idx_orders_ordered_at;
DROP INDEX IF EXISTS idx_orders_status;
DROP INDEX IF EXISTS idx_orders_customer;
DROP TABLE IF EXISTS orders;
DROP SEQUENCE IF EXISTS order_number_seq;
