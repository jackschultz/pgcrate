-- Inventory movements for tracking stock changes

CREATE TABLE inventory_movements (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL, -- positive = in, negative = out
    movement_type VARCHAR(50) NOT NULL, -- 'purchase', 'sale', 'adjustment', 'return'
    reference_type VARCHAR(50), -- 'order', 'purchase_order', 'manual'
    reference_id INTEGER, -- ID of the related record
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by VARCHAR(100)
);

-- Index on product for lookup
CREATE INDEX idx_inventory_movements_product ON inventory_movements(product_id);

-- Index on created_at for time-based queries
CREATE INDEX idx_inventory_movements_created ON inventory_movements(created_at);

-- Index on movement_type for filtering
CREATE INDEX idx_inventory_movements_type ON inventory_movements(movement_type);

---- down
DROP INDEX IF EXISTS idx_inventory_movements_type;
DROP INDEX IF EXISTS idx_inventory_movements_created;
DROP INDEX IF EXISTS idx_inventory_movements_product;
DROP TABLE IF EXISTS inventory_movements;
