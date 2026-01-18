-- materialized: table
-- description: Product sales performance metrics

SELECT
    p.id AS product_id,
    p.sku,
    p.name AS product_name,
    cat.name AS category_name,
    p.price AS current_price,
    p.stock_quantity,
    p.reorder_level,
    CASE
        WHEN p.stock_quantity <= p.reorder_level THEN 'low_stock'
        WHEN p.stock_quantity = 0 THEN 'out_of_stock'
        ELSE 'in_stock'
    END AS stock_status,
    COUNT(oi.id) AS times_ordered,
    COALESCE(SUM(oi.quantity), 0) AS units_sold,
    COALESCE(SUM(oi.total_price), 0) AS total_revenue,
    COALESCE(AVG(oi.unit_price), p.price) AS avg_sale_price,
    NOW() AS refreshed_at
FROM products p
LEFT JOIN categories cat ON cat.id = p.category_id
LEFT JOIN order_items oi ON oi.product_id = p.id
LEFT JOIN orders o ON o.id = oi.order_id
    AND o.status NOT IN ('cancelled', 'refunded')
WHERE p.is_active = TRUE
GROUP BY
    p.id,
    p.sku,
    p.name,
    cat.name,
    p.price,
    p.stock_quantity,
    p.reorder_level
