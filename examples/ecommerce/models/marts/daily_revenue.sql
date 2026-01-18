-- materialized: table
-- description: Daily revenue aggregation
-- deps: staging.stg_orders

SELECT
    order_date,
    COUNT(*) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(subtotal) AS subtotal,
    SUM(shipping_cost) AS shipping_revenue,
    SUM(tax) AS tax_collected,
    SUM(total) AS total_revenue,
    AVG(total) AS avg_order_value,
    NOW() AS refreshed_at
FROM staging.stg_orders
WHERE status NOT IN ('cancelled', 'refunded')
GROUP BY order_date
ORDER BY order_date DESC
