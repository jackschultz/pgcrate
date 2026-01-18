-- materialized: view
-- description: Cleaned and enriched order data for analytics

SELECT
    o.id AS order_id,
    o.order_number,
    o.customer_id,
    c.email AS customer_email,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.shipping_method_id,
    sm.name AS shipping_method,
    o.status,
    o.subtotal,
    o.shipping_cost,
    o.tax,
    o.total,
    o.ordered_at,
    o.shipped_at,
    o.delivered_at,
    CASE
        WHEN o.delivered_at IS NOT NULL THEN 'delivered'
        WHEN o.shipped_at IS NOT NULL THEN 'in_transit'
        WHEN o.status = 'cancelled' THEN 'cancelled'
        ELSE 'processing'
    END AS fulfillment_status,
    DATE_TRUNC('day', o.ordered_at) AS order_date,
    DATE_TRUNC('month', o.ordered_at) AS order_month
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN shipping_methods sm ON sm.id = o.shipping_method_id
