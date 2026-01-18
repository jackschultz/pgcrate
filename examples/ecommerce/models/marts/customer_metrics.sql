-- materialized: table
-- description: Customer lifetime value and order metrics
-- deps: staging.stg_orders, staging.stg_customers

SELECT
    c.customer_id,
    c.email,
    c.full_name,
    c.country_name,
    c.signup_date,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total), 0) AS lifetime_value,
    COALESCE(AVG(o.total), 0) AS avg_order_value,
    MIN(o.ordered_at) AS first_order_at,
    MAX(o.ordered_at) AS last_order_at,
    CASE
        WHEN COUNT(o.order_id) = 0 THEN 'never_ordered'
        WHEN COUNT(o.order_id) = 1 THEN 'new'
        WHEN COUNT(o.order_id) < 5 THEN 'growing'
        ELSE 'loyal'
    END AS customer_segment,
    NOW() AS refreshed_at
FROM staging.stg_customers c
LEFT JOIN staging.stg_orders o ON o.customer_id = c.customer_id
    AND o.status NOT IN ('cancelled', 'refunded')
GROUP BY
    c.customer_id,
    c.email,
    c.full_name,
    c.country_name,
    c.signup_date
