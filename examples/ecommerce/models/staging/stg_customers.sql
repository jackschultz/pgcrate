-- materialized: view
-- description: Cleaned customer data with country info

SELECT
    c.id AS customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.country_code,
    co.name AS country_name,
    c.phone,
    c.created_at,
    DATE_TRUNC('day', c.created_at) AS signup_date,
    DATE_TRUNC('month', c.created_at) AS signup_month
FROM customers c
LEFT JOIN countries co ON co.code = c.country_code
