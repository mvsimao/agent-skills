-- Legacy Spark SQL Query for Customer Enrichment
SELECT
    c.id AS customer_id,
    TRIM(UPPER(c.name)) AS customer_name,
    c.country_iso AS country_code,
    o.order_id,
    COALESCE(o.amount, 0.0) AS order_amount,
    CURRENT_TIMESTAMP() AS processed_at
FROM
    bronze_erp_customers.raw_customers c
LEFT JOIN
    silver_orders_cleansed.orders_v2 o ON c.id = o.customer_id
WHERE
    c.is_active = 1
