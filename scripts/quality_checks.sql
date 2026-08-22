SELECT COUNT(*)
FROM dev_catalog.silver_clean.silver_orders
WHERE order_id IS NULL;

SELECT COUNT(*)
FROM dev_catalog.silver_clean.silver_customers
WHERE customer_id IS NULL;

SELECT COUNT(*)
FROM dev_catalog.silver_clean.silver_order_items
WHERE order_id IS NULL;

SELECT
    product_category_name,
    COUNT(*) AS product_count
FROM dev_catalog.silver_clean.silver_products
GROUP BY product_category_name
ORDER BY product_count DESC;