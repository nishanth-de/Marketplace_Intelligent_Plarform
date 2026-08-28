SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_orders;
SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_customers;
SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_products;
SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_order_items;
SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_order_payments;
SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_order_reviews;
SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_sellers;
SELECT COUNT(*) FROM dev_catalog.silver_clean.silver_geolocation;

SELECT
    COUNT(*) AS total_rows,
    COUNT(customer_key) AS matched_customers,
    COUNT(order_date_key) AS matched_dates
FROM dev_catalog.gold_warehouse.fact_orders;

SELECT
    COUNT(*) AS total_rows,
    COUNT(product_key) AS matched_products,
    COUNT(seller_key) AS matched_sellers,
    COUNT(customer_key) AS matched_customers,
    COUNT(order_date_key) AS matched_dates
FROM dev_catalog.gold_warehouse.fact_order_items;