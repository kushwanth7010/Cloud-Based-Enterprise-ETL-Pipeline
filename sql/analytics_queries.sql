-- Portable ANSI-style analytics queries; currency is stored as integer cents.
-- Run these against warehouse.db (SQLite) or your configured PostgreSQL database.

-- 1. Revenue and profit by product category.
SELECT p.category,
       COUNT(*) AS order_count,
       ROUND(SUM(s.net_revenue_cents) / 100.0, 2) AS net_revenue,
       ROUND(SUM(s.gross_profit_cents) / 100.0, 2) AS gross_profit
FROM fact_sales AS s
JOIN dim_product AS p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY net_revenue DESC;

-- 2. Top customers by net revenue.
SELECT c.customer_id, c.customer_name, c.region,
       COUNT(*) AS order_count,
       ROUND(SUM(s.net_revenue_cents) / 100.0, 2) AS net_revenue
FROM fact_sales AS s
JOIN dim_customer AS c ON s.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.region
ORDER BY net_revenue DESC
LIMIT 10;

-- 3. Last five pipeline runs, including rejected source rows.
SELECT started_at_utc, source_sha256, orders_valid,
       orders_rejected, warehouse_order_count
FROM etl_runs
ORDER BY started_at_utc DESC
LIMIT 5;

-- 4. Monthly regional sales and profitability.
SELECT SUBSTR(s.order_date, 1, 7) AS order_month, c.region,
       COUNT(*) AS orders,
       ROUND(SUM(s.net_revenue_cents) / 100.0, 2) AS net_revenue,
       ROUND(SUM(s.gross_profit_cents) / 100.0, 2) AS gross_profit
FROM fact_sales AS s
JOIN dim_customer AS c ON s.customer_id = c.customer_id
GROUP BY SUBSTR(s.order_date, 1, 7), c.region
ORDER BY order_month, c.region;
