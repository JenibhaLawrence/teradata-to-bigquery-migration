SELECT
    c.country,
    c.region,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_amount) AS total_revenue,
    AVG(o.order_amount) AS avg_order_amount,
    MAX(o.order_amount) AS max_order_amount,
    MIN(o.order_amount) AS min_order_amount
FROM customer c
JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.order_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
GROUP BY
    c.country,
    c.region
ORDER BY total_revenue DESC;
