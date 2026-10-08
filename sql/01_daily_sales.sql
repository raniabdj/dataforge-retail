SELECT order_date, COUNT(*) AS orders,
 ROUND(SUM(CASE WHEN status='completed' THEN revenue WHEN status='returned' THEN -revenue ELSE 0 END),2) AS net_revenue,
 ROUND(AVG(CASE WHEN status='completed' THEN revenue END),2) AS avg_order_value,
 SUM(CASE WHEN status='returned' THEN 1 ELSE 0 END) AS returns
FROM fact_orders GROUP BY order_date ORDER BY order_date;
