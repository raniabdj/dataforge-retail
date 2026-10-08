SELECT p.category, COUNT(*) AS orders,
 ROUND(SUM(CASE WHEN f.status='completed' THEN f.revenue WHEN f.status='returned' THEN -f.revenue ELSE 0 END),2) AS net_revenue,
 ROUND(100.0*SUM(CASE WHEN f.status='returned' THEN 1 ELSE 0 END)/COUNT(*),2) AS return_rate_pct
FROM fact_orders f JOIN dim_product p USING(product_id)
GROUP BY p.category ORDER BY net_revenue DESC;
