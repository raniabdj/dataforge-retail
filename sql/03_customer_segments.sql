WITH customer_value AS (
 SELECT c.segment,c.region,f.customer_id,
 SUM(CASE WHEN f.status='completed' THEN f.revenue WHEN f.status='returned' THEN -f.revenue ELSE 0 END) AS net_revenue
 FROM fact_orders f JOIN dim_customer c USING(customer_id)
 GROUP BY c.segment,c.region,f.customer_id
)
SELECT segment,region,COUNT(*) AS active_customers,ROUND(SUM(net_revenue),2) AS net_revenue,
 ROUND(AVG(net_revenue),2) AS revenue_per_customer
FROM customer_value GROUP BY segment,region ORDER BY net_revenue DESC;
