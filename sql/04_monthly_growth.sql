WITH monthly AS (
 SELECT substr(order_date,1,7) AS month,
 SUM(CASE WHEN status='completed' THEN revenue WHEN status='returned' THEN -revenue ELSE 0 END) AS net_revenue
 FROM fact_orders GROUP BY 1
)
SELECT month,ROUND(net_revenue,2) AS net_revenue,
 ROUND(100.0*(net_revenue-LAG(net_revenue) OVER(ORDER BY month))/NULLIF(LAG(net_revenue) OVER(ORDER BY month),0),2) AS mom_growth_pct
FROM monthly ORDER BY month;
