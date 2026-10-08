
{{ config(materialized='table') }}

select
    customer_segment,
    count(distinct customer_id) as total_customers,
    count(*) as total_orders,
    sum(quantity) as total_units_sold,
    round(sum(revenue), 2) as total_revenue,
    round(sum(estimated_profit), 2) as estimated_profit,
    round(avg(revenue), 2) as average_order_value,
    round(
        sum(revenue) / nullif(count(distinct customer_id), 0),
        2
    ) as revenue_per_customer

from {{ ref('fct_sales') }}

group by customer_segment
