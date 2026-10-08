
{{ config(materialized='table') }}

select
    order_date,
    count(*) as total_orders,
    sum(quantity) as total_units_sold,
    round(sum(revenue), 2) as total_revenue,
    round(sum(total_cost), 2) as total_cost,
    round(sum(estimated_profit), 2) as estimated_profit,
    round(avg(revenue), 2) as average_order_value

from {{ ref('fct_sales') }}

group by order_date
