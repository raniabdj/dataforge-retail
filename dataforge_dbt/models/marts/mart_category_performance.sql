
{{ config(materialized='table') }}

select
    category,
    count(*) as total_orders,
    sum(quantity) as units_sold,
    round(sum(revenue), 2) as total_revenue,
    round(sum(total_cost), 2) as total_cost,
    round(sum(estimated_profit), 2) as estimated_profit,
    round(
        100.0 * sum(estimated_profit)
        / nullif(sum(revenue), 0),
        2
    ) as estimated_profit_margin_pct

from {{ ref('fct_sales') }}

group by category
