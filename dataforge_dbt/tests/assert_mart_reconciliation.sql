
with source_totals as (
    select
        count(*) as total_orders,
        sum(revenue) as total_revenue,
        sum(estimated_profit) as total_profit
    from {{ ref('fct_sales') }}
),

mart_totals as (
    select
        'daily_sales' as mart_name,
        sum(total_orders) as total_orders,
        sum(total_revenue) as total_revenue,
        sum(estimated_profit) as total_profit
    from {{ ref('mart_daily_sales') }}

    union all

    select
        'category_performance',
        sum(total_orders),
        sum(total_revenue),
        sum(estimated_profit)
    from {{ ref('mart_category_performance') }}

    union all

    select
        'customer_segments',
        sum(total_orders),
        sum(total_revenue),
        sum(estimated_profit)
    from {{ ref('mart_customer_segments') }}
)

select
    m.mart_name,
    m.total_orders,
    m.total_revenue,
    m.total_profit
from mart_totals m
cross join source_totals s
where
    m.total_orders is distinct from s.total_orders
    or abs(m.total_revenue - s.total_revenue) > 0.01
    or abs(m.total_profit - s.total_profit) > 0.01
    or m.total_revenue is null
    or m.total_profit is null
