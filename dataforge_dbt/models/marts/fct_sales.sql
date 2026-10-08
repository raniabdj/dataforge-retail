
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    o.order_id,
    o.event_id,
    o.order_date,
    o.order_status,
    o.customer_id,
    c.region,
    c.customer_segment,
    o.product_id,
    p.category,
    o.quantity,
    o.revenue,
    p.unit_cost,
    round(o.quantity * p.unit_cost, 2) as total_cost,
    round(o.revenue - (o.quantity * p.unit_cost), 2) as estimated_profit

from orders o
left join customers c
    on o.customer_id = c.customer_id
left join products p
    on o.product_id = p.product_id
