
{{ config(materialized='view') }}

select
    cast(order_id as varchar) as order_id,
    cast(event_id as varchar) as event_id,
    cast(customer_id as varchar) as customer_id,
    cast(product_id as varchar) as product_id,
    try_cast(order_date as date) as order_date,
    lower(trim(status)) as order_status,
    try_cast(revenue as decimal(18, 2)) as revenue,
    try_cast(quantity as integer) as quantity
from {{ ref('fact_orders') }}
