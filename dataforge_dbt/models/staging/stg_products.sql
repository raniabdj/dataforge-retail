
{{ config(materialized='view') }}

select
    cast(product_id as varchar) as product_id,
    trim(category) as category,
    try_cast(unit_cost as decimal(18, 2)) as unit_cost
from {{ ref('dim_product') }}
