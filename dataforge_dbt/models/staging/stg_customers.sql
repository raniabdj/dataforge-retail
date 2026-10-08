
{{ config(materialized='view') }}

select
    cast(customer_id as varchar) as customer_id,
    trim(region) as region,
    trim(segment) as customer_segment
from {{ ref('dim_customer') }}
