{{ config(
    materialized='table'
)}}

select *, 'Amazon' as Platform
from {{ ref('Sales_with_Returns') }}

union all

select *, 'Shopify' as Platform
from {{ ref('Orders_Shopify') }}