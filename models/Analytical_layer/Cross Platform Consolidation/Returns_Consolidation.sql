{{ config(
    materialized='table'
)}}

select *, 'Amazon' as Platform
from {{ ref('Amazon_Returns_Consolidated') }}

union all

select *, 'Shopify' as Platform
from {{ ref('Refunds_Shopify') }}