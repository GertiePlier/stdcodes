{{ config(
    materialized='table'
)}}

select *, 'Amazon' as Platform
from {{ ref('Margin_Amazon') }}

union all

select *, 'Shopify' as Platform
from {{ ref('Margin_Shopify') }}