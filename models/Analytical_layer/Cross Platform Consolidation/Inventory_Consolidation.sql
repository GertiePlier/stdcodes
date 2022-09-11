{{ config(
    materialized='table'
)}}

select *, 'Amazon' as Platform
from {{ ref('AmazonInventoryHealth') }}

union all

select *, 'Shopify' as Platform
from {{ ref('Shopify_Inventory') }}