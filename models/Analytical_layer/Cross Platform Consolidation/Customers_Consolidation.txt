{{ config(
    materialized='table'
)}}

select *, 'Amazon' as Platform
from {{ ref('Amazon_Shipments') }}

union all

select *, 'Shopify' as Platform
from {{ ref('Shopify_Customers') }}

