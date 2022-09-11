{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('SalesChannel') }} AS MarketplaceKey, 
SalesChannel
from {{ ref('Orders_Consolidation') }}