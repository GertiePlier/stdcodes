{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('SKU') }} AS SKUKey,
SKU
from {{ ref('Orders_Consolidation') }}

