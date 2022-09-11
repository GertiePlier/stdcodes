{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('Product') }} AS ProductKey, 
Product
from {{ ref('Orders_Consolidation') }}
