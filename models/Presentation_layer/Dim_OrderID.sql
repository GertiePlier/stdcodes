{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('OrderID') }} AS OrderIDKey,
OrderId
from {{ ref('Advertising_Consolidation') }}