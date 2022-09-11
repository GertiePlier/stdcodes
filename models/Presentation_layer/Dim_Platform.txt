{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('Platform') }} AS PlatformKey,
Brand
from {{ ref('Orders_Consolidation') }}

