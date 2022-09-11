{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('Adgroup') }} AS AdgroupKey,
Adgroup
from {{ ref('Advertising_Consolidation') }}