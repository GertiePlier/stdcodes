{{ config(
    materialized='table')
}}

select
distinct {{ dbt_utils.surrogate_key('AdType') }} AS AdTypeKey,
AdType
from {{ ref('Advertising_Consolidation') }}