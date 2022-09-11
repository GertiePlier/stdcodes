{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
Brand
from {{ ref('Orders_Consolidation') }}

