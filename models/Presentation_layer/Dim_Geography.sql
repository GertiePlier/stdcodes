{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('City', 'State', 'PINCODE', 'Country') }} AS GeographyKey,
City, 
State, 
PINCODE, 
Country 
from {{ ref('Orders_Consolidation') }}