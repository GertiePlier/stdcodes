{{ config(
    materialized='table'
)}}


select 
distinct {{ dbt_utils.surrogate_key('ASIN') }} AS ASINKey,
ASIN
from 
(
select distinct ASIN
from {{ ref('Orders_Consolidation') }}

union all

select distinct ASIN
from {{ ref('Inventory_Consolidation') }}
)
