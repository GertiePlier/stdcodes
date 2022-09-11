{{ config(
    materialized='table'
)}}

select 
Date,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
Date,
Gender,
AgeBracket,
Sessions,
Bounces,
Transactions,
ProductSales
from {{ ref('GenderBifurcation') }}

