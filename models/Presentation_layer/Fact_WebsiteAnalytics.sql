{{ config(
    materialized='table'
)}}

select 
Date,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
Date,
Page
Channel,
DeviceCategory,
ShoppingStage,
Sessions,
Bounces,
UniquePurchases,
ProductSales
from {{ ref('WebsiteAnalytics') }}