{{ config(
    materialized='table'
)}}

select 
Date,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
{{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
{{ dbt_utils.surrogate_key('asin') }} AS ASINKey,
Title,
Sessions,
PageViews,
FeaturedOfferBuyBoxPercentage,
UnitsOrdered,
OrderedProductSales,
TotalOrderItems

from {{ ref('Business_Report') }}

