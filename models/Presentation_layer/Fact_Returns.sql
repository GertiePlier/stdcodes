{{ config(
    materialized='table'
)}}

select
OrderID, 
Date,
{{ dbt_utils.surrogate_key('OrderID') }} AS OrderIDKey,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
{{ dbt_utils.surrogate_key('Fulfillment') }} AS FulfillmentKey,
{{ dbt_utils.surrogate_key('Marketplace') }} AS MarketplaceKey,
{{ dbt_utils.surrogate_key('ASIN') }} AS ASINKey,  
{{ dbt_utils.surrogate_key('SKU') }} AS SKUKey,
{{ dbt_utils.surrogate_key('Platform') }} AS PlatformKey,
Date as ReturnRequestDate,
ReturnStatus,
ReturnReason,
ReturnType,
ReturnedUnits

from {{ ref('Returns_Consolidation') }}


