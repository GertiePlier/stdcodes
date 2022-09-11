{{ config(
    materialized='table'
)}}

select
OrderID, 
Date,
{{ dbt_utils.surrogate_key('OrderID') }} AS OrderIDKey,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
{{ dbt_utils.surrogate_key('Fulfillment') }} AS FulfillmentKey,
{{ dbt_utils.surrogate_key('SalesChannel') }} AS MarketplaceKey,
{{ dbt_utils.surrogate_key('Product') }} AS ProductKey,  
{{ dbt_utils.surrogate_key('ASIN') }} AS ASINKey,  
{{ dbt_utils.surrogate_key('SKU') }} AS SKUKey,
{{ dbt_utils.surrogate_key('City', 'State', 'PINCODE', 'Country') }} AS GeographyKey,
{{ dbt_utils.surrogate_key('Platform') }} AS PlatformKey,
ReturnedUnits, 
UnitsSold, 
Currency, 
ItemPrice as ProductSales, 
ItemTax, 
ShippingPrice, 
ShippingTax, 
GiftwrapPrice,
GiftwrapTax, 
ItemPromotionalDiscount, 
ShippingPromotionalDiscount

from {{ ref('Orders_Consolidation') }}


