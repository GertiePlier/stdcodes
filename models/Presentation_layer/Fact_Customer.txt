{{ config(
    materialized='table'
)}}

select
	  {{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
        {{ dbt_utils.surrogate_key('SalesChannel') }} AS MarketplaceKey,
        {{ dbt_utils.surrogate_key('SKU') }} AS SKUKey, 
	  {{ dbt_utils.surrogate_key('Product') }} AS ProductKey,
        {{ dbt_utils.surrogate_key('Order_Id') }} AS OrderIDKey,
        {{ dbt_utils.surrogate_key('BuyerEmail') }} AS BuyerEmailKey,
	  {{ dbt_utils.surrogate_key('Platform') }} AS PlatformKey,
        Date,
        ship_city,
	  bill_city,
        EstimatedArrivalDate,
        BuyerEmail,
        ShippedUnits,
        ItemPrice as ProductSales,
        ReturnedUnits,
        ReturnItemPrice,
        ItemPromotionDiscount,
        ShippingPromotionDiscount


	  from {{ ref('Customers_Consolidation') }}


