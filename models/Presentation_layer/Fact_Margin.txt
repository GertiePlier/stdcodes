 {{ config(
    materialized='table'
)}}

        select 
        {{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
        {{ dbt_utils.surrogate_key('Marketplace') }} AS MarketplaceKey, 
        {{ dbt_utils.surrogate_key('SKU') }} AS SKUKey, 
	  {{ dbt_utils.surrogate_key('Platform') }} AS PlatformKey, 
        Date,
        AmountType,
        TransactionType,
        OrderID,
        UnitsSold,
        ChargeType,
        Currency,
        Amount
        from {{ ref('Margin_Consolidation') }}