  {{ config(
    materialized='table'
)}}
    
        select 
        {{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
        {{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
        {{ dbt_utils.surrogate_key('Campaign', 'CampaignId') }} AS CampaignKey,
        {{ dbt_utils.surrogate_key('ASIN') }} AS ASINKey, 
        {{ dbt_utils.surrogate_key('SKU') }} AS SKUKey, 
        {{ dbt_utils.surrogate_key('Adgroup') }} AS AdgroupKey, 
        Date,
        Conversions,
        Clicks, 
        Impressions,
        AdSpend,
        AdSales,
        SpendType
        from {{ ref('SpendAnalysis') }}