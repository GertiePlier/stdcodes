  {{ config(
    materialized='table'
)}}
    
        select 
        {{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
        {{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
        {{ dbt_utils.surrogate_key('Campaign', 'CampaignId') }} AS CampaignKey,
        Date,
        Conversions,
        Clicks, 
        Impressions,
        AdSpend,
        AdSales,
        SpendType
        from {{ ref('SpendAnalysisCampaign') }}