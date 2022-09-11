  {{ config(
    materialized='table'
)}}
    
        select 
        {{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
        {{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
        Date,
        Conversions,
        Clicks, 
        Impressions,
        KeywordText,
        AdSpend,
        AdSales,
        SpendType
        from {{ ref('SpendAnalysisKeyword') }}