{{ config(
    materialized='table'
)}}

select
Date,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
{{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
{{ dbt_utils.surrogate_key('AdType') }} AS AdTypeKey,
{{ dbt_utils.surrogate_key('Campaign', 'CampaignId') }} AS CampaignKey,
MatchType,
Impressions,
Clicks,
Spend as AdSpend,
KeywordBid,
SearchTerm,
Conversions,
UnitsSold,
AdSales,
KeywordText

from {{ ref('Keywords_Consolidated') }}


