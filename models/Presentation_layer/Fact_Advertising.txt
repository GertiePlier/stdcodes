{{ config(
    materialized='table'
)}}

select 
Date,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
{{ dbt_utils.surrogate_key('AdType') }} AS AdTypeKey,
{{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
{{ dbt_utils.surrogate_key('Campaign', 'CampaignId') }} AS CampaignKey,
{{ dbt_utils.surrogate_key('Adgroup') }} AS AdgroupKey,
{{ dbt_utils.surrogate_key('ASIN') }} AS ASINKey,  
{{ dbt_utils.surrogate_key('SKU') }} AS SKUKey,
{{ dbt_utils.surrogate_key('Product') }} AS ProductKey,
Impressions,
Clicks,
Spend as AdSpend,
Currency,
Conversions,
UnitsSold,
AdSales
from {{ ref('Advertising_Consolidation') }}

