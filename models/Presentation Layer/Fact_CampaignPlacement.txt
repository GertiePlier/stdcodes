{{ config(
    materialized='table'
)}}

select 
Date,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
{{ dbt_utils.surrogate_key('AdType') }} AS AdTypeKey,
{{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
{{ dbt_utils.surrogate_key('Campaign', 'CampaignId') }} AS CampaignKey,
{{ dbt_utils.surrogate_key('ASIN') }} AS ASINKey,  
{{ dbt_utils.surrogate_key('SKU') }} AS SKUKey,
Placement,
Impressions,
Clicks,
Spend as AdSpend,
Currency,
Conversions,
UnitsSold,
AdSales,
Top_of_Search_on_Amazon_Spend,
Detail_Page_on_Amazon_Spend,
Other_on_Amazon_Spend,
Other_Placements_Spend,
Top_of_Search_on_Amazon_AdSales,
Detail_Page_on_Amazon_AdSales,
Other_on_Amazon_AdSales,
Other_Placements_AdSales
from {{ ref('Placement_Consolidated') }}

