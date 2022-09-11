{{ config(
    materialized='table'
)}}

select
SnapshotDate,
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
{{ dbt_utils.surrogate_key('Country') }} AS MarketplaceKey,
{{ dbt_utils.surrogate_key('SKU') }} AS SKUKey,
{{ dbt_utils.surrogate_key('ASIN') }} AS ASINKey,
{{ dbt_utils.surrogate_key('Product') }} AS ProductKey,
{{ dbt_utils.surrogate_key('Platform') }} AS PlatformKey,
Disposition,
AvailableQuantity,
inv_age_0_to_90_days, 		
inv_age_181_to_270_days,		
inv_age_271_to_365_days,			
inv_age_365_plus_days,		
Currency,				
units_shipped_t7,		
units_shipped_t30,			
units_shipped_t60,			
units_shipped_t90,		
Alert,		
YourPrice,		
SalesPrice,			
RecommededAction,		
HealthyInventoryLevel,		
RecommendedSalesPrice,			
ProductGroup,		
SalesRank,		
DOH

from {{ ref('Inventory_Consolidation') }}


