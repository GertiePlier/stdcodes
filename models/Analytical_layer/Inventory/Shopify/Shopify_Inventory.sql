{{ config(
    materialized='table'
)}}



with unnested_inventory_levels as (
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `sixth-pager-341010`.Foundation_Layer_Test.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%shopify%inventory%' 
{% endset %}  


{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[0] %}

SELECT  except(row_number)
FROM (
    select 
    '{{id}}' as Brand,
    'United States' as Country,
    , 
    inventory_levels.available as AvailableQuanitity,
    ROW_NUMBER() OVER  (PARTITION BY inventory_levels.inventory_item_id, inventory_levels.updated_at
    order by _daton_batch_runtime desc) row_number 
    FROM  {{i}}
    cross join unnest(inventory_levels) inventory_levels)
    where row_number = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

inventory_item as (

        select

        Brand,
        Country,
        date(inventory_item.created_at) as SnapshotDate,
        inventory_item.SKU,
        'NA' as ASIN,
        'NA' as Product,
        'NA' as Disposition,
        cast(AvailableQuanitity as int) AvailableQuanitity,
        null as inv_age_0_to_90_days, 		
        null as inv_age_181_to_270_days,		
        null as inv_age_271_to_365_days,			
        null as inv_age_365_plus_days,		
        'NA' as Currency,				
        null as units_shipped_t7,		
        null as units_shipped_t30,			
        null as units_shipped_t60,			
        null as units_shipped_t90,		
        'NA' as Alert,		
        null as YourPrice,		
        null as	SalesPrice,			
        'NA' as RecommededAction,		
        null as HealthyInventoryLevel,		
        null as RecommendedSalesPrice,			
        'NA' as ProductGroup,		
        null as SalesRank,		
        null as DOH
        
        from unnested_inventory_levels
        cross join unnest(inventory_item) inventory_item
        
)


select  
from inventory_item
