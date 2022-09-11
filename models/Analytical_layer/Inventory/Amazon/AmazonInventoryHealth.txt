{{ config(
    materialized='table'
)}}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `sixth-pager-341010`.Foundation_Layer_Test.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%fbamanageinventoryhealthr%' 
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
    SELECT * except(row_num) 
    From (
        select '{{id}}' as Brand,
        marketplaceid as Country,
        snapshot_date as SnapshotDate,
        sku as SKU,
        asin as ASIN,
        product_name as Product,
        condition as Disposition,
        available as AvailableQuantity,
        inv_age_0_to_90_days, 		
        inv_age_181_to_270_days,		
        inv_age_271_to_365_days,			
        inv_age_365_plus_days,		
        currency as Currency,				
        units_shipped_t7,		
        units_shipped_t30,			
        units_shipped_t60,			
        units_shipped_t90,		
        alert as Alert,		
        your_price as YourPrice,		
        sales_price	SalesPrice,			
        recommended_action as RecommededAction,		
        healthy_inventory_level as HealthyInventoryLevel,		
        recommended_sales_price as RecommendedSalesPrice,			
        product_group as ProductGroup,		
        sales_rank as SalesRank,		
        days_of_supply as DOH,
        DENSE_RANK() OVER (PARTITION BY sku,
        date(snapshot_date) order by _daton_batch_runtime desc) row_num
        from {{i}})
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}