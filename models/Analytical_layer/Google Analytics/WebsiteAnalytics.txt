{{ config(
    materialized='table'
)}}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `sixth-pager-341010`.Foundation_Layer_Test.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%website_analytics%' 
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
        EndDate as Date,
        D_ga_pageTitle as Page,
        D_ga_sourceMedium as Channel,
        D_ga_deviceCategory as DeviceCategory,
        D_ga_shoppingStage as ShoppingStage,
        M_ga_sessions as Sessions,
        M_ga_bounces as Bounces,
        M_ga_uniquePurchases as UniquePurchases,
        M_ga_transactionRevenue as ProductSales,
        DENSE_RANK() OVER (PARTITION BY D_ga_date
        order by _daton_batch_runtime desc) row_num
        from {{i}})
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}