{{ config(
    materialized='table'
)}}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `upscalio-elt`.bq_upscalio.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%salesandtrafficreportbychildasin%' 
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
    SELECT * 
        FROM 
        (
select 
'{{id}}' as Brand,
date,
marketplacename as Country,
parentAsin,
childASIN as asin,
cast(mobileAppSessions as float64) as MobileSessions,
cast(browserSessions as float64) as BrowserSessions,
cast(sessions as float64) as Sessions,
cast(browserPageViews as float64) as BrowserPageViews,
cast(pageViews as float64) as PageViews,    
cast(mobileAppPageViews as float64) as MobilePageViews,
cast(buyBoxPercentage as float64)/100 as Featured_Offer_Buy_Box_Percentage,
cast(unitsOrdered as float64) as Units_Ordered,
cast(unitSessionPercentage as float64)/100 as Unit_Session_Percentage,
cast(totalOrderItems as float64) as Total_Order_Items,
cast(orderedProductSales_amount as float64) as Ordered_Product_Sales,
cast(_daton_batch_runtime as numeric) _daton_batch_runtime,
DENSE_RANK() OVER (PARTITION BY '{{id}}', date, parentAsin, childASIN order by _daton_batch_runtime desc) as row_num
from {{i}}) where row_num = 1
{% if not loop.last %} union all {% endif %}
{% endfor %}
