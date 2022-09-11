{{ config(
    materialized='table'
)}}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `sparkel-warehouse`.Sparkel.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%fbareturnsreport' 
{% endset %}  



{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

with cte as (
{% for i in results_list %}
    {% set id =i.split('.')[2].split('_')[0] %}
    SELECT * except(row_num) 
    From (
        select '{{id}}' as Brand,
		SKU,
		ASIN,
		license_plate_number,
		fnsku,
		_daton_batch_runtime,
        cast(ReportstartDate as DATE) ReportStartDate,
      	cast(ReportendDate as DATE) ReportEndDate,  
		order_id as OrderID,
		'Amazon' as Fulfillment,
		cast(return_date as DATE) as ReturnRequestDate,
		status as ReturnStatus,
		reason as ReturnReason,
		'NA' as ReturnType,
		quantity as ReturnedUnits,
		marketplaceName as Marketplace, 
        Dense_Rank() OVER (PARTITION BY return_date, order_id, sku, asin, fnsku, license_plate_number order by _daton_batch_runtime desc) row_num
	    from {{i}}) where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

final as (
Select *, Dense_Rank() OVER (PARTITION BY OrderID, sku, asin, fnsku, license_plate_number order by _daton_batch_runtime desc) row_num 
from cte)

Select * except(fnsku, license_plate_number,_daton_batch_runtime,row_num)
From final
Where row_num = 1

