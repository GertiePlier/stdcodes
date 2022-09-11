{{ config(
    materialized='table'
)}}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `sparkel-warehouse`.Sparkel.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%flatfilereturnsreportbyreturndate' 
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
		ASIN,
		Merchant_SKU as  SKU,
        cast(ReportstartDate as DATE) ReportStartDate,
      	cast(ReportendDate as DATE) ReportEndDate,  
		Order_ID as OrderID,
		'Merchant' as Fulfillment,
		cast(Return_request_date as DATE) as ReturnRequestDate,
		Return_request_status as ReturnStatus,
		Return_Reason as ReturnReason,
		Return_type as ReturnType,
		Return_Quantity as ReturnedUnits,
		marketplaceName as Marketplace, 
		_daton_batch_runtime,
        Dense_Rank() OVER (PARTITION BY Order_id, asin order by _daton_batch_runtime desc) row_num
	    from {{i}}) where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

final as (
Select *, Dense_Rank() OVER (PARTITION BY OrderID, asin order by _daton_batch_runtime desc) row_num 
from cte)

Select * except(row_num,_daton_batch_runtime)
From final
Where row_num = 1