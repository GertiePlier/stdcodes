{{ config(
    materialized='table'
)}}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from  `sparkel-warehouse`.Sparkel.INFORMATION_SCHEMA.TABLES
where lower(table_name) like '%shopify_us_transactions%'
or lower(table_name) like '%shopify_ca_transactions%'
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
        select 'Sparkel' as Brand,
        case 
        when currency = 'USD' then 'United States'
        when currency = 'CAD' then 'Canada'
        else currency end as Country,
        id as TransactionID,
        type as Type,
        currency as Currency,
        CAST(amount AS NUMERIC) as Amount,
        CAST(fee AS NUMERIC) Fees,
        source_type as SourceType,
        cast(source_order_id as string) as OrderID,
        DENSE_RANK() OVER (PARTITION BY id, type, source_id, source_order_id order by _daton_batch_runtime desc) row_num
        from {{i}})
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}