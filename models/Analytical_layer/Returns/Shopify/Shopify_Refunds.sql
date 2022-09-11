{{ config(
    materialized='table'
)}}



with unnested_line_items as (
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `telosdatondw`.telos_raw_data.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%shopify_uk_orders%' 
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

SELECT * except(row_number)
FROM (
    select 
    '{{id}}' as Brand,
    case 
    when currency = 'GBP' then 'United Kingdom'
    when currency = 'USD' then 'United States'
    else currency end as Marketplace,
    *, 
    ROW_NUMBER() OVER  (PARTITION BY order_number
    order by updated_at desc) row_number 
    FROM  {{i}})
    where row_number = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),




shopify_refunds as (

        select
        Brand,
        Marketplace,
        refunds.order_id as OrderID,
        refunds.refund_line_items,
        cast(refunds.created_at as Date) Date,
        from unnested_line_items
        cross join unnest(refunds) refunds
        
),

shopify_refund_line_item as (

        select
        Brand,
        Marketplace,
        OrderID,
        Date,
        refund_line_items.line_item,
        from shopify_refunds
        cross join unnest(refund_line_items) refund_line_items      
),

shopify_refund_line_item_line_item as (

        select
        Brand,  
        line_item.sku as SKU,
        'NA' as  ASIN,
        cast(null as DATE) as ReportStartDate,
        cast(null as DATE) as ReportEndDate,
        OrderID,
        line_item.fulfillment_service as Fulfillment,
        Date as ReturnRequestDate,
        'NA' as ReturnStatus,
        'NA' as ReturnReason,
        'NA' as ReturnType,
        line_item.quantity as ReturnedUnits,
        Marketplace
        from shopify_refund_line_item
        cross join unnest(line_item) line_item      
)

select * 
from shopify_refund_line_item_line_item

