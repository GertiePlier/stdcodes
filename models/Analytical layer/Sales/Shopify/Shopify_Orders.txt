{{ config(
    materialized='table'
)}}

with cte as (
with unnested_line_items as (
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `kos-data-warehouse`.KOS.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%shopify_%_orders' 
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
    '{{id}}'  as Brand,
    *,
    case 
    when currency = 'CA' then 'Canada'
    when currency = 'USD' then 'United States'
    else currency end as Marketplace, 
    id as OrderID,
    ROW_NUMBER() OVER  (PARTITION BY order_number
    order by updated_at desc) row_number 
    FROM  {{i}})
    where row_number = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

        select
        Brand,
        Cast(null as date) as ReportStartDate,
        Cast(null as date) as ReportEndDate,
        cast(OrderID as string) as OrderID, 
        cast(created_at as Date) Date,
        CASE when cancel_reason = 'null' then 'Shipping' 
        else 'Cancelled' end as Status,
        line_items.fulfillment_service as Fulfillment,
        Marketplace as SalesChannel,
        line_items.title as Product,
        line_items.sku as SKU,
        CAST(line_items.product_id as string) as ASIN,
        CASE when line_items.requires_shipping = True then 'Shipping' 
        else 'Others' end as ItemStatus,
        line_items.quantity as UnitsSold,
        'NA' as Currency,
        line_items.price*line_items.quantity as ItemPrice,
        cast(total_tax as numeric) as ItemTax,
        cast(null as int) as ShippingPrice,
        cast(null as int) as ShippingTax,
        cast(null as int) as GiftWrapPrice,
        cast(null as int) as GiftWrapTax,
        cast(line_items.total_discount as numeric) ItemPromotionalDiscount,
        cast(null as numeric) as ShippingPromotionalDiscount,
        cast(null as string) as City,
        cast(null as string) as State,
        cast(null as string) as PINCODE,
        cast(null as string) as Country,
        cast(null as string) as ReplacementOrder,
        cast(null as string) as OriginalOrderID,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        from unnested_line_items
        cross join unnest(line_items) line_items

       ),

       cte2 as(
    	 Select OrderId, SKU, sum(ReturnedUnits) as ReturnedUnits
       From {{ref('Refunds_Shopify')}}
       Group by OrderID,SKU
       ),

	final as (
	select cte.*, 
	cte2.ReturnedUnits ,
	Dense_Rank() OVER (PARTITION BY cte.OrderID, cte.sku order by _daton_batch_runtime desc) row_num
	from cte left join cte2
	on cte.OrderID = cte2.OrderID
	and cte.SKU = cte2.SKU