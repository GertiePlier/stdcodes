{{ config(
    materialized='table'
)}}

with cte as (
with unnested_line_items as (
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `kos-data-warehouse`.KOS.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%shopify_us_orders%' 
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
        billing_address.country as SalesChannel,
        billing_address.city as ship_city,
        null as bill_city,
        cast(created_at as Date) Date,
        null as EstimatedArrivalDate,
        email as BuyerEmail,
        line_items.sku as SKU,
        cast(OrderID as string) as OrderID, 
        line_items.title as Product,
        line_items.quantity as ShippedUnits,
        cast(line_items.price*line_items.quantity as float64) as ItemPrice,
        cast(line_items.total_discount as numeric) ItemPromotionalDiscount,
        cast(null as numeric) as ShippingPromotionalDiscount,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        from unnested_line_items
        cross join unnest(line_items) line_items
        cross join unnest(billing_address) billing_address
),

cte2 as(
    Select OrderId, SKU, sum(ReturnedUnits) as ReturnedUnits
    From {{ref('Shopify_Refunds')}}
    Group by OrderID,SKU
),

final as (
select cte.*, 
cte2.ReturnedUnits ,
Dense_Rank() OVER (PARTITION BY cte.OrderID, cte.SKU order by _daton_batch_runtime desc) row_num
from cte left join cte2
on cte.OrderID = cte2.OrderID
and cte.SKU = cte2.SKU
)

select * except(row_num),
CASE when returnedunits is not null then ItemPrice else 0 end as ReturnItemPrice,
CASE when returnedunits is not null then ItemPromotionalDiscount else 0 end as ReturnItemPromotionalDiscount,
CASE when returnedunits is not null then ShippingPromotionalDiscount else 0 end as ReturnShippingPromotionalDiscount 
from final where row_num = 1
