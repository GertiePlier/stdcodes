{{ config(
    materialized='table'
)}}

with cte as 
(
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `asinwiserdatondw`.asinwiser_raw_data.INFORMATION_SCHEMA.TABLES 
where lower(table_name) like '%fulfilledshipments%' 
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
         case 
        when sales_channel = 'Amazon.de' then 'Germany'
        when sales_channel = 'Amazon.es' then 'Spain'
        when sales_channel = 'Amazon.fr' then 'France'
        when sales_channel = 'Amazon.it' then 'Italy'
        when sales_channel = 'Amazon.nl' then 'Netherlands'
        when sales_channel = 'Amazon.co.uk' then 'UK'
        else sales_channel end as SalesChannel, 
        ship_city,
	  bill_city,
        case when purchase_date = '' then null
        else cast(DATETIME_ADD(cast(purchase_date as date), INTERVAL -7 HOUR ) as Date) end as Date,
        date(CAST(estimated_arrival_date as timestamp)) EstimatedArrivalDate,
        buyer_email BuyerEmail,
        sku as SKU,
        amazon_order_id as Order_Id,
        product_name as Product,
        cast(quantity_shipped as int) as ShippedUnits,
        cast(item_price as numeric) as ItemPrice,
        cast(item_promotion_discount as numeric) as ItemPromotionalDiscount,
        cast(ship_promotion_discount as numeric) as ShippingPromotionalDiscount,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        DENSE_RANK() OVER (PARTITION BY amazon_order_id, sku
        order by _daton_batch_runtime desc) row_num
        from {{i}})
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}

),

cte2 as(
    Select OrderId, SKU, sum(ReturnedUnits) as ReturnedUnits
    From {{ref('Amazon_Returns_Consolidated')}}
    Group by OrderID,SKU
),

final as (
select cte.*, 
cte2.ReturnedUnits ,
Dense_Rank() OVER (PARTITION BY cte.Order_ID, cte.SKU order by _daton_batch_runtime desc) row_num
from cte left join cte2
on cte.Order_ID = cte2.OrderID
and cte.SKU = cte2.SKU
)

select * except(row_num),
CASE when returnedunits is not null then ItemPrice else 0 end as ReturnItemPrice,
CASE when returnedunits is not null then ItemPromotionalDiscount else 0 end as ReturnItemPromotionalDiscount,
CASE when returnedunits is not null then ShippingPromotionalDiscount else 0 end as ReturnShippingPromotionalDiscount 
from final where row_num = 1
