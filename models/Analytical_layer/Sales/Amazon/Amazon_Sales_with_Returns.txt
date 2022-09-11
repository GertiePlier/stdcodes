{{ config(
    materialized='table'
)}}


with cte as (

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `asinwiserdatondw`.asinwiser_raw_data.INFORMATION_SCHEMA.TABLES  
where lower(table_name) like '%flatfileallordersreportbylastupdate' 
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
        select 'Upstyle' as Brand,
        cast(ReportstartDate as DATE) ReportStartDate, 
        cast(ReportendDate as DATE)ReportEndDate, 
        amazon_order_id as OrderID, 
        cast(DATETIME_ADD(cast(purchase_date as date), INTERVAL -7 HOUR ) as Date) Date,
        order_status as Status, 
        fulfillment_channel as Fulfillment, 
        case 
        when sales_channel = 'Amazon.de' then 'Germany'
        when sales_channel = 'Amazon.es' then 'Spain'
        when sales_channel = 'Amazon.co.uk' then 'UK'
        else sales_channel end as SalesChannel, 
        product_name as Product, 
        sku as SKU, 
        asin as ASIN, 
        item_status as ItemStatus, 
        cast(quantity as int) as UnitsSold, 
        currency as Currency, 
        cast(item_price as FLOAT64) as ItemPrice, 
        cast(item_tax as int) ItemTax, 
        cast(shipping_price as int) ShippingPrice, 
        cast(shipping_tax as int) ShippingTax, 
        cast(gift_wrap_price as int) GiftwrapPrice,
        cast(gift_wrap_tax as int) GiftwrapTax, 
        cast(item_promotion_discount as int) ItemPromotionalDiscount, 
        cast(ship_promotion_discount as int) ShippingPromotionalDiscount,
        ship_city as City, 
        ship_state as State, 
        cast(ship_postal_code as string) PINCODE, 
        ship_country as Country,   
        cast(is_replacement_order as string) ReplacementOrder,
        cast(original_order_id as string) as OriginalOrderID, 
        _daton_user_id, 
        _daton_batch_runtime, 
        _daton_batch_id, 
        Dense_Rank() OVER (PARTITION BY amazon_order_id, asin order by _daton_batch_runtime desc) row_num
    from {{i}}) where row_num = 1 and Status <> 'Cancelled' and Status <> 'Pending' and salesChannel <> 'Non-Amazon'
    {% if not loop.last %} union all {% endif %}
{% endfor %}

),

cte2 as(
    Select OrderId, ASIN, sum(ReturnedUnits) as ReturnedUnits
    From {{ref('Amazon_Returns_Consolidated')}}
    Group by OrderID,ASIN
),

final as (
select cte.*, 
cte2.ReturnedUnits ,
Dense_Rank() OVER (PARTITION BY cte.OrderID, cte.asin order by _daton_batch_runtime desc) row_num
from cte left join cte2
on cte.OrderID = cte2.OrderID
and cte.ASIN = cte2.ASIN
)

select * except(row_num),
CASE when returnedunits is not null then ItemPrice else 0 end as ReturnItemPrice,
CASE when returnedunits is not null then ItemPromotionalDiscount else 0 end as ReturnItemPromotionalDiscount,
CASE when returnedunits is not null then ShippingPromotionalDiscount else 0 end as ReturnShippingPromotionalDiscount 
from final where row_num = 1

