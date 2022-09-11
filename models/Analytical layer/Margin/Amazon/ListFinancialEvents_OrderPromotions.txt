{{ config(
    materialized='table'
)}}


with unnested_shipmenteventlist as (
{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `asinwiserdatondw`.asinwiser_raw_data.INFORMATION_SCHEMA.TABLES
where lower(table_name) like '%listfinancialevents%' 
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
FROM (
    select 
    '{{id}}' as Brand,
    date(ShipmentEventlist.posteddate) as Date,
    ShipmentEventlist.amazonorderid as OrderID,
    ShipmentEventlist.marketplacename as Marketplace,
    ShipmentEventlist.ShipmentItemList,
    _daton_batch_runtime 
    FROM  {{i}}
    cross join unnest(ShipmentEventlist) ShipmentEventlist)
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

ShipmentItemList as (

        select 
        Brand, 
        Date,
        OrderID,
        case 
        when Marketplace = 'Amazon.de' then 'Germany'
        when Marketplace = 'Amazon.es' then 'Spain'
        when Marketplace = 'Amazon.fr' then 'France'
        when Marketplace = 'Amazon.it' then 'Italy'
        when Marketplace = 'Amazon.nl' then 'Netherlands'
        when Marketplace = 'Amazon.co.uk' or Marketplace = 'SI UK Prod Marketplace' then 'UK'
        else Marketplace end as Marketplace,
        ShipmentItemList.sellerSKU as SKU,
        ShipmentItemList.quantityshipped as UnitsSold,
        ShipmentItemList.PromotionList,
        _daton_batch_runtime
        from unnested_shipmenteventlist
        cross join unnest(ShipmentItemList) ShipmentItemList
        
),

PromotionList as (

        select 
        Brand,
        Date, 
        OrderID,
        Marketplace,
        SKU,
        UnitsSold,
        PromotionList.PromotionAmount,
        PromotionList.PromotionType,
        _daton_batch_runtime
        from ShipmentItemList
        cross join unnest(PromotionList) PromotionList

),

PromotionAmount as (

        select 
        Brand, 
        Date,
        'Promotion' as AmountType,
        'Order' as TransactionType,
        OrderID,
        Marketplace,
        SKU,
        UnitsSold,
        PromotionType as ChargeType,
        PromotionAmount.CurrencyCode as Currency,
        PromotionAmount.CurrencyAmount as Amount,
        _daton_batch_runtime
        from PromotionList
        cross join unnest(PromotionAmount) PromotionAmount

),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY Date, SKU, OrderID, ChargeType, AmountType, TransactionType order by _daton_batch_runtime desc) row_num
from PromotionAmount
)

select * except(row_num, _daton_batch_runtime)
from dedup 
where row_num = 1