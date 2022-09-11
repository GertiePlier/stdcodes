{{ config(
    materialized='table'
)}}



with unnested_refundeventlist as (
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
    date(RefundEventlist.posteddate) as Date,
    RefundEventlist.amazonorderid as OrderID,
    RefundEventlist.marketplacename as Marketplace,
    RefundEventlist.ShipmentItemAdjustmentList,
    _daton_batch_runtime 
    FROM  {{i}}
    cross join unnest(RefundEventlist) RefundEventlist)
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

ShipmentItemAdjustmentList as (

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
        ShipmentItemAdjustmentList.sellerSKU as SKU,
        ShipmentItemAdjustmentList.quantityshipped as UnitsSold,
        ShipmentItemAdjustmentList.ItemChargeAdjustmentList,
        _daton_batch_runtime
        from unnested_refundeventlist
        cross join unnest(ShipmentItemAdjustmentList) ShipmentItemAdjustmentList
        
),

ItemChargeAdjustmentList as (

        select 
        Brand,
        Date, 
        OrderID,
        Marketplace,
        SKU,
        UnitsSold,
        ItemChargeAdjustmentList.ChargeType,
        ItemChargeAdjustmentList.ChargeAmount,
        _daton_batch_runtime
        from ShipmentItemAdjustmentList
        cross join unnest(ItemChargeAdjustmentList) ItemChargeAdjustmentList

),

ChargeAmount as (

        select 
        Brand,
        Date,
        'Revenue' as AmountType,
        'Refund' as TransactionType,
        OrderID,
        Marketplace,
        SKU,
        UnitsSold,
        ChargeType,
        ChargeAmount.CurrencyCode as Currency,
        ChargeAmount.CurrencyAmount as Amount,
        _daton_batch_runtime
        from ItemChargeAdjustmentList
        cross join unnest(ChargeAmount) ChargeAmount

),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY Date, SKU, OrderID, ChargeType, AmountType, TransactionType order by _daton_batch_runtime desc) row_num
from ChargeAmount
)

select * except(row_num, _daton_batch_runtime)
from dedup 
where row_num = 1