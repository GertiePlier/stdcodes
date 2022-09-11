{{ config(
    materialized='table'
)}}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `xp-strategy-data-warehouse`.XPStrategy.INFORMATION_SCHEMA.TABLES
where lower(table_name) like '%la4ve%sponsoredbrands_placementcampaignsreport' 
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
        'Sponsored Brands' as AdType,
        countryName as Country,
        accountName as Account,
        accountId as AccountID,
        reportDate as Date,
        placement as Placement,
        CAST(null as int) CampaignId,
        CAST(impressions as int) Impressions,
        CAST(clicks as int) Clicks,
        CAST(cost as int) as Spend,
        CASE
        WHEN countryName = 'Canada' then 'CAD'
        WHEN countryName = 'United States' then 'USD' ELSE 'USD' END as Currency,
        'NA' as ASIN,
        'NA' as SKU,
        CAST(attributedConversions14d as int) Conversions,
        CAST(unitsSold14d as int) UnitsSold,
        CAST(attributedSales14d as int) AdSales,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        Campaignname as Campaign,
        DENSE_RANK() OVER (PARTITION BY reportDate,
        Campaignname, placement order by _daton_batch_runtime desc) row_num
        from {{i}})
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}