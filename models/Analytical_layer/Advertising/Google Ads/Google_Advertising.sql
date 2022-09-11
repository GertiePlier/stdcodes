{{ config(
    materialized='table'
)}} 

{% set table_name_query %}

select
concat(table_catalog,'.',table_schema,'.',table_name) as tables
from  `sparkel-warehouse`.Sparkel.INFORMATION_SCHEMA.TABLES 

where lower(table_name) like '%_google_ads_campaign' 

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
    select 
    '{{id}}' as Brand,
    'Google Ads' as AdType,
    'United States' as Country,
    'NA' as Account,
    'NA' as AccountID,
    date(segments_date) as Date,
    campaign_name as Campaign,
    'NA' as Adgroup,
    cast(campaign_id as int) CampaignID,
    cast(metrics_impressions as int) Impressions,
    cast(metrics_clicks as int) Clicks,
    cast(metrics_cost_micros/1000000 as int) Spend,
    customer_currency_code as Currency,
    'NA' as ASIN,
    'NA' as SKU,
    cast(metrics_conversions as int) as Conversions,
    cast(null as int) UnitsSold,
    cast(metrics_conversions_value as int) as AdSales,
    _daton_user_id,
    _daton_batch_runtime,
    _daton_batch_id,
    CAST(null as string) as Product,
    DENSE_RANK() OVER (PARTITION BY campaign_id, date(segments_date) order by _daton_batch_runtime desc) row_num
    from {{i}}) AS {{id}}  
    where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
