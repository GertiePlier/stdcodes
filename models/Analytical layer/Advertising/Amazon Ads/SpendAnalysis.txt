  {{ config(
    materialized='table'
)}}
        with cte as (
        select 
        Brand,
        Country,
	  Currency,
        Date,
        Adgroup,
        Campaign,
        CampaignID,
        ASIN,
        SKU,
        sum(Spend) AdSpend,
        sum(AdSales) AdSales,
        sum(conversions) Conversions,
        sum(clicks) Clicks,
        sum(impressions) Impressions
        from {{ ref('Advertising_Consolidated') }}
        group by 1,2,3,4,5,6,7,8,9

         )

        select *, 
        case when AdSales = 0 and AdSpend != 0 then 'Wasted Spend'
        when AdSales < AdSpend then 'Concerned Spend'
        else 'Safe Spend' end as SpendType
        from cte