  {{ config(
    materialized='table'
)}}
        with cte as (
        select 
        Brand,
        Country,
	  Currency,
        Date,
        SearchTerm,
        sum(Spend) AdSpend,
        sum(AdSales) AdSales,
        sum(conversions) Conversions,
        sum(clicks) Clicks,
        sum(impressions) Impressions
        from {{ ref('Keywords_Consolidated') }}
        group by 1,2,3,4,5

        )

        select *, 
        case when AdSales = 0 and AdSpend != 0 then 'Wasted Spend'
        when AdSales < AdSpend then 'Concerned Spend'
        else 'Safe Spend' end as SpendType
        from cte