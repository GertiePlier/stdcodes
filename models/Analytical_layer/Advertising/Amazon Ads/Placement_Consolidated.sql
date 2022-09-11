{{ config(
    materialized='table'
)}}
      
     with final as ( 
     with cte as ( 
      select * from {{ ref('SP_Placement') }}
      UNION ALL
      select * from {{ ref('SBV_Placement') }}
  
     ),

     campaign as (
     with cte2 as (
         select distinct CampaignID, Campaign,
         DENSE_RANK() OVER (PARTITION BY CampaignID order by Campaign desc) row_num
         from {{ ref('Advertising_Consolidated') }}
     ) 
     
     select * from cte2 where row_num = 1
     )

     select cte.*, b.Campaign
     from cte left join campaign b
     on cte.CampaignID = b.CampaignID

     union ALL
     select * from {{ ref('SB_Placement') }}

     )
     select *,
     case when placement = 'Top of Search on-Amazon' then Spend
        else 0 end as Top_of_Search_on_Amazon_Spend,
        case when placement = 'Detail Page on-Amazon' then Spend
        else 0 end as Detail_Page_on_Amazon_Spend,
        case when placement = 'Other on-Amazon' then Spend
        else 0 end as Other_on_Amazon_Spend,
        case when placement = 'Other Placements' then Spend
        else 0 end as Other_Placements_Spend,
    case when placement = 'Top of Search on-Amazon' then AdSales
        else 0 end as Top_of_Search_on_Amazon_AdSales,
        case when placement = 'Detail Page on-Amazon' then AdSales
        else 0 end as Detail_Page_on_Amazon_AdSales,
        case when placement = 'Other on-Amazon' then AdSales
        else 0 end as Other_on_Amazon_AdSales,
        case when placement = 'Other Placements' then AdSales
        else 0 end as Other_Placements_AdSales,
     from final