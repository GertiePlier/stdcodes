{{ config(materialized='table')}}
      

     with cte as 
     ( 
      select * from {{ ref('SP_Advertising') }}
      UNION ALL
      select * from {{ ref('SB_Advertising') }}
      UNION ALL
      select * from {{ ref('SD_Advertising') }}
	UNION ALL
      select * from {{ ref('SBV_Advertising') }}
     ),

     product as (
     with cte2 as (
         select distinct Product, ASIN,
         DENSE_RANK() OVER (PARTITION BY ASIN order by Product desc) row_num
         from {{ ref('Sales_With_Returns') }}
     ) select * from cte2 where row_num = 1
     )

    select cte.*, b.Product
    from cte
    left join product b
    on cte.ASIN = b.ASIN
    