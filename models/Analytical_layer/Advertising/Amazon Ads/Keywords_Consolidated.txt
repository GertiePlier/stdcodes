{{ config(
    materialized='table'
)}}
      
      
    --  with cte as ( 
      select * from {{ ref('SP_Keywords') }}
      UNION ALL
      select * from {{ ref('SB_Keywords') }}
      UNION ALL
      select * from {{ ref('SBV_Keywords') }}
    --  )

 