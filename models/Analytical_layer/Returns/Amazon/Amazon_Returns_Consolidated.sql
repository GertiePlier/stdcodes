{{ config(
    materialized='table'
)}}

      select * from {{ ref('FBA_Returns') }}
      UNION ALL
      select * from {{ ref('FBM_Returns') }}
      
