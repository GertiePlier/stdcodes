{{ config(
    materialized='table'
)}}

select *
from {{ ref('Advertising_Consolidated') }}

union all

select *
from {{ ref('Facebook_Advertising') }}


union all

select *
from {{ ref('Google_Advertising') }}