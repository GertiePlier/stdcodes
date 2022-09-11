{{ config(
    materialized='table'
)}}

select
distinct {{ dbt_utils.surrogate_key('Campaign', 'CampaignId') }} AS CampaignKey,
Campaign,
CampaignId
from {{ ref('Advertising_Consolidation') }}