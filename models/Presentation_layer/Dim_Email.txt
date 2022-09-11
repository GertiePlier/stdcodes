select 
distinct {{ dbt_utils.surrogate_key('BuyerEmail') }} AS BuyerEmailKey,
BuyerEmail
from {{ ref('Shipments') }}