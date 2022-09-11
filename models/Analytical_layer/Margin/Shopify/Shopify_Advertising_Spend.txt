{{ config(
    materialized='table'
)}}

select 
Brand,
Date,
'Marketing' as AmountType,
AdType as TransactionType,
'NA' as OrderID,
Country as Marketplace,
SKU,
UnitsSold,
'Advertising Spend' as ChargeType,
Currency,
Spend as Amount
from {{ ref('Facebook_Advertising') }}

union all

select 
Brand,
Date,
'Marketing' as AmountType,
AdType as TransactionType,
'NA' as OrderID,
Country as Marketplace,
SKU,
UnitsSold,
'Advertising Spend' as ChargeType,
Currency,
Spend as Amount
from {{ ref('Google_Advertising') }}