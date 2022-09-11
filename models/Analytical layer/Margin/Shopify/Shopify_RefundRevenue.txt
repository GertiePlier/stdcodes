{{ config(
    materialized='table'
)}}

select 
Brand,
Date,
'Revenue' as AmountType,
'Refund' as TransactionType,
OrderID,
SalesChannel as Marketplace,
SKU,
UnitsSold,
'Item Price' as ChargeType,
Currency,
ItemPrice as Amount
from {{ ref('Orders_Shopify') }}
where returnedunits is not null