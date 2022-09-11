{{ config(
    materialized='table'
)}}

select 
Brand,
Date,
'Taxes' as AmountType,
'Refund' as TransactionType,
OrderID,
SalesChannel as Marketplace,
SKU,
UnitsSold,
'Taxes' as ChargeType,
Currency,
ItemTax + ShippingTax + GiftwrapTax as Amount
from {{ ref('Orders_Shopify') }}
where returnedunits is not null