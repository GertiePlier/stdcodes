{{ config(
    materialized='table'
)}}

select 
Brand,
Date,
'Promotion' as AmountType,
'Refund' as TransactionType,
OrderID,
SalesChannel as Marketplace,
SKU,
UnitsSold,
'Promotional Discount' as ChargeType,
Currency,
ItemPromotionalDiscount + ShippingPromotionalDiscount as Amount
from {{ ref('Orders_Shopify') }}
where returnedunits is not null