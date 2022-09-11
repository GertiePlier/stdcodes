{{ config(
    materialized='table'
)}}

with date as (
    select distinct OrderID, Date from {{ ref('Orders_Shopify') }}
),

Fees as (
    select
    Brand,
    OrderID,
    Country,
    Currency,
    sum(Fees) Fees
    from {{ ref('Transactions_Shopify') }}
    group by 1,2,3,4
)


    select
    a.Brand,
    b.Date,
    'Fees' as AmountType,
    'Order' as TransactionType, 
    b.OrderID,
    a.Country as Marketplace,
    'NA' as SKU,
    null as UnitsSold,
    'Shopify Transaction Charges' as ChargeType,
    a.Currency,
    a.Fees
    from Fees a
    left join date b
    on a.OrderID = b.OrderID
