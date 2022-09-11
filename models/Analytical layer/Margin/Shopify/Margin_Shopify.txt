{{ config(materialized='table')}}


select * from {{ ref('Shopify_OrderFees') }}
UNION ALL
select * from {{ ref('Shopify_OrderRevenue') }}
UNION ALL
select * from {{ ref('Shopify_OrderTaxes') }}
UNION ALL
select * from {{ ref('Shopify_RefundFees') }}
UNION ALL
select * from {{ ref('Shopify_RefundRevenue') }}
UNION ALL
select * from {{ ref('Shopify_RefundTaxes') }}
UNION ALL
select * from {{ ref('Shopify_Advertising_Spend') }}
UNION ALL
select * from {{ ref('Shopify_RefundPromotions') }}
UNION ALL
select * from {{ ref('Shopify_OrderPromotions') }}
-- UNION ALL
-- select * from {{ ref('COGS') }}
