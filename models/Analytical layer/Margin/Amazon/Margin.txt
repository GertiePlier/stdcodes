{{ config(materialized='table')}}


select * from {{ ref('ListFinancialEvents_OrderFees') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_OrderRevenue') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_OrderTaxes') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_RefundFees') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_RefundRevenue') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_RefundTaxes') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_AdvertisingSpend') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_RefundPromotions') }}
UNION ALL
select * from {{ ref('ListFinancialEvents_OrderPromotions') }}
-- UNION ALL
-- select * from {{ ref('COGS') }}
