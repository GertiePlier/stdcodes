{{ config(
    materialized='table'
)}}


cte1 as (
Select 
Partner as Brand,
Commission_Rate,
Flat_Rate,
Revenue_Max,
Revenue_Min,
 CAST((CASE WHEN Revenue_Cap = '' THEN null ELSE Revenue_Cap END) AS INT) AS Revenue_Cap,
Commission_Type
from Foundation_Layer_Test.Commissions
),

sales as (
select Brand,
Date,
sum(ProductSales) as ProductSales
from {{ ref('Sales_With_Returns') }}
Group by 1,2
),

select a.Brand, a.Date, c.Commission_Rate, Flat_Rate
from sales a join cte1 c on a.Brand = c.Brand















