with sales as (

    select * from {{ ref('Sales_with_Returns') }}

),

product_data as (
 
    select DISTINCT date,Brand,SalesChannel,
    OrderId,Product
    from sales
)
,

same_order as (
    select
        a.date as date,a.OrderId as OrderId,a.Brand,a.SalesChannel,
        a.Product as ProductA, 
        b.Product as productB,
    from product_data a

    inner join product_data b on a.OrderId = b.OrderId and a.product <> b.product
    and a.date = b.date and a.Brand = b.Brand and a.SalesChannel = b.SalesChannel
)

select 
date,
{{ dbt_utils.surrogate_key('ProductA', 'productB') }} AS sameorderkey,
{{ dbt_utils.surrogate_key('OrderId') }}OrderIdkey,
{{ dbt_utils.surrogate_key('ProductA') }}Productkey,
{{ dbt_utils.surrogate_key('SalesChannel') }} AS MarketplaceKey, 
{{ dbt_utils.surrogate_key('Brand') }} AS BrandKey,
productA,productB
from same_order
