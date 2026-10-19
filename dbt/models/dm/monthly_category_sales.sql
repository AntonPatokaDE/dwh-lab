{{ config(
    materialized='table'
) }}

SELECT
    d.year,
    d.month,
    p.productcategoryid,
    p.category_name,
    sum(f.line_amount) as sales_amount,
    sum(f.orderqty) as units_sold,
    count(distinct f.salesorderid) as distinct_orders,
    NOW() AS _dm_load_dttm

FROM {{ ref('fact_sales') }} AS f

INNER JOIN {{ ref('dim_date') }} AS d
    ON f.orderdate::date = d.date
INNER JOIN {{ ref('dim_product') }} AS p
    ON f.productid = p.productid

group by d.month,d.year,p.productcategoryid,p.category_name