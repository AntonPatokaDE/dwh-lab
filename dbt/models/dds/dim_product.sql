{{ config(
    materialized='table',
    distributed_by='productid'
) }}

SELECT
    p.productid,
    p.name,
    p.productnumber,
    p.color,
    p.size,
    p.standardcost,
    p.listprice,

    psc.productsubcategoryid,
    psc.name AS subcategory_name,

    pc.productcategoryid,
    pc.name AS category_name,
    NOW() AS _dds_load_dttm

FROM {{ ref('product') }} AS p

LEFT JOIN {{ ref('productsubcategory') }} AS psc
    ON p.productsubcategoryid = psc.productsubcategoryid
LEFT JOIN {{ref('productcategory')}} as pc
    ON psc.productcategoryid = pc.productcategoryid