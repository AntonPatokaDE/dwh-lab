SELECT *
FROM {{ ref('fact_sales') }} AS f
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('dim_product') }} AS p
    WHERE f.productid = p.productid
)