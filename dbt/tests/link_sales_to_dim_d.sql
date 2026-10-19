SELECT *
FROM {{ ref('fact_sales') }} AS f
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('dim_date') }} AS d
    WHERE f.orderdate::date = d.date
)