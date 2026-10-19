-- Сверка количества проданных единиц при переходе DDS -> DM

WITH f AS (
    SELECT
        SUM(orderqty) AS f_units_sold
    FROM {{ ref('fact_sales') }}
),

dm AS (
    SELECT
        SUM(units_sold) AS dm_units_sold
    FROM {{ ref('monthly_category_sales') }}
)

SELECT
    f_units_sold,
    dm_units_sold
FROM f
CROSS JOIN dm
WHERE f_units_sold <> dm_units_sold