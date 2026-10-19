-- Сверка потерей или дублей суммы продаж

WITH f AS (
    SELECT
        SUM(line_amount) AS f_line_amount
    FROM {{ ref('fact_sales') }}
),

dm AS (
    SELECT
        SUM(sales_amount) AS dm_line_amount
    FROM {{ ref('monthly_category_sales') }}
)

SELECT
    f_line_amount,
    dm_line_amount
FROM f
CROSS JOIN dm
WHERE f_line_amount <> dm_line_amount