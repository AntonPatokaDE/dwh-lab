WITH rows_ods AS (
    SELECT
        COUNT(*) AS count_rows_ods
    FROM {{ ref('salesorderdetail') }}
),

rows_dds AS (
    SELECT
        COUNT(*) AS count_rows_dds
    FROM {{ ref('fact_sales') }}
)

SELECT
    count_rows_ods,
    count_rows_dds
FROM rows_ods
CROSS JOIN rows_dds
WHERE count_rows_ods <> count_rows_dds