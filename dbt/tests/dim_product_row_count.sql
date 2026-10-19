WITH rows_ods AS (
    SELECT
        COUNT(*) AS count_rows_ods
    FROM {{ ref('product') }}
),

rows_dds AS (
    SELECT
        COUNT(*) AS count_rows_dds
    FROM {{ ref('dim_product') }}
)

SELECT
    count_rows_ods,
    count_rows_dds
FROM rows_ods
CROSS JOIN rows_dds
WHERE count_rows_ods <> count_rows_dds