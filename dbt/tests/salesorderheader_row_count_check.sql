WITH qunic_orders_stage AS (
    SELECT
        COUNT(DISTINCT salesorderid) AS count_qunic_id
    FROM {{ source('stage', 'salesorderheader') }}
),

rows_ods AS (
    SELECT
        COUNT(*) AS count_rows_ods
    FROM {{ ref('salesorderheader') }}
)

SELECT
    count_qunic_id,
    count_rows_ods
FROM qunic_orders_stage
CROSS JOIN rows_ods
WHERE count_qunic_id <> count_rows_ods