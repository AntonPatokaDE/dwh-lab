{{ config(
    materialized='table',
    distributed_by='productid'
) }}

WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY productid
            ORDER BY modifieddate DESC, _load_dttm DESC
        ) AS rn
    FROM {{ source('stage', 'product') }}
)

SELECT
    productid,
    name,
    productnumber,
    color,
    size,
    standardcost,
    listprice,
    productsubcategoryid,
    modifieddate,
    _load_dttm,
    _batch_id,
    _source_system,
    NOW() AS _ods_load_dttm
FROM cte
WHERE rn = 1