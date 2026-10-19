{{ config(
    materialized='table',
    distributed_by='productcategoryid'
) }}

WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY productcategoryid
            ORDER BY modifieddate DESC, _load_dttm DESC
        ) AS rn
    FROM {{ source('stage', 'productcategory') }}
)

SELECT
    productcategoryid,
    name,
    modifieddate,
    _load_dttm,
    _batch_id,
    _source_system,
    NOW() AS _ods_load_dttm
FROM cte
WHERE rn = 1