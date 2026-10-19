{{ config(
    materialized='table',
    distributed_by='salesorderid'
) }}

WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY salesorderdetailid
            ORDER BY modifieddate DESC, _load_dttm DESC
        ) AS rn
    FROM {{ source('stage', 'salesorderdetail') }}
)

SELECT
    cte.salesorderid,
    cte.salesorderdetailid,
    cte.orderqty,
    cte.productid,
    cte.unitprice,
    cte.unitpricediscount,
    cte.modifieddate,
    cte._load_dttm,
    cte._batch_id,
    cte._source_system,
    NOW() AS _ods_load_dttm
FROM cte
WHERE rn = 1