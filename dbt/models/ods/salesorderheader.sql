{{ config(
    materialized='table',
    distributed_by='salesorderid'
) }}

WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY salesorderid
            ORDER BY modifieddate DESC, _load_dttm DESC
        ) AS rn
    FROM {{ source('stage', 'salesorderheader') }}
)

SELECT
    cte.salesorderid,
    cte.orderdate,
    cte.status,
    cte.onlineorderflag,
    cte.customerid,
    cte.territoryid,
    cte.subtotal,
    cte.totaldue,
    cte.modifieddate,
    cte._load_dttm,
    cte._batch_id,
    cte._source_system,
    NOW() AS _ods_load_dttm
FROM cte
WHERE rn = 1