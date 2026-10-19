{{ config(
    materialized='table',
    distributed_by='salesorderid'
) }}

SELECT
    h.salesorderid,
    d.salesorderdetailid,
    h.orderdate,
    h.customerid,
    h.territoryid,
    h.onlineorderflag,
    h.status,
    d.productid,
    d.orderqty,
    d.unitprice,
    d.unitpricediscount,
    d.orderqty * d.unitprice * (1 - d.unitpricediscount) AS line_amount,
    NOW() AS _dds_load_dttm

FROM {{ ref('salesorderheader') }} AS h

INNER JOIN {{ ref('salesorderdetail') }} AS d
    ON d.salesorderid = h.salesorderid