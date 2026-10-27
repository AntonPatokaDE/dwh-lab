{% macro create_stage_salesorderdetail() %}

    {% set sql %}  

        create TABLE if not exists stage.salesorderdetail(
            salesorderid integer,
            salesorderdetailid integer,
            orderqty smallint,
            productid integer,
            unitprice numeric,
            unitpricediscount numeric,
            modifieddate timestamp,
            _load_dttm timestamp,
            _batch_id varchar(36),
            _source_system varchar(50)
        )
        DISTRIBUTED BY (salesorderid);
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}