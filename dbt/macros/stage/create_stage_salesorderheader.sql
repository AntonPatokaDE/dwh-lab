{% macro create_stage_salesorderheader() %}

    {% set sql %}  

        create TABLE if not exists stage.salesorderheader(
            salesorderid integer,
            orderdate timestamp,
            status smallint,
            onlineorderflag boolean,
            customerid integer,
            territoryid integer,
            subtotal numeric(38,18),
            totaldue numeric(38,18),
            modifieddate timestamp,
            _load_dttm timestamp,
            _batch_id varchar(36),
            _source_system varchar(50)
        )
        DISTRIBUTED BY (salesorderid);
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}