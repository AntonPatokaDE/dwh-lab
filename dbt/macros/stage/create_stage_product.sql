{% macro create_stage_product() %}

    {% set sql %}  

        create TABLE if not exists stage.product(
            productid integer,
            name varchar(50),
            productnumber varchar(25),
            color varchar(15),
            size varchar(5),
            standardcost numeric,
            listprice numeric,
            productsubcategoryid integer,
            modifieddate timestamp,
            _load_dttm timestamp,
            _batch_id varchar(36),
            _source_system varchar(50)
        )
        DISTRIBUTED BY (productid);
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}